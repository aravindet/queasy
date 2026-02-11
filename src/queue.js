import { DEFAULT_RETRY_OPTIONS, DEQUEUE_INTERVAL, FAILJOB_RETRY_OPTIONS } from './constants.js';
import { generateId } from './utils.js';

// Import types:
/** @typedef {import('redis').RedisClientType} RedisClient */
/** @typedef {import('./types').DequeueOptions} DequeueOptions */
/** @typedef {import('./types').ListenOptions} ListenOptions */
/** @typedef {import('./types').JobOptions} JobOptions */
/** @typedef {import('./types').Job} Job */
/** @typedef {import('./types').DoneMessage} DoneMessage */
/** @typedef {import('./client').Client} Client */
/** @typedef {import('./pool').Pool} Pool */

/** @typedef {Required<Partial<Pick<Queue, keyof Queue>>>} ProcessingQueue */

/**
 * Queue instance for managing a named job queue
 */
export class Queue {
    /**
     * @param {string} key - Queue key
     * @param {Client} client - Redis client wrapper
     * @param {Pool | undefined} pool - Worker pool
     */
    constructor(key, client, pool) {
        this.key = key;
        this.client = client;
        this.pool = pool;

        /** @type {NodeJS.Timeout | undefined} */
        this.dequeueInterval = undefined;

        /** @type {Required<DequeueOptions> | undefined} */
        this.dequeueOptions = undefined;

        /** @type {string | undefined} */
        this.handlerPath = undefined;

        /** @type {string | undefined} */
        this.failKey = undefined;
    }

    /**
     * Attach handlers to process jobs from this queue
     * @param {string} handlerPath - Path to handler module
     * @param {ListenOptions} [options] - Retry strategy options and failure handler
     * @returns {Promise<void>}
     */
    async listen(handlerPath, { failHandler, failRetryOptions, ...retryOptions } = {}) {
        if (!this.pool) throw new Error('Can’t listen on a client without workers');

        this.handlerPath = handlerPath;
        this.dequeueOptions = { ...DEFAULT_RETRY_OPTIONS, ...retryOptions };

        // Initialize failure handler on all workers if provided
        if (failHandler) {
            this.failKey = `${this.key}-fail`;
            const failQueue = this.client.queue(this.failKey, true);
            failQueue.listen(failHandler, { ...FAILJOB_RETRY_OPTIONS, ...failRetryOptions });
        }

        if (!this.dequeueInterval) {
            this.dequeueInterval = setInterval(() => this.dequeue(), DEQUEUE_INTERVAL);
        }
    }

    /**
     * Add a job to the queue
     * @param {any} data - Job data (any JSON-serializable value)
     * @param {JobOptions} [options] - Job options
     * @returns {Promise<string>} Job ID
     */
    async dispatch(data, options = {}) {
        const {
            id = generateId(),
            runAt = 0,
            updateData = false,
            updateRunAt = false,
            resetCounts = false,
        } = options;

        await this.client.dispatch(this.key, id, runAt, data, updateData, updateRunAt, resetCounts);
        return id;
    }

    /**
     * Cancel a waiting job
     * @param {string} id - Job ID
     * @returns {Promise<boolean>} True if job was cancelled
     */
    async cancel(id) {
        return await this.client.cancel(this.key, id);
    }

    /**
     * Picks jobs from queues based on available capacity and executes them.
     * @returns {Promise<unknown>}
     */

    async dequeue() {
        const { pool, handlerPath } = /** @type {{pool: Pool, handlerPath: string}} */ (this);
        const { maxRetries, maxStalls, maxBackoff, minBackoff, size } =
            /** @type {Required<DequeueOptions>} */ (this.dequeueOptions);

        const capacity = pool.getCapacity(size);
        if (capacity <= 0) return;

        const jobs = await this.client.dequeue(this.key, capacity);

        // We use forEach to process jobs in parallel without keeping the
        // dequeue function waiting for the overall results
        jobs.forEach(async (job) => {
            // Check if job has exceeded stall limit
            if (job.stallCount >= maxStalls) {
                // Job has stalled too many times - fail it permanently
                if (!this.failKey) return this.client.finish(this.key, job.id);

                const failJobData = [job.id, job.data, { message: 'Max stalls exceeded' }];
                return this.client.fail(this.key, this.failKey, job.id, failJobData);
            }

            try {
                await pool.process(handlerPath, job, size);
            } catch (error) {
                const { retryAt = 0, isPermanent } = /** @type {DoneMessage} */ (error);

                if (isPermanent || job.retryCount >= maxRetries) {
                    if (!this.failKey) return this.client.finish(this.key, job.id);

                    const failJobData = [job.id, job.data, error];
                    await this.client.fail(this.key, this.failKey, job.id, failJobData);
                }

                const backoffUntil =
                    Date.now() + Math.min(maxBackoff, minBackoff * 2 ** job.retryCount);

                // Retriable error: call retry
                await this.client.retry(this.key, job.id, Math.max(retryAt, backoffUntil));
            }
        });
    }

    /**
     * Stop the dequeue interval and bump timer for this queue
     */
    close() {
        if (this.dequeueInterval) {
            clearInterval(this.dequeueInterval);
            this.dequeueInterval = undefined;
        }
    }
}
