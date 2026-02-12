import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getEnvironmentData } from 'node:worker_threads';
import { HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT } from './constants.js';
import { Pool } from './pool.js';
import { Queue } from './queue.js';
import { generateId } from './utils.js';

// Load Lua script
const __dirname = dirname(fileURLToPath(import.meta.url));
const luaScript = readFileSync(join(__dirname, 'queasy.lua'), 'utf8');

/** @typedef {import('redis').RedisClientType} RedisClient */
/** @typedef {import('./types').Job} Job */

/** @typedef {{ queue: Queue, bumpTimer?: NodeJS.Timeout }} QueueEntry */

/**
 * Parse job data from Redis response
 * @param {string[]} jobArray - Flat array from HGETALL
 * @returns {Job | null}
 */
export function parseJob(jobArray) {
    if (!jobArray || jobArray.length === 0) return null;

    /** @type {Record<string, string>} */
    const job = {};
    for (let i = 0; i < jobArray.length; i += 2) {
        const key = jobArray[i];
        const value = jobArray[i + 1];
        job[key] = value;
    }

    return {
        id: job.id,
        data: job.data ? JSON.parse(job.data) : undefined,
        runAt: job.run_at ? Number(job.run_at) : 0,
        retryCount: Number(job.retry_count || 0),
        stallCount: Number(job.stall_count || 0),
    };
}

export class Client {
    /**
     * @param {RedisClient} redis - Redis client
     * @param {number} workerCount - Allow this client to dequeue jobs.
     */
    constructor(redis, workerCount) {
        this.redis = redis;
        this.clientId = generateId();

        /** @type {Record<string, QueueEntry>} */
        this.queues = {};

        const inWorker = getEnvironmentData('queasy_worker_context');
        this.pool = !inWorker && workerCount !== 0 ? new Pool(workerCount) : undefined;

        // We are not awaiting this; we rely on Redis’ single-threaded blocking
        // nature to ensure that this load completes before other Redis commands
        // are processed.
        this.redis.sendCommand(['FUNCTION', 'LOAD', 'REPLACE', luaScript]);
    }

    /**
     * Create a queue object for interacting with a named queue
     * @param {string} name - Queue name (without braces - they will be added automatically)
     * @returns {Queue} Queue object with dispatch, cancel, and listen methods
     */
    queue(name, isKey = false) {
        const key = isKey ? name : `{${name}}`;
        if (!this.queues[key]) {
            this.queues[key] = /** @type {QueueEntry} */ ({
                queue: new Queue(key, this, this.pool),
            });
        }
        return this.queues[key].queue;
    }

    /**
     * This helps tests exit cleanly.
     */
    close() {
        for (const name in this.queues) {
            this.queues[name].queue.close();
            clearTimeout(this.queues[name].bumpTimer);
        }
        if (this.pool) this.pool.close();
        this.queues = {};
        this.pool = undefined;
    }

    /**
     * Schedule the next bump timer
     * @param {string} key
     */
    scheduleBump(key) {
        const queueEntry = this.queues[key];
        if (queueEntry.bumpTimer) clearTimeout(queueEntry.bumpTimer);
        queueEntry.bumpTimer = setTimeout(() => this.bump(key), HEARTBEAT_INTERVAL);
    }

    /**
     * @param {string} key
     */
    async bump(key) {
        // Set up the next bump first, in case this
        this.scheduleBump(key);
        const now = Date.now();
        const expiry = now + HEARTBEAT_TIMEOUT;
        await this.redis.fCall('queasy_bump', {
            keys: [key],
            arguments: [this.clientId, String(now), String(expiry)],
        });
    }

    /**
     * @param {string} key
     * @param {string} id
     * @param {number} runAt
     * @param {any} data
     * @param {boolean} updateData
     * @param {boolean | string} updateRunAt
     * @param {boolean} resetCounts
     */
    async dispatch(key, id, runAt, data, updateData, updateRunAt, resetCounts) {
        await this.redis.fCall('queasy_dispatch', {
            keys: [key],
            arguments: [
                id,
                String(runAt),
                JSON.stringify(data),
                String(updateData),
                String(updateRunAt),
                String(resetCounts),
            ],
        });
    }

    /**
     * @param {string} key
     * @param {string} id
     * @returns {Promise<boolean>}
     */
    async cancel(key, id) {
        const result = await this.redis.fCall('queasy_cancel', {
            keys: [key],
            arguments: [id],
        });
        return result === 1;
    }

    /**
     * @param {string} key
     * @param {number} count
     * @returns {Promise<Job[]>}
     */
    async dequeue(key, count) {
        const now = Date.now();
        const expiry = now + HEARTBEAT_TIMEOUT;
        const result = /** @type {string[][]} */ (
            await this.redis.fCall('queasy_dequeue', {
                keys: [key],
                arguments: [this.clientId, String(now), String(expiry), String(count)],
            })
        );

        // Heartbeats should start with the first dequeue.
        this.scheduleBump(key);

        return /** @type Job[] */ (result.map((jobArray) => parseJob(jobArray)).filter(Boolean));
    }

    /**
     * @param {string} key
     * @param {string} jobId
     */
    async finish(key, jobId) {
        await this.redis.fCall('queasy_finish', {
            keys: [key],
            arguments: [jobId, this.clientId, String(Date.now())],
        });
    }

    /**
     * @param {string} key
     * @param {string} failkey
     * @param {string} jobId
     * @param {any} failJobData
     */
    async fail(key, failkey, jobId, failJobData) {
        await this.redis.fCall('queasy_fail', {
            keys: [key, failkey],
            arguments: [
                jobId,
                this.clientId,
                generateId(),
                JSON.stringify(failJobData),
                String(Date.now()),
            ],
        });
    }

    /**
     * @param {string} key
     * @param {string} jobId
     * @param {number} retryAt
     */
    async retry(key, jobId, retryAt) {
        await this.redis.fCall('queasy_retry', {
            keys: [key],
            arguments: [jobId, this.clientId, String(retryAt), String(Date.now())],
        });
    }
}
