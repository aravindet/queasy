import type { Client } from './client.js';
import { DEFAULT_RETRY_OPTIONS, FAILJOB_RETRY_OPTIONS } from './constants.js';
import type { Manager } from './manager.js';
import type { Pool } from './pool.js';
import type { DoneMessage, HandlerOptions, Job, JobOptions, ListenOptions } from './types.js';
import { generateId } from './utils.js';

export class Queue {
    key: string;
    client: Client;
    pool: Pool | undefined;
    manager: Manager | undefined;
    handlerOptions: Required<HandlerOptions> | undefined;
    handlerPath: string | undefined;
    failKey: string | undefined;

    constructor(key: string, client: Client, pool: Pool | undefined, manager: Manager | undefined) {
        this.key = key;
        this.client = client;
        this.pool = pool;
        this.manager = manager;
        this.handlerOptions = undefined;
        this.handlerPath = undefined;
        this.failKey = undefined;
    }

    async listen(handlerPath: string, options: ListenOptions = {}): Promise<void> {
        const { failHandler, failRetryOptions, ...retryOptions } = options;
        if (this.client.disconnected) throw new Error("Can't listen: client disconnected");
        if (!this.pool || !this.manager) throw new Error("Can't listen: non-processing client");

        this.handlerPath = handlerPath;
        this.handlerOptions = { ...DEFAULT_RETRY_OPTIONS, ...retryOptions };

        if (failHandler) {
            this.failKey = `${this.key}-fail`;
            const failQueue = this.client.queue(this.failKey, true);
            failQueue.listen(failHandler, { ...FAILJOB_RETRY_OPTIONS, ...failRetryOptions });
        }

        this.manager.addQueue(this);
    }

    async dispatch(
        // biome-ignore lint/suspicious/noExplicitAny: Data is any serializable value
        data: any,
        options: JobOptions = {}
    ): Promise<string> {
        if (this.client.disconnected) throw new Error("Can't dispatch: client disconnected");
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

    async cancel(id: string): Promise<boolean> {
        if (this.client.disconnected) throw new Error("Can't cancel: client disconnected");
        return await this.client.cancel(this.key, id);
    }

    async dequeue(count: number): Promise<{ count: number; promise: Promise<unknown[]> }> {
        const pool = this.pool!;
        const handlerPath = this.handlerPath!;
        const { maxRetries, maxStalls, maxBackoff, minBackoff, size, timeout } =
            this.handlerOptions!;

        const jobs = await this.client.dequeue(this.key, count);

        const promise = Promise.all(
            jobs.map(async (job: Job) => {
                if (job.stallCount >= maxStalls) {
                    if (!this.failKey) return this.client.finish(this.key, job.id);

                    const failJobData = [job.id, job.data, { message: 'Max stalls exceeded' }];
                    return this.client.fail(this.key, this.failKey, job.id, failJobData);
                }

                try {
                    await pool.process(handlerPath, job, size, timeout);
                    await this.client.finish(this.key, job.id);
                } catch (message) {
                    const { error } = message as Required<DoneMessage>;
                    const { retryAt = 0, kind } = error;

                    if (kind === 'permanent' || job.retryCount >= maxRetries) {
                        if (!this.failKey) return this.client.finish(this.key, job.id);

                        const failJobData = [job.id, job.data, error];
                        return this.client.fail(this.key, this.failKey, job.id, failJobData);
                    }

                    const backoffUntil =
                        Date.now() + Math.min(maxBackoff, minBackoff * 2 ** job.retryCount);

                    await this.client.retry(this.key, job.id, Math.max(retryAt, backoffUntil));
                }
            })
        );

        return { count: jobs.length, promise };
    }
}
