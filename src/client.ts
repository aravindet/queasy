import EventEmitter from 'node:events';
import { readFileSync } from 'node:fs';
import os from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getEnvironmentData } from 'node:worker_threads';
import { createClient, createCluster } from 'redis';
import { HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, LUA_FUNCTIONS_VERSION } from './constants.js';
import { Manager } from './manager.js';
import { Pool } from './pool.js';
import { Queue } from './queue.js';
import type { Job, RedisOptions } from './types.js';
import { compareSemver, generateId, parseVersion } from './utils.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const luaScript = readFileSync(join(__dirname, 'queasy.lua'), 'utf8').replace(
    '__QUEASY_VERSION__',
    LUA_FUNCTIONS_VERSION
);

type RedisConnection = ReturnType<typeof createClient> | ReturnType<typeof createCluster>;

async function installLuaFunctions(redis: RedisConnection): Promise<boolean> {
    const installedVersionString = (await (redis as ReturnType<typeof createClient>)
        .fCall('queasy_version', { keys: [], arguments: [] })
        .catch(() => null)) as string | null;
    const installedVersion = parseVersion(installedVersionString);
    const availableVersion = parseVersion(LUA_FUNCTIONS_VERSION);

    if (compareSemver(availableVersion, installedVersion) > 0) {
        await (redis as ReturnType<typeof createClient>).sendCommand([
            'FUNCTION',
            'LOAD',
            'REPLACE',
            luaScript,
        ]);
        return false;
    }

    return installedVersion[0] > availableVersion[0];
}

function buildRedisConnection(options: RedisOptions): RedisConnection {
    if ('rootNodes' in options) {
        return createCluster(options);
    }
    return createClient(options);
}

interface QueueEntry {
    queue: Queue;
    bumpTimer?: NodeJS.Timeout;
}

export function parseJob(jobArray: string[]): Job | null {
    if (!jobArray || jobArray.length === 0) return null;

    const job: Record<string, string> = {};
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

export class Client extends EventEmitter {
    redis: RedisConnection;
    clientId: string;
    queues: Record<string, QueueEntry>;
    disconnected: boolean;
    pool: Pool | undefined;
    manager: Manager | undefined;

    constructor(
        options: RedisOptions = {},
        workerCount: number = os.cpus().length,
        callback?: (client: Client) => unknown
    ) {
        super();
        this.redis = buildRedisConnection(options);
        this.clientId = generateId();
        this.queues = {};
        this.disconnected = false;

        const inWorker = getEnvironmentData('queasy_worker_context');
        this.pool = !inWorker && workerCount !== 0 ? new Pool(workerCount) : undefined;
        if (this.pool) this.manager = new Manager(this.pool);

        this.redis
            .connect()
            .then(() => installLuaFunctions(this.redis))
            .then((disconnect) => {
                this.disconnected = disconnect;
                if (disconnect) this.emit('disconnected', 'Redis has incompatible queasy version.');
                else {
                    this.emit('connected');
                    if (callback) callback(this);
                }
            })
            .catch((err: Error) => {
                this.disconnected = true;
                this.emit('disconnected', err.message);
            });
    }

    queue(name: string, isKey = false): Queue {
        if (this.disconnected) throw new Error("Can't add queue: client disconnected");

        const key = isKey ? name : `{${name}}`;
        if (!this.queues[key]) {
            this.queues[key] = {
                queue: new Queue(key, this, this.pool, this.manager),
            };
        }
        return this.queues[key].queue;
    }

    scheduleBump(key: string): void {
        const queueEntry = this.queues[key];
        if (queueEntry.bumpTimer) clearTimeout(queueEntry.bumpTimer);
        queueEntry.bumpTimer = setTimeout(() => this.bump(key), HEARTBEAT_INTERVAL);
    }

    async bump(key: string): Promise<void> {
        if (this.disconnected) return;
        this.scheduleBump(key);
        const now = Date.now();
        const expiry = now + HEARTBEAT_TIMEOUT;
        const bumped = await (this.redis as ReturnType<typeof createClient>).fCall('queasy_bump', {
            keys: [key],
            arguments: [this.clientId, String(now), String(expiry)],
        });

        if (!bumped) {
            await this.close();
            this.emit('disconnected', 'Lost locks, possible main thread freeze');
        }
    }

    async close(): Promise<void> {
        if (this.pool) await this.pool.close();
        if (this.manager) await this.manager.close();
        this.queues = {};
        this.pool = undefined;
        this.disconnected = true;
        await this.redis.quit().catch(() => {});
    }

    async dispatch(
        key: string,
        id: string,
        runAt: number,
        // biome-ignore lint/suspicious/noExplicitAny: Data is any serializable value
        data: any,
        updateData: boolean,
        updateRunAt: boolean | string,
        resetCounts: boolean
    ): Promise<void> {
        await (this.redis as ReturnType<typeof createClient>).fCall('queasy_dispatch', {
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

    async cancel(key: string, id: string): Promise<boolean> {
        const result = await (this.redis as ReturnType<typeof createClient>).fCall(
            'queasy_cancel',
            {
                keys: [key],
                arguments: [id],
            }
        );
        return result === 1;
    }

    async dequeue(key: string, count: number): Promise<Job[]> {
        const now = Date.now();
        const expiry = now + HEARTBEAT_TIMEOUT;
        const result = (await (this.redis as ReturnType<typeof createClient>).fCall(
            'queasy_dequeue',
            {
                keys: [key],
                arguments: [this.clientId, String(now), String(expiry), String(count)],
            }
        )) as string[][];

        this.scheduleBump(key);

        const jobs = result.map((jobArray) => parseJob(jobArray)).filter(Boolean) as Job[];
        for (const job of jobs) this.emit('dequeue', key, job);
        return jobs;
    }

    async finish(key: string, jobId: string): Promise<void> {
        await (this.redis as ReturnType<typeof createClient>).fCall('queasy_finish', {
            keys: [key],
            arguments: [jobId, this.clientId, String(Date.now())],
        });
        this.emit('finish', key, jobId);
    }

    async fail(
        key: string,
        failkey: string,
        jobId: string,
        // biome-ignore lint/suspicious/noExplicitAny: Fail job data is any serializable value
        failJobData: any
    ): Promise<void> {
        await (this.redis as ReturnType<typeof createClient>).fCall('queasy_fail', {
            keys: [key, failkey],
            arguments: [
                jobId,
                this.clientId,
                generateId(),
                JSON.stringify(failJobData),
                String(Date.now()),
            ],
        });
        this.emit('fail', key, jobId);
    }

    async retry(key: string, jobId: string, retryAt: number): Promise<void> {
        await (this.redis as ReturnType<typeof createClient>).fCall('queasy_retry', {
            keys: [key],
            arguments: [jobId, this.clientId, String(retryAt), String(Date.now())],
        });
        this.emit('retry', key, jobId);
    }
}
