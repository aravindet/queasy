import EventEmitter from 'node:events';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { getEnvironmentData } from 'node:worker_threads';
import { HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, LUA_FUNCTIONS_VERSION } from './constants.js';
import { Manager } from './manager.js';
import { Pool } from './pool.js';
import { Queue } from './queue.js';
import { compareSemver, generateId, parseVersion } from './utils.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Check the installed version and load our Lua functions if needed.
 * Returns true if this client should be disconnected (newer major on server).
 * @param {RedisClient} redis
 * @returns {Promise<boolean>} Whether to disconnect.
 */
async function installLuaFunctions(redis) {
    const installedVersionString = /** @type {string?} */ (
        await redis.fCall('queasy_version', { keys: [], arguments: [] }).catch(() => null)
    );
    const installedVersion = parseVersion(installedVersionString);
    const availableVersion = parseVersion(LUA_FUNCTIONS_VERSION);

    // No script installed or our version is later
    if (compareSemver(availableVersion, installedVersion) > 0) {
        // Load Lua script and stamp version
        const luaScriptTemplate = await readFile(join(__dirname, 'queasy.lua'), 'utf8');
        const luaScript = luaScriptTemplate.replace('__QUEASY_VERSION__', LUA_FUNCTIONS_VERSION);
        await redis.sendCommand(['FUNCTION', 'LOAD', 'REPLACE', luaScript]);
        return false;
    }

    // Keep the installed (newer) version. Return disconnect=true if the major versions disagree
    return installedVersion[0] > availableVersion[0];
}

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

export class Client extends EventEmitter {
    /**
     * @param {RedisClient} redis - Redis client
     * @param {number?} workerCount - Allow this client to dequeue jobs.
     */
    constructor(redis, workerCount) {
        super();
        this.redis = redis;
        this.clientId = generateId();

        /** @type {Record<string, QueueEntry>} */
        this.queues = {};
        this.disconnected = false;

        const inWorker = getEnvironmentData('queasy_worker_context');
        this.pool = !inWorker && workerCount !== 0 ? new Pool(workerCount) : undefined;
        if (this.pool) this.manager = new Manager(this.pool);

        // Not awaited — Redis' single-threaded ordering ensures this completes
        // before any subsequent fCalls from user code.
        installLuaFunctions(this.redis).then((disconnect) => {
            this.disconnected = disconnect;
            if (disconnect) this.emit('disconnected', 'Redis has incompatible queasy version.');
        });
    }

    /**
     * Create a queue object for interacting with a named queue
     * @param {string} name - Queue name (without braces - they will be added automatically)
     * @returns {Queue} Queue object with dispatch, cancel, and listen methods
     */
    queue(name, isKey = false) {
        if (this.disconnected) throw new Error('Can’t add queue: client disconnected');

        const key = isKey ? name : `{${name}}`;
        if (!this.queues[key]) {
            this.queues[key] = /** @type {QueueEntry} */ ({
                queue: new Queue(key, this, this.pool, this.manager),
            });
        }
        return this.queues[key].queue;
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
        if (this.disconnected) return;
        // Set up the next bump first, in case this
        this.scheduleBump(key);
        const now = Date.now();
        const expiry = now + HEARTBEAT_TIMEOUT;
        const bumped = await this.redis.fCall('queasy_bump', {
            keys: [key],
            arguments: [this.clientId, String(now), String(expiry)],
        });

        if (!bumped) {
            // This client’s lock was lost and its jobs retried.
            // We must stop processing jobs here to avoid duplication.
            await this.close();
            this.emit('disconnected', 'Lost locks, possible main thread freeze');
        }
    }

    /**
     * This marks this as disconnected.
     */
    async close() {
        if (this.pool) await this.pool.close();
        if (this.manager) await this.manager.close();
        this.queues = {};
        this.pool = undefined;
        this.disconnected = true;
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
