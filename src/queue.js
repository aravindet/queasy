import { readFileSync } from 'node:fs';
import { cpus } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';

// Import types:
/** @typedef {import('redis').RedisClientType} RedisClient */
/** @typedef {import('./types.js').DefaultJobOptions} DefaultJobOptions */
/** @typedef {import('./types.js').JobOptions} JobOptions */
/** @typedef {import('./types.js').Job} Job */
/** @typedef {import('./types.js').ParentToWorkerMessage} ParentToWorkerMessage */
/** @typedef {import('./types.js').WorkerToParentMessage} WorkerToParentMessage */

// Types used only in this file
/** @typedef {{worker: Worker, jobIds: Set<string>, id: string}} WorkerEntry */

/** @type {Required<DefaultJobOptions>} */
const DEFAULT_JOB_OPTIONS = {
	maxRetries: 10,
	maxStalls: 3,
	minBackoff: 2_000,
	maxBackoff: 300_000, // 5 minutes
	updateData: true,
	updateRunAt: true,
	updateRetryStrategy: false,
	resetCounts: false,
};

/** @type {Required<DefaultJobOptions>} */
const DEFAULT_FAILJOB_OPTIONS = {
	maxRetries: 100,
	maxStalls: 3,
	minBackoff: 10_000,
	maxBackoff: 900_000, // 15 minutes
	updateData: true,
	updateRunAt: true,
	updateRetryStrategy: false,
	resetCounts: false,
};

const HEARTBEAT_INTERVAL = 5000; // 5 seconds
const WORKER_CAPACITY = 10;
const DEQUEUE_INTERVAL = 100; // ms

// Load Lua script
const __dirname = dirname(fileURLToPath(import.meta.url));
const luaScript = readFileSync(join(__dirname, 'queasy.lua'), 'utf8');

/** @type {WeakSet<RedisClient>} */
const initializedClients = new WeakSet();

/** @type {Array<WorkerEntry>} */
const workers = [];

/** @type {Map<string, Queue>} */
const jobMap = new Map();

function ensureWorkerPool() {
	if (workers.length > 0) return;
	const count = cpus().length;
	for (let i = 0; i < count; i++) {
		const worker = new Worker(new URL('./worker.js', import.meta.url));
		const entry = { worker, jobIds: new Set(), id: generateId() };
		worker.on('message', (msg) => handleWorkerMessage(entry, msg));
		workers.push(entry);
	}
}

/**
 * @param {WorkerEntry} workerEntry
 * @param {WorkerToParentMessage} msg
 */
async function handleWorkerMessage(workerEntry, msg) {
	switch (msg.op) {
		case 'bump':
			await bump(workerEntry);
			break;
		case 'done':
			await done(workerEntry, msg);
			break;
	}
}

/**
 * Handle job completion (success, retry, or permanent failure)
 * @param {WorkerEntry} workerEntry
 * @param {WorkerToParentMessage} msg
 */
async function done(workerEntry, msg) {
	const queue = jobMap.get(msg.jobId);
	if (!queue) return;

	workerEntry.jobIds.delete(msg.jobId);
	jobMap.delete(msg.jobId);

	await queue.done(msg.jobId, workerEntry.id, msg.error, msg.retryAt);
}

/**
 * Send heartbeat bump to Redis for every active job on this worker
 * @param {WorkerEntry} workerEntry
 */
async function bump(workerEntry) {
	const now = String(Date.now());
	for (const jobId of workerEntry.jobIds) {
		const queue = jobMap.get(jobId);
		if (!queue) continue;
		try {
			await queue.redis.fCall('queasy_bump', {
				keys: [queue.activeKey],
				arguments: [jobId, workerEntry.id, now],
			});
		} catch (err) {
			// Redis client may be closed during tests or shutdown - ignore these errors
			if (err.message?.includes('closed')) {
				continue;
			}
			throw err;
		}
	}
}

/**
 * Generate a random alphanumeric ID
 * @param {number} length - Length of the ID
 * @returns {string}
 */
function generateId(length = 20) {
	const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
	let id = '';
	for (let i = 0; i < length; i++) {
		id += chars.charAt(Math.floor(Math.random() * chars.length));
	}
	return id;
}

/**
 * Parse job data from Redis response
 * @param {string[]} jobArray - Flat array from HGETALL
 * @returns {Job | null}
 */
function parseJob(jobArray) {
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
		maxRetries: Number(job.max_retries),
		maxStalls: Number(job.max_stalls),
		minBackoff: Number(job.min_backoff),
		maxBackoff: Number(job.max_backoff),
		retryCount: Number(job.retry_count),
		stallCount: Number(job.stall_count),
	};
}

/**
 * Queue instance for managing a named job queue
 */
class Queue {
	/**
	 * @param {string} name - Queue name
	 * @param {RedisClient} redis - Redis client
	 * @param {Partial<DefaultJobOptions>} [defaultJobOptions] - Default options for jobs
	 * @param {Partial<DefaultJobOptions>} [failureJobOptions] - Default options for failure handler jobs
	 */
	constructor(name, redis, defaultJobOptions = {}, failureJobOptions = {}) {
		this.name = name;
		this.redis = redis;
		this.baseOpts = { ...DEFAULT_JOB_OPTIONS, ...defaultJobOptions };
		this.failOpts = { ...DEFAULT_FAILJOB_OPTIONS, ...failureJobOptions };
		this.waitingKey = `${name}:waiting`;
		this.activeKey = `${name}:active`;
		this.dequeueStarted = false;
	}

	async ensureRedisInitialized() {
		if (!initializedClients.has(this.redis)) {
			await this.redis.sendCommand(['FUNCTION', 'LOAD', 'REPLACE', luaScript]);
			initializedClients.add(this.redis);
		}
	}

	/**
	 * Add a job to the queue
	 * @param {any} data - Job data (any JSON-serializable value)
	 * @param {Partial<JobOptions>} [options] - Job options
	 * @returns {Promise<string>} Job ID
	 */
	async dispatch(data, options = {}) {
		await this.ensureRedisInitialized();

		const opts = { ...this.baseOpts, ...options };
		const id = opts.id || generateId();
		const runAt = opts.runAt ?? 0;
		const resetCounts = options.updateData ? opts.updateData : opts.resetCounts;

		await this.redis.fCall('queasy_dispatch', {
			keys: [this.waitingKey, this.activeKey],
			arguments: [
				id,
				String(runAt),
				JSON.stringify(data),
				String(opts.maxRetries),
				String(opts.maxStalls),
				String(opts.minBackoff),
				String(opts.maxBackoff),
				String(opts.updateData),
				String(opts.updateRunAt),
				String(opts.updateRetryStrategy),
				String(resetCounts),
			],
		});

		return id;
	}

	/**
	 * Cancel a waiting job
	 * @param {string} id - Job ID
	 * @returns {Promise<boolean>} True if job was cancelled
	 */
	async cancel(id) {
		await this.ensureRedisInitialized();

		const result = await this.redis.fCall('queasy_cancel', {
			keys: [this.waitingKey],
			arguments: [id],
		});

		return result === 1;
	}

	startDequeue() {
		if (this.dequeueStarted) return;
		this.dequeueStarted = true;
		setInterval(() => this.dequeue(), DEQUEUE_INTERVAL);
	}

	async dequeue() {
		for (const workerEntry of workers) {
			const available = WORKER_CAPACITY - workerEntry.jobIds.size;
			if (available <= 0) continue;

			try {
				/** @type {Job} */
				const jobs = await this.redis.fCall('queasy_dequeue', {
					keys: [this.waitingKey, this.activeKey],
					arguments: [workerEntry.id, String(Date.now()), String(available)],
				});

				for (const rawJob of jobs) {
					const job = parseJob(rawJob);
					if (!job) continue;
					workerEntry.jobIds.add(job.id);
					jobMap.set(job.id, this);
					workerEntry.worker.postMessage({ op: 'exec', queue: this.name, job });
				}
			} catch (err) {
				// Redis client may be closed during tests or shutdown - ignore these errors
				if (err.message?.includes('closed')) {
					return;
				}
				throw err;
			}
		}
	}

	/**
	 * Attach handlers to process jobs from this queue
	 * @param {string} handlerPath - Path to handler module
	 * @returns {Promise<void>}
	 */
	async listen(handlerPath) {
		await this.ensureRedisInitialized();
		ensureWorkerPool();
		for (const { worker } of workers) {
			worker.postMessage({ op: 'init', queue: this.name, handler: handlerPath });
		}
		this.startDequeue();
	}

	/**
	 * Handle job completion - calls finish, retry, or fail based on outcome
	 * @param {string} jobId - Job ID
	 * @param {string} workerId - Worker ID
	 * @param {string} [error] - Error message (JSON-serialized)
	 * @param {number} [retryAt] - Retry timestamp
	 */
	async done(jobId, workerId, error, retryAt) {
		if (!error) {
			// Success: call finish
			await this.redis.fCall('queasy_finish', {
				keys: [this.waitingKey, this.activeKey],
				arguments: [jobId, workerId],
			});
		} else if (retryAt != null) {
			// Retriable error: call retry
			await this.redis.fCall('queasy_retry', {
				keys: [this.waitingKey, this.activeKey],
				arguments: [jobId, workerId, String(retryAt), error],
			});
		} else {
			// Permanent error: call fail
			await this.redis.fCall('queasy_fail', {
				keys: [this.waitingKey, this.activeKey],
				arguments: [jobId, workerId, error],
			});
		}
	}
}

/**
 * Create a queue object for interacting with a named queue
 * @param {string} name - Queue name
 * @param {RedisClient} redis - Redis client
 * @param {Partial<DefaultJobOptions>} [defaultJobOptions] - Default options for jobs
 * @param {Partial<DefaultJobOptions>} [failureJobOptions] - Default options for failure handler jobs
 * @returns {Queue} Queue object with dispatch, cancel, and listen methods
 */
export function queue(name, redis, defaultJobOptions, failureJobOptions) {
	return new Queue(name, redis, defaultJobOptions, failureJobOptions);
}
