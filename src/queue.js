import { readFileSync } from 'node:fs';
import { cpus } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';

// Import types:
/** @typedef {import('redis').RedisClientType} RedisClient */
/** @typedef {import('./types.js').DefaultJobOptions} DefaultJobOptions */
/** @typedef {import('./types.js').Queue} Queue */
/** @typedef {import('./types.js').JobOptions} JobOptions */
/** @typedef {import('./types.js').Job} Job */
/** @typedef {import('./types.js').ParentToWorkerMessage} ParentToWorkerMessage */
/** @typedef {import('./types.js').WorkerToParentMessage} WorkerToParentMessage */

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

/** @type {Map<string, {redis: RedisClient, waitingKey: string, activeKey: string, workerId: string}>} */
const jobMeta = new Map();

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
		case 'done': {
			const meta = jobMeta.get(msg.jobId);
			if (!meta) break;

			workerEntry.jobIds.delete(msg.jobId);
			jobMeta.delete(msg.jobId);

			if (!msg.error) {
				// Success: call finish
				await meta.redis.fCall('queasy_finish', {
					keys: [meta.waitingKey, meta.activeKey],
					arguments: [msg.jobId, meta.workerId],
				});
			} else if (msg.retryAt != null) {
				// Retriable error: call retry
				await meta.redis.fCall('queasy_retry', {
					keys: [meta.waitingKey, meta.activeKey],
					arguments: [msg.jobId, meta.workerId, String(msg.retryAt), msg.error],
				});
			} else {
				// Permanent error: call fail
				await meta.redis.fCall('queasy_fail', {
					keys: [meta.waitingKey, meta.activeKey],
					arguments: [msg.jobId, meta.workerId, msg.error],
				});
			}
			break;
		}
	}
}

/**
 * Send heartbeat bump to Redis for every active job on this worker
 * @param {WorkerEntry} workerEntry
 */
async function bump(workerEntry) {
	const now = String(Date.now());
	for (const jobId of workerEntry.jobIds) {
		const meta = jobMeta.get(jobId);
		if (!meta) continue;
		await meta.redis.fCall('queasy_bump', {
			keys: [meta.activeKey],
			arguments: [jobId, workerEntry.id, now],
		});
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
 * Create a queue object for interacting with a named queue
 * @param {string} name - Queue name
 * @param {RedisClient} redis - Redis client
 * @param {Partial<DefaultJobOptions>} [defaultJobOptions] - Default options for jobs
 * @param {Partial<DefaultJobOptions>} [failureJobOptions] - Default options for failure handler jobs
 * @returns {Queue} Queue object with dispatch, cancel, and listen methods
 */
export function queue(name, redis, defaultJobOptions = {}, failureJobOptions = {}) {
	async function ensureRedisInitialized() {
		if (!initializedClients.has(redis)) {
			await redis.sendCommand(['FUNCTION', 'LOAD', 'REPLACE', luaScript]);
			initializedClients.add(redis);
		}
	}

	const baseOpts = { ...DEFAULT_JOB_OPTIONS, ...defaultJobOptions };
	const failOpts = { ...DEFAULT_FAILJOB_OPTIONS, ...failureJobOptions };
	const waitingKey = `${name}:waiting`;
	const activeKey = `${name}:active`;

	/**
	 * Add a job to the queue
	 * @param {any} data - Job data (any JSON-serializable value)
	 * @param {Partial<JobOptions>} [options] - Job options
	 * @returns {Promise<string>} Job ID
	 */
	async function dispatch(data, options = {}) {
		await ensureRedisInitialized();

		const opts = { ...baseOpts, ...options };
		const id = opts.id || generateId();
		const runAt = opts.runAt ?? 0;
		const resetCounts = options.updateData ? opts.updateData : opts.resetCounts;

		// Convert boolean/string values to strings for Lua

		await redis.fCall('queasy_dispatch', {
			keys: [waitingKey, activeKey],
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
	async function cancel(id) {
		await ensureRedisInitialized();

		const result = await redis.fCall('queasy_cancel', {
			keys: [waitingKey],
			arguments: [id],
		});

		return result === 1;
	}

	let dequeueStarted = false;

	function startDequeue() {
		if (dequeueStarted) return;
		dequeueStarted = true;
		setInterval(dequeue, DEQUEUE_INTERVAL);
	}

	async function dequeue() {
		for (const workerEntry of workers) {
			const available = WORKER_CAPACITY - workerEntry.jobIds.size;
			if (available <= 0) continue;

			const jobs = await redis.fCall('queasy_dequeue', {
				keys: [waitingKey, activeKey],
				arguments: [workerEntry.id, String(Date.now()), String(available)],
			});

			for (const rawJob of jobs) {
				const job = parseJob(rawJob);
				if (!job) continue;
				workerEntry.jobIds.add(job.id);
				jobMeta.set(job.id, { redis, waitingKey, activeKey, workerId: workerEntry.id });
				workerEntry.worker.postMessage({ op: 'exec', queue: name, job });
			}
		}
	}

	/**
	 * Attach handlers to process jobs from this queue
	 * @param {string} handlerPath - Path to handler module
	 * @returns {Promise<void>}
	 */
	async function listen(handlerPath) {
		await ensureRedisInitialized();
		ensureWorkerPool();
		for (const { worker } of workers) {
			worker.postMessage({ op: 'init', queue: name, handler: handlerPath });
		}
		startDequeue();
	}

	return { dispatch, cancel, listen };
}
