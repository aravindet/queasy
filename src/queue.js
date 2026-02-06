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
const HEARTBEAT_TIMEOUT = 10000; // 10 seconds

// Load Lua script
const __dirname = dirname(fileURLToPath(import.meta.url));
const luaScript = readFileSync(join(__dirname, 'queasy.lua'), 'utf8');

/** @type {WeakSet<RedisClient>} */
const initializedClients = new WeakSet();

/** @type {Array<WorkerEntry>} */
const workers = [];

/** @type {Map<string, Queue>} */
const jobMap = new Map();

/** @type {Set<Queue>} */
const allQueues = new Set();

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
 * @param {DoneMessage} msg
 */
async function done(workerEntry, msg) {
	const queue = jobMap.get(msg.jobId);
	if (!queue) return;

	workerEntry.jobIds.delete(msg.jobId);
	jobMap.delete(msg.jobId);

	await queue.done(msg.jobId, workerEntry.id, msg.error, msg.customRetryAt, msg.isPermanent);
}

/**
 * Send heartbeat bump to Redis for every active job on this worker
 * @param {WorkerEntry} workerEntry
 */
async function bump(workerEntry) {
	const expiry = String(Date.now() + HEARTBEAT_TIMEOUT);
	for (const jobId of workerEntry.jobIds) {
		const queue = jobMap.get(jobId);
		if (!queue) continue;
		await queue.redis.fCall('queasy_bump', {
			keys: [queue.activeKey],
			arguments: [jobId, workerEntry.id, expiry],
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
		retryCount: Number(job.retry_count || 0),
		stallCount: Number(job.stall_count || 0),
	};
}

/**
 * Queue instance for managing a named job queue
 */
class Queue {
	/**
	 * @param {string} name - Queue name
	 * @param {RedisClient} redis - Redis client
	 */
	constructor(name, redis) {
		this.name = name;
		this.redis = redis;
		this.waitingKey = `{${name}}:waiting`;
		this.activeKey = `{${name}}:active`;
		this.dequeueStarted = false;
		this.dequeueInterval = null;
		this.retryOptions = null;
		allQueues.add(this);
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
	 * @param {Partial<import('./types.js').JobCoreOptions & import('./types.js').JobUpdateOptions>} [options] - Job options
	 * @returns {Promise<string>} Job ID
	 */
	async dispatch(data, options = {}) {
		await this.ensureRedisInitialized();

		const id = options.id || generateId();
		const runAt = options.runAt ?? 0;
		const updateData = options.updateData ?? false;
		const updateRunAt = options.updateRunAt ?? false;
		const resetCounts = options.resetCounts ?? false;

		await this.redis.fCall('queasy_dispatch', {
			keys: [this.waitingKey, this.activeKey],
			arguments: [
				id,
				String(runAt),
				JSON.stringify(data),
				String(updateData),
				String(updateRunAt),
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
		this.dequeueInterval = setInterval(() => this.dequeue(), DEQUEUE_INTERVAL);
	}

	async dequeue() {
		for (const workerEntry of workers) {
			const available = WORKER_CAPACITY - workerEntry.jobIds.size;
			if (available <= 0) continue;

			try {
				const now = String(Date.now());
				const expiry = String(Date.now() + HEARTBEAT_TIMEOUT);
				/** @type {Job} */
				const jobs = await this.redis.fCall('queasy_dequeue', {
					keys: [this.waitingKey, this.activeKey],
					arguments: [workerEntry.id, now, expiry, String(available)],
				});

				for (const rawJob of jobs) {
					const job = parseJob(rawJob);
					if (!job) continue;

					// Check if job has exceeded stall limit
					if (this.retryOptions && job.stallCount >= this.retryOptions.maxStalls) {
						// Job has stalled too many times - fail it permanently
						await this.redis.fCall('queasy_fail', {
							keys: [this.waitingKey, this.activeKey],
							arguments: [
								job.id,
								workerEntry.id,
								JSON.stringify({ message: 'Max stalls exceeded' }),
							],
						});
						continue;
					}

					// Add retry options to job for worker
					const jobWithOptions = { ...job, ...this.retryOptions };

					workerEntry.jobIds.add(job.id);
					jobMap.set(job.id, this);
					workerEntry.worker.postMessage({ op: 'exec', queue: this.name, job: jobWithOptions });
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
	 * @param {Partial<import('./types.js').ListenOptions>} [options] - Retry strategy options
	 * @returns {Promise<void>}
	 */
	async listen(handlerPath, options = {}) {
		await this.ensureRedisInitialized();
		ensureWorkerPool();

		// Check if handler exports a failure handler
		const { pathToFileURL } = await import('node:url');
		const handlerModule = await import(pathToFileURL(handlerPath).href);
		this.hasFailureHandler = typeof handlerModule.handleFailure === 'function';

		// Store retry options with defaults
		this.retryOptions = {
			maxRetries: options.maxRetries ?? DEFAULT_JOB_OPTIONS.maxRetries,
			maxStalls: options.maxStalls ?? DEFAULT_JOB_OPTIONS.maxStalls,
			minBackoff: options.minBackoff ?? DEFAULT_JOB_OPTIONS.minBackoff,
			maxBackoff: options.maxBackoff ?? DEFAULT_JOB_OPTIONS.maxBackoff,
		};

		// Create failure handler queue name if needed
		if (this.hasFailureHandler) {
			this.failQueueName = `${this.name}-fail`;
		}

		for (const { worker } of workers) {
			worker.postMessage({
				op: 'init',
				queue: this.name,
				handler: handlerPath,
				retryOptions: this.retryOptions,
			});
		}
		this.startDequeue();
	}

	/**
	 * Handle job completion - calls finish, retry, or fail based on outcome
	 * @param {string} jobId - Job ID
	 * @param {string} workerId - Worker ID
	 * @param {string} [error] - Error message (JSON-serialized)
	 * @param {number} [customRetryAt] - Custom retry timestamp from error
	 * @param {boolean} [isPermanent] - Whether error is permanent
	 */
	async done(jobId, workerId, error, customRetryAt, isPermanent) {
		if (!error) {
			// Success: call finish
			await this.redis.fCall('queasy_finish', {
				keys: [this.waitingKey, this.activeKey],
				arguments: [jobId, workerId],
			});
			return;
		}

		// Get job to check retry count
		const activeJobKey = `{${this.name}}:active_job:${jobId}`;
		const jobData = await this.redis.hGetAll(activeJobKey);
		const retryCount = Number(jobData.retry_count || 0);

		// Determine if job should fail permanently
		const shouldFail =
			isPermanent || !this.retryOptions || retryCount >= this.retryOptions.maxRetries;

		if (shouldFail) {
			// Job failed permanently
			if (this.hasFailureHandler) {
				// Create fail job data
				const failJobId = generateId();
				const failWaitingKey = `{${this.failQueueName}}:waiting`;
				const failActiveKey = `{${this.failQueueName}}:active`;

				// Pack original job id, data, and error into array
				const failJobData = JSON.stringify([jobId, jobData.data || '{}', error || '{}']);

				// Call queasy_fail to dispatch fail job and finish original
				await this.redis.fCall('queasy_fail', {
					keys: [this.waitingKey, this.activeKey, failWaitingKey, failActiveKey],
					arguments: [jobId, workerId, failJobId, failJobData],
				});
			} else {
				// No failure handler - just finish the job
				await this.redis.fCall('queasy_finish', {
					keys: [this.waitingKey, this.activeKey],
					arguments: [jobId, workerId],
				});
			}
			return;
		}

		// Calculate retry time with exponential backoff
		const baseBackoff = this.retryOptions.minBackoff * Math.pow(2, retryCount);
		const backoff = Math.min(this.retryOptions.maxBackoff, baseBackoff);
		const calculatedRetryAt = Date.now() + backoff;
		const retryAt = Math.max(customRetryAt ?? 0, calculatedRetryAt);

		// Retriable error: call retry
		await this.redis.fCall('queasy_retry', {
			keys: [this.waitingKey, this.activeKey],
			arguments: [jobId, workerId, String(retryAt), error],
		});
	}

	/**
	 * Stop the dequeue interval for this queue
	 */
	close() {
		if (this.dequeueInterval) {
			clearInterval(this.dequeueInterval);
			this.dequeueInterval = null;
			this.dequeueStarted = false;
		}
	}
}

/**
 * Create a queue object for interacting with a named queue
 * @param {string} name - Queue name (without braces - they will be added automatically)
 * @param {RedisClient} redis - Redis client
 * @returns {Queue} Queue object with dispatch, cancel, and listen methods
 */
export function queue(name, redis) {
	return new Queue(name, redis);
}

/**
 * Terminate all worker threads and clear all queue intervals
 * Call this to allow clean process exit
 */
export function closeWorkers() {
	// Close all queue dequeue intervals
	for (const queue of allQueues) {
		queue.close();
	}
	allQueues.clear();

	// Terminate all worker threads
	for (const { worker } of workers) {
		worker.terminate();
	}
	workers.length = 0;
	jobMap.clear();
}
