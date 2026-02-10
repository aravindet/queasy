import { readFileSync } from 'node:fs';
import { cpus } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';

import {
	DEFAULT_RETRY_OPTIONS,
	DEQUEUE_INTERVAL,
	HEARTBEAT_INTERVAL,
	WORKER_CAPACITY,
} from './constants.js';

// Import types:
/** @typedef {import('redis').RedisClientType} RedisClient */
/** @typedef {import('./types').JobRetryOptions} JobRetryOptions */
/** @typedef {import('./types').Job} Job */
/** @typedef {import('./types').ParentToWorkerMessage} ParentToWorkerMessage */
/** @typedef {import('./types').WorkerToParentMessage} WorkerToParentMessage */
/** @typedef {import('./types').DoneMessage} DoneMessage */

// Types used only in this file
/** @typedef {{worker: Worker, jobIds: Set<string>, id: string}} WorkerEntry */

// Load Lua script
const __dirname = dirname(fileURLToPath(import.meta.url));
const luaScript = readFileSync(join(__dirname, 'queasy.lua'), 'utf8');

/** @type {WeakSet<RedisClient>} */
const initializedClients = new WeakSet();

/** @type {Array<WorkerEntry>} */
const workers = [];

/** @type {Map<string, Queue>} */
const jobMap = new Map();

/** @type {Map<string, {retryCount: number, data: string}>} */
const jobMetadata = new Map();

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
	await done(workerEntry, msg);
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

	await queue.done(msg.jobId, msg.error, msg.customRetryAt, msg.isPermanent);

	// Clean up job metadata after done is called
	jobMetadata.delete(msg.jobId);
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

// This client (process) has an ID:
const clientId = generateId();

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
		this.queueKey = `{${name}}`;
		this.dequeueStarted = false;
		this.dequeueInterval = null;
		this.retryOptions = null;
		/** @type {NodeJS.Timeout | null} */
		this.bumpTimer = null;
		allQueues.add(this);
	}

	async ensureRedisInitialized() {
		if (!initializedClients.has(this.redis)) {
			await this.redis.sendCommand(['FUNCTION', 'LOAD', 'REPLACE', luaScript]);
			initializedClients.add(this.redis);
		}
	}

	/**
	 * Schedule the next bump timer
	 */
	scheduleBump() {
		if (this.bumpTimer) clearTimeout(this.bumpTimer);
		this.bumpTimer = setTimeout(() => this.bump(), HEARTBEAT_INTERVAL);
	}

	/**
	 * Send heartbeat bump to Redis for this client
	 */
	async bump() {
		try {
			await this.redis.fCall('queasy_bump', {
				keys: [this.queueKey],
				arguments: [clientId, String(Date.now())],
			});
		} catch (err) {
			const error = /** @type {Error} */ (err);
			if (error.message?.includes('closed')) return;
			throw err;
		}
		this.scheduleBump();
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
			keys: [this.queueKey],
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
			keys: [this.queueKey],
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
				const result = await this.redis.fCall('queasy_dequeue', {
					keys: [this.queueKey],
					arguments: [clientId, now, String(available)],
				});
				/** @type {string[][]} */
				const jobs = /** @type {string[][]} */ (result);

				for (const rawJob of jobs) {
					const job = parseJob(rawJob);
					if (!job) continue;

					// Check if job has exceeded stall limit
					if (this.retryOptions && job.stallCount >= this.retryOptions.maxStalls) {
						// Job has stalled too many times - fail it permanently
						const now = String(Date.now());
						if (this.failQueueName) {
							const failJobId = generateId();
							const failQueueKey = `{${this.failQueueName}}`;
							const failJobData = JSON.stringify([
								job.id,
								'{}',
								JSON.stringify({ message: 'Max stalls exceeded' }),
							]);
							await this.redis.fCall('queasy_fail', {
								keys: [this.queueKey, failQueueKey],
								arguments: [job.id, clientId, failJobId, failJobData, now],
							});
						} else {
							await this.redis.fCall('queasy_finish', {
								keys: [this.queueKey],
								arguments: [job.id, clientId, now],
							});
						}
						continue;
					}

					// Store job metadata for later use in done()
					// Extract raw data string from the rawJob array
					let rawData = '{}';
					for (let i = 0; i < rawJob.length; i += 2) {
						if (rawJob[i] === 'data') {
							rawData = rawJob[i + 1] || '{}';
							break;
						}
					}
					jobMetadata.set(job.id, {
						retryCount: job.retryCount,
						data: rawData,
					});

					// Add retry options to job for worker
					const jobWithOptions = { ...job, ...this.retryOptions };

					workerEntry.jobIds.add(job.id);
					jobMap.set(job.id, this);
					workerEntry.worker.postMessage({ op: 'exec', queue: this.name, job: jobWithOptions });
				}

				if (jobs.length > 0) {
					this.scheduleBump();
				}
			} catch (err) {
				// Redis client may be closed during tests or shutdown - ignore these errors
				const error = /** @type {Error} */ (err);
				if (error.message?.includes('closed')) {
					return;
				}
				throw err;
			}
		}
	}

	/**
	 * Attach handlers to process jobs from this queue
	 * @param {string} handlerPath - Path to handler module
	 * @param {import('./types').ListenOptions} [options] - Retry strategy options and failure handler
	 * @returns {Promise<void>}
	 */
	async listen(handlerPath, options = {}) {
		await this.ensureRedisInitialized();
		ensureWorkerPool();

		// Store retry options with defaults
		this.retryOptions = { ...DEFAULT_RETRY_OPTIONS, ...options };

		// Initialize main handler on all workers
		for (const { worker } of workers) {
			worker.postMessage({
				op: 'init',
				queue: this.name,
				handler: handlerPath,
				retryOptions: this.retryOptions,
			});
		}

		// Initialize failure handler on all workers if provided
		if (options.failHandler) {
			this.failQueueName = `${this.name}-fail`;
			for (const { worker } of workers) {
				worker.postMessage({
					op: 'init',
					queue: this.failQueueName,
					handler: options.failHandler,
					retryOptions: this.retryOptions,
				});
			}
		}

		this.startDequeue();
	}

	/**
	 * Handle job completion - calls finish, retry, or fail based on outcome
	 * @param {string} jobId - Job ID
	 * @param {string} [error] - Error message (JSON-serialized)
	 * @param {number} [customRetryAt] - Custom retry timestamp from error
	 * @param {boolean} [isPermanent] - Whether error is permanent
	 */
	async done(jobId, error, customRetryAt, isPermanent) {
		const now = String(Date.now());

		if (!error) {
			// Success: call finish
			await this.redis.fCall('queasy_finish', {
				keys: [this.queueKey],
				arguments: [jobId, clientId, now],
			});
			this.scheduleBump();
			return;
		}

		// Get job metadata from map instead of Redis
		const metadata = jobMetadata.get(jobId);
		if (!metadata) {
			// Metadata not found - this shouldn't happen in normal flow
			// Fall back to just finishing the job
			await this.redis.fCall('queasy_finish', {
				keys: [this.queueKey],
				arguments: [jobId, clientId, now],
			});
			this.scheduleBump();
			return;
		}

		const retryCount = metadata.retryCount;

		// Determine if job should fail permanently
		const shouldFail =
			isPermanent || !this.retryOptions || retryCount >= this.retryOptions.maxRetries;

		if (shouldFail) {
			// Job failed permanently
			if (this.failQueueName) {
				// Create fail job data
				const failJobId = generateId();
				const failQueueKey = `{${this.failQueueName}}`;

				// Pack original job id, data, and error into array
				const failJobData = JSON.stringify([jobId, metadata.data, error]);

				// Call queasy_fail to dispatch fail job and finish original
				await this.redis.fCall('queasy_fail', {
					keys: [this.queueKey, failQueueKey],
					arguments: [jobId, clientId, failJobId, failJobData, now],
				});
			} else {
				// No failure handler - just finish the job
				await this.redis.fCall('queasy_finish', {
					keys: [this.queueKey],
					arguments: [jobId, clientId, now],
				});
			}
			this.scheduleBump();
			return;
		}

		// Calculate retry time with exponential backoff
		// retryOptions is guaranteed to exist here because shouldFail would be true otherwise
		if (!this.retryOptions) return; // Type guard for TypeScript
		const baseBackoff = this.retryOptions.minBackoff * 2 ** retryCount;
		const backoff = Math.min(this.retryOptions.maxBackoff, baseBackoff);
		const calculatedRetryAt = Date.now() + backoff;
		const retryAt = Math.max(customRetryAt ?? 0, calculatedRetryAt);

		// Retriable error: call retry
		await this.redis.fCall('queasy_retry', {
			keys: [this.queueKey],
			arguments: [jobId, clientId, String(retryAt), error, now],
		});
		this.scheduleBump();
	}

	/**
	 * Stop the dequeue interval and bump timer for this queue
	 */
	close() {
		if (this.dequeueInterval) {
			clearInterval(this.dequeueInterval);
			this.dequeueInterval = null;
			this.dequeueStarted = false;
		}
		if (this.bumpTimer) {
			clearTimeout(this.bumpTimer);
			this.bumpTimer = null;
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
	jobMetadata.clear();
}
