import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

// Import types:
/** @typedef {import('redis').RedisClientType} RedisClient */
/** @typedef {import('./types.js').DefaultJobOptions} DefaultJobOptions */
/** @typedef {import('./types.js').Queue} Queue */
/** @typedef {import('./types.js').JobOptions} JobOptions */

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

// Load Lua script
const __dirname = dirname(fileURLToPath(import.meta.url));
const luaScript = readFileSync(join(__dirname, 'queasy.lua'), 'utf8');

/** @type {WeakSet<RedisClient>} */
const initializedClients = new WeakSet();

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

	// Default options
	const baseOpts = { ...DEFAULT_JOB_OPTIONS, ...defaultJobOptions };
	const failOpts = { ...DEFAULT_FAILJOB_OPTIONS, ...failureJobOptions };
	const waitingKey = `${name}:waiting`;
	const activeKey = `${name}:active`;

	return {
		/**
		 * Add a job to the queue
		 * @param {any} data - Job data (any JSON-serializable value)
		 * @param {Partial<JobOptions>} [options] - Job options
		 * @returns {Promise<string>} Job ID
		 */
		async dispatch(data, options = {}) {
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
		},

		/**
		 * Cancel a waiting job
		 * @param {string} id - Job ID
		 * @returns {Promise<boolean>} True if job was cancelled
		 */
		async cancel(id) {
			await ensureRedisInitialized();

			const result = await redis.fCall('queasy_cancel', {
				keys: [waitingKey],
				arguments: [id],
			});

			return result === 1;
		},

		/**
		 * Attach handlers to process jobs from this queue
		 * @param {string} handlerPath - Path to handler module
		 * @returns {Promise<void>}
		 */
		async listen(handlerPath) {
			await ensureRedisInitialized();
			// TODO: Implement worker logic
			throw new Error('listen() not yet implemented');
		},
	};
}
