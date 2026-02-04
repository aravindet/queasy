/**
 * @typedef {Object} JobOptions
 * @property {string} [id] - Alphanumeric string; if not provided, a unique random string is generated
 * @property {number} [runAt] - Wall clock timestamp before which this job must not be run; default: 0
 * @property {number} [maxFailures] - Maximum number of failures before permanent failure; default: 10
 * @property {number} [maxStalls] - Maximum number of stalls before permanent failure; default: 3
 * @property {number} [minBackoff] - Minimum backoff in milliseconds; default: 2000
 * @property {number} [maxBackoff] - Maximum backoff in milliseconds; default: 300000
 * @property {boolean} [updateData] - Whether to replace data of waiting job with same ID; default: true
 * @property {boolean | 'ifLater' | 'ifEarlier'} [updateRunAt] - How to update runAt; default: true
 * @property {boolean} [updateMaxFailures] - Whether to update maxFailures; default: false
 * @property {boolean} [updateMaxStalls] - Whether to update maxStalls; default: false
 * @property {boolean} [updateMinBackoff] - Whether to update minBackoff; default: false
 * @property {boolean} [updateMaxBackoff] - Whether to update maxBackoff; default: false
 * @property {boolean} [resetCounts] - Whether to reset failure and stall counts; default: same as updateData
 */

/**
 * @typedef {Object} Queue
 * @property {(data: any, options?: JobOptions) => Promise<string>} notify - Adds a job to the queue
 * @property {(id: string) => Promise<boolean>} cancel - Cancels a waiting job
 * @property {(handler: string) => Promise<void>} listen - Attaches handlers to process jobs
 */

/**
 * Creates a queue object for interacting with a named queue
 * @param {string} name - Queue name
 * @param {import('ioredis').Redis} redisConnection - ioredis connection object
 * @param {Partial<JobOptions>} [defaultJobOptions] - Default options for jobs
 * @param {Partial<JobOptions>} [failureJobOptions] - Default options for failure handler jobs
 * @returns {Queue} Queue object
 */
export function queue(name, redisConnection, defaultJobOptions = {}, failureJobOptions = {}) {
	// TODO: Implement queue
	return {
		notify: async (data, options = {}) => {
			// TODO: Implement notify
			return '';
		},
		cancel: async (id) => {
			// TODO: Implement cancel
			return false;
		},
		listen: async (handler) => {
			// TODO: Implement listen
		},
	};
}

/**
 * @typedef {Object} Lock
 * @property {(timeout?: number) => Promise<Lock | null>} acquire - Acquires the lock
 * @property {() => Promise<void>} release - Releases the lock
 */

/**
 * Creates a lock object for locking arbitrary resources
 * @param {string} name - Lock name
 * @param {import('ioredis').Redis} redisConnection - ioredis connection object
 * @returns {Lock} Lock object
 */
export function lock(name, redisConnection) {
	// TODO: Implement lock
	return {
		acquire: async (timeout) => {
			// TODO: Implement acquire
			return null;
		},
		release: async () => {
			// TODO: Implement release
		},
	};
}

/**
 * Error class to indicate permanent job failure
 */
export class PermanentError extends Error {
	/**
	 * @param {string} message - Error message
	 */
	constructor(message) {
		super(message);
		this.name = 'PermanentError';
	}
}

/**
 * Error class to indicate job stalled
 */
export class StallError extends Error {
	/**
	 * @param {string} message - Error message
	 */
	constructor(message) {
		super(message);
		this.name = 'StallError';
	}
}
