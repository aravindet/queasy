/** @typedef {import('./types').JobRetryOptions} JobRetryOptions */
/** @typedef {import('./types').JobUpdateOptions} JobUpdateOptions */

/** @type {Required<JobRetryOptions>} */
export const DEFAULT_RETRY_OPTIONS = {
	maxRetries: 10,
	maxStalls: 3,
	minBackoff: 2_000,
	maxBackoff: 300_000, // 5 minutes
};

/** @type {Required<JobUpdateOptions>} */
export const DEFAULT_UPDATE_OPTIONS = {
	updateData: true,
	updateRunAt: true,
	resetCounts: false,
};

/** @type {Required<JobRetryOptions>} */
export const FAILJOB_RETRY_OPTIONS = {
	maxRetries: 100,
	maxStalls: 3,
	minBackoff: 10_000,
	maxBackoff: 900_000, // 15 minutes
};

export const HEARTBEAT_INTERVAL = 5000; // 5 seconds
export const HEARTBEAT_TIMEOUT = 10000; // 10 seconds
export const WORKER_CAPACITY = 10;
export const DEQUEUE_INTERVAL = 100; // ms
