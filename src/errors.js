/**
 * Error thrown to indicate a job should not be retried
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
 * Error indicating a job stalled (worker stopped sending heartbeats)
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
