import type { RedisClientType } from 'redis';

/**
 * Core job identification and data
 */
export interface JobCoreOptions {
	/** Job ID (auto-generated if not provided) */
	id?: string;
	/** Job data (any JSON-serializable value) */
	// biome-ignore lint/suspicious/noExplicitAny: Data is any serializable value
	data?: any;
	/** Wall clock timestamp before which job must not run */
	runAt?: number;
}

/**
 * Retry strategy configuration
 */
export interface JobRetryOptions {
	/** Maximum number of retries before permanent failure */
	maxRetries?: number;
	/** Maximum number of stalls before permanent failure */
	maxStalls?: number;
	/** Minimum backoff in milliseconds */
	minBackoff?: number;
	/** Maximum backoff in milliseconds */
	maxBackoff?: number;
}

/**
 * Update behavior flags
 */
export interface JobUpdateOptions {
	/** Whether to replace data of waiting job with same ID */
	updateData?: boolean;
	/** How to update runAt */
	updateRunAt?: boolean | 'if_later' | 'if_earlier';
	/** Whether to update retry strategy fields */
	updateRetryStrategy?: boolean;
	/** Whether to reset retry_count and stall_count to 0 */
	resetCounts?: boolean;
}

/**
 * Default options for jobs (used at queue level)
 */
export type DefaultJobOptions = JobRetryOptions & JobUpdateOptions;

/**
 * Complete options accepted by dispatch()
 */
export type JobOptions = DefaultJobOptions & JobCoreOptions;

/**
 * Job runtime state
 */
export interface JobState {
	/** Number of times this job has been retried */
	retryCount: number;
	/** Number of times this job has stalled */
	stallCount: number;
}

/**
 * Complete job representation passed to handlers
 */
export type Job = JobCoreOptions & JobRetryOptions & JobState;

/**
 * Queue interface
 */
export interface Queue {
	/** Add a job to the queue */
	dispatch(data: any, options?: Partial<JobOptions>): Promise<string>;
	/** Cancel a waiting job */
	cancel(id: string): Promise<boolean>;
	/** Attach handlers to process jobs */
	listen(handlerPath: string): Promise<void>;
}
