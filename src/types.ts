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
 * Retry strategy options for listen()
 */
export type ListenOptions = JobRetryOptions;

/**
 * Messages from the main thread to a worker
 */
export type InitMessage = {
	op: 'init';
	queue: string;
	handler: string;
	retryOptions: Required<JobRetryOptions>;
};

export type ExecMessage = {
	op: 'exec';
	queue: string;
	job: Job;
};

export type ParentToWorkerMessage = InitMessage | ExecMessage;

/**
 * Messages from a worker back to the main thread
 */
export type BumpMessage = {
	op: 'bump';
};

export type DoneMessage = {
	op: 'done';
	jobId: string;
	error?: string;
	retryAt?: number;
};

export type WorkerToParentMessage = BumpMessage | DoneMessage;
