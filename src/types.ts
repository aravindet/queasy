/**
 * Core job identification and data
 */
export interface JobCoreOptions {
	/** Job ID (auto-generated if not provided) */
	id?: string;
	/** Job data (any JSON-serializable value) */
	// biome-ignore lint/suspicious/noExplicitAny: Data is any serializable value
	data?: any;
	/** Wall clock timestamp (ms) before which job must not run */
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
 * Options for listen() - queue-level retry strategy
 */
export interface ListenOptions extends JobRetryOptions {
	/** Path to failure handler module (optional) */
	failHandler?: string;
}

/**
 * Update behavior flags
 */
export interface JobUpdateOptions {
	/** Whether to replace data of waiting job with same ID */
	updateData?: boolean;
	/** How to update runAt */
	updateRunAt?: boolean | 'if_later' | 'if_earlier';
	/** Whether to reset retry_count and stall_count to 0 */
	resetCounts?: boolean;
}

/**
 * Complete options accepted by dispatch()
 */
export type JobOptions = JobCoreOptions & JobUpdateOptions;

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
export type Job = Required<JobCoreOptions> & JobState;

/**
 * Messages between the main thread and workers
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

export type BumpMessage = {
	op: 'bump';
};

export type DoneMessage = {
	op: 'done';
	jobId: string;
	error?: string;
	customRetryAt?: number;
	isPermanent?: boolean;
};

export type ParentToWorkerMessage = InitMessage | ExecMessage;
export type WorkerToParentMessage = BumpMessage | DoneMessage;
