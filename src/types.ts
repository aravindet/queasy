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
 * Handler options
 */
export interface HandlerOptions {
    /** Maximum number of retries before permanent failure */
    maxRetries?: number;
    /** Maximum number of stalls before permanent failure */
    maxStalls?: number;
    /** Minimum backoff in milliseconds */
    minBackoff?: number;
    /** Maximum backoff in milliseconds */
    maxBackoff?: number;
    /** Size of the job (as a percent of total worker capacity) */
    size?: number;
    /** Maximum processing duration before considering stalled */
    timeout?: number;
    /** Priority of this queue (vs other queues) */
    priority?: number;
}

/**
 * Options for listen() - queue-level retry strategy
 */
export interface ListenOptions extends HandlerOptions {
    /** Path to failure handler module (optional) */
    failHandler?: string;

    /** Retry options of the fail job */
    failRetryOptions?: HandlerOptions;
}

export type ExecMessage = {
    op: 'exec';
    queue: string;
    job: Job;
};

export type DoneMessage = {
    op: 'done';
    jobId: string;
    error?: {
        name: string;
        message: string;
        retryAt?: number;
        kind?: 'retriable' | 'permanent' | 'stall';
    };
};
