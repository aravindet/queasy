import { availableParallelism } from 'node:os';
import { Worker } from 'node:worker_threads';
import { WORKER_CAPACITY } from './constants.js';
import type { DoneMessage, Job } from './types.js';
import { generateId } from './utils.js';

interface WorkerEntry {
    worker: Worker;
    capacity: number;
    id: string;
    jobCount: number;
    stalledJobs: Set<string>;
}

interface JobEntry {
    resolve: (value: DoneMessage) => void;
    reject: (reason: DoneMessage) => void;
    size: number;
    timer: NodeJS.Timeout;
}

export class Pool {
    workers: Set<WorkerEntry>;
    activeJobs: Map<string, JobEntry>;
    capacity: number;

    constructor(targetCount?: number | null) {
        this.workers = new Set();
        this.activeJobs = new Map();
        this.capacity = 0;

        const count = targetCount ?? availableParallelism();
        for (let i = 0; i < count; i++) this.createWorker();
    }

    createWorker(): void {
        const worker = new Worker(new URL('./worker.js', import.meta.url));
        const entry: WorkerEntry = {
            worker,
            capacity: WORKER_CAPACITY,
            id: generateId(),
            jobCount: 0,
            stalledJobs: new Set(),
        };
        this.capacity += WORKER_CAPACITY;
        worker.on('message', (message) => this.handleWorkerMessage(entry, message));
        this.workers.add(entry);
    }

    handleWorkerMessage(workerEntry: WorkerEntry, message: DoneMessage): void {
        const { jobId, error } = message;
        const jobEntry = this.activeJobs.get(jobId);
        if (!jobEntry) {
            console.warn('Worker message with unknown Job ID; Ignoring.');
            return;
        }
        clearTimeout(jobEntry.timer);
        workerEntry.capacity += jobEntry.size;
        this.capacity += jobEntry.size;
        workerEntry.jobCount -= 1;

        if (workerEntry.stalledJobs.has(jobId)) workerEntry.stalledJobs.delete(jobId);

        this.activeJobs.delete(jobId);
        jobEntry[error ? 'reject' : 'resolve'](message);

        if (!this.workers.has(workerEntry)) this.terminateIfEmpty(workerEntry);
    }

    handleTimeout(workerEntry: WorkerEntry, jobId: string): void {
        workerEntry.stalledJobs.add(jobId);

        if (this.workers.delete(workerEntry)) this.createWorker();
        this.capacity -= workerEntry.capacity;

        this.terminateIfEmpty(workerEntry);
    }

    async terminateIfEmpty({ stalledJobs, jobCount, worker }: WorkerEntry): Promise<void> {
        if (jobCount > stalledJobs.size) return;
        await worker.terminate();

        for (const jobId of stalledJobs) {
            const jobEntry = this.activeJobs.get(jobId);
            this.activeJobs.delete(jobId);
            jobEntry?.reject({
                op: 'done',
                jobId,
                error: { name: 'StallError', message: 'Job stalled', kind: 'stall' },
            });
        }
    }

    process(handlerPath: string, job: Job, size: number, timeout: number): Promise<DoneMessage> {
        let workerEntry: WorkerEntry | null = null;
        for (const entry of this.workers) {
            if (!workerEntry || entry.capacity > workerEntry.capacity) workerEntry = entry;
        }

        if (!workerEntry) throw Error("Can't process job without workers");

        const timer = setTimeout(() => this.handleTimeout(workerEntry!, job.id), timeout);

        return new Promise((resolve, reject) => {
            this.activeJobs.set(job.id, { resolve, reject, size, timer });
            workerEntry!.capacity -= size;
            this.capacity -= size;
            workerEntry!.jobCount += 1;
            workerEntry!.worker.postMessage({ op: 'exec', handlerPath, job });
        });
    }

    async close(): Promise<void> {
        await Promise.all([...this.workers].map(async ({ worker }) => worker.terminate()));
        for (const [jobId, { reject, timer }] of this.activeJobs.entries()) {
            clearTimeout(timer);
            reject({
                op: 'done',
                jobId,
                error: { name: 'StallError', message: 'Pool is closing', kind: 'stall' },
            });
        }

        this.workers = new Set();
        this.activeJobs.clear();
    }
}
