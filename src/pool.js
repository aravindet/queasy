import { availableParallelism } from 'node:os';
import { Worker } from 'node:worker_threads';
import { WORKER_CAPACITY } from './constants.js';
import { generateId } from './utils.js';

/** @typedef {import('./types').DoneMessage} DoneMessage */
/** @typedef {import('./types').Job} Job */

/** @typedef {{
 *      worker: Worker,
 *      capacity: number,
 *      id: string,
 *      jobCount: number,
 *      stalledJobs: Set<string>
 * }} WorkerEntry */

/** @typedef {{
 *      resolve: (value: DoneMessage) => void,
 *      reject: (reason: DoneMessage) => void,
 *      size: number,
 *      timer: NodeJS.Timeout
 *  }} JobEntry */

export class Pool {
    /** @param {number?} targetCount - Number of desired workers */
    constructor(targetCount) {
        /** @type {Set<WorkerEntry>} */
        this.workers = new Set();
        /** @type {Map<string, JobEntry>} */
        this.activeJobs = new Map();

        this.capacity = 0;

        const count = targetCount ?? availableParallelism();
        for (let i = 0; i < count; i++) this.createWorker();
    }

    createWorker() {
        const worker = new Worker(new URL('./worker.js', import.meta.url));
        const entry = {
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

    /**
     * @param {WorkerEntry} workerEntry
     * @param {DoneMessage} message
     */
    handleWorkerMessage(workerEntry, message) {
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

        // If this job was previously marked as stalled, unmark it.
        if (workerEntry.stalledJobs.has(jobId)) workerEntry.stalledJobs.delete(jobId);

        this.activeJobs.delete(jobId);
        jobEntry[error ? 'reject' : 'resolve'](message);

        // If this worker is no longer in the the pool, check if it can be terminated.
        if (!this.workers.has(workerEntry)) this.terminateIfEmpty(workerEntry);
    }

    /**
     *
     * @param {WorkerEntry} workerEntry
     * @param {string} jobId
     */
    handleTimeout(workerEntry, jobId) {
        workerEntry.stalledJobs.add(jobId);

        // Remove and replace this worker in the pool (if it wasn’t already).
        if (this.workers.delete(workerEntry)) this.createWorker();
        this.capacity -= workerEntry.capacity;

        // If this is the last job in this worker, terminate it.
        this.terminateIfEmpty(workerEntry);
    }

    /**
     * Stops adding new jobs to a worker if it has stalled jobs.
     * Terminates workers if all remaining jobs are stalled.
     * @param {WorkerEntry} workerEntry
     * @returns
     */
    async terminateIfEmpty({ stalledJobs, jobCount, worker }) {
        // Don't destroy if there are still non-stalled jobs running.
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

    /**
     * Processes a job to the most free worker
     * @param {string} handlerPath
     * @param {Job} job
     * @param {number} size
     * @param {number} timeout - Maximum time in ms
     * @returns {Promise<DoneMessage>}
     */
    process(handlerPath, job, size, timeout) {
        // Find worker with most capacity
        let workerEntry = null;
        for (const entry of this.workers) {
            if (!workerEntry || entry.capacity > workerEntry.capacity) workerEntry = entry;
        }

        if (!workerEntry) throw Error('Can’t process job without workers');

        const timer = setTimeout(() => this.handleTimeout(workerEntry, job.id), timeout);

        return new Promise((resolve, reject) => {
            this.activeJobs.set(job.id, { resolve, reject, size, timer });
            workerEntry.capacity -= size;
            this.capacity -= size;
            workerEntry.jobCount += 1;
            workerEntry.worker.postMessage({ op: 'exec', handlerPath, job });
        });
    }

    /**
     * Terminates all workers
     */
    close() {
        for (const { worker } of this.workers) {
            worker.terminate();
        }

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
