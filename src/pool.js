import { cpus } from 'node:os';
import { Worker } from 'node:worker_threads';
import { WORKER_CAPACITY } from './constants.js';
import { generateId } from './utils.js';

/** @typedef {import('./types').DoneMessage} DoneMessage */
/** @typedef {import('./types').Job} Job */

/** @typedef {{worker: Worker, capacity: number, id: string}} WorkerEntry */
/** @typedef {{resolve: (value: DoneMessage) => void, reject: (reason: DoneMessage) => void, size: number}} JobEntry */

export class Pool {
    /**
     *
     * @param {number} rawCount
     */
    constructor(rawCount) {
        /** @type {Array<WorkerEntry>} */
        this.workers = [];
        /** @type {Map<string, JobEntry>} */
        this.activeJobs = new Map();

        const count = rawCount ?? cpus().length;
        for (let i = 0; i < count; i++) {
            const worker = new Worker(new URL('./worker.js', import.meta.url));
            const entry = { worker, capacity: WORKER_CAPACITY, id: generateId() };
            worker.on('message', (message) => this.handleWorkerMessage(entry, message));
            this.workers.push(entry);
        }
    }

    /**
     * @param {WorkerEntry} workerEntry
     * @param {DoneMessage} message
     */
    handleWorkerMessage = (workerEntry, message) => {
        const activeJob = this.activeJobs.get(message.jobId);
        if (!activeJob) {
            console.warn('Worker message with unknown Job ID; Ignoring.');
            return;
        }
        workerEntry.capacity += activeJob.size;
        this.activeJobs.delete(message.jobId);
        activeJob[message.error ? 'reject' : 'resolve'](message);
    };

    /**
     * Reports total spare capacity across all workers
     * @param {number} size - Size of the job to dispatch
     * @returns {number} Number of jobs that there is capacity for
     */
    getCapacity(size) {
        let total = 0;
        for (const entry of this.workers) {
            total += entry.capacity / size;
        }
        return total;
    }

    /**
     * Processes a job to the most free worker
     * @param {string} handlerPath
     * @param {Job} job
     * @param {number} size
     * @returns {Promise<DoneMessage>}
     */
    async process(handlerPath, job, size) {
        // Find worker with most capacity
        let workerEntry = this.workers[0];
        for (const entry of this.workers) {
            if (entry.capacity > workerEntry.capacity) workerEntry = entry;
        }

        return new Promise((resolve, reject) => {
            this.activeJobs.set(job.id, { resolve, reject, size });
            workerEntry.capacity -= size;
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
        this.workers = [];
        this.activeJobs.clear();
    }
}
