import { pathToFileURL } from 'node:url';
import { parentPort } from 'node:worker_threads';
import { PermanentError } from './errors.js';

/** @typedef {import('./types.js').ParentToWorkerMessage} ParentToWorkerMessage */
/** @typedef {import('./types.js').WorkerToParentMessage} WorkerToParentMessage */

if (!parentPort) throw new Error('Worker cannot be executed directly.');

const HEARTBEAT_INTERVAL = 5000;

/** @type {Map<string, { handle: Function, handleFailure?: Function }>} */
const handlers = new Map();
const activeJobs = new Set();

let bumpInterval = null;

function ensureBumpTimer() {
	if (bumpInterval) return;
	bumpInterval = setInterval(() => {
		if (activeJobs.size > 0) {
			parentPort.postMessage({ op: 'bump' });
		}
	}, HEARTBEAT_INTERVAL);
}

/** @param {ParentToWorkerMessage} msg */
parentPort.on('message', async (msg) => {
	switch (msg.op) {
		case 'init': {
			const mod = await import(pathToFileURL(msg.handler).href);
			handlers.set(msg.queue, mod);
			ensureBumpTimer();
			break;
		}
		case 'exec': {
			const { queue, job } = msg;
			const handler = handlers.get(queue);
			activeJobs.add(job.id);
			try {
				await handler.handle(job.data, job);
				parentPort.postMessage({ op: 'done', jobId: job.id });
			} catch (err) {
				// Serialize error as JSON object with enumerable properties
				const errorObj = { message: err.message, name: err.name };
				for (const key in err) {
					errorObj[key] = err[key];
				}
				const done = { op: 'done', jobId: job.id, error: JSON.stringify(errorObj) };

				// Check if error is retriable (not PermanentError and under retry limit)
				if (!(err instanceof PermanentError) && job.retryCount < job.maxRetries) {
					const baseBackoff = job.minBackoff * Math.pow(2, job.retryCount || 0);
					const backoff = Math.min(job.maxBackoff, baseBackoff);
					const calculatedRetryAt = Date.now() + backoff;
					done.retryAt = Math.max(err.retryAt ?? 0, calculatedRetryAt);
				}

				parentPort.postMessage(done);
			} finally {
				activeJobs.delete(job.id);
			}
			break;
		}
	}
});
