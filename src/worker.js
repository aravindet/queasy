import { pathToFileURL } from 'node:url';
import { parentPort } from 'node:worker_threads';
import { PermanentError } from './errors.js';

/** @typedef {import('./types.js').ParentToWorkerMessage} ParentToWorkerMessage */
/** @typedef {import('./types.js').WorkerToParentMessage} WorkerToParentMessage */
/** @typedef {import('./types.js').JobRetryOptions} JobRetryOptions */

if (!parentPort) throw new Error('Worker cannot be executed directly.');

/** @type {Map<string, { handle: Function, handleFailure?: Function, retryOptions: Required<JobRetryOptions> }>} */
const handlers = new Map();

/** @param {ParentToWorkerMessage} msg */
parentPort.on('message', async (msg) => {
	switch (msg.op) {
		case 'init': {
			const mod = await import(pathToFileURL(msg.handler).href);
			handlers.set(msg.queue, { ...mod, retryOptions: msg.retryOptions });
			break;
		}
		case 'exec': {
			const { queue, job } = msg;
			const handler = handlers.get(queue);
			if (!handler) throw new Error(`Handler not initialized for queue: ${queue}`);
			try {
				await handler.handle(job.data, job);
				parentPort?.postMessage({ op: 'done', jobId: job.id });
			} catch (err) {
				// Serialize error as JSON object with enumerable properties
				const error = /** @type {Error & Record<string, any>} */ (err);
				/** @type {Record<string, any>} */
				const errorObj = { message: error.message, name: error.name };
				for (const key in error) {
					errorObj[key] = error[key];
				}

				// Include custom retryAt if specified by the error
				const done = {
					op: 'done',
					jobId: job.id,
					error: JSON.stringify(errorObj),
					customRetryAt: error.retryAt,
					isPermanent: error instanceof PermanentError,
				};

				parentPort?.postMessage(done);
			}
			break;
		}
	}
});
