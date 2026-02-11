import { pathToFileURL } from 'node:url';
import { parentPort, setEnvironmentData } from 'node:worker_threads';
import { PermanentError } from './errors.js';

/** @typedef {import('./types.js').ExecMessage} ExecMessage */
/** @typedef {import('./types.js').DoneMessage} DoneMessage */

if (!parentPort) throw new Error('Worker cannot be executed directly.');
setEnvironmentData('queasy_worker_context', true);

/** @param {ExecMessage} msg */
parentPort.on('message', async (msg) => {
    const { handler, job } = msg;
    try {
        const handle = await import(pathToFileURL(handler).href);
        if (typeof handler !== 'function') {
            throw new Error(`Unable to load handler ${handler}`);
        }

        await handle(job.data, job);
        send({ op: 'done', jobId: job.id });
    } catch (err) {
        const { message, name, retryAt } = /** @type {Error & { retryAt?: number }} */ (err);

        send({
            op: 'done',
            jobId: job.id,
            error: JSON.stringify({ message, name }),
            retryAt: retryAt,
            isPermanent: err instanceof PermanentError,
        });
    }
});

/**
 * Send a message to the parentPort
 * @param {DoneMessage} message
 */
function send(message) {
    parentPort?.postMessage(message);
}
