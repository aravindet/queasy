import { pathToFileURL } from 'node:url';
import { parentPort, setEnvironmentData } from 'node:worker_threads';
import { PermanentError } from './errors.ts';
import type { DoneMessage, ExecMessage } from './types.ts';

if (!parentPort) throw new Error('Worker cannot be executed directly.');
setEnvironmentData('queasy_worker_context', true);

parentPort.on('message', async (msg: ExecMessage) => {
    const { handlerPath, job } = msg;
    try {
        const mod = await import(pathToFileURL(handlerPath).href);
        if (typeof mod.handle !== 'function') {
            throw new Error(`Unable to load handler ${handlerPath}`);
        }

        await mod.handle(job.data, job);
        send({ op: 'done', jobId: job.id });
    } catch (err) {
        const { message, name, retryAt } = err as Error & { retryAt?: number };

        send({
            op: 'done',
            jobId: job.id,
            error: {
                name,
                message,
                retryAt,
                kind: err instanceof PermanentError ? 'permanent' : 'retriable',
            },
        });
    }
});

function send(message: DoneMessage): void {
    parentPort?.postMessage(message);
}
