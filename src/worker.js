import { parentPort } from 'node:worker_threads';

if (!parentPort) throw new Error('Worker cannot be executed directly.');

parentPort.on('message', ({ queue, handlerPath }) => {
	console.log(`[worker] queue: ${queue}, handler: ${handlerPath}`);
});
