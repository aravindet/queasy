/**
 * cascade-a handler — dispatches one or two cascade-b jobs on normal completion.
 * Subject to all chaos behaviors.
 */

import { BroadcastChannel } from 'node:worker_threads';
import { createClient } from 'redis';
import { Client, PermanentError } from '../../src/index.js';
import { pickChaos } from '../shared/chaos.js';
import { emitEvent } from '../shared/stream.js';

const redis = createClient();
const eventRedis = createClient();

await redis.connect();
await eventRedis.connect();

// Dispatch-only queasy client (await ready to avoid Function not found race)
const client = await new Promise((resolve) => new Client(redis, 0, resolve));
const cascadeBQueue = client.queue('{fuzz}:cascade-b', true);

const crashChannel = new BroadcastChannel('fuzz-crash');

/**
 * @param {any} data
 * @param {import('../../src/types.js').Job} job
 */
export async function handle(data, job) {
	const startedAt = Date.now();
	await emitEvent(eventRedis, {
		type: 'start',
		queue: '{fuzz}:cascade-a',
		id: job.id,
		pid: String(process.pid),
		runAt: String(job.runAt),
		startedAt: String(startedAt),
	});

	const chaos = pickChaos();
	await emitEvent(eventRedis, {
		type: 'chaos',
		queue: '{fuzz}:cascade-a',
		id: job.id,
		chaos,
	});

	if (chaos === 'crash') {
		crashChannel.postMessage({ type: 'crash' });
		await new Promise(() => {});
	}

	if (chaos === 'stall') {
		await new Promise(() => {});
	}

	if (chaos === 'spin') {
		const end = Date.now() + 10_000;
		while (Date.now() < end) { /* busy wait */ }
	}

	if (chaos === 'permanent') {
		throw new PermanentError('cascade-a: permanent chaos');
	}

	if (chaos === 'retriable') {
		throw new Error('cascade-a: retriable chaos');
	}

	// Normal completion: dispatch 1-2 cascade-b jobs
	const count = Math.random() < 0.5 ? 1 : 2;
	const runAtOffset = Math.random() * 2000;
	const dispatchPromises = [];
	for (let i = 0; i < count; i++) {
		dispatchPromises.push(
			cascadeBQueue.dispatch({ from: job.id, index: i }, {
				runAt: Date.now() + runAtOffset,
			}),
		);
	}
	const ids = await Promise.all(dispatchPromises);

	await emitEvent(eventRedis, {
		type: 'finish',
		queue: '{fuzz}:cascade-a',
		id: job.id,
		finishedAt: String(Date.now()),
		dispatched: ids.join(','),
	});
}
