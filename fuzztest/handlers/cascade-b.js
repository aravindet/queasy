/**
 * cascade-b handler — terminal handler, dispatches no further jobs.
 * Subject to all chaos behaviors.
 */

import { BroadcastChannel } from 'node:worker_threads';
import { createClient } from 'redis';
import { PermanentError } from '../../src/index.js';
import { pickChaos } from '../shared/chaos.js';
import { emitEvent } from '../shared/stream.js';

const eventRedis = createClient();
await eventRedis.connect();

const crashChannel = new BroadcastChannel('fuzz-crash');

/**
 * @param {any} data
 * @param {import('../../src/types.js').Job} job
 */
export async function handle(data, job) {
	const startedAt = Date.now();
	await emitEvent(eventRedis, {
		type: 'start',
		queue: '{fuzz}:cascade-b',
		id: job.id,
		pid: String(process.pid),
		runAt: String(job.runAt),
		startedAt: String(startedAt),
	});

	const chaos = pickChaos();
	await emitEvent(eventRedis, {
		type: 'chaos',
		queue: '{fuzz}:cascade-b',
		id: job.id,
		chaos,
	});

	if (chaos === 'crash') {
		crashChannel.postMessage({ type: 'crash' });
		// Stall so the process exits before we return
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
		throw new PermanentError('cascade-b: permanent chaos');
	}

	if (chaos === 'retriable') {
		throw new Error('cascade-b: retriable chaos');
	}

	// Normal completion
	await emitEvent(eventRedis, {
		type: 'finish',
		queue: '{fuzz}:cascade-b',
		id: job.id,
		finishedAt: String(Date.now()),
	});
}
