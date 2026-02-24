/**
 * Fail handler — invoked for every job that exhausts retries/stalls on any queue.
 *
 * Received data: [originalJobId, originalData, errorObject]
 *
 * For periodic jobs (id starts with 'fuzz-periodic-'), re-dispatches the job
 * so that periodic jobs keep running indefinitely even after permanent failure.
 * For cascade jobs, simply records the failure.
 */

import { createClient } from 'redis';
import { Client } from '../../src/index.js';
import { emitEvent } from '../shared/stream.js';

const redis = createClient();
const eventRedis = createClient();

await redis.connect();
await eventRedis.connect();

// Dispatch-only queasy client (await ready to avoid Function not found race)
const client = await new Promise((resolve) => new Client(redis, 0, resolve));

// Queue references (keys already include braces)
const periodicQueue = client.queue('{fuzz}:periodic', true);

/**
 * @param {[string, any, {message: string}]} data
 * @param {import('../../src/types.js').Job} job
 */
export async function handle(data, job) {
	const [originalId, originalData, error] = data;

	await emitEvent(eventRedis, {
		type: 'fail',
		queue: 'fail',
		id: originalId,
		failJobId: job.id,
		error: error?.message ?? String(error),
	});

	// Re-dispatch periodic jobs so they continue indefinitely.
	if (originalId.startsWith('fuzz-periodic-')) {
		const delay = 1000 + Math.random() * 4000;
		await periodicQueue.dispatch(originalData, {
			id: originalId,
			runAt: Date.now() + delay,
			updateRunAt: true,
			updateData: true,
		});
	}
}
