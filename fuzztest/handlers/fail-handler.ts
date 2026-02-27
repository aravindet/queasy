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
import type { RedisClientType } from 'redis';
import { Client } from '../../src/index.ts';
import type { Job } from '../../src/types.ts';
import { emitEvent } from '../shared/stream.ts';

const eventRedis = createClient() as RedisClientType;
await eventRedis.connect();

// Dispatch-only queasy client (await ready to avoid Function not found race)
const client = await new Promise<Client>((resolve) => new Client({}, 0, resolve));

// Queue references (keys already include braces)
const periodicQueue = client.queue('{fuzz}:periodic', true);

export async function handle(data: [string, unknown, { message: string }], job: Job): Promise<void> {
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
