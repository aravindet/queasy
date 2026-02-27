/**
 * periodic handler — re-queues itself with the same job ID so it fires indefinitely.
 * Also dispatches cascade-a jobs on normal completion.
 * Subject to all chaos behaviors.
 */

import { BroadcastChannel } from 'node:worker_threads';
import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { Client, PermanentError } from '../../src/index.ts';
import type { Job } from '../../src/types.ts';
import { pickChaos } from '../shared/chaos.ts';
import { emitEvent } from '../shared/stream.ts';

const eventRedis = createClient() as RedisClientType;
await eventRedis.connect();

// Dispatch-only queasy client (await ready to avoid Function not found race)
const client = await new Promise<Client>((resolve) => new Client({}, 0, resolve));
const periodicQueue = client.queue('{fuzz}:periodic', true);
const cascadeAQueue = client.queue('{fuzz}:cascade-a', true);

const crashChannel = new BroadcastChannel('fuzz-crash');

// biome-ignore lint/suspicious/noExplicitAny: Job data is arbitrary
export async function handle(data: any, job: Job): Promise<void> {
    const startedAt = Date.now();
    await emitEvent(eventRedis, {
        type: 'start',
        queue: '{fuzz}:periodic',
        id: job.id,
        pid: String(process.pid),
        runAt: String(job.runAt),
        startedAt: String(startedAt),
    });

    const chaos = pickChaos();
    await emitEvent(eventRedis, {
        type: 'chaos',
        queue: '{fuzz}:periodic',
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
        while (Date.now() < end) {
            /* busy wait */
        }
    }

    if (chaos === 'permanent') {
        throw new PermanentError('periodic: permanent chaos');
    }

    if (chaos === 'retriable') {
        throw new Error('periodic: retriable chaos');
    }

    // Normal completion: dispatch a cascade-a job and re-queue self
    const cascadeRunAt = Date.now() + Math.random() * 2000;
    const selfDelay = 1000 + Math.random() * 4000;

    const [cascadeId] = await Promise.all([
        cascadeAQueue.dispatch({ from: job.id }, { runAt: cascadeRunAt }),
        periodicQueue.dispatch(data, {
            id: job.id,
            runAt: Date.now() + selfDelay,
            updateRunAt: true,
        }),
    ]);

    await emitEvent(eventRedis, {
        type: 'finish',
        queue: '{fuzz}:periodic',
        id: job.id,
        finishedAt: String(Date.now()),
        dispatched: cascadeId,
    });
}
