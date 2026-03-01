/**
 * cascade-a handler — dispatches one or two cascade-b jobs on normal completion.
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
const cascadeBQueue = client.queue('{fuzz}:cascade-b', true);

const crashChannel = new BroadcastChannel('fuzz-crash');

// biome-ignore lint/suspicious/noExplicitAny: Job data is arbitrary
export async function handle(_data: any, job: Job): Promise<void> {
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
        while (Date.now() < end) {
            /* busy wait */
        }
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
    const dispatchPromises: Promise<string>[] = [];
    for (let i = 0; i < count; i++) {
        dispatchPromises.push(
            cascadeBQueue.dispatch(
                { from: job.id, index: i },
                {
                    runAt: Date.now() + runAtOffset,
                }
            )
        );
    }
    const ids = await Promise.all(dispatchPromises);

    await emitEvent(eventRedis, {
        type: 'finish',
        queue: '{fuzz}:cascade-a',
        id: job.id,
        finishedAt: process.uptime().toFixed(2),
        dispatched: ids.join(','),
    });
}
