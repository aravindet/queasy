/**
 * cascade-b handler — terminal handler, dispatches no further jobs.
 * Subject to all chaos behaviors.
 */

import { BroadcastChannel } from 'node:worker_threads';
import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { PermanentError } from '../../src/index.ts';
import type { Job } from '../../src/types.ts';
import { pickChaos } from '../shared/chaos.ts';
import { emitEvent } from '../shared/stream.ts';

const eventRedis = createClient() as RedisClientType;
await eventRedis.connect();

const crashChannel = new BroadcastChannel('fuzz-crash');

// biome-ignore lint/suspicious/noExplicitAny: Job data is arbitrary
export async function handle(_data: any, job: Job): Promise<void> {
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
        while (Date.now() < end) {
            /* busy wait */
        }
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
