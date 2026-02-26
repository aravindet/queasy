/**
 * Fuzz test child process entry point.
 *
 * Each child process creates one queasy Client (with worker threads) and
 * calls listen() on all three queues. Handlers run inside queasy's internal
 * worker threads and communicate crash signals via BroadcastChannel.
 */

import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { BroadcastChannel } from 'node:worker_threads';
import { Client } from '../src/index.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Configuration ─────────────────────────────────────────────────────────────

const WORKER_THREADS = 2; // worker threads per child process

const BASE_OPTIONS = {
    maxRetries: 3,
    maxStalls: 2,
    minBackoff: 200,
    maxBackoff: 2000,
    timeout: 3000,
    size: 10,
};

const FAIL_RETRY_OPTIONS = {
    maxRetries: 5,
    minBackoff: 200,
};

// ── Handler paths ──────────────────────────────────────────────────────────────

const failHandlerPath = join(__dirname, 'handlers', 'fail-handler.js');
const periodicPath = join(__dirname, 'handlers', 'periodic.js');
const cascadeAPath = join(__dirname, 'handlers', 'cascade-a.js');
const cascadeBPath = join(__dirname, 'handlers', 'cascade-b.js');

// ── Crash signal ───────────────────────────────────────────────────────────────

// Handlers running in worker threads post to this channel to trigger a crash.
const crashChannel = new BroadcastChannel('fuzz-crash');
crashChannel.onmessage = () => {
    process.exit(1);
};

// ── Redis + queasy client ──────────────────────────────────────────────────────

const client = await new Promise((resolve) => new Client({}, WORKER_THREADS, resolve));

client.on('disconnected', (reason) => {
    console.error(`[process ${process.pid}] Client disconnected: ${reason}`);
    process.exit(1);
});

// Forward client lifecycle events to the orchestrator via IPC.
// The orchestrator uses these to authoritatively track active jobs.
client.on('dequeue', (queue, job) => {
    process.send({ type: 'dequeue', queue, jobId: job.id, runAt: job.runAt });
});
client.on('finish', (queue, jobId) => {
    process.send({ type: 'finish', queue, jobId });
});
client.on('retry', (queue, jobId) => {
    process.send({ type: 'retry', queue, jobId });
});
client.on('fail', (queue, jobId) => {
    process.send({ type: 'fail', queue, jobId });
});

// ── Queue setup ────────────────────────────────────────────────────────────────

const periodicQueue = client.queue('{fuzz}:periodic', true);
const cascadeAQueue = client.queue('{fuzz}:cascade-a', true);
const cascadeBQueue = client.queue('{fuzz}:cascade-b', true);

await Promise.all([
    periodicQueue.listen(periodicPath, {
        ...BASE_OPTIONS,
        priority: 300,
        failHandler: failHandlerPath,
        failRetryOptions: FAIL_RETRY_OPTIONS,
    }),
    cascadeAQueue.listen(cascadeAPath, {
        ...BASE_OPTIONS,
        priority: 200,
        failHandler: failHandlerPath,
        failRetryOptions: FAIL_RETRY_OPTIONS,
    }),
    cascadeBQueue.listen(cascadeBPath, {
        ...BASE_OPTIONS,
        priority: 100,
        failHandler: failHandlerPath,
        failRetryOptions: FAIL_RETRY_OPTIONS,
    }),
]);

console.log(`[process ${process.pid}] Ready`);
