import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';

const QUEUE_NAME = 'mgr-test';

describe('Manager scheduling', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    /** @type {import('../src/client.js').Client} */
    let client;

    beforeEach(async () => {
        redis = createClient();
        await redis.connect();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);

        // Do NOT mock manager.addQueue — let the manager drive dequeuing
        client = new Client(redis, 1);
    });

    afterEach(async () => {
        client.close();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);
        await redis.quit();
    });

    it('should dequeue and process jobs via manager without explicit dequeue', async () => {
        const q = client.queue(QUEUE_NAME);
        const jobId = await q.dispatch({ task: 'managed' });

        const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
        await q.listen(handlerPath);

        // Wait for manager to dequeue and process the job fully
        await poll(async () => {
            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            const activeExists = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId}`);
            return score === null && activeExists === 0;
        });
    });

    it('should process jobs from multiple queues', async () => {
        const staleKeys = await redis.keys('{mgr-q2}*');
        if (staleKeys.length > 0) await redis.del(staleKeys);

        const q1 = client.queue(QUEUE_NAME);
        const q2 = client.queue('mgr-q2');
        const jobId1 = await q1.dispatch({ q: 1 });
        const jobId2 = await q2.dispatch({ q: 2 });

        const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
        await q1.listen(handlerPath);
        await q2.listen(handlerPath);

        // Wait for both jobs to be fully processed
        await poll(async () => {
            const s1 = await redis.zScore(`{${QUEUE_NAME}}`, jobId1);
            const a1 = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId1}`);
            const s2 = await redis.zScore('{mgr-q2}', jobId2);
            const a2 = await redis.exists(`{mgr-q2}:active_job:${jobId2}`);
            return s1 === null && a1 === 0 && s2 === null && a2 === 0;
        });

        // Cleanup
        const keys = await redis.keys('{mgr-q2}*');
        if (keys.length > 0) await redis.del(keys);
    });
});

/**
 * Poll a condition until it returns true, with a timeout.
 * @param {() => Promise<boolean>} fn
 * @param {number} [timeout=5000]
 * @param {number} [interval=20]
 */
async function poll(fn, timeout = 5000, interval = 20) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await fn()) return;
        await new Promise((r) => setTimeout(r, interval));
    }
    assert.fail('Poll timed out');
}
