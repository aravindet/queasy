import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';

const QUEUE_NAME = 'pool-test';

describe('Pool stall and timeout', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    /** @type {import('../src/client.js').Client} */
    let client;

    beforeEach(async () => {
        redis = createClient();
        await redis.connect();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);

        client = new Client(redis, 1);
        if (client.manager) client.manager.addQueue = mock.fn();
    });

    afterEach(async () => {
        client.close();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);
        await redis.quit();
    });

    it('should handle worker timeout as a stall', async () => {
        const q = client.queue(QUEUE_NAME);
        const jobId = await q.dispatch({ delay: 500 });

        const handlerPath = new URL('./fixtures/slow-handler.js', import.meta.url).pathname;
        // Very short timeout (50ms) so pool.handleTimeout fires before the 500ms handler completes
        await q.listen(handlerPath, { timeout: 50, maxRetries: 5 });

        await (await q.dequeue(1)).promise;

        // Job should be back in waiting set (retried after timeout stall)
        const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
        assert.ok(score !== null, 'Job should be retried back to waiting set');
    });

    it('should reject active jobs when pool is closed', async () => {
        const q = client.queue(QUEUE_NAME);
        const jobId = await q.dispatch({ delay: 2000 });

        const handlerPath = new URL('./fixtures/slow-handler.js', import.meta.url).pathname;
        await q.listen(handlerPath, { maxRetries: 5 });

        // Dequeue but don't await the promise — job is still processing
        const { promise } = await q.dequeue(1);

        // Close the client immediately while job is active
        client.close();

        // The promise should resolve (the rejection gets handled by the catch block in dequeue)
        await promise;

        // Job should be back in waiting set (retry after stall rejection)
        const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
        assert.ok(score !== null, 'Job should be retried after pool close');
    });
});
