import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';

const QUEUE_NAME = 'client-test';

describe('Client heartbeat', () => {
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
        await client.close();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);
        await redis.quit();
    });

    it('should update expiry key when bump is called', async () => {
        const q = client.queue(QUEUE_NAME);
        await q.dispatch({ task: 'test' });

        const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
        await q.listen(handlerPath);

        // Dequeue to start heartbeats
        await (await q.dequeue(1)).promise;

        // Manually call bump to exercise the bump method
        await client.bump(`{${QUEUE_NAME}}`);

        // Expiry key should have our client's entry
        const expiryScore = await redis.zScore(`{${QUEUE_NAME}}:expiry`, client.clientId);
        assert.ok(expiryScore !== null, 'Expiry entry should exist after bump');
        assert.ok(expiryScore > Date.now(), 'Expiry should be in the future');
    });
});
