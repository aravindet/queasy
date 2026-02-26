import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { Client } from '../src/index.ts';

const QUEUE_NAME = 'client-test';

describe('Client heartbeat', () => {
    let redis: RedisClientType;
    let client: Client;

    beforeEach(async () => {
        redis = createClient();
        await redis.connect();
        client = await new Promise<Client>((res) => new Client({}, 1, res));
        client.scheduleBump = mock.fn();
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

        const handlerPath = new URL('./fixtures/success-handler.ts', import.meta.url).pathname;
        await q.listen(handlerPath);

        // Dequeue to start heartbeats
        await (await q.dequeue(1)).promise;

        // Manually call bump to exercise the bump method
        await client.bump(`{${QUEUE_NAME}}`);

        // Expiry key should have our client's entry
        const expiryScore = await redis.zScore(`{${QUEUE_NAME}}:expiry`, client.clientId);
        assert.ok(expiryScore !== null, 'Expiry entry should exist after dequeue');
        assert.ok(expiryScore > Date.now(), 'Expiry should be in the future');
    });
});
