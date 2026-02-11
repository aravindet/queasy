import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { createClient } from 'redis';

describe('Redis connection', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;

    before(async () => {
        // Connect to Redis running in Docker
        redis = createClient({
            socket: {
                host: 'localhost',
                port: 6379,
                reconnectStrategy: (retries) => {
                    if (retries > 10) {
                        return new Error('Max retries reached');
                    }
                    return Math.min(retries * 50, 2000); // Exponential backoff
                },
            },
        });

        await redis.connect();
        // Wait for connection to be ready
        await redis.ping();
    });

    after(async () => {
        redis.destroy();
    });

    it('should connect to Redis and execute basic commands', async () => {
        // Test SET command
        const setResult = await redis.set('test:key', 'test-value');
        assert.equal(setResult, 'OK');

        // Test GET command
        const getValue = await redis.get('test:key');
        assert.equal(getValue, 'test-value');

        // Test DEL command
        const delResult = await redis.del('test:key');
        assert.equal(delResult, 1);

        // Verify key is deleted
        const deletedValue = await redis.get('test:key');
        assert.equal(deletedValue, null);
    });

    it('should execute PING command', async () => {
        const result = await redis.ping();
        assert.equal(result, 'PONG');
    });
});
