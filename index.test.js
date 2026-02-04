import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import Redis from 'ioredis';

describe('Redis connection', () => {
	/** @type {Redis} */
	let redis;

	before(async () => {
		// Connect to Redis running in Docker
		redis = new Redis({
			host: 'localhost',
			port: 6379,
			maxRetriesPerRequest: 3,
			retryStrategy: (times) => {
				if (times > 10) {
					return null; // Stop retrying
				}
				return Math.min(times * 50, 2000); // Exponential backoff
			},
		});

		// Wait for connection to be ready
		await redis.ping();
	});

	after(async () => {
		if (redis) {
			await redis.quit();
		}
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
