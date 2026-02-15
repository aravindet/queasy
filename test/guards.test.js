import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';

describe('Client disconnect guard', () => {
	/** @type {import('redis').RedisClientType} */
	let redis;
	/** @type {import('../src/client.js').Client} */
	let client;

	beforeEach(async () => {
		redis = createClient();
		await redis.connect();
		client = new Client(redis, 0);
	});

	afterEach(async () => {
		client.close();
		await redis.quit();
	});

	it('should throw from queue() when disconnected', () => {
		client.disconnected = true;
		assert.throws(() => client.queue('test'), /disconnected/);
	});
});

describe('Queue disconnect guards', () => {
	/** @type {import('redis').RedisClientType} */
	let redis;
	/** @type {import('../src/client.js').Client} */
	let client;

	beforeEach(async () => {
		redis = createClient();
		await redis.connect();
		client = new Client(redis, 1);
		if (client.manager) client.manager.addQueue = mock.fn();
		// Wait for installLuaFunctions to settle
		await new Promise((r) => setTimeout(r, 50));
	});

	afterEach(async () => {
		client.close();
		await redis.quit();
	});

	it('should throw from dispatch() when disconnected', async () => {
		const q = client.queue('guard-test');
		client.disconnected = true;
		await assert.rejects(() => q.dispatch({ data: 1 }), /disconnected/);
	});

	it('should throw from cancel() when disconnected', async () => {
		const q = client.queue('guard-test');
		client.disconnected = true;
		await assert.rejects(() => q.cancel('some-id'), /disconnected/);
	});

	it('should throw from listen() when disconnected', async () => {
		const q = client.queue('guard-test');
		client.disconnected = true;
		await assert.rejects(() => q.listen('/some/handler.js'), /disconnected/);
	});

	it('should throw from listen() on non-processing client', async () => {
		const nonProcessingClient = new Client(redis, 0);
		// Wait for installLuaFunctions to settle
		await new Promise((r) => setTimeout(r, 50));
		const q = nonProcessingClient.queue('guard-test');
		await assert.rejects(() => q.listen('/some/handler.js'), /non-processing/);
		nonProcessingClient.close();
	});
});

describe('Client bump lost lock', () => {
	/** @type {import('redis').RedisClientType} */
	let redis;
	/** @type {import('../src/client.js').Client} */
	let client;

	const QUEUE_NAME = 'bump-lock-test';

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

	it('should close and emit disconnected when bump returns 0', async () => {
		const q = client.queue(QUEUE_NAME);
		await q.dispatch({ task: 'test' });
		const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
		await q.listen(handlerPath);

		// Dequeue to register the client in the expiry set and start heartbeats
		await (await q.dequeue(1)).promise;

		// Remove the client from the expiry set to simulate a lost lock
		await redis.zRem(`{${QUEUE_NAME}}:expiry`, client.clientId);

		const disconnected = new Promise((resolve) => client.on('disconnected', resolve));

		// Bump should detect it's been evicted
		await client.bump(`{${QUEUE_NAME}}`);

		const reason = await disconnected;
		assert.ok(String(reason).includes('Lost locks'));
		assert.equal(client.disconnected, true);
	});

	it('should skip bump when already disconnected', async () => {
		const q = client.queue(QUEUE_NAME);
		// Set up the queue entry so scheduleBump doesn't error
		await q.dispatch({ task: 'test' });
		const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
		await q.listen(handlerPath);
		await (await q.dequeue(1)).promise;

		client.disconnected = true;

		// Should return early without calling fCall
		await client.bump(`{${QUEUE_NAME}}`);
		// If we get here without error, the early return worked
		assert.equal(client.disconnected, true);
	});
});
