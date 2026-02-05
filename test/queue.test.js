import assert from 'node:assert/strict';
import { after, afterEach, before, describe, it } from 'node:test';
import { createClient } from 'redis';
import { PermanentError, StallError, queue } from '../src/index.js';

describe('Queue API', () => {
	/** @type {import('redis').RedisClientType} */
	let redis;
	const QUEUE_NAME = '{test-api-queue}';

	before(async () => {
		redis = createClient({
			socket: {
				host: 'localhost',
				port: 6379,
			},
		});

		await redis.connect();
		await redis.ping();
	});

	after(async () => {
		// Clean up all test keys
		const keys = await redis.keys(`${QUEUE_NAME}*`);
		if (keys.length > 0) {
			await redis.del(keys);
		}
		redis.destroy();
	});

	afterEach(async () => {
		// Clean up between tests
		const keys = await redis.keys(`${QUEUE_NAME}*`);
		if (keys.length > 0) {
			await redis.del(keys);
		}
	});

	describe('queue()', () => {
		it('should create a queue object', () => {
			const q = queue(QUEUE_NAME, redis);
			assert.ok(q);
			assert.equal(typeof q.dispatch, 'function');
			assert.equal(typeof q.cancel, 'function');
			assert.equal(typeof q.listen, 'function');
		});

		it('should load Lua functions when first dispatch is called', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test' });
			assert.ok(jobId);
			assert.equal(typeof jobId, 'string');
		});
	});

	describe('dispatch()', () => {
		it('should add a job to the queue', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test-job' });

			assert.ok(jobId);

			// Verify job is in waiting queue
			const score = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
			assert.ok(score !== null);

			// Verify job data is stored
			const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(jobData.data, JSON.stringify({ task: 'test-job' }));
			assert.equal(jobData.max_retries, '10');
			assert.equal(jobData.max_stalls, '3');
		});

		it('should generate an ID if not provided', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId1 = await q.dispatch({ task: 'test1' });
			const jobId2 = await q.dispatch({ task: 'test2' });

			assert.ok(jobId1);
			assert.ok(jobId2);
			assert.notEqual(jobId1, jobId2);
		});

		it('should use provided ID', async () => {
			const q = queue(QUEUE_NAME, redis);
			const customId = 'my-custom-id';
			const jobId = await q.dispatch({ task: 'test' }, { id: customId });

			assert.equal(jobId, customId);
		});

		it('should respect default job options', async () => {
			const q = queue(QUEUE_NAME, redis, {
				maxRetries: 5,
				maxStalls: 2,
				minBackoff: 1000,
				maxBackoff: 100000,
			});

			const jobId = await q.dispatch({ task: 'test' });
			const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);

			assert.equal(jobData.max_retries, '5');
			assert.equal(jobData.max_stalls, '2');
			assert.equal(jobData.min_backoff, '1000');
			assert.equal(jobData.max_backoff, '100000');
		});

		it('should override default options with provided options', async () => {
			const q = queue(QUEUE_NAME, redis, {
				maxRetries: 5,
			});

			const jobId = await q.dispatch({ task: 'test' }, { maxRetries: 15 });
			const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);

			assert.equal(jobData.max_retries, '15');
		});

		it('should set runAt to 0 by default', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test' });

			const score = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(Number(score), 0);
		});

		it('should respect custom runAt', async () => {
			const q = queue(QUEUE_NAME, redis);
			const runAt = Date.now() + 10000;
			const jobId = await q.dispatch({ task: 'test' }, { runAt });

			const score = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(Number(score), runAt);
		});

		it('should update existing job when updateData is true', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = 'update-test';

			await q.dispatch({ value: 1 }, { id: jobId });
			await q.dispatch({ value: 2 }, { id: jobId, updateData: true });

			const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(jobData.data, JSON.stringify({ value: 2 }));
		});

		it('should not update existing job when updateData is false', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = 'no-update-test';

			await q.dispatch({ value: 1 }, { id: jobId });
			await q.dispatch({ value: 2 }, { id: jobId, updateData: false });

			const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(jobData.data, JSON.stringify({ value: 1 }));
		});
	});

	describe('cancel()', () => {
		it('should cancel a waiting job', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test' });

			const cancelled = await q.cancel(jobId);
			assert.equal(cancelled, true);

			// Verify job is removed
			const score = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(score, null);

			const exists = await redis.exists(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(exists, 0);
		});

		it('should return false if job does not exist', async () => {
			const q = queue(QUEUE_NAME, redis);
			const cancelled = await q.cancel('nonexistent-job');
			assert.equal(cancelled, false);
		});

		it('should return false if job is already active', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test' });

			// Manually move job to active (simulate dequeue)
			await redis.zRem(`${QUEUE_NAME}:waiting`, jobId);
			await redis.rename(`${QUEUE_NAME}:waiting_job:${jobId}`, `${QUEUE_NAME}:active_job:${jobId}`);

			const cancelled = await q.cancel(jobId);
			assert.equal(cancelled, false);
		});
	});

	describe('Error classes', () => {
		it('should export PermanentError', () => {
			const error = new PermanentError('test error');
			assert.ok(error instanceof Error);
			assert.equal(error.name, 'PermanentError');
			assert.equal(error.message, 'test error');
		});

		it('should export StallError', () => {
			const error = new StallError('test error');
			assert.ok(error instanceof Error);
			assert.equal(error.name, 'StallError');
			assert.equal(error.message, 'test error');
		});
	});
});
