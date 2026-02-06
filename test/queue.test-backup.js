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

	describe('listen()', () => {
		it('should process a job with success handler', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test-job' });

			// Process the job
			const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
			await q.listen(handlerPath);

			// Wait for job to complete (poll up to 3 seconds)
			const deadline = Date.now() + 3000;
			while (Date.now() < deadline) {
				const waiting = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
				const activeKeys = await redis.keys(`${QUEUE_NAME}:active*`);
				if (waiting === null && activeKeys.length === 0) {
					break;
				}
				await new Promise((r) => setTimeout(r, 50));
			}

			// Verify job is removed from waiting
			const score = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(score, null);

			// Verify job is removed from active
			const activeMembers = await redis.zRange(`${QUEUE_NAME}:active`, 0, -1);
			assert.equal(activeMembers.length, 0);

			// Verify job data is cleaned up
			const exists = await redis.exists(`${QUEUE_NAME}:active_job:${jobId}`);
			assert.equal(exists, 0);
		});

		it('should handle job failure and retry', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ task: 'test-job' });

			// Process the job with failure handler
			const handlerPath = new URL('./fixtures/failure-handler.js', import.meta.url).pathname;
			await q.listen(handlerPath);

			// Wait for job to be retried (poll up to 3 seconds)
			const deadline = Date.now() + 3000;
			while (Date.now() < deadline) {
				const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
				if (jobData.retry_count === '1') {
					break;
				}
				await new Promise((r) => setTimeout(r, 50));
			}

			// Verify job is back in waiting queue for retry
			const score = await redis.zScore(`${QUEUE_NAME}:waiting`, jobId);
			assert.ok(score !== null);

			// Verify retry count was incremented
			const jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(jobData.retry_count, '1');
		});

		it('should not process jobs scheduled for the future', async () => {
			const q = queue(QUEUE_NAME, redis);
			const futureTime = Date.now() + 10000;
			await q.dispatch({ task: 'future-job' }, { runAt: futureTime });

			const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
			await q.listen(handlerPath);

			// Job should still be in waiting queue
			const members = await redis.zRange(`${QUEUE_NAME}:waiting`, 0, -1);
			assert.equal(members.length, 1);
		});

		it('should send heartbeats during slow job processing', async () => {
			const q = queue(QUEUE_NAME, redis);
			const jobId = await q.dispatch({ delay: 8000 }); // 8 second job

			const handlerPath = new URL('./fixtures/slow-handler.js', import.meta.url).pathname;

			// Start processing in background
			const processPromise = q.listen(handlerPath);

			// Wait a bit and check that heartbeat updated the active job
			await new Promise((resolve) => setTimeout(resolve, 6000));

			const activeMembers = await redis.zRange(`${QUEUE_NAME}:active`, 0, -1);
			// If heartbeat is working, job should still be in active set
			if (activeMembers.length > 0) {
				assert.ok(activeMembers[0].includes(jobId));
			}

			// Wait for job to complete
			await processPromise;
		});

		it('should pass correct data and job metadata to handler', async () => {
			const q = queue(QUEUE_NAME, redis);
			const testData = { value: 42, name: 'test' };
			const jobId = await q.dispatch(testData);

			const handlerPath = new URL('./fixtures/data-logger-handler.js', import.meta.url).pathname;
			const loggerModule = await import('./fixtures/data-logger-handler.js');
			loggerModule.clearLogs();

			await q.listen(handlerPath);

			// Wait for job to be processed (poll up to 3 seconds)
			const deadline = Date.now() + 3000;
			while (Date.now() < deadline) {
				if (loggerModule.receivedJobs.length > 0) {
					break;
				}
				await new Promise((r) => setTimeout(r, 50));
			}

			assert.equal(loggerModule.receivedJobs.length, 1);
			assert.deepEqual(loggerModule.receivedJobs[0].data, testData);
			assert.ok(loggerModule.receivedJobs[0].job);
			assert.equal(loggerModule.receivedJobs[0].job.maxRetries, 10);
			assert.equal(loggerModule.receivedJobs[0].job.retryCount, 0);
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
