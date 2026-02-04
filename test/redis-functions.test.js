import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { after, afterEach, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import Redis from 'ioredis';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('Redis Lua functions', () => {
	/** @type {Redis} */
	let redis;
	const QUEUE_NAME = 'test-queue';

	before(async () => {
		redis = new Redis({
			host: 'localhost',
			port: 6379,
		});

		await redis.ping();

		// Load Lua script
		const luaScript = readFileSync(join(__dirname, '../src/queasy.lua'), 'utf8');

		// Load the library into Redis
		try {
			await redis.call('FUNCTION', 'LOAD', 'REPLACE', luaScript);
		} catch (error) {
			console.error('Failed to load Lua functions:', error);
			throw error;
		}
	});

	after(async () => {
		// Clean up all test keys
		const keys = await redis.keys(`${QUEUE_NAME}*`);
		if (keys.length > 0) {
			await redis.del(...keys);
		}
		await redis.quit();
	});

	afterEach(async () => {
		// Clean up between tests
		const keys = await redis.keys(`${QUEUE_NAME}*`);
		if (keys.length > 0) {
			await redis.del(...keys);
		}
	});

	describe('dispatch', () => {
		it('should add a new job to waiting queue', async () => {
			const jobId = 'job1';
			const runAt = Date.now();
			const jobData = {
				id: jobId,
				data: JSON.stringify({ task: 'test' }),
				max_failures: '10',
				max_stalls: '3',
				failure_count: '0',
				stall_count: '0',
				update_data: 'true',
				update_run_at: 'true',
			};

			const result = await redis.fcall(
				'queasy_dispatch',
				3,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				jobId,
				runAt.toString(),
				JSON.stringify(jobData)
			);

			assert.equal(result, 'OK');

			// Verify job is in waiting queue
			const score = await redis.zscore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(Number(score), runAt);

			// Verify job data is stored
			const storedData = await redis.hgetall(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(storedData.id, jobId);
		});

		it('should block job if active job with same ID exists', async () => {
			const jobId = 'job2';
			const runAt = Date.now() + 1000;

			// Create an active job first
			await redis.hset(`${QUEUE_NAME}:active_job:${jobId}`, 'id', jobId);

			const jobData = {
				id: jobId,
				data: JSON.stringify({ task: 'test' }),
				max_failures: '10',
				update_data: 'true',
				update_run_at: 'true',
			};

			await redis.fcall(
				'queasy_dispatch',
				3,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				jobId,
				runAt.toString(),
				JSON.stringify(jobData)
			);

			// Verify job has negative score (blocked)
			const score = await redis.zscore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(Number(score), -runAt);

			// Cleanup
			await redis.del(`${QUEUE_NAME}:active_job:${jobId}`);
		});

		it('should respect update_run_at=false flag', async () => {
			const jobId = 'job3';
			const runAt1 = Date.now();
			const runAt2 = Date.now() + 5000;

			// Add job first time
			const jobData1 = {
				id: jobId,
				data: JSON.stringify({ task: 'test1' }),
				update_data: 'true',
				update_run_at: 'true',
			};

			await redis.fcall(
				'queasy_dispatch',
				3,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				jobId,
				runAt1.toString(),
				JSON.stringify(jobData1)
			);

			// Try to update with update_run_at=false
			const jobData2 = {
				id: jobId,
				data: JSON.stringify({ task: 'test2' }),
				update_data: 'true',
				update_run_at: 'false',
			};

			await redis.fcall(
				'queasy_dispatch',
				3,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				jobId,
				runAt2.toString(),
				JSON.stringify(jobData2)
			);

			// Score should still be runAt1
			const score = await redis.zscore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(Number(score), runAt1);
		});
	});

	describe('cancel', () => {
		it('should remove job from waiting queue', async () => {
			const jobId = 'job-cancel';
			const runAt = Date.now();

			// Add a job
			await redis.zadd(`${QUEUE_NAME}:waiting`, runAt, jobId);
			await redis.hset(`${QUEUE_NAME}:waiting_job:${jobId}`, 'id', jobId);

			// Cancel it
			const result = await redis.fcall(
				'queasy_cancel',
				2,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				jobId
			);

			assert.equal(result, 1);

			// Verify it's gone
			const score = await redis.zscore(`${QUEUE_NAME}:waiting`, jobId);
			assert.equal(score, null);

			const exists = await redis.exists(`${QUEUE_NAME}:waiting_job:${jobId}`);
			assert.equal(exists, 0);
		});

		it('should return 0 if job does not exist', async () => {
			const result = await redis.fcall(
				'queasy_cancel',
				2,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:nonexistent`,
				'nonexistent'
			);

			assert.equal(result, 0);
		});
	});

	describe('dequeue', () => {
		it('should dequeue jobs ready to run', async () => {
			const now = Date.now();
			const workerId = 'worker1';
			const jobId1 = 'job-deq-1';
			const jobId2 = 'job-deq-2';

			// Add jobs ready to run
			await redis.zadd(`${QUEUE_NAME}:waiting`, now - 1000, jobId1);
			await redis.hset(`${QUEUE_NAME}:waiting_job:${jobId1}`, 'id', jobId1);

			await redis.zadd(`${QUEUE_NAME}:waiting`, now - 500, jobId2);
			await redis.hset(`${QUEUE_NAME}:waiting_job:${jobId2}`, 'id', jobId2);

			// Dequeue (no heartbeat_timeout parameter - it's now a constant)
			const result = /** @type {string[]} */ (
				await redis.fcall(
					'queasy_dequeue',
					2,
					`${QUEUE_NAME}:waiting`,
					`${QUEUE_NAME}:active`,
					workerId,
					now.toString(),
					'10'
				)
			);

			assert.equal(result.length, 2);
			assert.equal(result[0], jobId1);
			assert.equal(result[1], jobId2);

			// Verify jobs are in active set
			const activeMembers = await redis.zrange(`${QUEUE_NAME}:active`, 0, -1);
			assert.equal(activeMembers.length, 2);
			assert.ok(activeMembers.includes(`${jobId1}:${workerId}`));
			assert.ok(activeMembers.includes(`${jobId2}:${workerId}`));

			// Verify job data moved to active
			const exists1 = await redis.exists(`${QUEUE_NAME}:active_job:${jobId1}`);
			assert.equal(exists1, 1);
		});

		it('should not dequeue jobs scheduled for future', async () => {
			const now = Date.now();
			const workerId = 'worker1';
			const jobId = 'job-future';

			// Add job scheduled for future
			await redis.zadd(`${QUEUE_NAME}:waiting`, now + 10000, jobId);
			await redis.hset(`${QUEUE_NAME}:waiting_job:${jobId}`, 'id', jobId);

			// Try to dequeue
			const result = /** @type {string[]} */ (
				await redis.fcall(
					'queasy_dequeue',
					2,
					`${QUEUE_NAME}:waiting`,
					`${QUEUE_NAME}:active`,
					workerId,
					now.toString(),
					'10'
				)
			);

			assert.equal(result.length, 0);
		});

		it('should not dequeue blocked jobs', async () => {
			const now = Date.now();
			const workerId = 'worker1';
			const jobId = 'job-blocked';

			// Add blocked job (negative score)
			await redis.zadd(`${QUEUE_NAME}:waiting`, -now, jobId);
			await redis.hset(`${QUEUE_NAME}:waiting_job:${jobId}`, 'id', jobId);

			// Try to dequeue
			const result = /** @type {string[]} */ (
				await redis.fcall(
					'queasy_dequeue',
					2,
					`${QUEUE_NAME}:waiting`,
					`${QUEUE_NAME}:active`,
					workerId,
					now.toString(),
					'10'
				)
			);

			assert.equal(result.length, 0);
		});
	});

	describe('bump', () => {
		it('should update heartbeat for active job', async () => {
			const now = Date.now();
			const workerId = 'worker1';
			const jobId = 'job-hb';

			// Add job to active set
			await redis.zadd(`${QUEUE_NAME}:active`, now + 5000, `${jobId}:${workerId}`);

			// Send heartbeat (no heartbeat_timeout parameter - it's now a constant)
			const result = await redis.fcall(
				'queasy_bump',
				1,
				`${QUEUE_NAME}:active`,
				jobId,
				workerId,
				now.toString()
			);

			assert.equal(result, 0); // ZADD XX returns 0 when updating

			// Verify score was updated to now + 10000ms (constant)
			const score = await redis.zscore(`${QUEUE_NAME}:active`, `${jobId}:${workerId}`);
			assert.ok(Number(score) >= now + 10000);
		});

		it('should return 0 if lock was lost', async () => {
			const now = Date.now();
			const workerId = 'worker1';
			const jobId = 'job-lost';

			// Job not in active set
			const result = await redis.fcall(
				'queasy_bump',
				1,
				`${QUEUE_NAME}:active`,
				jobId,
				workerId,
				now.toString()
			);

			assert.equal(result, 0);
		});
	});

	describe('finish', () => {
		it('should remove active job', async () => {
			const workerId = 'worker1';
			const jobId = 'job-clear';

			// Setup active job
			await redis.zadd(`${QUEUE_NAME}:active`, Date.now(), `${jobId}:${workerId}`);
			await redis.hset(`${QUEUE_NAME}:active_job:${jobId}`, 'id', jobId);

			// Finish (clear active)
			const result = await redis.fcall(
				'queasy_finish',
				4,
				`${QUEUE_NAME}:active`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				jobId,
				workerId
			);

			assert.equal(result, 'OK');

			// Verify job is removed
			const activeExists = await redis.exists(`${QUEUE_NAME}:active_job:${jobId}`);
			assert.equal(activeExists, 0);

			const inActive = await redis.zscore(`${QUEUE_NAME}:active`, `${jobId}:${workerId}`);
			assert.equal(inActive, null);
		});

		it('should unblock waiting job when active completes', async () => {
			const workerId = 'worker1';
			const jobId = 'job-unblock';
			const runAt = Date.now() + 5000;

			// Setup active job
			await redis.zadd(`${QUEUE_NAME}:active`, Date.now(), `${jobId}:${workerId}`);
			await redis.hset(`${QUEUE_NAME}:active_job:${jobId}`, 'id', jobId);

			// Setup blocked waiting job
			await redis.zadd(`${QUEUE_NAME}:waiting`, -runAt, jobId);
			await redis.hset(
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				'id',
				jobId,
				'update_run_at',
				'true'
			);

			// Finish (clear active)
			await redis.fcall(
				'queasy_finish',
				4,
				`${QUEUE_NAME}:active`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				jobId,
				workerId
			);

			// Verify waiting job is unblocked (positive score)
			const score = await redis.zscore(`${QUEUE_NAME}:waiting`, jobId);
			assert.ok(Number(score) > 0);
			assert.equal(Number(score), runAt);
		});
	});

	describe('retry', () => {
		it('should increment failure count and retry', async () => {
			const jobId = 'job-onfail';
			const workerId = 'worker1';
			const nextRunAt = Date.now() + 5000;

			// Setup active job
			await redis.zadd(`${QUEUE_NAME}:active`, Date.now(), `${jobId}:${workerId}`);
			await redis.hset(
				`${QUEUE_NAME}:active_job:${jobId}`,
				'id',
				jobId,
				'failure_count',
				'0',
				'max_failures',
				'3'
			);

			// Call retry
			const result = await redis.fcall(
				'queasy_retry',
				4,
				`${QUEUE_NAME}:active`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				jobId,
				workerId,
				nextRunAt.toString(),
				QUEUE_NAME
			);

			assert.equal(result, 'OK');

			// Verify failure count incremented
			const failureCount = await redis.hget(`${QUEUE_NAME}:waiting_job:${jobId}`, 'failure_count');
			assert.equal(failureCount, '1');
		});

		it('should trigger permanent failure when max_failures reached', async () => {
			const jobId = 'job-maxfail';
			const workerId = 'worker1';
			const nextRunAt = Date.now() + 5000;

			// Setup failure queue (simulate handlers registered)
			await redis.zadd(`${QUEUE_NAME}-fail:waiting`, 0, 'dummy');

			// Setup active job at max failures
			await redis.zadd(`${QUEUE_NAME}:active`, Date.now(), `${jobId}:${workerId}`);
			await redis.hset(
				`${QUEUE_NAME}:active_job:${jobId}`,
				'id',
				jobId,
				'failure_count',
				'2',
				'max_failures',
				'3'
			);

			// Call retry
			const result = await redis.fcall(
				'queasy_retry',
				4,
				`${QUEUE_NAME}:active`,
				`${QUEUE_NAME}:active_job:${jobId}`,
				`${QUEUE_NAME}:waiting`,
				`${QUEUE_NAME}:waiting_job:${jobId}`,
				jobId,
				workerId,
				nextRunAt.toString(),
				QUEUE_NAME
			);

			assert.equal(result, 'PERMANENT_FAILURE');

			// Verify job is not in active or waiting
			const activeExists = await redis.exists(`${QUEUE_NAME}:active_job:${jobId}`);
			assert.equal(activeExists, 0);
		});
	});

	describe('sweep', () => {
		it('should process stalled jobs', async () => {
			const now = Date.now();
			const workerId = 'worker1';
			const jobId = 'job-stall';
			const nextRunAt = now + 5000;

			// Setup stalled job (heartbeat expired)
			await redis.zadd(`${QUEUE_NAME}:active`, now - 10000, `${jobId}:${workerId}`);
			await redis.hset(
				`${QUEUE_NAME}:active_job:${jobId}`,
				'id',
				jobId,
				'stall_count',
				'0',
				'max_stalls',
				'3'
			);

			// Sweep (clear stalls)
			const result = /** @type {string[]} */ (
				await redis.fcall(
					'queasy_sweep',
					1,
					`${QUEUE_NAME}:active`,
					now.toString(),
					QUEUE_NAME,
					nextRunAt.toString()
				)
			);

			assert.equal(result.length, 1);
			assert.equal(result[0], jobId);

			// Verify stall count incremented and job moved to waiting
			const stallCount = await redis.hget(`${QUEUE_NAME}:waiting_job:${jobId}`, 'stall_count');
			assert.equal(stallCount, '1');
		});
	});
});
