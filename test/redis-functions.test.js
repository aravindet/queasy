import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { after, afterEach, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { createClient } from 'redis';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('Redis Lua functions', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    const QUEUE_NAME = '{test-queue}';

    before(async () => {
        redis = createClient({
            socket: {
                host: 'localhost',
                port: 6379,
            },
        });

        await redis.connect();
        await redis.ping();

        // Load Lua script
        const luaScript = readFileSync(join(__dirname, '../src/queasy.lua'), 'utf8');

        // Load the library into Redis
        try {
            await redis.sendCommand(['FUNCTION', 'LOAD', 'REPLACE', luaScript]);
        } catch (error) {
            console.error('Failed to load Lua functions:', error);
            throw error;
        }
    });

    after(async () => {
        // Clean up all test keys
        const keys = await redis.keys(`${QUEUE_NAME}*`);
        if (keys.length > 0) {
            await redis.del(keys);
        }
        await redis.disconnect();
    });

    afterEach(async () => {
        // Clean up between tests
        const keys = await redis.keys(`${QUEUE_NAME}*`);
        if (keys.length > 0) {
            await redis.del(keys);
        }
    });

    describe('dispatch', () => {
        it('should add a new job to waiting queue', async () => {
            const jobId = 'job1';
            const runAt = Date.now();

            const result = await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt.toString(),
                    JSON.stringify({ task: 'test' }),
                    'true', // update_data
                    'true', // update_run_at
                    'false', // reset_counts
                ],
            });

            assert.equal(result, 'OK');

            // Verify job is in waiting queue
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.equal(Number(score), runAt);

            // Verify job data is stored
            const storedData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
            assert.equal(storedData.id, jobId);
            assert.equal(storedData.data, JSON.stringify({ task: 'test' }));
            assert.equal(storedData.retry_count, '0');
            assert.equal(storedData.stall_count, '0');
        });

        it('should block job if active job with same ID exists', async () => {
            const jobId = 'job2';
            const runAt = Date.now() + 1000;

            // Create an active job first
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, 'id', jobId);

            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt.toString(),
                    JSON.stringify({ task: 'test' }),
                    'true',
                    'true',
                    'false',
                ],
            });

            // Verify job has negative score (blocked)
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.equal(Number(score), -runAt);

            // Cleanup
            await redis.del(`${QUEUE_NAME}:active_job:${jobId}`);
        });

        it('should respect update_run_at=false flag', async () => {
            const jobId = 'job3';
            const runAt1 = Date.now();
            const runAt2 = Date.now() + 5000;

            // Add job first time
            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt1.toString(),
                    JSON.stringify({ task: 'test1' }),
                    'true',
                    'true',
                    'false',
                ],
            });

            // Try to update with update_run_at=false
            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt2.toString(),
                    JSON.stringify({ task: 'test2' }),
                    'true',
                    'false',
                    'false',
                ],
            });

            // Score should still be runAt1
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.equal(Number(score), runAt1);
        });

        it('should respect update_data=false flag', async () => {
            const jobId = 'job4';
            const runAt = Date.now();

            // Add job first time
            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt.toString(),
                    JSON.stringify({ task: 'original' }),
                    'true',
                    'true',
                    'false',
                ],
            });

            // Try to update with update_data=false
            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt.toString(),
                    JSON.stringify({ task: 'updated' }),
                    'false',
                    'true',
                    'false',
                ],
            });

            // Data should still be original
            const data = await redis.hGet(`${QUEUE_NAME}:waiting_job:${jobId}`, 'data');
            assert.equal(data, JSON.stringify({ task: 'original' }));
        });

        it('should reset counts when reset_counts=true', async () => {
            const jobId = 'job5';
            const runAt = Date.now();

            // Add job
            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt.toString(),
                    JSON.stringify({ task: 'test' }),
                    'true',
                    'true',
                    'false',
                ],
            });

            // Manually increment counts
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId}`, {
                retry_count: '5',
                stall_count: '2',
            });

            // Verify counts are set
            let jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
            assert.equal(jobData.retry_count, '5');
            assert.equal(jobData.stall_count, '2');

            // Update with reset_counts=true
            await redis.fCall('queasy_dispatch', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    runAt.toString(),
                    JSON.stringify({ task: 'test' }),
                    'true',
                    'true',
                    'true',
                ],
            });

            // Verify counts are reset
            jobData = await redis.hGetAll(`${QUEUE_NAME}:waiting_job:${jobId}`);
            assert.equal(jobData.retry_count, '0');
            assert.equal(jobData.stall_count, '0');
        });
    });

    describe('cancel', () => {
        it('should remove job from waiting queue', async () => {
            const jobId = 'job-cancel';
            const runAt = Date.now();

            // Add a job
            await redis.zAdd(QUEUE_NAME, { score: runAt, value: jobId });
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId}`, 'id', jobId);

            // Cancel it
            const result = await redis.fCall('queasy_cancel', {
                keys: [QUEUE_NAME],
                arguments: [jobId],
            });

            assert.equal(result, 1);

            // Verify it's gone
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.equal(score, null);

            const exists = await redis.exists(`${QUEUE_NAME}:waiting_job:${jobId}`);
            assert.equal(exists, 0);
        });

        it('should return 0 if job does not exist', async () => {
            const result = await redis.fCall('queasy_cancel', {
                keys: [QUEUE_NAME],
                arguments: ['nonexistent'],
            });

            assert.equal(result, 0);
        });
    });

    describe('dequeue', () => {
        it('should dequeue jobs ready to run', async () => {
            const now = Date.now();
            const clientId = 'client1';
            const jobId1 = 'job-deq-1';
            const jobId2 = 'job-deq-2';

            // Add jobs ready to run
            await redis.zAdd(QUEUE_NAME, { score: now - 1000, value: jobId1 });
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId1}`, 'id', jobId1);

            await redis.zAdd(QUEUE_NAME, { score: now - 500, value: jobId2 });
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId2}`, 'id', jobId2);

            // Dequeue
            const result = await redis.fCall('queasy_dequeue', {
                keys: [QUEUE_NAME],
                arguments: [clientId, now.toString(), '10'],
            });

            assert.equal(result.length, 2);
            assert.deepEqual(result[0], ['id', jobId1]);
            assert.deepEqual(result[1], ['id', jobId2]);

            // Verify jobs are in checkouts set
            const checkouts = await redis.sMembers(`${QUEUE_NAME}:checkouts:${clientId}`);
            assert.equal(checkouts.length, 2);
            assert.ok(checkouts.includes(jobId1));
            assert.ok(checkouts.includes(jobId2));

            // Verify client is in expiry set
            const expiry = await redis.zScore(`${QUEUE_NAME}:expiry`, clientId);
            assert.ok(expiry !== null);
            assert.equal(Number(expiry), now + 10000);

            // Verify job data moved to active
            const exists1 = await redis.exists(`${QUEUE_NAME}:active_job:${jobId1}`);
            assert.equal(exists1, 1);
        });

        it('should not dequeue jobs scheduled for future', async () => {
            const now = Date.now();
            const clientId = 'client1';
            const jobId = 'job-future';

            // Add job scheduled for future
            await redis.zAdd(QUEUE_NAME, { score: now + 10000, value: jobId });
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId}`, 'id', jobId);

            // Try to dequeue
            const result = await redis.fCall('queasy_dequeue', {
                keys: [QUEUE_NAME],
                arguments: [clientId, now.toString(), '10'],
            });

            assert.equal(result.length, 0);
        });

        it('should not dequeue blocked jobs', async () => {
            const now = Date.now();
            const clientId = 'client1';
            const jobId = 'job-blocked';

            // Add blocked job (negative score)
            await redis.zAdd(QUEUE_NAME, { score: -now, value: jobId });
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId}`, 'id', jobId);

            // Try to dequeue
            const result = await redis.fCall('queasy_dequeue', {
                keys: [QUEUE_NAME],
                arguments: [clientId, now.toString(), '10'],
            });

            assert.equal(result.length, 0);
        });
    });

    describe('bump', () => {
        it('should update heartbeat expiry for client', async () => {
            const now = Date.now();
            const clientId = 'client1';

            // Setup: client in expiry set
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 5000,
                value: clientId,
            });

            // Bump
            const result = await redis.fCall('queasy_bump', {
                keys: [QUEUE_NAME],
                arguments: [clientId, now.toString()],
            });

            assert.equal(result, 1);

            // Verify expiry was updated
            const expiry = await redis.zScore(`${QUEUE_NAME}:expiry`, clientId);
            assert.equal(Number(expiry), now + 10000);
        });

        it('should return 0 if client does not exist', async () => {
            const now = Date.now();
            const clientId = 'nonexistent-client';

            const result = await redis.fCall('queasy_bump', {
                keys: [QUEUE_NAME],
                arguments: [clientId, now.toString()],
            });

            assert.equal(result, 0);
        });
    });

    describe('finish', () => {
        it('should remove active job', async () => {
            const now = Date.now();
            const clientId = 'client1';
            const jobId = 'job-clear';

            // Setup: active job with checkouts + expiry
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: clientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, 'id', jobId);

            // Finish
            const result = await redis.fCall('queasy_finish', {
                keys: [QUEUE_NAME],
                arguments: [jobId, clientId, now.toString()],
            });

            assert.equal(result, 'OK');

            // Verify job is removed
            const activeExists = await redis.exists(`${QUEUE_NAME}:active_job:${jobId}`);
            assert.equal(activeExists, 0);

            // Verify job removed from checkouts
            const inCheckouts = await redis.sIsMember(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            assert.equal(inCheckouts, 0);
        });

        it('should unblock waiting job when active completes', async () => {
            const now = Date.now();
            const clientId = 'client1';
            const jobId = 'job-unblock';
            const runAt = Date.now() + 5000;

            // Setup: active job with checkouts + expiry
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: clientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, 'id', jobId);

            // Setup blocked waiting job
            await redis.zAdd(QUEUE_NAME, { score: -runAt, value: jobId });
            await redis.hSet(`${QUEUE_NAME}:waiting_job:${jobId}`, {
                id: jobId,
                update_run_at: 'true',
            });

            // Finish
            await redis.fCall('queasy_finish', {
                keys: [QUEUE_NAME],
                arguments: [jobId, clientId, now.toString()],
            });

            // Verify waiting job is unblocked (positive score)
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.ok(Number(score) > 0);
            assert.equal(Number(score), runAt);
        });
    });

    describe('retry', () => {
        it('should increment retry count and move job back to waiting', async () => {
            const now = Date.now();
            const jobId = 'job-retry';
            const clientId = 'client1';
            const nextRunAt = Date.now() + 5000;

            // Setup: active job with checkouts + expiry
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: clientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, {
                id: jobId,
                retry_count: '0',
            });

            // Call retry
            const result = await redis.fCall('queasy_retry', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    clientId,
                    nextRunAt.toString(),
                    '{"message":"error"}',
                    now.toString(),
                ],
            });

            assert.equal(result, 'OK');

            // Verify retry count incremented
            const retryCount = await redis.hGet(
                `${QUEUE_NAME}:waiting_job:${jobId}`,
                'retry_count'
            );
            assert.equal(retryCount, '1');

            // Verify job is in waiting queue
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.equal(Number(score), nextRunAt);

            // Verify job is not in active
            const activeExists = await redis.exists(`${QUEUE_NAME}:active_job:${jobId}`);
            assert.equal(activeExists, 0);

            // Verify job removed from checkouts
            const inCheckouts = await redis.sIsMember(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            assert.equal(inCheckouts, 0);
        });

        it('should always retry regardless of retry count', async () => {
            const now = Date.now();
            const jobId = 'job-retry-many';
            const clientId = 'client1';
            const nextRunAt = Date.now() + 5000;

            // Setup: active job with checkouts + expiry and high retry count
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: clientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, {
                id: jobId,
                retry_count: '100', // Very high retry count
            });

            // Call retry - should still work
            const result = await redis.fCall('queasy_retry', {
                keys: [QUEUE_NAME],
                arguments: [
                    jobId,
                    clientId,
                    nextRunAt.toString(),
                    '{"message":"error"}',
                    now.toString(),
                ],
            });

            assert.equal(result, 'OK');

            // Verify retry count incremented to 101
            const retryCount = await redis.hGet(
                `${QUEUE_NAME}:waiting_job:${jobId}`,
                'retry_count'
            );
            assert.equal(retryCount, '101');
        });
    });

    describe('fail', () => {
        it('should dispatch fail job and finish original job', async () => {
            const now = Date.now();
            const jobId = 'job-fail';
            const clientId = 'client1';
            const failJobId = 'fail-job-1';
            const failJobData = JSON.stringify([
                jobId,
                '{"original":"data"}',
                '{"message":"error"}',
            ]);

            // Setup: active job with checkouts + expiry
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: clientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, {
                id: jobId,
                data: '{"original":"data"}',
            });

            // Call fail
            const result = await redis.fCall('queasy_fail', {
                keys: [QUEUE_NAME, `${QUEUE_NAME}-fail`],
                arguments: [jobId, clientId, failJobId, failJobData, now.toString()],
            });

            assert.equal(result, 'OK');

            // Verify original job is finished (removed)
            const activeExists = await redis.exists(`${QUEUE_NAME}:active_job:${jobId}`);
            assert.equal(activeExists, 0);

            const inCheckouts = await redis.sIsMember(`${QUEUE_NAME}:checkouts:${clientId}`, jobId);
            assert.equal(inCheckouts, 0);

            // Verify fail job was dispatched
            const failScore = await redis.zScore(`${QUEUE_NAME}-fail`, failJobId);
            assert.ok(failScore !== null);

            const failData = await redis.hGet(
                `${QUEUE_NAME}-fail:waiting_job:${failJobId}`,
                'data'
            );
            assert.equal(failData, failJobData);
        });
    });

    describe('sweep (via bump)', () => {
        it('should process stalled jobs when bump is called', async () => {
            const now = Date.now();
            const stalledClientId = 'stalled-client';
            const healthyClientId = 'healthy-client';
            const jobId = 'job-stall';

            // Setup stalled client (heartbeat expired)
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now - 10000,
                value: stalledClientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${stalledClientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, {
                id: jobId,
                stall_count: '0',
            });

            // Setup healthy client
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: healthyClientId,
            });

            // Bump from healthy client triggers sweep
            const result = await redis.fCall('queasy_bump', {
                keys: [QUEUE_NAME],
                arguments: [healthyClientId, now.toString()],
            });

            assert.equal(result, 1);

            // Verify stall count incremented and job moved to waiting
            const stallCount = await redis.hGet(
                `${QUEUE_NAME}:waiting_job:${jobId}`,
                'stall_count'
            );
            assert.equal(stallCount, '1');

            // Verify job is in waiting queue
            const score = await redis.zScore(QUEUE_NAME, jobId);
            assert.ok(score !== null);

            // Verify stalled client cleaned up
            const stalledExpiry = await redis.zScore(`${QUEUE_NAME}:expiry`, stalledClientId);
            assert.equal(stalledExpiry, null);

            const stalledCheckouts = await redis.exists(
                `${QUEUE_NAME}:checkouts:${stalledClientId}`
            );
            assert.equal(stalledCheckouts, 0);
        });

        it('should always retry stalled jobs regardless of stall count', async () => {
            const now = Date.now();
            const stalledClientId = 'stalled-client';
            const healthyClientId = 'healthy-client';
            const jobId = 'job-stall-many';

            // Setup stalled client with high stall count
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now - 10000,
                value: stalledClientId,
            });
            await redis.sAdd(`${QUEUE_NAME}:checkouts:${stalledClientId}`, jobId);
            await redis.hSet(`${QUEUE_NAME}:active_job:${jobId}`, {
                id: jobId,
                stall_count: '100', // Very high stall count
            });

            // Setup healthy client
            await redis.zAdd(`${QUEUE_NAME}:expiry`, {
                score: now + 10000,
                value: healthyClientId,
            });

            // Bump from healthy client triggers sweep
            const result = await redis.fCall('queasy_bump', {
                keys: [QUEUE_NAME],
                arguments: [healthyClientId, now.toString()],
            });

            assert.equal(result, 1);

            // Verify stall count incremented to 101
            const stallCount = await redis.hGet(
                `${QUEUE_NAME}:waiting_job:${jobId}`,
                'stall_count'
            );
            assert.equal(stallCount, '101');
        });
    });
});
