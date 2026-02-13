import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';

const QUEUE_NAME = 'test';

describe('Queue E2E', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    let client;

    beforeEach(async () => {
        redis = createClient();
        await redis.connect();
        const keys = await redis.keys(`${QUEUE_NAME}*`);
        if (keys.length > 0) {
            await redis.del(keys);
        }

        client = new Client(redis);
    });

    afterEach(async () => {
        // Clean up all queue data
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) {
            await redis.del(keys);
        }

        // Terminate worker threads to allow clean exit
        await client.close();

        // Close Redis connection
        await redis.quit();
    });

    /**
     * Poll until job completes or timeout
     * @param {string} jobId
     * @param {number} timeoutMs
     * @param {string} queueName - Optional queue name, defaults to QUEUE_NAME
     * @returns {Promise<boolean>} True if completed, false if timeout
     */
    async function waitForJobCompletion(jobId, timeoutMs = 3000, queueName = QUEUE_NAME) {
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
            const waiting = await redis.zScore(`{${queueName}}`, jobId);
            const activeJobExists = await redis.exists(`{${queueName}}:active_job:${jobId}`);
            if (waiting === null && activeJobExists === 0) {
                return true;
            }
            await new Promise((r) => setTimeout(r, 50));
        }
        return false;
    }

    describe('dispatch()', () => {
        it('should dispatch a job and store it in waiting queue', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId = await q.dispatch({ task: 'test-job' });

            assert.ok(jobId);
            assert.equal(typeof jobId, 'string');

            // Job should be in waiting queue
            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.ok(score !== null);

            // Job data should exist
            const jobData = await redis.hGetAll(`{${QUEUE_NAME}}:waiting_job:${jobId}`);
            assert.equal(jobData.id, jobId);
            assert.equal(jobData.data, JSON.stringify({ task: 'test-job' }));
        });

        it('should accept custom job ID', async () => {
            const q = client.queue(QUEUE_NAME);
            const customId = 'my-custom-id';
            const jobId = await q.dispatch({ task: 'test' }, { id: customId });

            assert.equal(jobId, customId);

            const score = await redis.zScore(`{${QUEUE_NAME}}`, customId);
            assert.ok(score !== null);
        });

        it('should respect runAt option', async () => {
            const q = client.queue(QUEUE_NAME);
            const futureTime = Date.now() + 10000;
            const jobId = await q.dispatch({ task: 'future' }, { runAt: futureTime });

            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.equal(score, futureTime);
        });

        it('should update existing job when updateData is true', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId = 'update-test';

            await q.dispatch({ value: 1 }, { id: jobId });
            await q.dispatch({ value: 2 }, { id: jobId, updateData: true });

            const jobData = await redis.hGetAll(`{${QUEUE_NAME}}:waiting_job:${jobId}`);
            assert.equal(jobData.data, JSON.stringify({ value: 2 }));
        });

        it('should not update existing job when updateData is false', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId = 'no-update-test';

            await q.dispatch({ value: 1 }, { id: jobId });
            await q.dispatch({ value: 2 }, { id: jobId, updateData: false });

            const jobData = await redis.hGetAll(`{${QUEUE_NAME}}:waiting_job:${jobId}`);
            assert.equal(jobData.data, JSON.stringify({ value: 1 }));
        });
    });

    describe('cancel()', () => {
        it('should cancel a waiting job', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId = await q.dispatch({ task: 'test' });

            const cancelled = await q.cancel(jobId);
            assert.equal(cancelled, true);

            // Job should be removed
            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.equal(score, null);

            const exists = await redis.exists(`{${QUEUE_NAME}}:waiting_job:${jobId}`);
            assert.equal(exists, 0);
        });

        it('should return false for nonexistent job', async () => {
            const q = client.queue(QUEUE_NAME);
            const cancelled = await q.cancel('nonexistent-job-id');
            assert.equal(cancelled, false);
        });
    });

    describe('listen() and job processing', () => {
        it('should process a job successfully', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId = await q.dispatch({ greeting: 'hello' });

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath);

            const completed = await waitForJobCompletion(jobId);
            assert.ok(completed, 'Job should complete within timeout');

            // Job should be removed from all queues
            const waitingScore = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.equal(waitingScore, null);

            const activeJobExists = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId}`);
            assert.equal(activeJobExists, 0);
        });

        it('should process multiple jobs', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobIds = await Promise.all([
                q.dispatch({ id: 1 }),
                q.dispatch({ id: 2 }),
                q.dispatch({ id: 3 }),
            ]);

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath);

            // Wait for all jobs to complete
            const results = await Promise.all(jobIds.map((id) => waitForJobCompletion(id)));
            assert.ok(
                results.every((r) => r === true),
                'All jobs should complete'
            );

            // All jobs should be cleaned up
            const waitingJobs = await redis.zRange(`{${QUEUE_NAME}}`, 0, -1);
            assert.equal(waitingJobs.length, 0);

            const activeJobKeys = await redis.keys(`{${QUEUE_NAME}}:active_job:*`);
            assert.equal(activeJobKeys.length, 0);
        });

        it('should not process jobs scheduled for the future', async () => {
            const q = client.queue(QUEUE_NAME);
            const futureTime = Date.now() + 10000;
            const jobId = await q.dispatch({ task: 'future' }, { runAt: futureTime });

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath);

            // Wait a bit
            await new Promise((r) => setTimeout(r, 500));

            // Job should still be waiting
            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.ok(score !== null);
            assert.equal(score, futureTime);
        });

        it('should handle cancelling a job before it processes', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId1 = await q.dispatch({ id: 1 });
            const jobId2 = await q.dispatch({ id: 2 });

            // Cancel job1 before listening
            const cancelled = await q.cancel(jobId1);
            assert.ok(cancelled);

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath);

            // Only job2 should complete
            const job2Completed = await waitForJobCompletion(jobId2);
            assert.ok(job2Completed);

            // job1 should not exist
            const job1Score = await redis.zScore(`{${QUEUE_NAME}}`, jobId1);
            assert.equal(job1Score, null);

            const job1Exists = await redis.exists(`{${QUEUE_NAME}}:waiting_job:${jobId1}`);
            assert.equal(job1Exists, 0);
        });
    });

    describe('multiple queues', () => {
        it('should handle multiple independent queues', async () => {
            const queue1 = client.queue('queue1');
            const queue2 = client.queue('queue2');

            const jobId1 = await queue1.dispatch({ queue: 1 });
            const jobId2 = await queue2.dispatch({ queue: 2 });

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await queue1.listen(handlerPath);
            await queue2.listen(handlerPath);

            // Both jobs should complete
            const [completed1, completed2] = await Promise.all([
                waitForJobCompletion(jobId1, 3000, 'queue1'),
                waitForJobCompletion(jobId2, 3000, 'queue2'),
            ]);

            assert.ok(completed1, 'Queue 1 job should complete');
            assert.ok(completed2, 'Queue 2 job should complete');

            // Cleanup
            const keys2 = await redis.keys('{queue2}*');
            if (keys2.length > 0) await redis.del(keys2);

            const keys1 = await redis.keys('{queue1}*');
            if (keys1.length > 0) await redis.del(keys1);
        });
    });

    describe('failure handlers', () => {
        it('should process failed jobs with separate failure handler', async () => {
            const q = client.queue('fail-test');
            await q.dispatch({ task: 'will-fail' });

            const handlerPath = new URL('./fixtures/with-failure-handler.js', import.meta.url)
                .pathname;
            const failHandlerPath = new URL('./fixtures/failure-handler.js', import.meta.url)
                .pathname;

            await q.listen(handlerPath, {
                maxRetries: 0, // Fail immediately without retries
                failHandler: failHandlerPath,
            });

            // Wait for job to fail and fail job to be created
            await new Promise((r) => setTimeout(r, 500));

            // Fail job should exist in fail queue
            const failJobIds = await redis.zRange('{fail-test}-fail', 0, -1);
            assert.ok(failJobIds.length > 0, 'Fail job should be created');

            // Verify fail job data structure
            const failJobId = failJobIds[0];
            const failJobData = await redis.hGet(
                `{fail-test}-fail:waiting_job:${failJobId}`,
                'data'
            );
            const parsedFailData = JSON.parse(failJobData);
            assert.ok(Array.isArray(parsedFailData), 'Fail job data should be an array');
            assert.equal(parsedFailData.length, 3, 'Fail job should have [jobId, data, error]');

            // Verify the structure contains the original data
            const originalData = JSON.parse(parsedFailData[1]);
            assert.deepEqual(originalData, { task: 'will-fail' });

            // Verify error is present
            const error = JSON.parse(parsedFailData[2]);
            assert.ok(error.message, 'Error should have a message');

            // Cleanup
            const keys = await redis.keys('{fail-test}*');
            if (keys.length > 0) await redis.del(keys);
        });
    });
});
