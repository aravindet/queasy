import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';

const QUEUE_NAME = 'test';

describe('Queue E2E', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    /** @type {import('../src/client.js').Client}*/
    let client;

    beforeEach(async () => {
        redis = createClient();
        await redis.connect();
        const keys = await redis.keys(`${QUEUE_NAME}*`);
        if (keys.length > 0) {
            await redis.del(keys);
        }

        client = new Client(redis, 1);

        // Mock this so that no actual work is dequeued by the manager.
        if (client.manager) client.manager.addQueue = mock.fn();
    });

    afterEach(async () => {
        // Terminate worker threads to allow clean exit
        await client.close();

        // Clean up all queue data
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) {
            await redis.del(keys);
        }

        // Close Redis connection
        await redis.quit();
    });

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
            await (await q.dequeue(1)).promise;

            // Job should be removed from all queues
            const waitingScore = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.equal(waitingScore, null);

            const activeJobExists = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId}`);
            assert.equal(activeJobExists, 0);
        });

        it('should process multiple jobs', async () => {
            const q = client.queue(QUEUE_NAME);
            await Promise.all([
                q.dispatch({ id: 1 }),
                q.dispatch({ id: 2 }),
                q.dispatch({ id: 3 }),
            ]);

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath);
            await (await q.dequeue(5)).promise;

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
            await (await q.dequeue(1)).promise;

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
            await (await q.dequeue(1)).promise;

            // job1 should not exist
            const job1Score = await redis.zScore(`{${QUEUE_NAME}}`, jobId1);
            assert.equal(job1Score, null);

            const job1Exists = await redis.exists(`{${QUEUE_NAME}}:waiting_job:${jobId1}`);
            assert.equal(job1Exists, 0);

            // job2 should be fully processed
            const job2Score = await redis.zScore(`{${QUEUE_NAME}}`, jobId2);
            assert.equal(job2Score, null);

            const job2Active = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId2}`);
            assert.equal(job2Active, 0);
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
            // First wait for these jobs to be dequeued and sent to workers
            const dequeued = await Promise.all([queue1.dequeue(1), queue2.dequeue(1)]);

            // Now wait for the workers to finish processing
            await Promise.all(dequeued.map(({ promise }) => promise));

            // Both jobs should be cleaned up
            const score1 = await redis.zScore('{queue1}', jobId1);
            assert.equal(score1, null, 'Queue 1 job should be processed');

            const score2 = await redis.zScore('{queue2}', jobId2);
            assert.equal(score2, null, 'Queue 2 job should be processed');

            // Cleanup
            const keys2 = await redis.keys('{queue2}*');
            if (keys2.length > 0) await redis.del(keys2);

            const keys1 = await redis.keys('{queue1}*');
            if (keys1.length > 0) await redis.del(keys1);
        });
    });

    describe('failHandler option', () => {
        it('should set up fail queue via failHandler listen option', async () => {
            const staleKeys = await redis.keys('{fail-opt}*');
            if (staleKeys.length > 0) await redis.del(staleKeys);

            const q = client.queue('fail-opt');
            await q.dispatch({ task: 'will-fail' });

            const mainHandler = new URL('./fixtures/with-failure-handler.js', import.meta.url)
                .pathname;
            const failHandler = new URL('./fixtures/success-handler.js', import.meta.url).pathname;

            // listen with failHandler option — this covers queue.js lines 60-63
            await q.listen(mainHandler, { maxRetries: 0, failHandler });

            // Dequeue from the main queue
            await (await q.dequeue(1)).promise;

            // Fail job should exist in the fail queue
            const failJobIds = await redis.zRange('{fail-opt}-fail', 0, -1);
            assert.ok(failJobIds.length > 0, 'Fail job should be created in fail queue');

            // Cleanup
            const keys = await redis.keys('{fail-opt}*');
            if (keys.length > 0) await redis.del(keys);
        });
    });

    describe('retry and backoff', () => {
        it('should retry a failed job with exponential backoff', async () => {
            const q = client.queue(QUEUE_NAME);
            const jobId = await q.dispatch({ task: 'will-fail' });

            const handlerPath = new URL('./fixtures/always-fail-handler.js', import.meta.url)
                .pathname;
            await q.listen(handlerPath, { maxRetries: 2, minBackoff: 1000, maxBackoff: 10000 });

            const before = Date.now();
            await (await q.dequeue(1)).promise;

            // Job should be back in the waiting set with a backoff score
            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            assert.ok(score !== null, 'Job should be back in waiting set');
            assert.ok(score >= before + 1000, 'Score should include backoff');
        });
    });

    describe('maxStalls handling', () => {
        it('should fail a stalled job when maxStalls exceeded and failKey is set', async () => {
            const staleKeys = await redis.keys('{stall-test}*');
            if (staleKeys.length > 0) await redis.del(staleKeys);

            const q = client.queue('stall-test');
            const jobId = await q.dispatch({ task: 'stalled' });

            // Manually set stall_count to exceed maxStalls (default 3)
            await redis.hSet(`{stall-test}:waiting_job:${jobId}`, 'stall_count', '5');

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath, { maxStalls: 3 });
            q.failKey = '{stall-test}-fail';

            await (await q.dequeue(1)).promise;

            // Job should be removed from waiting set
            const score = await redis.zScore('{stall-test}', jobId);
            assert.equal(score, null, 'Job should be removed from waiting');

            // Fail job should exist in fail queue
            const failJobIds = await redis.zRange('{stall-test}-fail', 0, -1);
            assert.ok(failJobIds.length > 0, 'Fail job should be created');

            // Verify fail job data contains stall message
            const failJobId = failJobIds[0];
            const failJobData = await redis.hGet(
                `{stall-test}-fail:waiting_job:${failJobId}`,
                'data'
            );
            const parsed = JSON.parse(failJobData || 'null');
            assert.deepEqual(parsed[2], { message: 'Max stalls exceeded' });

            // Cleanup
            const keys = await redis.keys('{stall-test}*');
            if (keys.length > 0) await redis.del(keys);
        });

        it('should finish a stalled job when maxStalls exceeded and no failKey', async () => {
            const staleKeys = await redis.keys('{stall-nofail}*');
            if (staleKeys.length > 0) await redis.del(staleKeys);

            const q = client.queue('stall-nofail');
            const jobId = await q.dispatch({ task: 'stalled' });

            await redis.hSet(`{stall-nofail}:waiting_job:${jobId}`, 'stall_count', '5');

            const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
            await q.listen(handlerPath, { maxStalls: 3 });
            // No failKey set — job should just be finished

            await (await q.dequeue(1)).promise;

            // Job should be fully removed
            const score = await redis.zScore('{stall-nofail}', jobId);
            assert.equal(score, null, 'Job should be removed from waiting');

            const activeExists = await redis.exists(`{stall-nofail}:active_job:${jobId}`);
            assert.equal(activeExists, 0, 'Active job should be cleaned up');

            // Cleanup
            const keys = await redis.keys('{stall-nofail}*');
            if (keys.length > 0) await redis.del(keys);
        });
    });

    describe('invalid handler', () => {
        it('should fail when handler has no handle export', async () => {
            const staleKeys = await redis.keys('{bad-handler}*');
            if (staleKeys.length > 0) await redis.del(staleKeys);

            const q = client.queue('bad-handler');
            await q.dispatch({ task: 'test' });

            const handlerPath = new URL('./fixtures/no-handle-handler.js', import.meta.url)
                .pathname;
            await q.listen(handlerPath, { maxRetries: 0 });
            q.failKey = '{bad-handler}-fail';

            await (await q.dequeue(1)).promise;

            // Fail job should exist
            const failJobIds = await redis.zRange('{bad-handler}-fail', 0, -1);
            assert.ok(failJobIds.length > 0, 'Fail job should be created');

            // Verify error message
            const failJobId = failJobIds[0];
            const failJobData = await redis.hGet(
                `{bad-handler}-fail:waiting_job:${failJobId}`,
                'data'
            );
            const parsed = JSON.parse(failJobData || 'null');
            assert.ok(
                parsed[2].message.includes('Unable to load handler'),
                'Error should mention unable to load handler'
            );

            // Cleanup
            const keys = await redis.keys('{bad-handler}*');
            if (keys.length > 0) await redis.del(keys);
        });
    });

    describe('failure handlers', () => {
        it('should dispatch fail job on permanent failure', async () => {
            // Clean stale keys from previous runs
            const staleKeys = await redis.keys('{fail-test}*');
            if (staleKeys.length > 0) await redis.del(staleKeys);

            const q = client.queue('fail-test');
            const jobId = await q.dispatch({ task: 'will-fail' });

            const handlerPath = new URL('./fixtures/with-failure-handler.js', import.meta.url)
                .pathname;

            // Listen without failHandler so no fail queue listener races us
            await q.listen(handlerPath, { maxRetries: 0 });
            // Set failKey manually so the fail job is created but not consumed
            q.failKey = `${q.key}-fail`;
            await (await q.dequeue(1)).promise;

            // Original job should be cleaned up
            const activeExists = await redis.exists(`{fail-test}:active_job:${jobId}`);
            assert.equal(activeExists, 0);

            // Fail job should exist in fail queue
            const failJobIds = await redis.zRange('{fail-test}-fail', 0, -1);
            assert.ok(failJobIds.length > 0, 'Fail job should be created');

            // Verify fail job data structure
            const failJobId = failJobIds[0];
            const failJobData = await redis.hGet(
                `{fail-test}-fail:waiting_job:${failJobId}`,
                'data'
            );
            const parsedFailData = JSON.parse(failJobData || 'null');
            assert.ok(Array.isArray(parsedFailData), 'Fail job data should be an array');
            assert.equal(parsedFailData.length, 3, 'Fail job should have [jobId, data, error]');

            // Verify the structure contains the original data
            assert.deepEqual(parsedFailData[1], { task: 'will-fail' });

            // Verify error is present
            assert.ok(parsedFailData[2].message, 'Error should have a message');

            // Cleanup
            const keys = await redis.keys('{fail-test}*');
            if (keys.length > 0) await redis.del(keys);
        });
    });
});
