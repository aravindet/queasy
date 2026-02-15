import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';
import { Pool } from '../src/pool.js';

describe('Pool unit tests', () => {
    /** Helper to create a fake job entry with a no-op timer */
    function fakeJobEntry(size = 10) {
        const timer = setTimeout(() => {}, 0);
        return { resolve: () => {}, reject: () => {}, size, timer };
    }

    it('should warn and return on message with unknown job ID', () => {
        const pool = new Pool(1);
        const warn = mock.method(console, 'warn');
        const workerEntry = pool.workers.values().next().value;
        pool.handleWorkerMessage(workerEntry, { op: 'done', jobId: 'unknown' });
        assert.equal(warn.mock.callCount(), 1);
        warn.mock.restore();
        pool.close();
    });

    it('should unmark stalled jobs when they complete', () => {
        const pool = new Pool(1);
        const workerEntry = pool.workers.values().next().value;
        const jobId = 'stalled-job';

        pool.activeJobs.set(jobId, fakeJobEntry());
        workerEntry.jobCount = 1;
        workerEntry.stalledJobs.add(jobId);

        pool.handleWorkerMessage(workerEntry, { op: 'done', jobId });

        assert.equal(workerEntry.stalledJobs.has(jobId), false);
        pool.close();
    });

    it('should call terminateIfEmpty when worker is no longer in pool', () => {
        const pool = new Pool(1);
        const workerEntry = pool.workers.values().next().value;
        const jobId = 'orphan-job';

        // Remove the worker from the pool (as handleTimeout would)
        pool.workers.delete(workerEntry);

        pool.activeJobs.set(jobId, fakeJobEntry());
        workerEntry.jobCount = 1;
        workerEntry.stalledJobs.add(jobId);

        // Message arrives for the orphaned worker — should trigger terminateIfEmpty
        pool.handleWorkerMessage(workerEntry, { op: 'done', jobId });

        assert.equal(pool.activeJobs.has(jobId), false);
        pool.close();
    });

    it('should not replace worker in handleTimeout if already removed', () => {
        const pool = new Pool(1);
        const workerEntry = pool.workers.values().next().value;

        const entry = fakeJobEntry();
        pool.activeJobs.set('j1', entry);
        workerEntry.jobCount = 1;

        // Remove the worker first (simulating a prior timeout)
        pool.workers.delete(workerEntry);
        const workerCountBefore = pool.workers.size;

        pool.handleTimeout(workerEntry, 'j1');
        clearTimeout(entry.timer);

        assert.equal(pool.workers.size, workerCountBefore);
        pool.close();
    });

    it('should not terminate worker in terminateIfEmpty if non-stalled jobs remain', async () => {
        const pool = new Pool(1);
        const workerEntry = pool.workers.values().next().value;

        // 2 jobs active, only 1 stalled — should not terminate
        workerEntry.jobCount = 2;
        workerEntry.stalledJobs.add('stalled-1');

        await pool.terminateIfEmpty(workerEntry);

        assert.equal(workerEntry.stalledJobs.size, 1);
        pool.close();
    });
});

const QUEUE_NAME = 'pool-test';

describe('Pool stall and timeout', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    /** @type {import('../src/client.js').Client} */
    let client;

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

    it('should handle worker timeout as a stall', async () => {
        const q = client.queue(QUEUE_NAME);
        const jobId = await q.dispatch({ delay: 500 });

        const handlerPath = new URL('./fixtures/slow-handler.js', import.meta.url).pathname;
        // Very short timeout (50ms) so pool.handleTimeout fires before the 500ms handler completes
        await q.listen(handlerPath, { timeout: 50, maxRetries: 5 });

        await (await q.dequeue(1)).promise;

        // Job should be back in waiting set (retried after timeout stall)
        const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
        assert.ok(score !== null, 'Job should be retried back to waiting set');
    });

    it('should reject active jobs when pool is closed', async () => {
        const q = client.queue(QUEUE_NAME);
        const jobId = await q.dispatch({ delay: 2000 });

        const handlerPath = new URL('./fixtures/slow-handler.js', import.meta.url).pathname;
        await q.listen(handlerPath, { maxRetries: 5 });

        // Dequeue but don't await the promise — job is still processing
        const { promise } = await q.dequeue(1);

        // Close the client immediately while job is active
        client.close();

        // The promise should resolve (the rejection gets handled by the catch block in dequeue)
        await promise;

        // Job should be back in waiting set (retry after stall rejection)
        const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
        assert.ok(score !== null, 'Job should be retried after pool close');
    });
});
