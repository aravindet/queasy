import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { createClient } from 'redis';
import { Client } from '../src/index.js';
import { Manager } from '../src/manager.js';

describe('Manager unit tests', () => {
    /**
     * @param {object} [overrides]
     * @returns {import('../src/manager.js').Manager}
     */
    function createManager(overrides = {}) {
        const pool = /** @type {import('../src/pool.js').Pool} */ ({
            capacity: 100,
            ...overrides,
        });
        return new Manager(pool);
    }

    /**
     * @param {object} [overrides]
     */
    function mockQueue(overrides = {}) {
        return /** @type {import('../src/queue.js').ProcessingQueue} */ ({
            handlerOptions: { size: 10, priority: 100, ...overrides.handlerOptions },
            dequeue: overrides.dequeue ?? mock.fn(async () => ({ count: 0 })),
            ...overrides,
        });
    }

    it('should return early from next() when queues are empty', async () => {
        const mgr = createManager();
        // next() with no queues should not throw
        await mgr.next();
        mgr.close();
    });

    it('should return early from next() when pool capacity is insufficient', async () => {
        const mgr = createManager({ capacity: 5 });
        const q = mockQueue({ handlerOptions: { size: 10, priority: 100 } });
        mgr.addQueue(q);
        // Wait for next() to run (addQueue calls next via setTimeout)
        await new Promise((r) => setTimeout(r, 50));
        // dequeue should NOT have been called since capacity (5) < size (10)
        assert.equal(q.dequeue.mock.callCount(), 0);
        mgr.close();
    });

    it('should clear existing timer when next() runs', async () => {
        const mgr = createManager();
        // Set a fake timer
        mgr.timer = setTimeout(() => {}, 10000);
        const q = mockQueue();
        mgr.queues.push({ queue: q, lastDequeuedAt: 0, isBusy: false });
        mgr.busyCount = 1;
        await mgr.next();
        // Timer should have been cleared and reset
        assert.notEqual(mgr.timer, null);
        mgr.close();
    });

    it('should set delay to 0 when queue is busy', async () => {
        const mgr = createManager({ capacity: 100 });
        // dequeue returns count equal to batchSize, making queue busy
        const q = mockQueue({
            dequeue: mock.fn(async () => ({ count: 10 })),
        });
        mgr.queues.push({ queue: q, lastDequeuedAt: 0, isBusy: false });
        mgr.busyCount = 1;
        await mgr.next();
        assert.notEqual(mgr.timer, null);
        mgr.close();
    });

    it('should sort queues: busy before non-busy', async () => {
        const mgr = createManager({ capacity: 100 });
        const q1 = mockQueue();
        const q2 = mockQueue();
        const entries = [
            { queue: q1, lastDequeuedAt: 0, isBusy: false },
            { queue: q2, lastDequeuedAt: 0, isBusy: true },
        ];
        mgr.queues = [...entries];
        mgr.busyCount = 1;
        // Manually call next to trigger the sort
        await mgr.next();
        // The busy queue should be first after sort
        assert.equal(mgr.queues[0].isBusy, true);
        mgr.close();
    });

    it('should exercise priority sort branches', async () => {
        const mgr = createManager({ capacity: 200 });
        const qProcessed = mockQueue({ handlerOptions: { size: 10, priority: 100 } });
        const qLow = mockQueue({ handlerOptions: { size: 10, priority: 50 } });
        const qHigh = mockQueue({ handlerOptions: { size: 10, priority: 200 } });
        mgr.queues = [
            { queue: qProcessed, lastDequeuedAt: 0, isBusy: false },
            { queue: qHigh, lastDequeuedAt: 0, isBusy: false },
            { queue: qLow, lastDequeuedAt: 0, isBusy: false },
        ];
        mgr.busyCount = 3;
        await mgr.next();
        // Lower priority value sorts before higher
        const priorities = mgr.queues.map((e) => e.queue.handlerOptions.priority);
        assert.ok(priorities.indexOf(50) < priorities.indexOf(200));
        mgr.close();
    });

    it('should exercise lastDequeuedAt sort branches', async () => {
        const mgr = createManager({ capacity: 200 });
        const qProcessed = mockQueue();
        const qOld = mockQueue();
        const qNew = mockQueue();
        mgr.queues = [
            { queue: qProcessed, lastDequeuedAt: 0, isBusy: false },
            { queue: qOld, lastDequeuedAt: 100, isBusy: false },
            { queue: qNew, lastDequeuedAt: 200, isBusy: false },
        ];
        mgr.busyCount = 3;
        await mgr.next();
        // Among the two unprocessed: newer lastDequeuedAt sorts before older
        const [, ...rest] = mgr.queues; // skip the processed entry (now has Date.now())
        const oldIdx = rest.findIndex((e) => e.queue === qOld);
        const newIdx = rest.findIndex((e) => e.queue === qNew);
        if (oldIdx !== -1 && newIdx !== -1) {
            assert.ok(newIdx < oldIdx);
        }
        mgr.close();
    });

    it('should exercise size sort branches', async () => {
        const mgr = createManager({ capacity: 200 });
        const now = Date.now();
        const qProcessed = mockQueue({ handlerOptions: { size: 10, priority: 50 } });
        const qSmall = mockQueue({ handlerOptions: { size: 5, priority: 100 } });
        const qLarge = mockQueue({ handlerOptions: { size: 20, priority: 100 } });
        mgr.queues = [
            { queue: qProcessed, lastDequeuedAt: now, isBusy: false },
            { queue: qSmall, lastDequeuedAt: now, isBusy: false },
            { queue: qLarge, lastDequeuedAt: now, isBusy: false },
        ];
        mgr.busyCount = 3;
        await mgr.next();
        // Among entries with same priority and lastDequeuedAt: smaller size sorts first
        const sizes = mgr.queues.map((e) => e.queue.handlerOptions.size);
        assert.ok(sizes.indexOf(5) < sizes.indexOf(20));
        mgr.close();
    });
});

const QUEUE_NAME = 'mgr-test';

describe('Manager scheduling', () => {
    /** @type {import('redis').RedisClientType} */
    let redis;
    /** @type {import('../src/client.js').Client} */
    let client;

    beforeEach(async () => {
        redis = createClient();
        await redis.connect();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);

        // Do NOT mock manager.addQueue — let the manager drive dequeuing
        client = new Client(redis, 1);
    });

    afterEach(async () => {
        client.close();
        const keys = await redis.keys(`{${QUEUE_NAME}}*`);
        if (keys.length > 0) await redis.del(keys);
        await redis.quit();
    });

    it('should dequeue and process jobs via manager without explicit dequeue', async () => {
        const q = client.queue(QUEUE_NAME);
        const jobId = await q.dispatch({ task: 'managed' });

        const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
        await q.listen(handlerPath);

        // Wait for manager to dequeue and process the job fully
        await poll(async () => {
            const score = await redis.zScore(`{${QUEUE_NAME}}`, jobId);
            const activeExists = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId}`);
            return score === null && activeExists === 0;
        });
    });

    it('should process jobs from multiple queues', async () => {
        const staleKeys = await redis.keys('{mgr-q2}*');
        if (staleKeys.length > 0) await redis.del(staleKeys);

        const q1 = client.queue(QUEUE_NAME);
        const q2 = client.queue('mgr-q2');
        const jobId1 = await q1.dispatch({ q: 1 });
        const jobId2 = await q2.dispatch({ q: 2 });

        const handlerPath = new URL('./fixtures/success-handler.js', import.meta.url).pathname;
        await q1.listen(handlerPath);
        await q2.listen(handlerPath);

        // Wait for both jobs to be fully processed
        await poll(async () => {
            const s1 = await redis.zScore(`{${QUEUE_NAME}}`, jobId1);
            const a1 = await redis.exists(`{${QUEUE_NAME}}:active_job:${jobId1}`);
            const s2 = await redis.zScore('{mgr-q2}', jobId2);
            const a2 = await redis.exists(`{mgr-q2}:active_job:${jobId2}`);
            return s1 === null && a1 === 0 && s2 === null && a2 === 0;
        });

        // Cleanup
        const keys = await redis.keys('{mgr-q2}*');
        if (keys.length > 0) await redis.del(keys);
    });
});

/**
 * Poll a condition until it returns true, with a timeout.
 * @param {() => Promise<boolean>} fn
 * @param {number} [timeout=5000]
 * @param {number} [interval=20]
 */
async function poll(fn, timeout = 5000, interval = 20) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await fn()) return;
        await new Promise((r) => setTimeout(r, interval));
    }
    assert.fail('Poll timed out');
}
