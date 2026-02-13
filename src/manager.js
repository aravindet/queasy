/**
 * This class manages resource allocation between
 * different queues based on the size of the queue
 */

import { DEQUEUE_INTERVAL } from './constants.js';

/** @typedef {import('./pool').Pool} Pool */
/** @typedef {import('./queue').ProcessingQueue} Queue */
/** @typedef {{ queue: Queue, lastDequeuedAt: number, isBusy: boolean }} QueueEntry */

export class Manager {
    /** @param {Pool} pool */
    constructor(pool) {
        this.pool = pool;

        /** @type {Array<QueueEntry>} */
        this.queues = [];

        /** @type {NodeJS.Timeout?} */
        this.timer = null;

        this.busyCount = 0;
    }

    /** @param {Queue} queue */
    addQueue(queue) {
        console.log('real addQueue called');
        // Add this at the beginning so we dequeue it at the next available opportunity.
        this.queues.unshift({ queue, lastDequeuedAt: 0, isBusy: false });
        this.busyCount += 1;

        // This delay is required for queue listen tests
        // as they need to be able to control dequeueing
        this.next();
    }

    async next() {
        // If this function is called while the previous execution is in progress,
        // we do not want both executions to use the same queue.
        const entry = this.queues.shift();
        if (!entry) return;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }

        const batchSize = Math.max(
            1,
            Math.floor(this.pool.capacity / this.busyCount / entry.queue.handlerOptions.size)
        );
        entry.lastDequeuedAt = Date.now(); // We store the time just before the call to dequeue.
        const { count } = await entry.queue.dequeue(batchSize);

        // Update
        const nowBusy = count >= batchSize;
        this.busyCount += Number(nowBusy) - Number(entry.isBusy);
        entry.isBusy = nowBusy;

        this.queues.push(entry);
        this.queues.sort(compareQueueEntries);

        if (!this.timer && this.queues.length) {
            const { isBusy, lastDequeuedAt } = this.queues[0];
            // If the current top queue is busy, retry now.
            const delay = isBusy ? 0 : Math.max(0, lastDequeuedAt - Date.now() + DEQUEUE_INTERVAL);
            this.timer = setTimeout(() => this.next(), delay);
        }
    }

    close() {
        if (this.timer) clearTimeout(this.timer);
    }
}

/**
 * @param {QueueEntry} a
 * @param {QueueEntry} b
 * @returns -1 | 0 | 1
 */
function compareQueueEntries(a, b) {
    if (a.isBusy > b.isBusy) return -1; // a busy, b not -> a first
    if (a.isBusy < b.isBusy) return 1; // a free, b busy -> b first

    if (a.lastDequeuedAt > b.lastDequeuedAt) return -1; // a newer -> b first
    if (a.lastDequeuedAt < b.lastDequeuedAt) return 1; // a older -> a first

    if (a.queue.handlerOptions.size > b.queue.handlerOptions.size) return 1; // a larger -> a first
    if (a.queue.handlerOptions.size < b.queue.handlerOptions.size) return -1; // b larger -> a first

    return 0;
}
