import { DEQUEUE_INTERVAL } from './constants.ts';
import type { Pool } from './pool.ts';
import type { Queue } from './queue.ts';

interface QueueEntry {
    queue: Queue;
    lastDequeuedAt: number;
    isBusy: boolean;
}

export class Manager {
    pool: Pool;
    queues: QueueEntry[];
    timer: NodeJS.Timeout | null;
    busyCount: number;

    constructor(pool: Pool) {
        this.pool = pool;
        this.queues = [];
        this.timer = null;
        this.busyCount = 0;
    }

    addQueue(queue: Queue): void {
        this.queues.unshift({ queue, lastDequeuedAt: 0, isBusy: false });
        this.busyCount += 1;
        this.next();
    }

    async next(): Promise<void> {
        const entry = this.queues.shift();
        if (!entry) return;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }

        const size = entry.queue.handlerOptions!.size;
        if (this.pool.capacity < size) return;

        const batchSize = Math.max(1, Math.floor(this.pool.capacity / this.busyCount / size));
        entry.lastDequeuedAt = Date.now();
        const { count } = await entry.queue.dequeue(batchSize);

        const nowBusy = count >= batchSize;
        this.busyCount += Number(nowBusy) - Number(entry.isBusy);
        entry.isBusy = nowBusy;

        this.queues.push(entry);
        this.queues.sort(compareQueueEntries);

        if (!this.timer && this.queues.length) {
            const { isBusy, lastDequeuedAt } = this.queues[0];
            const delay = isBusy ? 0 : Math.max(0, lastDequeuedAt - Date.now() + DEQUEUE_INTERVAL);
            this.timer = setTimeout(() => this.next(), delay);
        }
    }

    close(): void {
        if (this.timer) clearTimeout(this.timer);
    }
}

function compareQueueEntries(a: QueueEntry, b: QueueEntry): -1 | 0 | 1 {
    if (a.isBusy > b.isBusy) return -1;
    if (a.isBusy < b.isBusy) return 1;

    if (a.queue.handlerOptions!.priority > b.queue.handlerOptions!.priority) return 1;
    if (a.queue.handlerOptions!.priority < b.queue.handlerOptions!.priority) return -1;

    if (a.lastDequeuedAt > b.lastDequeuedAt) return -1;
    if (a.lastDequeuedAt < b.lastDequeuedAt) return 1;

    if (a.queue.handlerOptions!.size > b.queue.handlerOptions!.size) return 1;
    if (a.queue.handlerOptions!.size < b.queue.handlerOptions!.size) return -1;

    return 0;
}
