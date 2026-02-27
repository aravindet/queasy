/**
 * Redis stream helpers for the fuzz test event log.
 * All events are written to the 'fuzz:events' stream.
 */

import type { RedisClientType } from 'redis';

export const STREAM_KEY = 'fuzz:events';

/**
 * Emit a structured event to the fuzz:events stream.
 */
export async function emitEvent(redis: RedisClientType, fields: Record<string, string>): Promise<void> {
    try {
        await redis.xAdd(STREAM_KEY, '*', fields);
    } catch {
        // Never let event emission crash a handler
    }
}

/**
 * Async generator that yields parsed event objects from the fuzz:events stream.
 * Blocks for up to 1 second waiting for new events, then yields control back.
 */
export async function* readEvents(
    redis: RedisClientType,
    lastId = '0'
): AsyncGenerator<Record<string, string>> {
    let id = lastId;
    while (true) {
        const results = await redis.xRead({ key: STREAM_KEY, id }, { BLOCK: 1000, COUNT: 100 });
        if (!results) continue;
        for (const { messages } of results) {
            for (const { id: msgId, message } of messages) {
                id = msgId;
                yield message;
            }
        }
    }
}
