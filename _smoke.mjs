import { queue } from './src/index.js';
import { createClient } from 'redis';

const redis = createClient();
await redis.connect();

const QUEUE = '{e2e-smoke}';
const q = queue(QUEUE, redis);

// Clean up any leftovers
const keys = await redis.keys(`${QUEUE}*`);
if (keys.length) await redis.del(keys);

// Dispatch a job
const jobId = await q.dispatch({ greeting: 'hello' });
console.log('dispatched:', jobId);

// Start listening — workers will pick up the job via the dequeue loop
const handlerPath = '/home/aravind/code/queasy/test/fixtures/success-handler.js';
await q.listen(handlerPath);

// Poll until the job is fully gone from Redis or 3 s timeout
const deadline = Date.now() + 3000;
while (Date.now() < deadline) {
    const waiting = await redis.zScore(`${QUEUE}:waiting`, jobId);
    const activeKeys = await redis.keys(`${QUEUE}:active*`);
    if (waiting === null && activeKeys.length === 0) {
        console.log('job completed and cleaned up');
        break;
    }
    await new Promise(r => setTimeout(r, 50));
}

// Cleanup
const finalKeys = await redis.keys(`${QUEUE}*`);
if (finalKeys.length) await redis.del(finalKeys);
redis.destroy();
process.exit(0);
