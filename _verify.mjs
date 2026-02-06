import { queue } from './src/index.js';
import { createClient } from 'redis';

const redis = createClient();
await redis.connect();

const q = queue('{verify}', redis);
const jobId = await q.dispatch({ test: 'refactor' });

const score = await redis.zScore('{verify}:waiting', jobId);
const data = await redis.hGet(`{verify}:waiting_job:${jobId}`, 'data');

console.log('Queue class works:', score !== null && data === '{"test":"refactor"}');

await redis.del(await redis.keys('{verify}*'));
redis.destroy();
