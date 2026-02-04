# Queasy

A simple Redis-backed queue library for Node.js with support for:

- Calling task handlers with at-least-once semantics
- Retrying jobs on failure, with exponential backoff 
- API for task handlers to override retry delay or signal permanent failure
- Locking active jobs with heartbeats and retrying stalled jobs
- Calling permanent failure handlers in case of repeated job failure or stall
- Running jobs in threads to support CPU-bound tasks
- Killing jobs that run too long by terminating the thread
- Scheduled jobs
- Cancelling scheduled jobs
- Updating scheduled jobs
- Singleton semantics: prevent instances of repeated jobs running concurrently 
- Dynamic concurrency based on worker CPU utilization

This library is NOT resilient to Redis failures. If the Redis instance crashes, jobs may crash, multiple workers might be assigned the same job, etc.

## API

### `queue(name, redisConnection, defaultJobOptions, failureJobOptions)`

Returns a queue object for interacting with this named queue at the defined Redis server.
- name is a string queue name. Redis data structures related to a queue will be placed on the same node in a Redis cluster.
- redisConnection is an ioredis connection object.
- defaultJobOptions are defaults for the options argument to queue.notify() below, except for `runAt`
- failureJobOptions are default options for jobs used to invoke failure handlers, except for `runAt`.

### `queue.notify(data, options)`

Adds a job to the queue. `data` may be any JSON value, which will be passed unchanged to the workers. Options may include:
- `id`: alphanumeric string; if not provided, a unique random string is generated
- `runAt`: number; wall clock timestamp before which this job must not be run; default: 0
- `maxFailures`: number; default: 10
- `maxStalls`: number; default: 3
- `minBackoff`: number; in milliseconds; default: 2000
- `maxBackoff`: number; default: 300_000

The following options take effect if an `id` is provided, and it matches that of a job already in the queue.
- `updateData`: boolean; whether to replace the data of any waiting job with the same ID; default: true
- `updateRunAt`: boolean | 'ifLater' | 'ifEarlier'; default: true
- `updateMaxFailures`, `updateMaxStalls`, `updateMinBackoff`, `updateMaxBackoff`: boolean
- `resetCounts`: boolean; Whether to reset the internal failure and stall counts to 0; default: same as updateData

Returns a promise that resolves to the job ID when the job has been added to Redis.

### `queue.cancel(id)`

Removes the job with the given ID if it exists in the waiting state, no-op otherwise.

Returns a promise that resolves to a boolean (true if a job with this ID existed and has been removed).

### `queue.listen(handler)`
Attaches handlers to a queue to process jobs that are added to it.
- `handler`: The path to a JavaScript module that exports the task (and, optionally, failure) handlers

## Handlers

The handler module must have a named export `handle`, a function that is called with each job. It may additionally have a named export 
`handleFailure`, which is called if the job fails permanently.

### `handle`

It receives two arguments:
- `data`, the JSON value passed to notify
- `job`, an object contains all the job options as well as `failureCount` and `stallCount`

This function may throw (or return a Promise that rejects) to indicate job failure. If the thrown error is an
instance of `PermanentError`, or if `maxFailures` has been reached, the job is not retried. Otherwise, the job
is queued to be retried with `maxFailures` incremented.

If the thrown error has a property `retryAt`, the job’s `runAt` is set to this value; otherwise, it’s set using
the exponential backoff algorithm.

If it returns any value apart from a Promise that rejects, the job is considered to have completed successfully.

### `handleFailure`

This function receives three arguments:
- `data`, the JSON value passed to notify
- `job`
- `error`, a JSON object with a copy of the enumerable properties of the error thrown by the final call to handle,
  or an instance of `StallError` if the final call to handle didn’t return or throw.

If this function throws an error (or returns a Promise that rejects), it is retried using exponential backoff. It
is expected that failure jobs are retried for a long time (e.g. days).

## Implementation

### Data structures

We use two sorted sets, `{queue}:waiting` and `{queue}:active`, and two hashes `{queue}:waiting_job:{id}` and 
`{queue}:active_job:{id}`. The hash has members `id`, `data`, `maxFailures`, `failureCount`, `lockedBy` etc.

The members of the waiting set is the job `id`, while that of the active set is `{id}:{worker_id}` with the ID of the
worker that has dequeued this job.

The scores of the active set equal the deadline by which the next
heartbeat must be received, failing which this job will be considered stalled. The scores of the waiting sorted set
equal `runAt`, except for blocked jobs whose scores are `-runAt`, preventing them from being picked up.

**Blocked jobs** are those that cannot be picked up, because there is an active job with the same `id`.

### Adding a job to a queue

In Javascript:
- Populate all job options. If no `id` is specified, generate a random UUID v4.
- Execute the Lua "upsertJob" function.

Lua upsertJob function:
- HMSET the waiting hash at id, setting all the fields with update<Field> true
- HSETNX the waiting hash at id, setting each field with update<Field> false
- Check if `active_job:{id}` EXISTS.
- If it does, set `score` to `-runAt`, otherwise set it to `runAt`
- Call addToWaiting with id, score and the updateRunAt flag

Lua addToWaiting function:
- ZADD id, score to the waiting zset, with a flag depending on the value of updateRunAt:
  - false: NX
  - true: no flag
  - 'ifLater': GT if score is positive, LT otherwise
  - 'ifEarlier': LT if score is positive, GT otherwise

### Canceling a job

Lua cancelWaiting function:
- ZREM from the waiting zset
- DEL the waiting hashmap key

### De-queuing and locking N jobs

Lua dequeue function:

- Use ZRANGE to get jobs with the lowest positive scores from the waiting zset
- ZREM them from the waiting zset
- RENAME the job hashes from waiting to active
- Append the worker_id to the job ids to calculate members of the active zset
- ZADD these to the active zset using now + heartbeat_timeout as score

### Heartbeats to extend a lock

In JS:
- Call the Lua function. If it returns a null, kill the worker thread, as the lock was lost.

Lua heartbeat function:
- ZADD {id}:{worker_id} to the active zset using now + heartbeat_timeout as the score,
  with the XX flag to do nothing if it doesn't exist in the set.

### Job success

Lua "clearActive" function:

- ZREM the completed item from the active queue.
- DEL the active hashmap key
- ZSCORE to get any entry in the waiting queue with the same id
- If it exists, HGET its "updateRunAt" value from the waiting hset
- Call addToWaiting with id, score and updateRunAt


### Job failure (retriable)

In JS:
- Calculate nextRunAt and call the Lua onRetry function with it

Lua "onRetry" function:
- ZREM the completed item from the active queue.
- Use HGET from active hash to get failureCount and maxFailures
- If failureCount = maxFailures, call onFailure
- Otherwise, HINCRBY failureCount and call doRetry

Lua "doRetry" function:
- ZSCORE to get the score of any entry in the waiting queue with the same id, flip its sign and store in blockedRunAt
- If it exists, HGETALL all its data from waiting hashmap into local blockedJobData
- RENAME the active hashmap key to waiting
- ZADD the id back into the waiting queue, with nextRunAt as score
- Call upsertJob() with blockedJobData and blockedRunAt

### Job failure (permanent)

Lua "onFailure" function:
- Check if the key `{queue}-fail:waiting` EXISTS, to see if failure handlers have been registered by any worker.
- If it does, HGETALL job properties, and call invoke the upsertJob function for the fail queue
- Call clearActive

### Processing stalled jobs

Lua clearStall function:
- Use ZRANGE in the active zset to get jobs that have stalled
- Parse out their ids and use HGET to find their stallCounts and maxStalls
- For those with stallCount = maxStall, call onFailure
- For others, HINCRBY stallCount and call doRetry
