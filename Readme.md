# Queasy 🤢

A Redis-backed job queue for Node.js

### Terminology

A _client_ is an instance of Quesy that connects to a Redis database. A _job_ is the basic unit of work, and it is _dispatched_ into a _queue_.

A _handler_ is a function that carries out some work. There are two kinds of handlers: _task handlers_, which process jobs, and _fail handlers_, which are invoked when a job fails permanently. Handlers run on _workers_, which are Node.js worker threads. By default, a Queasy client automatically creates one worker per CPU.

### Job properties

These properties may be specified during dispatch:
- `id`: string; generated if unspecified. See _update semantics_ below for more information.
- `data`: a JSON-serializable value passed to handlers
- `runAt`: number; a unix timestamp, to delay job execution until at least that time
- `updateRunAt`, `updateData`, `resetCounts`: 

### Job lifecycle

A job when first dispatched is created in the _waiting_ state, unless another job 

### Updating jobs

A job ID may be used to 


The handler for each job will be invoked at least once. If it throws an error, or if the process crashes or becomes unresponsive, the task will be retried a certain number of times. 

### Failure handling

- If the task handler throws an error jobs on failure, with exponential backoff 
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

This library is NOT resilient to Redis failures. If the Redis instance crashes, jobs may crash, multiple workers might be assigned the same job, etc. If that makes you queasy, this is not the queue library for you.

## API

### `client(redisConnection, workerCount)`
Returns a Queasy client.
- `redisConnection`: a node-redis connection object.
- `workerCount`: number; Size of the worker pool. If 0, or if called in a queasy worker thread, no pool is created. Defaults to the number of CPUs. 


### `client.queue(name, defaultJobOptions, failureJobOptions)`

Returns a queue object for interacting with this named queue at the defined Redis server.
- name is a string queue name. Redis data structures related to a queue will be placed on the same node in a Redis cluster.
- defaultJobOptions are defaults for the options argument to queue.dispatch() below, except for `runAt`
- failureJobOptions are default options for jobs used to invoke failure handlers, except for `runAt`.

### `queue.dispatch(data, options)`

Adds a job to the queue. `data` may be any JSON value, which will be passed unchanged to the workers. Options may include:
- `id`: alphanumeric string; if not provided, a unique random string is generated
- `runAt`: number; wall clock timestamp before which this job must not be run; default: 0

The following options take effect if an `id` is provided, and it matches that of a job already in the queue.
- `updateData`: boolean; whether to replace the data of any waiting job with the same ID; default: true
- `updateRunAt`: boolean | 'ifLater' | 'ifEarlier'; default: true
- `updateRetryStrategy`: boolean; whether to replace `maxRetries`, `maxStalls`, `minBackoff` and `maxBackoff`
- `resetCounts`: boolean; Whether to reset the internal failure and stall counts to 0; default: same as updateData

Returns a promise that resolves to the job ID when the job has been added to Redis.

### `queue.cancel(id)`

Removes the job with the given ID if it exists in the waiting state, no-op otherwise.

Returns a promise that resolves to a boolean (true if a job with this ID existed and has been removed).

### `queue.listen(handler, options)`
Attaches handlers to a queue to process jobs that are added to it.
- `handler`: The path to a JavaScript module that exports the task (and, optionally, failure) handlers

The following options control retry behavior:
- `maxRetries`: number; default: 10
- `maxStalls`: number; default: 3
- `minBackoff`: number; in milliseconds; default: 2000
- `maxBackoff`: number; default: 300_000

## Handlers

The handler module must have a named export `handle`, a function that is called with each job. It may additionally have a named export 
`handleFailure`, which is called if the job fails permanently.

### `handle`

It receives two arguments:
- `data`, the JSON value passed to dispatch
- `job`, an object contains all the job options as well as `failureCount` and `stallCount`

This function may throw (or return a Promise that rejects) to indicate job failure. If the thrown error is an
instance of `PermanentError`, or if `maxRetries` has been reached, the job is not retried. Otherwise, the job
is queued to be retried with `maxRetries` incremented.

If the thrown error has a property `retryAt`, the job’s `runAt` is set to this value; otherwise, it’s set using
the exponential backoff algorithm.

If it returns any value apart from a Promise that rejects, the job is considered to have completed successfully.

### `handleFailure`

This function receives three arguments:
- `data`, the JSON value passed to dispatch
- `job`
- `error`, a JSON object with a copy of the enumerable properties of the error thrown by the final call to handle,
  or an instance of `StallError` if the final call to handle didn’t return or throw.

If this function throws an error (or returns a Promise that rejects), it is retried using exponential backoff. It
is expected that failure jobs are retried for a long time (e.g. days).
