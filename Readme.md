# Queasy 🤢

A Redis-backed job queue for Node.js, featuring (in comparison with design inspiration BullMQ):

- **Singleton jobs**: Guarantees that no more than one job with a given ID is being processed at any time, without trampolines or dropping jobs (“unsafe deduplication”).
- **Fail handlers**: Guaranteed at-least-once handlers for failed or stalled jobs, enabling reliable periodic jobs without a external scheduling or “reviver” systems.
- **Instant config changes**: Most configuration changes take effect immediately no matter the queue length, as they apply at dequeue time.
- **Worker threads**: Jobs are processed in worker threads, preventing main process stalling and failing health checks due to CPU-bound jobs 
- **Capacity model**: Worker capacity flexibly shared between heterogenous queues based on priority and demand, rather than queue-specific “concurrency”.
- **Job timeout**: Timed out jobs are killed by draining and terminating the worker threads it runs on
- **Zombie protection**: Clients that have lost locks while stalled before recovering detect this and terminate themselves immediately
- **Fine-grained updates**: Control over individual attributes when one job updates another with the same ID

### Terminology

A _client_ is an instance of Queasy that connects to a Redis database. A _job_ is the basic unit of work that is _dispatched_ into a _queue_.

A _handler_ is JavaScript code that performs work. There are two kinds of handlers: _task handlers_, which process jobs, and _fail handlers_, which are invoked when a job fails permanently. Handlers run on _workers_, which are Node.js worker threads. By default, a Queasy client automatically creates one worker per CPU.

### Job attributes

- `id`: string; generated if unspecified. See _update semantics_ below for more information.
- `data`: a JSON-serializable value passed to handlers
- `runAt`: number; a unix timestamp, to delay job execution until at least that time
- `retryCount`: number; how many times has this job been retried for any reason?
- `stallCount`: number; how many times did the client processing this job stop sending heartbeats?
- `timeoutCount`: number; how many times did this job fail to complete in the allocated time?

### Job lifecycle

1. A job when first dispatched is created in the _waiting_ state, unless there is a currently active job with the same ID. In that case, it is created in the _blocked_ state.
2. Jobs are dequeued from the _waiting_ state and enter the _active_ state.
3. When an active job finishes or fails permanently, it is deleted. Any blocked job with the same ID then moves to the _waiting_ state. (A new job may also be added to the separate fail queue.) 
4. When an active job stalls or fails (with retries left), it returns to the _waiting_ state. Any blocked job with the same ID then updates this waiting job.

### Worker capacity

When the client is created, a pool of worker threads are created. Every worker is initialized with 100 units of _capacity_. When a handler is registered, it specifies its _size_ using the same units. When a worker starts processing a job with that handler, its capacity is decreased by this size; this is reversed when that job completes or fails.

Queues are dequeued based on their priority and the ratio of available capacity to handler size.

### Timeout handling

When a worker start processing a job, a timer is started; if the job completes or throws, the timer is cleared. If the timeout occurs, the job is marked stalled and the worker is removed from the pool so it no longer receives new jobs. A new worker is also created and added to the pool to replace it. 

The unhealthy worker (with at least one stalled job) continues to run until it has *only* stalled jobs remaining. When this happens, the worker is terminated, and all its stalled jobs are retried.

### Stall handling

The client (in the main thread) sends periodic heartbeats to Redis for each queue it’s processing. If heartbeats from a client stop, a Lua script in Redis removes this client and returns all its active jobs into the waiting state with their stall count property incremented.

When a job is dequeued, if its stall count exceeds the configured maximum, it is immediately considered permanently failed; its task handler is not invoked.

The response of the heartbeat Lua function indicates whether the client had been removed due to an earlier stall; if it receives this response, the client terminates all its worker threads immediately and re-initializes the pool and queues.

## API

### `client(redisConnection, workerCount)`
Returns a Queasy client.
- `redisConnection`: a node-redis connection object.
- `workerCount`: number; Size of the worker pool. If 0, or if called in a queasy worker thread, no pool is created. Defaults to the number of CPUs.

The client object returned is an EventEmitter, which emits a 'disconnect' event when it fails permanently for any reason, such as library version mismatch between different workers connected to the same Redis insance, or a lost locks situation. When this happens, in general the application should exit the worker process and allow the supervisor to restart it.

### `client.queue(name)`

Returns a queue object for interacting with this named queue at the defined Redis server.
- name is a string queue name. Redis data structures related to a queue will be placed on the same node in a Redis cluster.

### `queue.dispatch(data, options)`

Adds a job to the queue. `data` may be any JSON value, which will be passed unchanged to the workers. Options may include:
- `id`: alphanumeric string; if not provided, a unique random string is generated
- `runAt`: number; wall clock timestamp before which this job must not be run; default: 0

The following options take effect if an `id` is provided, and it matches that of a job already in the queue.
- `updateData`: boolean; whether to replace the data of any waiting job with the same ID; default: true
- `updateRunAt`: boolean | 'ifLater' | 'ifEarlier'; default: true
- `resetCounts`: boolean; Whether to reset the retry, timeout and stall counts to 0; default: same as updateData

Returns a promise that resolves to the job ID when the job has been added to Redis.

### `queue.cancel(id)`

Removes the job with the given ID if it exists in the waiting state, no-op otherwise.

Returns a promise that resolves to a boolean (true if a job with this ID existed and has been removed).

### `queue.listen(handler, options)`
Attaches handlers to a queue to process jobs that are added to it.
- `handler`: The path to a JavaScript module that exports the task handler

The following options control retry behavior:
- `maxRetries`: number; default: 10
- `maxStalls`: number; default: 3
- `maxTimeouts`: number, default: 3
- `minBackoff`: number; in milliseconds; default: 2,000
- `maxBackoff`: number; default: 300,000
- `size`: number; default: 10
- `timeout`: number; in milliseconds; default: 60,000
- `priority`: number; higher numbers are processed first

Additional options affect failure handling:
- `failHandler`: The path to a JavaScript module that exports the handler for failure jobs
- `failRetryOptions`: Retry options (as above) for the failure jobs

## Handlers

Every handler module must have a named export `handle`, a function that is called with each job.

### Task handlers

They receive two arguments:
- `data`, the JSON value passed to dispatch
- `job`, a Job object containing other job attributes (excluding data)

This function may throw (or return a Promise that rejects) to indicate job failure. If the thrown error contains
a property `kind` with the value `permanent`, or if `maxRetries` has been reached, the job is not retried. 
Otherwise, the job is queued to be retried with `retryCount` incremented.

If the thrown error has a property `retryAt`, the job’s `runAt` is set to this value; otherwise, it’s set using
the exponential backoff algorithm.

If it returns any value apart from a Promise that rejects, the job is considered to have completed successfully.

### Failure handlers

This function receives three arguments:
- `data`, a tuple (array) containing three items:
  - `originalData`
  - `originalJob`
  - `error`, a JSON object with the name, message and kind properties of the error thrown by the final call to handle. Kind might be `permanent`, `retriable` or `stall`. In case of stall, the name property is either `StallError` or `TimeoutError`.
- `job`, details of the failure handling job

If this function throws an error (or returns a Promise that rejects), it is retried using exponential backoff.
