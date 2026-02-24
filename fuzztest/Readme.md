# Queasy Fuzz Test Plan

A long-running end-to-end fuzz test that simulates random failures and continuously verifies core system invariants.

## Invariants Verified

1. **Mutual exclusion**: Two jobs with the same Job ID are never processed by different clients or worker threads simultaneously.
2. **No re-processing of successful jobs**: A job that has succeeded is never processed again.
3. **Scheduling**: No job is processed before its `run_at` time.
4. **Priority ordering within a queue**: No job starts processing while another job in the same queue with a lower `run_at` is still waiting (i.e., eligible jobs are dequeued in order).
5. **Fail handler completeness**: If a fail handler is registered, every job that does not eventually succeed MUST result in the fail handler being invoked.
6. **Queue progress (priority starvation prevention)**: Non-empty queues at the highest priority level always make progress. When they drain, queues at the next priority level begin making progress.

## Structure Overview

```
fuzztest/
  Readme.md              # This file
  fuzz.js                # Orchestrator: spawns child processes, monitors shared state
  process.js             # Child process: sets up clients and listens on all queues
  handlers/
    periodic.js          # Re-queues itself; dispatches cascade jobs; occasionally stalls/crashes
    cascade-a.js         # Dispatched by periodic; dispatches into cascade-b
    cascade-b.js         # Dispatched by cascade-a; final handler
    fail-handler.js      # Shared fail handler for all queues; records invocations
  shared/
    state.js             # In-process shared state helpers (for the orchestrator)
    log.js               # Structured logger (writes to fuzz-output.log, never throws)
```

## Process Architecture

The orchestrator (`fuzz.js`) spawns **N child processes** (default: 4). Each child process creates one Redis client and calls `listen()` on every queue. The orchestrator itself does not process jobs — it only monitors invariants and manages the lifecycle.

Handlers write events (job start, finish, fail, stall) directly to a Redis stream (`fuzz:events`). The orchestrator reads from this stream and maintains a shared in-memory log of events, checking invariants after each one. Child processes do not need to forward events to the orchestrator themselves — the stream is the shared channel.

Child processes are deliberately killed and restarted periodically to simulate crashes. A killed process' checked-out jobs will be swept and retried/failed by the remaining processes.

## Queue Configuration

Three queues at different priority levels, all listened on by every child process. Parameters are kept small to produce many events quickly:

| Parameter | `{fuzz}:periodic` | `{fuzz}:cascade-a` | `{fuzz}:cascade-b` |
|---|---|---|---|
| Handler | `periodic.js` | `cascade-a.js` | `cascade-b.js` |
| Priority | 300 | 200 | 100 |
| `maxRetries` | 3 | 3 | 3 |
| `maxStalls` | 2 | 2 | 2 |
| `minBackoff` | 200 ms | 200 ms | 200 ms |
| `maxBackoff` | 2 000 ms | 2 000 ms | 2 000 ms |
| `timeout` | 3 000 ms | 3 000 ms | 3 000 ms |
| `size` | 10 | 10 | 10 |
| `failHandler` | `fail-handler.js` | `fail-handler.js` | `fail-handler.js` |
| `failRetryOptions.maxRetries` | 5 | 5 | 5 |
| `failRetryOptions.minBackoff` | 200 ms | 200 ms | 200 ms |

The short `timeout` (3 s) means stalling jobs are detected and swept quickly. The short `minBackoff` / `maxBackoff` window (200 ms – 2 s) means retries cycle fast. With `maxRetries: 3` and `maxStalls: 2`, most failed jobs reach the fail handler within seconds.

## Periodic Jobs (Seed)

A fixed set of periodic job IDs (e.g., `periodic-0` through `periodic-4`) are dispatched by the orchestrator at startup. Each periodic handler:

1. Records the current processing event by writing `{ type: 'start', queue, id, threadId, clientId, startedAt }` to the `fuzz:events` Redis stream.
2. Optionally sleeps for a random short delay.
3. Dispatches a cascade-a job with a unique ID and a `runAt` randomly up to 2 seconds in the future.
4. Re-dispatches itself (same job ID, `updateRunAt: true`) with a delay of 1–5 seconds, so the job continues to fire periodically.
5. On success, writes `{ type: 'finish', queue, id, threadId, clientId, finishedAt }` to the `fuzz:events` stream.

The fail handler for periodic jobs also re-dispatches the same periodic job ID (with a delay), ensuring periodic jobs survive permanent failures. This lets the orchestrator assert that periodic jobs keep running indefinitely.

## Cascade Jobs

`cascade-a.js`:
- Records start/finish events.
- Dispatches one or two `cascade-b` jobs with unique IDs.
- Subject to all chaos behaviors (see below).

`cascade-b.js`:
- Records start/finish events.
- Terminal handler; does not dispatch further jobs.
- Subject to all chaos behaviors (see below).

## Chaos Behaviors

All handlers are subject to all chaos behaviors. The probabilities below are per-invocation and apply uniformly across `periodic.js`, `cascade-a.js`, and `cascade-b.js`:

| Behavior | Probability | Notes |
|---|---|---|
| Normal completion | ~65% | Dispatches downstream jobs (if any), then returns |
| Retriable error (throws `Error`) | ~15% | No downstream dispatch |
| Permanent error (throws `PermanentError`) | ~5% | No downstream dispatch |
| Stall (returns a never-resolving promise) | ~10% | Detected after `timeout` (3 s); counts as a stall |
| CPU spin (blocks the worker thread) | ~3% | Tight loop until the process detects the hang and kills the thread (via `timeout`) |
| Crash (causes the child process to exit) | ~2% | Handler writes a "crash-me" flag to Redis; main thread polls and exits |

With `timeout: 3000`, stalling and spinning jobs are swept within ~3–13 seconds (timeout + heartbeat sweep interval). With `maxStalls: 2`, two stalls exhaust the stall budget and the job is sent to the fail handler, cycling fast.

When a child process crashes, the orchestrator detects the exit event and restarts a new child process after a short delay.

## Event Logging and Invariant Checking

The orchestrator maintains an append-only in-memory event log. Each entry contains:
```js
{ type, queue, id, threadId, clientId, timestamp }
```
where `type` is one of: `start`, `finish`, `fail`, `stall`, `cancel`.

After each event is appended, the orchestrator runs incremental invariant checks:

### Invariant 1: Mutual Exclusion
Maintain a `Map<jobId, { clientId, threadId, startedAt }>` of currently-active jobs. On `start`, check that the job ID is not already in the map. On `finish`/`fail`/`stall`, remove it.

If a `start` event arrives for a job ID already in the map → **VIOLATION**.

### Invariant 2: No Re-processing of Succeeded Jobs
Maintain a `Set<jobId>` of successfully finished job IDs. On `start`, check that the ID is not in this set.

If a `start` event arrives for a job ID in the succeeded set → **VIOLATION**.

Note: Re-processing after a stall or retry is expected and must not be flagged.

### Invariant 3: Scheduling (No Early Processing)
Each `start` event includes `startedAt` (wall clock). Each job dispatch records an intended `runAt`. On `start`, verify `startedAt >= runAt - CLOCK_TOLERANCE_MS`.

If `startedAt < runAt - CLOCK_TOLERANCE_MS` → **VIOLATION**.

`CLOCK_TOLERANCE_MS` accounts for clock skew between the orchestrator, child processes, and Redis (default: 100ms).

### Invariant 4: Priority Ordering
Track the earliest-known `runAt` for jobs dispatched into each queue but not yet started. When a `start` event arrives for a job in that queue, verify no other eligible job (with `runAt <= now`) in the same queue has a lower `runAt` that has been waiting longer.

This invariant is best-effort and checked with a configurable lag (e.g., 200ms) to account for the inherent race between dequeue polling and dispatch. A violation is only flagged when the ordering difference exceeds this lag.

### Invariant 5: Fail Handler Completeness
Track every job that has been dispatched (by ID). When a job exceeds its `maxRetries` or receives a permanent error, a fail event should be observed. Maintain a map `{ jobId → { exhausted: bool, failSeen: bool } }`. After a configurable drain period (e.g., 30 seconds after a queue goes quiet), check that every exhausted job has a corresponding `fail` event.

### Invariant 6: Queue Progress
The orchestrator monitors the time since the last `start` event per queue. If a queue is known to be non-empty (based on dispatched vs finished counts) and no `start` event has been seen for more than a configurable `STALL_THRESHOLD_MS` (e.g., 30 seconds), flag a progress violation.

Priority starvation is checked by verifying that the low-priority queue does not process jobs while the high-priority queue has outstanding jobs older than the dequeue poll interval.

## Output and Reporting

Violations are logged to stdout and to `fuzz-output.log` with full context. The process does **not** exit on a violation — it logs and continues, accumulating a count of violations. A summary is printed periodically (every 60 seconds) and on `SIGINT`.

Log format (newline-delimited JSON):
```json
{ "time": "...", "level": "info|warn|error", "msg": "...", "data": { ... } }
```

Violation entries use level `"error"` and include the invariant name, the offending event, and relevant recent history.

## Configuration

All tunable parameters live at the top of `fuzz.js` as named constants:

```js
const NUM_PROCESSES = 4;         // Child processes
const NUM_PERIODIC_JOBS = 5;     // Fixed periodic job IDs
const PERIODIC_MIN_DELAY = 1000; // ms before re-queuing self
const PERIODIC_MAX_DELAY = 5000;
const CRASH_INTERVAL_MS = 30000; // Orchestrator kills a random child process this often
const CLOCK_TOLERANCE_MS = 100;
const STALL_THRESHOLD_MS = 30000;
const PRIORITY_LAG_MS = 200;
const LOG_FILE = 'fuzz-output.log';
```

## Running

The fuzz test is separate from the default test suite and is never run by `npm test`. It is started manually:

```sh
node fuzztest/fuzz.js
```

It runs indefinitely. Stop with `Ctrl+C`. A summary of violations and events processed will be printed on exit.

## Notes on Implementation

- Child processes use the `queasy` library's public API (`queue()`, `dispatch()`, `listen()`). They do not talk directly to Redis.
- The orchestrator does not import from `src/`; it only spawns child processes and learns about child process lifecycle only from the `spawn` and `exit` events.
- All handler modules in `fuzztest/handlers/` must be self-contained ESM modules that can be passed as `handlerPath` to `queue.listen()`.
- Handlers write events to the `fuzz:events` Redis stream using a dedicated Redis client created at handler module load time. The orchestrator reads from this stream via `XREAD BLOCK`. This is the only communication channel between handlers and the orchestrator — no IPC is used.
- The chaos crash behavior must be triggered from the child process's main thread, not from inside a handler's worker thread. To simulate a crash, the handler uses `postMessage` to send a `{ type: 'crash' }` message to the main thread, which listens for it and calls `process.exit()`.
