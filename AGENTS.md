This file provides guidance to coding agents working in this repository.

## Commands

```sh
npm test                  # Run all tests (requires Redis on localhost:6379)
npm test -- --test-name-pattern="pattern"  # Run tests matching a pattern
npm test -- test/queue.test.js             # Run a single test file
npm run lint              # Lint with Biome
npm run format            # Auto-format with Biome
npm run typecheck         # TypeScript check via jsconfig.json (JSDoc types)
npm run docker:up         # Start Redis via Docker Compose
npm run docker:down       # Stop Redis
```

Tests require a running Redis instance. Use `docker:up` first if needed.

## Architecture

Queasy is a Redis-backed job queue for Node.js with **at-least-once** delivery semantics, singleton jobs (only one active job per ID), fail handlers, and worker-thread-based processing.

### JS layer

The JS side is split across several modules:

- **`src/client.js`** (`Client` class): Top-level entry point. Wraps a `node-redis` connection, loads the Lua script into Redis via `FUNCTION LOAD REPLACE` on construction, and manages named `Queue` instances. Generates a unique `clientId` for heartbeats. All Redis `fCall` invocations live here (`dispatch`, `cancel`, `dequeue`, `finish`, `fail`, `retry`, `bump`). Exported from `src/index.js`.
- **`src/queue.js`** (`Queue` class): Represents a single named queue. Holds dequeue options and handler path. `listen()` attaches a handler and starts a `setInterval` polling loop that calls `dequeue()`. `dequeue()` checks pool capacity, fetches jobs from Redis, and processes each via the pool. Handles retry/fail logic (backoff calculation, stall-count checks) on the JS side.
- **`src/pool.js`** (`Pool` class): Manages a set of `Worker` threads. Each worker has a `capacity` (default 100 units). `process()` picks the worker with the most spare capacity, posts the job, and returns a promise. Handles job timeouts: a timed-out job marks the worker as unhealthy, replaces it with a fresh one, and terminates the old worker once only stalled jobs remain.
- **`src/worker.js`**: Runs inside a `Worker` thread. Receives `exec` messages, dynamically imports the handler module, calls `handle(data, job)`, and posts back `done` messages (with optional error info).
- **`src/constants.js`**: Default retry options, heartbeat/timeout intervals, worker capacity, dequeue polling interval.
- **`src/errors.js`**: `PermanentError` (thrown to skip retries) and `StallError`.
- **`src/utils.js`**: `generateId()` helper.
- **`src/types.ts`**: JSDoc type definitions for IDE support (not runtime code).

### Lua layer (`src/queasy.lua`)

All queue state mutations are atomic Redis functions registered under the `queasy` library. The Lua functions are the single source of truth for state transitions — no queue logic should be duplicated in JS.

Each registered function calls `redis.setresp(3)` for RESP3 typed responses.

### Redis data structures

All keys for a queue named `foo` share the `{foo}` hash tag for cluster compatibility:

| Key pattern | Type | Purpose |
|---|---|---|
| `{foo}` | Sorted set | Waiting jobs. Score = `run_at`. Blocked jobs have score `-run_at` (negative, so they're excluded by the `ZRANGEBYSCORE 0 now` in dequeue). |
| `{foo}:expiry` | Sorted set | Client heartbeat expiries. Member = `client_id`, score = expiry timestamp. |
| `{foo}:checkouts:{client_id}` | Set | Job IDs currently checked out by this client. |
| `{foo}:waiting_job:{id}` | Hash | Metadata for a waiting job (`id`, `data`, `retry_count`, `stall_count`, and update flags if blocked). |
| `{foo}:active_job:{id}` | Hash | Metadata for an active job (same fields, moved via `RENAME` on dequeue). |

### Job lifecycle

`dispatch` → waiting set → `dequeue` → active (hash renamed, added to client's checkouts) → `finish` (success) or `retry` (retriable failure) or stall (via `sweep`)

Key state transitions in Lua:
- **Blocked jobs**: When dispatching a job whose `id` already has an `active_job` hash, the waiting entry gets a negative score to prevent dequeue. On `finish`, the negative score is flipped positive to unblock.
- **Retry** (`do_retry`): `RENAME`s `active_job` back to `waiting_job` and `ZADD`s with the backoff timestamp. If a blocked waiting job exists for the same id, its saved update flags are re-applied.
- **Permanent failure** (`fail`): Dispatches a new job into the fail queue (`{name}-fail`) with `data = [original_id, job_data, error]`, then calls `finish` to clean up.
- **Stall detection** (`sweep`): Embedded inside `bump` and `dequeue` — not a standalone registered function. Finds clients in the expiry set whose score is <= `now`, retrieves their checkouts via `SMEMBERS`, calls `handle_stall` on each job, then cleans up the client.

### Heartbeats

Heartbeats are **per-client, per-queue** (not per-job). The JS side sends `queasy_bump` calls at `HEARTBEAT_INTERVAL = 5000ms`. The Lua side stores client expiry in the `{queue}:expiry` sorted set with a score of `now + HEARTBEAT_TIMEOUT` (10000ms). If a client's expiry passes, `sweep` returns all its checked-out jobs to the waiting state with incremented `stall_count`.

`bump` returns `0` if the client was already swept (removed from the expiry set), signaling the JS side that it has been evicted.

### Update semantics on re-dispatch

When dispatching a job with an existing `id`, the `update_data`, `update_run_at`, and `reset_counts` flags control which fields are overwritten vs. preserved (`HSET` vs. `HSETNX` in Lua). If the job is blocked (active copy exists), these flags are stored in the waiting hash and re-applied when the blocked job is eventually retried via `do_retry`.

## Lua function reference

| Function | Keys | Args | Purpose |
|---|---|---|---|
| `queasy_dispatch` | `{queue}` | id, run_at, data, update_data, update_run_at, reset_counts | Add/update a waiting job |
| `queasy_dequeue` | `{queue}` | client_id, now, expiry, limit | Dequeue ready jobs; also triggers sweep |
| `queasy_cancel` | `{queue}` | id | Remove a waiting job |
| `queasy_bump` | `{queue}` | client_id, now, expiry | Client heartbeat; also triggers sweep. Returns 0 if client was evicted. |
| `queasy_finish` | `{queue}` | id, client_id, now | Job completed; unblocks waiting job if any |
| `queasy_retry` | `{queue}` | id, client_id, retry_at, now | Retriable failure; increments retry_count, calls do_retry |
| `queasy_fail` | `{queue}`, `{fail_queue}` | id, client_id, fail_job_id, fail_job_data, now | Permanent failure; dispatches fail job, then finishes original |
| `queasy_version` | (none) | (none) | Returns library version (currently 1) |

Note: `sweep`, `do_retry`, `handle_stall`, `finish`, `dispatch` also exist as internal Lua helpers but are not registered as Redis functions.

## Test structure

- `test/redis-functions.test.js` — Tests Lua functions directly via `redis.fCall()`. Exercises all state transitions at the Redis level.
- `test/queue.test.js` — Tests the JS `Client`/`Queue` API (dispatch, cancel, listen). The `listen()` tests use fixture handlers in `test/fixtures/`.
- `test/fixtures/` — Minimal handler modules. Each exports a `handle(data, job)` function.

Queue names in tests use `{curly-brace}` syntax (e.g., `{test-api-queue}`) to keep all related keys on the same Redis node.

## Conventions

- ESM modules throughout (`"type": "module"` in package.json).
- JSDoc types with a `types.ts` file for IDE support; run `npm run typecheck` to verify.
- Biome for formatting (tabs, single quotes, 100-char width). Run `npm run format` before committing.
- Lua booleans are passed as strings (`'true'`/`'false'`) between JS and Lua — comparisons in Lua use string equality (e.g., `args[4] == 'true'`).
- `redis.setresp(3)` is called in each registered Lua function for RESP3 typed responses. This means `HGETALL` returns `{ map = {...} }`, `SMEMBERS` returns `{ set = {...} }`, `ZSCORE` returns `{ double = number }`, etc.
