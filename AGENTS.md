# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
npm test                  # Run all tests (requires Redis on localhost:6379)
npm test -- --test-name-pattern="pattern"  # Run tests matching a pattern
node --test test/queue.test.js             # Run a single test file
npm run lint              # Lint with Biome
npm run format            # Auto-format with Biome
npm run typecheck         # TypeScript check via jsconfig.json (JSDoc types)
npm run docker:up         # Start Redis via Docker Compose
npm run docker:down       # Stop Redis
```

Tests require a running Redis instance. Use `docker:up` first if needed.

## Architecture

Queasy is a Redis-backed job queue with **at-least-once** delivery semantics. The core logic lives in two layers:

- **JS layer** (`src/queue.js`): The `queue()` factory returns `{ dispatch, cancel, listen }`. On first use, it uploads the Lua script to Redis via `FUNCTION LOAD REPLACE`. A `WeakSet` (`initializedClients`) tracks which Redis clients have already had the functions loaded. `listen()` is currently a TODO stub.
- **Lua layer** (`src/queasy.lua`): All queue state mutations are atomic Redis functions registered under the `queasy` library. No queue logic should be duplicated in JS — the Lua functions are the single source of truth for state transitions.

### Redis data structures

All keys for a queue named `foo` share the `{foo}` hash tag for cluster compatibility:

| Key pattern | Type | Purpose |
|---|---|---|
| `{foo}:waiting` | Sorted set | Waiting jobs. Score = `run_at`. Blocked jobs have score `-run_at` (negative, so they're excluded from dequeue). |
| `{foo}:active` | Sorted set | Active jobs. Member = `{id}:{worker_id}`, score = heartbeat deadline (`now + 10s`). |
| `{foo}:waiting_job:{id}` | Hash | Metadata for a waiting job. |
| `{foo}:active_job:{id}` | Hash | Metadata for an active job (same fields, moved via RENAME on dequeue). |

### Job lifecycle

`dispatch` → waiting set → `dequeue` → active set → `finish` (success) or `retry`/`fail` (failure) or `sweep` (stall)

Key state transitions in Lua:
- **Blocked jobs**: When dispatching a job whose `id` already has an active job, the waiting entry gets a negative score to prevent dequeue. On `finish` or `do_retry`, the negative score is flipped positive to unblock.
- **Retry**: `do_retry` RENAMEs `active_job` back to `waiting_job` and ZADDs with a backoff `next_run_at`. If a blocked waiting job exists for the same id, it is re-dispatched after the retry.
- **Permanent failure**: `fail` checks if a `{queue}-fail:waiting` key exists (i.e., a failure queue has been dispatched to). If so, it creates a new job there with `data = [original_id, job_data, error]`, then calls `finish` to clean up.
- **Stall detection**: `sweep` scans the active set for entries whose heartbeat deadline has passed, then calls `handle_stall` which increments `stall_count` and either retries or calls `fail`.

### Heartbeats

`ACTIVE_TIMEOUT_MS = 10000` (10s) in Lua. The JS side sends `queasy_bump` calls at `HEARTBEAT_INTERVAL = 5000` (5s) to keep active jobs alive.

### Update semantics on re-dispatch

When dispatching a job with an existing `id`, the `update_data`, `update_run_at`, `update_retry_strategy`, and `reset_counts` flags control which fields are overwritten vs. preserved (HSET vs. HSETNX in Lua). If the job is blocked (active copy exists), these flags are stored in the waiting hash and re-applied when the blocked job is eventually dispatched.

## Lua function reference

| Function | Keys | Args | When called |
|---|---|---|---|
| `queasy_dispatch` | waiting, active | id, run_at, data, retry opts, update flags | Adding/updating a job |
| `queasy_cancel` | waiting | id | Removing a waiting job |
| `queasy_dequeue` | waiting, active | worker_id, now, limit | Polling for ready jobs |
| `queasy_bump` | active | id, worker_id, now | Heartbeat keepalive |
| `queasy_finish` | waiting, active | id, worker_id | Job completed successfully |
| `queasy_retry` | waiting, active | id, worker_id, next_run_at, error | Job failed, may retry or fail permanently |
| `queasy_fail` | waiting, active | id, worker_id, error | Permanent failure |
| `queasy_sweep` | waiting, active | now, next_run_at | Detecting stalled jobs |

## Test structure

- `test/redis-functions.test.js` — Tests Lua functions directly via `redis.fCall()`. Exercises all state transitions at the Redis level.
- `test/queue.test.js` — Tests the JS `queue()` API (dispatch, cancel, listen). The `listen()` tests use fixture handlers in `test/fixtures/`.
- `test/fixtures/` — Minimal handler modules (`success-handler`, `failure-handler`, `slow-handler`, `data-logger-handler`, `permanent-error-handler`). Each exports a `handle(data, job)` function.

Queue names in tests use `{curly-brace}` syntax (e.g., `{test-api-queue}`) to keep all related keys on the same Redis node.

## Conventions

- ESM modules throughout (`"type": "module"` in package.json).
- JSDoc types with a `types.ts` file for IDE support; run `npm run typecheck` to verify.
- Biome for formatting (tabs, single quotes, 100-char width). Run `npm run format` before committing.
- All Lua booleans arrive as strings (`'true'`/`'false'`) from `redis.fCall` — comparisons in Lua must use string equality.
- `redis.setresp(3)` is called in each registered function to get RESP3 map responses (needed for `HGETALL` returning `{ map: {...} }` instead of a flat array).
