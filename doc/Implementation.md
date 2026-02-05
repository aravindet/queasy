## Implementation

### Data structures

Two sorted sets: `{queue}:waiting` and `{queue}:active`  
Two hash families: `{queue}:waiting_job:{id}` and `{queue}:active_job:{id}`

Hash fields: `data`, `max_retries`, `max_stalls`, `min_backoff`, `max_backoff`, `retry_count`, `stall_count`, and update flags (`update_data`, `update_run_at`, `update_retry_strategy`, `reset_counts`)

**Waiting set**: members are job `id`, scores are `run_at` (or `-run_at` for blocked jobs)  
**Active set**: members are `{id}:{worker_id}`, scores are heartbeat deadlines

**Blocked jobs**: jobs with an active job sharing the same `id` (score is negative to prevent dequeuing)

### Adding a job (dispatch)

- HSET/HSETNX fields in `waiting_job:{id}` based on update flags
- Check if `active_job:{id}` EXISTS to determine if blocked
- Set score to `-run_at` if blocked, else `run_at`
- If blocked, save update flags in waiting hash for later application
- ZADD to waiting set with appropriate flags (NX, GT, LT, or none) based on `update_run_at`

### Canceling a job (cancel)

- ZREM from waiting set
- DEL `waiting_job:{id}`

### Dequeuing jobs (dequeue)

- ZRANGEBYSCORE on waiting set for positive scores ≤ now
- For each job: ZREM from waiting set, RENAME `waiting_job:{id}` to `active_job:{id}`
- ZADD `{id}:{worker_id}` to active set with score = now + 10000ms

### Heartbeat (bump)

- ZADD `{id}:{worker_id}` to active set with score = now + 10000ms and XX flag (update only if exists)

### Job completion (finish)

- ZREM `{id}:{worker_id}` from active set
- DEL `active_job:{id}`
- If job exists in waiting set with negative score, flip score to positive and re-add with ZADD

### Retriable failure (retry)

- ZREM `{id}:{worker_id}` from active set
- Increment `retry_count` in `active_job:{id}`
- If `retry_count >= max_retries`, call fail; otherwise call do_retry

### Retry logic (do_retry)

- If blocked job exists in waiting: get its data, DEL waiting hash, RENAME active to waiting, ZADD with next_run_at, re-dispatch blocked job
- Otherwise: RENAME active to waiting, ZADD with next_run_at

### Permanent failure (fail)

- Check if `{queue}-fail:waiting` EXISTS
- If yes: generate new ID, create failure job with data `[original_id, job_data, error]`, dispatch to fail queue
- Call finish to clean up

### Stalled jobs (sweep)

- ZRANGEBYSCORE on active set for scores ≤ now (limit 100)
- For each stalled job: parse `{id}:{worker_id}`, call handle_stall

### Stall handling (handle_stall)

- ZREM from active set
- Increment `stall_count` in `active_job:{id}`
- If `stall_count >= max_stalls`, call fail; otherwise call do_retry
