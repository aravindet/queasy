#!lua name=queasy

--[[
Queasy: Redis Lua functions for job queue management

Key structure:
- {queue}:waiting - sorted set of waiting job IDs (score = run_at or -run_at if blocked)
- {queue}:active - sorted set of active jobs (member = {id}:{worker_id}, score = heartbeat deadline)
- {queue}:waiting_job:{id} - hash with job data for waiting jobs
- {queue}:active_job:{id} - hash with job data for active jobs
]]

-- Constants
local SWEEP_BATCH_SIZE = 100

-- Helper: Generate job key from queue key and id
local function get_job_key(queue_key, id)
    return queue_key .. '_job:' .. id
end

-- Helper: Add job to waiting queue with appropriate flags
local function add_to_waiting(waiting_key, id, score, update_run_at)
    score = tonumber(score)

    if update_run_at == 'false' then
        redis.call('ZADD', waiting_key, 'NX', score, id)
    elseif update_run_at == 'true' then
        redis.call('ZADD', waiting_key, score, id)
    elseif update_run_at == 'if_later' then
        if score >= 0 then
            redis.call('ZADD', waiting_key, 'GT', score, id)
        else
            redis.call('ZADD', waiting_key, 'LT', score, id)
        end
    elseif update_run_at == 'if_earlier' then
        if score >= 0 then
            redis.call('ZADD', waiting_key, 'LT', score, id)
        else
            redis.call('ZADD', waiting_key, 'GT', score, id)
        end
    end
end

-- Helper: Upsert job to waiting queue
local function dispatch(
    waiting_key, active_key,
    id, run_at, data,
    update_data, update_run_at, reset_counts
)
    local waiting_job_key = get_job_key(waiting_key, id)
    local active_job_key = get_job_key(active_key, id)

    -- id is always stored so that HGETALL (e.g. during dequeue) includes it
    redis.call('HSET', waiting_job_key, 'id', id)

    -- If reset_counts is true, reset counters to 0, otherwise initialize them
    if reset_counts == 'true' then
        redis.call('HSET', waiting_job_key, 'retry_count', '0', 'stall_count', '0')
    else
        redis.call('HSETNX', waiting_job_key, 'retry_count', '0')
        redis.call('HSETNX', waiting_job_key, 'stall_count', '0')
    end

    -- Handle data
    if update_data == 'true' then
        redis.call('HSET', waiting_job_key, 'data', data)
    else
        redis.call('HSETNX', waiting_job_key, 'data', data)
    end

    -- Check if there's an active job with this ID
    local is_blocked = redis.call('EXISTS', active_job_key) == 1
    local score = is_blocked and -run_at or run_at

    if is_blocked then
        -- save these flags in case they need to be applied later
        redis.call('HSET', waiting_job_key,
            'reset_counts', reset_counts,
            'update_data', update_data,
            'update_run_at', update_run_at)
    end

    -- Add to waiting queue
    add_to_waiting(waiting_key, id, score, update_run_at)

    return { ok = 'OK' }
end

-- Helper: Move job back to waiting for retry
local function do_retry(waiting_key, active_key, id, retry_at)
    local waiting_job_key = get_job_key(waiting_key, id)
    local active_job_key = get_job_key(active_key, id)

    local existing_score = redis.call('ZSCORE', waiting_key, id)

    if existing_score then
        if existing_score > 0 then return { err = 'ERR_UNBLOCKED_DUPLICATE_PREVENTS_RETRY' } end

        local run_at = -tonumber(existing_score)
        local job = redis.call('HGETALL', waiting_job_key)['map']

        redis.call('DEL', waiting_job_key)
        redis.call('RENAME', active_job_key, waiting_job_key)
        redis.call('ZADD', waiting_key, retry_at, id)

        if next(job) then
            dispatch(
                waiting_key, active_key,
                id, run_at, job.data,
                job.update_data, job.update_run_at, job.reset_counts
            )
        end
    else
        redis.call('RENAME', active_job_key, waiting_job_key)
        redis.call('ZADD', waiting_key, retry_at, id)
    end

    return { ok = 'OK' }
end

-- Helper: Clear active job and unblock waiting job
local function finish(waiting_key, active_key, id, worker_id)
    local waiting_job_key = get_job_key(waiting_key, id)
    local active_job_key = get_job_key(active_key, id)
    local active_item = id .. ':' .. worker_id

    redis.call('ZREM', active_key, active_item)
    redis.call('DEL', active_job_key)

    local score = redis.call('ZSCORE', waiting_key, id)

    if score then
        score = tonumber(score.double)
        if score < 0 then
            score = -score
        end

        local update_run_at = redis.call('HGET', waiting_job_key, 'update_run_at') or 'true'
        add_to_waiting(waiting_key, id, score, update_run_at)
    end

    return { ok = 'OK' }
end

-- Helper: Handle permanent failure
-- Creates a fail job and finishes the original job
local function fail(waiting_key, active_key, fail_waiting_key, fail_active_key, id, worker_id, fail_job_id, fail_job_data)
    -- Dispatch the fail job
    dispatch(fail_waiting_key, fail_active_key,
        fail_job_id, 0, fail_job_data,
        'false', 'false', 'false')

    -- Finish the original job
    finish(waiting_key, active_key, id, worker_id)

    return { ok = 'OK' }
end

-- Helper: Handle retriable failure
local function retry(waiting_key, active_key, id, worker_id, retry_at, error)
    local active_job_key = get_job_key(active_key, id)
    local active_item = id .. ':' .. worker_id
    redis.call('ZREM', active_key, active_item)

    local retry_count = tonumber(redis.call('HGET', active_job_key, 'retry_count'))

    retry_count = retry_count + 1
    redis.call('HSET', active_job_key, 'retry_count', retry_count)

    return do_retry(waiting_key, active_key, id, retry_at)
end

-- Helper: Handle stalled job
local function handle_stall(waiting_key, active_key, id, worker_id, retry_at)
    local active_job_key = get_job_key(active_key, id)
    local active_item = id .. ':' .. worker_id
    redis.call('ZREM', active_key, active_item)

    local stall_count = tonumber(redis.call('HGET', active_job_key, 'stall_count'))

    stall_count = stall_count + 1
    redis.call('HSET', active_job_key, 'stall_count', stall_count)

    return do_retry(waiting_key, active_key, id, retry_at)
end

-- Dequeue jobs from waiting queue
local function dequeue(waiting_key, active_key, worker_id, now, expiry, limit)
    local jobs = redis.call('ZRANGEBYSCORE', waiting_key, 0, now, 'LIMIT', 0, limit)
    local result = {}

    for _, id in ipairs(jobs) do
        local removed = redis.call('ZREM', waiting_key, id)

        if removed == 1 then
            local waiting_job_key = get_job_key(waiting_key, id)
            local active_job_key = get_job_key(active_key, id)

            local renamed = redis.call('RENAME', waiting_job_key, active_job_key)

            if renamed.ok == 'OK' then
                local job = redis.call('HGETALL', active_job_key)
                local active_item = id .. ':' .. worker_id

                redis.call('ZADD', active_key, expiry, active_item)
                table.insert(result, job)
            end
        end
    end

    return result
end

-- Cancel a waiting job
local function cancel(waiting_key, id)
    local waiting_job_key = get_job_key(waiting_key, id)
    local removed = redis.call('ZREM', waiting_key, id)
    if removed == 1 then
        redis.call('DEL', waiting_job_key)
    end
    return removed
end

-- Bump heartbeat for active job
local function bump(active_key, id, worker_id, expiry)
    local active_item = id .. ':' .. worker_id
    local result = redis.call('ZADD', active_key, 'XX', expiry, active_item)
    return result or 0
end

-- Sweep stalled jobs
local function sweep(waiting_key, active_key, now, retry_at)
    local stalled_items = redis.call('ZRANGEBYSCORE', active_key, 0, now, 'LIMIT', 0, SWEEP_BATCH_SIZE)
    local processed_jobs = {}

    for _, member in ipairs(stalled_items) do
        local colon_pos = member:find(':')
        if colon_pos then
            local id = member:sub(1, colon_pos - 1)
            local worker_id = member:sub(colon_pos + 1)

            handle_stall(waiting_key, active_key, id, worker_id, retry_at)
            table.insert(processed_jobs, id)
        end
    end

    return processed_jobs
end

-- Register: queasy_dispatch
redis.register_function {
    function_name = 'queasy_dispatch',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local active_key = keys[2]
        local id = args[1]
        local run_at = tonumber(args[2])
        local data = args[3]
        local update_data = args[4]
        local update_run_at = args[5]
        local reset_counts = args[6]

        redis.setresp(3)
        return dispatch(
            waiting_key, active_key,
            id, run_at, data,
            update_data, update_run_at, reset_counts
        )
    end,
    flags = {}
}

-- Register: queasy_dequeue
redis.register_function {
    function_name = 'queasy_dequeue',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local active_key = keys[2]
        local worker_id = args[1]
        local now = tonumber(args[2])
        local expiry = tonumber(args[3])
        local limit = tonumber(args[4])

        redis.setresp(3)
        return dequeue(waiting_key, active_key, worker_id, now, expiry, limit)
    end,
    flags = {}
}

-- Register: queasy_cancel
redis.register_function {
    function_name = 'queasy_cancel',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local id = args[1]

        redis.setresp(3)
        return cancel(waiting_key, id)
    end,
    flags = {}
}

-- Register: queasy_bump
redis.register_function {
    function_name = 'queasy_bump',
    callback = function(keys, args)
        local active_key = keys[1]
        local id = args[1]
        local worker_id = args[2]
        local expiry = tonumber(args[3])

        redis.setresp(3)
        return bump(active_key, id, worker_id, expiry)
    end,
    flags = {}
}

-- Register: queasy_finish
redis.register_function {
    function_name = 'queasy_finish',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local active_key = keys[2]
        local id = args[1]
        local worker_id = args[2]

        redis.setresp(3)
        return finish(waiting_key, active_key, id, worker_id)
    end,
    flags = {}
}

-- Register: queasy_retry
redis.register_function {
    function_name = 'queasy_retry',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local active_key = keys[2]
        local id = args[1]
        local worker_id = args[2]
        local retry_at = tonumber(args[3])
        local error = args[4]

        redis.setresp(3)
        return retry(waiting_key, active_key, id, worker_id, retry_at, error)
    end,
    flags = {}
}

-- Register: queasy_fail
redis.register_function {
    function_name = 'queasy_fail',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local active_key = keys[2]
        local fail_waiting_key = keys[3]
        local fail_active_key = keys[4]
        local id = args[1]
        local worker_id = args[2]
        local fail_job_id = args[3]
        local fail_job_data = args[4]

        redis.setresp(3)
        return fail(waiting_key, active_key, fail_waiting_key, fail_active_key, id, worker_id, fail_job_id, fail_job_data)
    end,
    flags = {}
}

-- Register: queasy_sweep
redis.register_function {
    function_name = 'queasy_sweep',
    callback = function(keys, args)
        local waiting_key = keys[1]
        local active_key = keys[2]
        local now = tonumber(args[1])
        local retry_at = tonumber(args[2])

        redis.setresp(3)
        return sweep(waiting_key, active_key, now, retry_at)
    end,
    flags = {}
}

-- Register: queasy_version
redis.register_function {
    function_name = 'queasy_version',
    callback = function(keys, args)
        return 1
    end,
    flags = {}
}
