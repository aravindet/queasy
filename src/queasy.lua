#!lua name=queasy

--[[
Queasy: Redis Lua functions for job queue management

Key structure:
- {queue} - sorted set of waiting job IDs (score = run_at or -run_at if blocked)
- {queue}:expiry - sorted set of client heartbeat expiries (member = client_id, score = expiry)
- {queue}:checkouts:{client_id} - set of job IDs checked out by this client
- {queue}:waiting_job:{id} - hash with job data for waiting jobs
- {queue}:active_job:{id} - hash with job data for active jobs
]]

-- Constants
local HEARTBEAT_TIMEOUT = 10000

-- Key helpers
local function get_waiting_job_key(queue_key, id)
    return queue_key .. ':waiting_job:' .. id
end

local function get_active_job_key(queue_key, id)
    return queue_key .. ':active_job:' .. id
end

local function get_expiry_key(queue_key)
    return queue_key .. ':expiry'
end

local function get_checkouts_key(queue_key, client_id)
    return queue_key .. ':checkouts:' .. client_id
end

-- Helper: Add job to waiting queue with appropriate flags
local function add_to_waiting(queue_key, id, score, update_run_at)
    local flag = nil

    if update_run_at == 'false' then
        flag = 'NX'
    elseif update_run_at == 'if_later' then
        flag = score >= 0 and 'GT' or 'LT'
    elseif update_run_at == 'if_earlier' then
        flag = score >= 0 and 'LT' or 'GT'
    end

    if flag then
        redis.call('ZADD', queue_key, flag, score, id)
    else
        redis.call('ZADD', queue_key, score, id)
    end
end

-- Helper: Upsert job to waiting queue
local function dispatch(
    queue_key,
    id, run_at, data,
    update_data, update_run_at, reset_counts
)
    local waiting_job_key = get_waiting_job_key(queue_key, id)
    local active_job_key = get_active_job_key(queue_key, id)

    -- id is always stored so that HGETALL (e.g. during dequeue) includes it
    redis.call('HSET', waiting_job_key, 'id', id)

    -- If reset_counts is true, reset counters to 0, otherwise initialize them
    redis.call(reset_counts and 'HSET' or 'HSETNX', waiting_job_key, 'retry_count', '0')
    redis.call(reset_counts and 'HSET' or 'HSETNX', waiting_job_key, 'stall_count', '0')

    -- Handle data
    redis.call(update_data and 'HSET' or 'HSETNX', waiting_job_key, 'data', data)

    -- Check if there's an active job with this ID
    local is_blocked = redis.call('EXISTS', active_job_key) == 1
    local score = is_blocked and -tonumber(run_at) or tonumber(run_at)

    if is_blocked then
        -- save these flags in case they need to be applied later
        redis.call('HSET', waiting_job_key,
            'reset_counts', tostring(reset_counts),
            'update_data', tostring(update_data),
            'update_run_at', update_run_at)
    end

    -- Add to waiting queue
    add_to_waiting(queue_key, id, score, update_run_at)

    return { ok = 'OK' }
end

-- Helper: Move job back to waiting for retry
local function do_retry(queue_key, id, retry_at)
    local waiting_job_key = get_waiting_job_key(queue_key, id)
    local active_job_key = get_active_job_key(queue_key, id)

    local existing_score = redis.call('ZSCORE', queue_key, id)

    if existing_score then
        local run_at = -existing_score.double
        local job = redis.call('HGETALL', waiting_job_key)['map']

        redis.call('RENAME', active_job_key, waiting_job_key)
        redis.call('ZADD', queue_key, retry_at, id)

        if next(job) then
            dispatch(
                queue_key,
                id, run_at, job.data,
                job.update_data == 'true', job.update_run_at, job.reset_counts == 'true'
            )
        end
    else
        redis.call('RENAME', active_job_key, waiting_job_key)
        redis.call('ZADD', queue_key, retry_at, id)
    end

    return { ok = 'OK' }
end

-- Forward declaration
local sweep

-- Helper: Clear active job and unblock waiting job
local function finish(queue_key, id, client_id, now)
    local waiting_job_key = get_waiting_job_key(queue_key, id)
    local active_job_key = get_active_job_key(queue_key, id)
    local checkouts_key = get_checkouts_key(queue_key, client_id)

    redis.call('SREM', checkouts_key, id)
    redis.call('DEL', active_job_key)

    local score = redis.call('ZSCORE', queue_key, id)

    if score then
        score = tonumber(score.double)
        if score < 0 then
            score = -score
        end

        local update_run_at = redis.call('HGET', waiting_job_key, 'update_run_at') or 'true'
        add_to_waiting(queue_key, id, score, update_run_at)
    end

    -- Update heartbeat and sweep
    local expiry_key = get_expiry_key(queue_key)
    redis.call('ZADD', expiry_key, 'XX', now + HEARTBEAT_TIMEOUT, client_id)
    sweep(queue_key, now)

    return { ok = 'OK' }
end

-- Helper: Handle permanent failure
-- Creates a fail job and finishes the original job
local function fail(queue_key, fail_queue_key, id, client_id, fail_job_id, fail_job_data, now)
    -- Dispatch the fail job
    dispatch(fail_queue_key,
        fail_job_id, 0, fail_job_data,
        'false', 'false', 'false')

    -- Finish the original job
    finish(queue_key, id, client_id, now)

    return { ok = 'OK' }
end

-- Helper: Handle retriable failure
local function retry(queue_key, id, client_id, retry_at, now)
    local active_job_key = get_active_job_key(queue_key, id)
    local checkouts_key = get_checkouts_key(queue_key, client_id)

    redis.call('SREM', checkouts_key, id)

    local retry_count = tonumber(redis.call('HGET', active_job_key, 'retry_count'))

    retry_count = retry_count + 1
    redis.call('HSET', active_job_key, 'retry_count', retry_count)

    local result = do_retry(queue_key, id, retry_at)

    -- Update heartbeat and sweep
    local expiry_key = get_expiry_key(queue_key)
    redis.call('ZADD', expiry_key, 'XX', now + HEARTBEAT_TIMEOUT, client_id)
    sweep(queue_key, now)

    return result
end

-- Helper: Handle stalled job
local function handle_stall(queue_key, id, retry_at)
    local active_job_key = get_active_job_key(queue_key, id)

    local stall_count = tonumber(redis.call('HGET', active_job_key, 'stall_count'))

    stall_count = stall_count + 1
    redis.call('HSET', active_job_key, 'stall_count', stall_count)

    return do_retry(queue_key, id, retry_at)
end

-- Dequeue jobs from waiting queue
local function dequeue(queue_key, client_id, now, limit)
    local expiry = now + HEARTBEAT_TIMEOUT
    local expiry_key = get_expiry_key(queue_key)
    local checkouts_key = get_checkouts_key(queue_key, client_id)
    local jobs = redis.call('ZRANGEBYSCORE', queue_key, 0, now, 'LIMIT', 0, limit)
    local result = {}

    for _, id in ipairs(jobs) do
        redis.call('ZREM', queue_key, id)
        local waiting_job_key = get_waiting_job_key(queue_key, id)
        local active_job_key = get_active_job_key(queue_key, id)

        redis.call('RENAME', waiting_job_key, active_job_key)
        local job = redis.call('HGETALL', active_job_key)

        redis.call('SADD', checkouts_key, id)
        table.insert(result, job)
    end

    if #result > 0 then
        redis.call('ZADD', expiry_key, expiry, client_id)
    end

    -- Sweep stalled clients
    sweep(queue_key, now)

    return result
end

-- Cancel a waiting job
local function cancel(queue_key, id)
    local waiting_job_key = get_waiting_job_key(queue_key, id)
    local removed = redis.call('ZREM', queue_key, id)
    if removed == 1 then
        redis.call('DEL', waiting_job_key)
    end
    return removed
end

-- Bump heartbeat for client and sweep stalled clients
local function bump(queue_key, client_id, now)
    local expiry_key = get_expiry_key(queue_key)
    local expiry = now + HEARTBEAT_TIMEOUT

    -- Check if this client exists in expiry set
    local existing = redis.call('ZSCORE', expiry_key, client_id)
    if not existing then
        return 0
    end

    -- Update expiry
    redis.call('ZADD', expiry_key, 'XX', expiry, client_id)

    -- Sweep stalled clients
    sweep(queue_key, now)

    return 1
end

-- Sweep stalled clients
sweep = function(queue_key, now)
    local expiry_key = get_expiry_key(queue_key)

    -- Find first stalled client
    local stalled = redis.call('ZRANGEBYSCORE', expiry_key, 0, now, 'LIMIT', 0, 1)

    if #stalled == 0 then
        return {}
    end

    local stalled_client_id = stalled[1]
    local checkouts_key = get_checkouts_key(queue_key, stalled_client_id)

    -- Get all job IDs checked out by this client
    -- RESP3 returns SMEMBERS as { set = { id1 = true, id2 = true, ... } }
    local members_resp = redis.call('SMEMBERS', checkouts_key)
    local processed_jobs = {}

    for id, _ in pairs(members_resp['set']) do
        handle_stall(queue_key, id, 0)
        table.insert(processed_jobs, id)
    end

    -- Clean up the stalled client
    redis.call('ZREM', expiry_key, stalled_client_id)
    redis.call('DEL', checkouts_key)

    return processed_jobs
end

-- Register: queasy_dispatch
redis.register_function {
    function_name = 'queasy_dispatch',
    callback = function(keys, args)
        local queue_key = keys[1]
        local id = args[1]
        local run_at = tonumber(args[2])
        local data = args[3]
        local update_data = args[4] == 'true'
        local update_run_at = args[5]
        local reset_counts = args[6] == 'true'

        redis.setresp(3)
        return dispatch(
            queue_key,
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
        local queue_key = keys[1]
        local client_id = args[1]
        local now = tonumber(args[2])
        local limit = tonumber(args[3])

        redis.setresp(3)
        return dequeue(queue_key, client_id, now, limit)
    end,
    flags = {}
}

-- Register: queasy_cancel
redis.register_function {
    function_name = 'queasy_cancel',
    callback = function(keys, args)
        local queue_key = keys[1]
        local id = args[1]

        redis.setresp(3)
        return cancel(queue_key, id)
    end,
    flags = {}
}

-- Register: queasy_bump
redis.register_function {
    function_name = 'queasy_bump',
    callback = function(keys, args)
        local queue_key = keys[1]
        local client_id = args[1]
        local now = tonumber(args[2])

        redis.setresp(3)
        return bump(queue_key, client_id, now)
    end,
    flags = {}
}

-- Register: queasy_finish
redis.register_function {
    function_name = 'queasy_finish',
    callback = function(keys, args)
        local queue_key = keys[1]
        local id = args[1]
        local client_id = args[2]
        local now = tonumber(args[3])

        redis.setresp(3)
        return finish(queue_key, id, client_id, now)
    end,
    flags = {}
}

-- Register: queasy_retry
redis.register_function {
    function_name = 'queasy_retry',
    callback = function(keys, args)
        local queue_key = keys[1]
        local id = args[1]
        local client_id = args[2]
        local retry_at = tonumber(args[3])
        local now = tonumber(args[5])

        redis.setresp(3)
        return retry(queue_key, id, client_id, retry_at, now)
    end,
    flags = {}
}

-- Register: queasy_fail
redis.register_function {
    function_name = 'queasy_fail',
    callback = function(keys, args)
        local queue_key = keys[1]
        local fail_queue_key = keys[2]
        local id = args[1]
        local client_id = args[2]
        local fail_job_id = args[3]
        local fail_job_data = args[4]
        local now = tonumber(args[5])

        redis.setresp(3)
        return fail(queue_key, fail_queue_key, id, client_id, fail_job_id, fail_job_data, now)
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
