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
local ACTIVE_TIMEOUT_MS = 10000
local SWEEP_BATCH_SIZE = 100

-- Helper: Generate job key from queue key and id
local function get_job_key(queue_key, id)
    return queue_key .. '_job:' .. id
end

-- Helper: Extract queue name from a key by taking the first segment before ":"
local function get_queue_name(key)
    local colon_pos = key:find(':')
    if colon_pos then
        return key:sub(1, colon_pos - 1)
    end
    return key
end

-- Helper: Generate a random 20-character ID
local function generate_id()
    local chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    local id = {}
    for i = 1, 20 do
        local rand_index = math.random(1, #chars)
        table.insert(id, chars:sub(rand_index, rand_index))
    end
    return table.concat(id)
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
    max_retries, max_stalls, min_backoff, max_backoff,
    update_data, update_run_at, update_retry_strategy, reset_counts
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

    -- Handle retry strategy
    if update_retry_strategy == 'true' then
        redis.call('HSET', waiting_job_key,
            'max_retries', max_retries,
            'max_stalls', max_stalls,
            'min_backoff', min_backoff,
            'max_backoff', max_backoff)
    else
        redis.call('HSETNX', waiting_job_key, 'max_retries', max_retries)
        redis.call('HSETNX', waiting_job_key, 'max_stalls', max_stalls)
        redis.call('HSETNX', waiting_job_key, 'min_backoff', min_backoff)
        redis.call('HSETNX', waiting_job_key, 'max_backoff', max_backoff)
    end

    -- Check if there's an active job with this ID
    local is_blocked = redis.call('EXISTS', active_job_key) == 1
    local score = is_blocked and -run_at or run_at

    if is_blocked then
        -- save these flags in case they need to be applied later
        redis.call('HSET', waiting_job_key,
            'reset_counts', reset_counts,
            'update_data', update_data,
            'update_run_at', update_run_at,
            'update_retry_strategy', update_retry_strategy)
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
                job.max_retries, job.max_stalls, job.min_backoff, job.max_backoff,
                job.update_data, job.update_run_at, job.update_retry_strategy, job.reset_counts
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
local function fail(waiting_key, active_key, id, worker_id, error)
    local active_job_key = get_job_key(active_key, id)
    local queue_name = get_queue_name(waiting_key)
    local fail_config_key = queue_name .. '-fail:config'
    local fail_config = redis.call('HGETALL', fail_config_key)['map']

    if fail_config then
        local job = redis.call('HGETALL', active_job_key)['map']

        if next(job) then
            local fail_job_id = generate_id()
            local fail_waiting_key = queue_name .. '-fail:waiting'
            local fail_active_key = queue_name .. '-fail:active'

            -- Wrap data in JSON array without deserializing
            local data = '[' .. id .. ',' .. (job.data or '{}') .. ',' .. (error or '{}') .. ']'

            dispatch(fail_waiting_key, fail_active_key,
                fail_job_id, 0, data,
                tostring(fail_config.max_retries), tostring(fail_config.max_stalls),
                tostring(fail_config.min_backoff), tostring(fail_config.max_backoff),
                'false', 'false', 'false', 'false')
        end
    end

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
    local max_stalls = tonumber(redis.call('HGET', active_job_key, 'max_stalls'))

    stall_count = stall_count + 1
    redis.call('HSET', active_job_key, 'stall_count', stall_count)

    if stall_count >= max_stalls then
        return fail(waiting_key, active_key, id, worker_id, '{"type":"stall"}')
    else
        return do_retry(waiting_key, active_key, id, retry_at)
    end
end

-- Dequeue jobs from waiting queue
local function dequeue(waiting_key, active_key, worker_id, now, limit)
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

                redis.call('ZADD', active_key, now + ACTIVE_TIMEOUT_MS, active_item)
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
local function bump(active_key, id, worker_id, now)
    local active_item = id .. ':' .. worker_id
    local result = redis.call('ZADD', active_key, 'XX', now + ACTIVE_TIMEOUT_MS, active_item)
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

-- Store default job configuration
local function configure(config_key, max_retries, max_stalls, min_backoff, max_backoff)
    return redis.call('HSET', config_key,
        'max_retries', max_retries,
        'max_stalls', max_stalls,
        'min_backoff', min_backoff,
        'max_backoff', max_backoff)
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
        local max_retries = args[4]
        local max_stalls = args[5]
        local min_backoff = args[6]
        local max_backoff = args[7]
        local update_data = args[8]
        local update_run_at = args[9]
        local update_retry_strategy = args[10]
        local reset_counts = args[11]

        redis.setresp(3)
        return dispatch(
            waiting_key, active_key,
            id, run_at, data,
            max_retries, max_stalls, min_backoff, max_backoff,
            update_data, update_run_at, update_retry_strategy, reset_counts
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
        local limit = tonumber(args[3])

        redis.setresp(3)
        return dequeue(waiting_key, active_key, worker_id, now, limit)
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
        local now = tonumber(args[3])

        redis.setresp(3)
        return bump(active_key, id, worker_id, now)
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
        local id = args[1]
        local worker_id = args[2]
        local error = args[3]

        redis.setresp(3)
        return fail(waiting_key, active_key, id, worker_id, error)
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

-- Register: queasy_configure
redis.register_function {
    function_name = 'queasy_configure',
    callback = function(keys, args)
        local config_key = keys[1]
        local max_retries = args[4]
        local max_stalls = args[5]
        local min_backoff = args[6]
        local max_backoff = args[7]

        redis.setresp(3)
        return configure(config_key, max_retries, max_stalls, min_backoff, max_backoff)
    end,
    flags = {}
}
