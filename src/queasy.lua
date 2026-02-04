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
local function dispatch(waiting_set_key, waiting_job_key, active_job_key, id, run_at, data,
                        max_retries, max_stalls, min_backoff, max_backoff,
                        update_data, update_run_at, update_retry_strategy, reset_counts)

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
	add_to_waiting(waiting_set_key, id, score, update_run_at)

	return 'OK'
end

-- Helper: Move job back to waiting for retry
local function do_retry(waiting_key, waiting_job_key, active_job_key, id, next_run_at)
	local existing_score = redis.call('ZSCORE', waiting_key, id)

	if existing_score then
		local run_at = -tonumber(existing_score)
		local job = redis.call('HGETALL', waiting_job_key)['map']

		redis.call('DEL', waiting_job_key)
		redis.call('RENAME', active_job_key, waiting_job_key)
		redis.call('ZADD', waiting_key, next_run_at, id)

		if next(job) then
            dispatch(
                waiting_key, waiting_job_key, active_job_key,
                id, run_at, job.data,
                job.max_retries, job.max_stalls, job.min_backoff, job.max_backoff,
			    job.update_data, job.update_run_at, job.update_retry_strategy, job.reset_counts
            )
		end
	else
		redis.call('RENAME', active_job_key, waiting_job_key)
		redis.call('ZADD', waiting_key, next_run_at, id)
	end

	return 'OK'
end

-- Helper: Clear active job and unblock waiting job
local function finish(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id)
	local active_member = id .. ':' .. worker_id

	redis.call('ZREM', active_key, active_member)
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

	return 'OK'
end

-- Helper: Handle permanent failure
local function fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name, error)
	local fail_waiting_key = queue_name .. '-fail:waiting'
    local fail_queue_exists = redis.call('EXISTS', fail_waiting_key)

	if fail_queue_exists == 1 then
        local job = redis.call('HGETALL', active_job_key)['map']

        if next(job) then
            local fail_job_id = generate_id()
			local fail_waiting_job_key = queue_name .. '-fail:waiting_job:' .. fail_job_id
            local fail_active_job_key = queue_name .. '-fail:active_job:' .. fail_job_id

			-- Wrap data in JSON array without deserializing
            local data = '[' .. id .. ',' .. (job.data or '{}') .. ',' .. (error or '{}') .. ']'

            dispatch(fail_waiting_key, fail_waiting_job_key, fail_active_job_key,
                fail_job_id, 0, data,
			    job.max_retries, job.max_stalls, job.min_backoff, job.max_backoff,
			    job.update_data, job.update_run_at, job.update_retry_strategy, job.reset_counts)
		end
	end

	finish(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id)

	return 'PERMANENT_FAILURE'
end

-- Helper: Handle retriable failure
local function retry(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name, error)
    local active_member = id .. ':' .. worker_id
    redis.call('ZREM', active_key, active_member)

	local retry_count = tonumber(redis.call('HGET', active_job_key, 'retry_count'))
	local max_retries = tonumber(redis.call('HGET', active_job_key, 'max_retries'))

	retry_count = retry_count + 1
	redis.call('HSET', active_job_key, 'retry_count', retry_count)

	if retry_count >= max_retries then
		return fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name, error)
	else
		return do_retry(waiting_key, waiting_job_key, active_job_key, id, next_run_at)
	end
end

-- Helper: Handle stalled job
local function handle_stall(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name)
	local active_member = id .. ':' .. worker_id
	redis.call('ZREM', active_key, active_member)

	local stall_count = tonumber(redis.call('HGET', active_job_key, 'stall_count'))
	local max_stalls = tonumber(redis.call('HGET', active_job_key, 'max_stalls'))

	stall_count = stall_count + 1
	redis.call('HSET', active_job_key, 'stall_count', stall_count)

	if stall_count >= max_stalls then
		return fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name, '{"type":"stall"}')
	else
		return do_retry(waiting_key, waiting_job_key, active_job_key, id, next_run_at)
	end
end

-- Dequeue jobs from waiting queue
local function dequeue(waiting_key, active_key, worker_id, now, limit)
	local jobs = redis.call('ZRANGEBYSCORE', waiting_key, 0, now, 'LIMIT', 0, limit)
    local result = {}

	for _, id in ipairs(jobs) do
		local removed = redis.call('ZREM', waiting_key, id)

		if removed == 1 then
			local waiting_job_key = waiting_key .. '_job:' .. id
			local active_job_key = active_key .. '_job:' .. id

            local renamed = redis.call('RENAME', waiting_job_key, active_job_key)

            if renamed.ok == 'OK' then
			    local job = redis.call('HGETALL', active_job_key)
                local active_member = id .. ':' .. worker_id

                redis.call('ZADD', active_key, now + ACTIVE_TIMEOUT_MS, active_member)
                table.insert(result, job)
			end
		end
	end

	return result
end

-- Cancel a waiting job
local function cancel(waiting_key, waiting_job_key, id)
	local removed = redis.call('ZREM', waiting_key, id)
	if removed == 1 then
		redis.call('DEL', waiting_job_key)
	end
	return removed
end

-- Bump heartbeat for active job
local function bump(active_key, id, worker_id, now)
    local active_member = id .. ':' .. worker_id
	local result = redis.call('ZADD', active_key, 'XX', now + ACTIVE_TIMEOUT_MS, active_member)
	return result or 0
end

-- Sweep stalled jobs
local function sweep(active_key, now, queue_name, next_run_at)
	local stalled_members = redis.call('ZRANGEBYSCORE', active_key, 0, now, 'LIMIT', 0, SWEEP_BATCH_SIZE)
	local processed_jobs = {}

	for _, member in ipairs(stalled_members) do
		local colon_pos = member:find(':')
		if colon_pos then
			local id = member:sub(1, colon_pos - 1)
			local worker_id = member:sub(colon_pos + 1)

			local active_job_key = queue_name .. ':active_job:' .. id
			local waiting_key = queue_name .. ':waiting'
			local waiting_job_key = queue_name .. ':waiting_job:' .. id

			handle_stall(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name)
			table.insert(processed_jobs, id)
		end
	end

	return processed_jobs
end

-- Register: queasy_dispatch
redis.register_function{
	function_name='queasy_dispatch',
	callback=function(keys, args)
		local waiting_set_key = keys[1]
		local waiting_job_key = keys[2]
		local active_job_key = keys[3]
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
            waiting_set_key, waiting_job_key, active_job_key,
            id, run_at, data,
            max_retries, max_stalls, min_backoff, max_backoff,
            update_data, update_run_at, update_retry_strategy, reset_counts
        )
	end,
	flags={}
}

-- Register: queasy_dequeue
redis.register_function{
	function_name='queasy_dequeue',
	callback=function(keys, args)
		local waiting_key = keys[1]
		local active_key = keys[2]
		local worker_id = args[1]
		local now = tonumber(args[2])
		local limit = tonumber(args[3])

		redis.setresp(3)
		return dequeue(waiting_key, active_key, worker_id, now, limit)
	end,
	flags={}
}

-- Register: queasy_cancel
redis.register_function{
	function_name='queasy_cancel',
	callback=function(keys, args)
		local waiting_key = keys[1]
		local waiting_job_key = keys[2]
		local id = args[1]

		redis.setresp(3)
		return cancel(waiting_key, waiting_job_key, id)
	end,
	flags={}
}

-- Register: queasy_bump
redis.register_function{
	function_name='queasy_bump',
	callback=function(keys, args)
		local active_key = keys[1]
		local id = args[1]
		local worker_id = args[2]
		local now = tonumber(args[3])

		redis.setresp(3)
		return bump(active_key, id, worker_id, now)
	end,
	flags={}
}

-- Register: queasy_finish
redis.register_function{
	function_name='queasy_finish',
	callback=function(keys, args)
		local active_key = keys[1]
		local active_job_key = keys[2]
		local waiting_key = keys[3]
		local waiting_job_key = keys[4]
		local id = args[1]
		local worker_id = args[2]

		redis.setresp(3)
		return finish(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id)
	end,
	flags={}
}

-- Register: queasy_retry
redis.register_function{
	function_name='queasy_retry',
    callback = function(keys, args)
		local active_key = keys[1]
		local active_job_key = keys[2]
		local waiting_key = keys[3]
		local waiting_job_key = keys[4]
		local id = args[1]
		local worker_id = args[2]
		local next_run_at = tonumber(args[3])
        local queue_name = args[4]
		local error = args[5]

		redis.setresp(3)
		return retry(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name, error)
	end,
	flags={}
}

-- Register: queasy_fail
redis.register_function{
	function_name='queasy_fail',
	callback=function(keys, args)
		local active_key = keys[1]
		local active_job_key = keys[2]
		local waiting_key = keys[3]
		local waiting_job_key = keys[4]
		local id = args[1]
		local worker_id = args[2]
        local queue_name = args[3]
		local error = args[4]

		redis.setresp(3)
		return fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name, error)
	end,
	flags={}
}

-- Register: queasy_sweep
redis.register_function{
	function_name='queasy_sweep',
	callback=function(keys, args)
		local active_key = keys[1]
		local now = tonumber(args[1])
		local queue_name = args[2]
		local next_run_at = tonumber(args[3])

		redis.setresp(3)
		return sweep(active_key, now, queue_name, next_run_at)
	end,
	flags={}
}
