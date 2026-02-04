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
local HEARTBEAT_TIMEOUT_MS = 10000

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
local function dispatch(waiting_set_key, waiting_job_key, active_job_key, id, run_at, job_data)
	local hset_args = {}

	for key, value in pairs(job_data) do
		local update_key = 'update_' .. key
		if key:match('^update_') or job_data[update_key] == 'true' then
			table.insert(hset_args, key)
			table.insert(hset_args, value)
		else
			redis.call('HSETNX', waiting_job_key, key, value)
		end
	end

	if #hset_args > 0 then
		redis.call('HSET', waiting_job_key, unpack(hset_args))
	end

	local active_exists = redis.call('EXISTS', active_job_key)
	local score = run_at
	if active_exists == 1 then
		score = -run_at
	end

	local update_run_at = job_data.update_run_at or 'true'
	add_to_waiting(waiting_set_key, id, score, update_run_at)

	return 'OK'
end

-- Helper: Move job back to waiting for retry
local function do_retry(waiting_key, waiting_job_key, active_job_key, id, next_run_at)
	local existing_score = redis.call('ZSCORE', waiting_key, id)

	if existing_score then
		existing_score = tonumber(existing_score)
		local blocked_run_at = -existing_score
		local blocked_job_data = redis.call('HGETALL', waiting_job_key)

		redis.call('DEL', waiting_job_key)
		redis.call('RENAME', active_job_key, waiting_job_key)
		redis.call('ZADD', waiting_key, next_run_at, id)

		if #blocked_job_data > 0 then
			local blocked_data = {}
			for i = 1, #blocked_job_data, 2 do
				blocked_data[blocked_job_data[i]] = blocked_job_data[i+1]
			end

			dispatch(waiting_key, waiting_job_key, active_job_key, id, blocked_run_at, blocked_data)
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
		score = tonumber(score)
		if score < 0 then
			score = -score
		end

		local update_run_at = redis.call('HGET', waiting_job_key, 'update_run_at') or 'true'
		add_to_waiting(waiting_key, id, score, update_run_at)
	end

	return 'OK'
end

-- Helper: Handle permanent failure
local function fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name)
	local fail_queue_waiting_key = queue_name .. '-fail:waiting'
	local fail_queue_exists = redis.call('EXISTS', fail_queue_waiting_key)

	if fail_queue_exists == 1 then
		local job_data = redis.call('HGETALL', active_job_key)

		if #job_data > 0 then
			local job_hash = {}
			for i = 1, #job_data, 2 do
				job_hash[job_data[i]] = job_data[i+1]
			end

			local fail_job_id = id .. ':failure'
			local fail_waiting_key = fail_queue_waiting_key
			local fail_waiting_job_key = queue_name .. '-fail:waiting_job:' .. fail_job_id
			local fail_active_job_key = queue_name .. '-fail:active_job:' .. fail_job_id

			dispatch(fail_waiting_key, fail_waiting_job_key, fail_active_job_key, fail_job_id, 0, job_hash)
		end
	end

	finish(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id)

	return 'PERMANENT_FAILURE'
end

-- Helper: Handle retriable failure
local function retry(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name)
	local active_member = id .. ':' .. worker_id
	redis.call('ZREM', active_key, active_member)

	local failure_count = tonumber(redis.call('HGET', active_job_key, 'failure_count') or 0)
	local max_failures = tonumber(redis.call('HGET', active_job_key, 'max_failures') or 10)

	failure_count = failure_count + 1
	redis.call('HSET', active_job_key, 'failure_count', failure_count)

	if failure_count >= max_failures then
		return fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name)
	else
		return do_retry(waiting_key, waiting_job_key, active_job_key, id, next_run_at)
	end
end

-- Helper: Handle stalled job
local function handle_stall(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name)
	local active_member = id .. ':' .. worker_id
	redis.call('ZREM', active_key, active_member)

	local stall_count = tonumber(redis.call('HGET', active_job_key, 'stall_count') or 0)
	local max_stalls = tonumber(redis.call('HGET', active_job_key, 'max_stalls') or 3)

	stall_count = stall_count + 1
	redis.call('HSET', active_job_key, 'stall_count', stall_count)

	if stall_count >= max_stalls then
		return fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name)
	else
		return do_retry(waiting_key, waiting_job_key, active_job_key, id, next_run_at)
	end
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
		local job_data = cjson.decode(args[3])

		return dispatch(waiting_set_key, waiting_job_key, active_job_key, id, run_at, job_data)
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

		local jobs = redis.call('ZRANGEBYSCORE', waiting_key, 0, now, 'LIMIT', 0, limit)
		local dequeued_jobs = {}

		for _, id in ipairs(jobs) do
			local removed = redis.call('ZREM', waiting_key, id)

			if removed == 1 then
				local waiting_job_key = waiting_key:gsub(':waiting$', ':waiting_job:' .. id)
				local active_job_key = waiting_key:gsub(':waiting$', ':active_job:' .. id)

				local renamed = redis.call('RENAMENX', waiting_job_key, active_job_key)

				if renamed == 1 then
					local active_member = id .. ':' .. worker_id
					redis.call('ZADD', active_key, now + HEARTBEAT_TIMEOUT_MS, active_member)
					table.insert(dequeued_jobs, id)
				end
			end
		end

		return dequeued_jobs
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

		local removed = redis.call('ZREM', waiting_key, id)
		if removed == 1 then
			redis.call('DEL', waiting_job_key)
		end
		return removed
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

		local active_member = id .. ':' .. worker_id
		local result = redis.call('ZADD', active_key, 'XX', now + HEARTBEAT_TIMEOUT_MS, active_member)
		return result or 0
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

		return finish(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id)
	end,
	flags={}
}

-- Register: queasy_retry
redis.register_function{
	function_name='queasy_retry',
	callback=function(keys, args)
		local active_key = keys[1]
		local active_job_key = keys[2]
		local waiting_key = keys[3]
		local waiting_job_key = keys[4]
		local id = args[1]
		local worker_id = args[2]
		local next_run_at = tonumber(args[3])
		local queue_name = args[4]

		return retry(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, next_run_at, queue_name)
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

		return fail(active_key, active_job_key, waiting_key, waiting_job_key, id, worker_id, queue_name)
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

		local stalled_members = redis.call('ZRANGEBYSCORE', active_key, 0, now)
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
	end,
	flags={}
}
