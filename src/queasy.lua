#!lua name=queasy

--[[
Queasy: Redis Lua functions for job queue management

Key structure:
- {queue}:waiting - sorted set of waiting job IDs (score = runAt or -runAt if blocked)
- {queue}:active - sorted set of active jobs (member = {id}:{worker_id}, score = heartbeat deadline)
- {queue}:waiting_job:{id} - hash with job data for waiting jobs
- {queue}:active_job:{id} - hash with job data for active jobs
]]

--[[
Helper function: Add job to waiting queue with appropriate flags
]]
local function addToWaiting(waitingKey, id, score, updateRunAt)
	score = tonumber(score)

	-- Handle both boolean false and string 'false'
	if updateRunAt == false or updateRunAt == 'false' then
		redis.call('ZADD', waitingKey, 'NX', score, id)
	elseif updateRunAt == true or updateRunAt == 'true' then
		redis.call('ZADD', waitingKey, score, id)
	elseif updateRunAt == 'ifLater' then
		if score >= 0 then
			redis.call('ZADD', waitingKey, 'GT', score, id)
		else
			redis.call('ZADD', waitingKey, 'LT', score, id)
		end
	elseif updateRunAt == 'ifEarlier' then
		if score >= 0 then
			redis.call('ZADD', waitingKey, 'LT', score, id)
		else
			redis.call('ZADD', waitingKey, 'GT', score, id)
		end
	end
end

-- Forward declarations for mutual recursion
local upsertJobImpl
local onFailureImpl

--[[
upsertJob implementation
]]
upsertJobImpl = function(waitingKey, waitingJobKey, activeJobKey, id, runAt, jobData)
	local fieldsToSet = {}
	local fieldsToSetNX = {}

	for key, value in pairs(jobData) do
		local updateKey = 'update' .. key:sub(1,1):upper() .. key:sub(2)
		if jobData[updateKey] == true or jobData[updateKey] == nil and not key:match('^update') then
			if not key:match('^update') then
				table.insert(fieldsToSet, key)
				table.insert(fieldsToSet, value)
			end
		elseif not key:match('^update') then
			table.insert(fieldsToSetNX, key)
			table.insert(fieldsToSetNX, value)
		end
	end

	if #fieldsToSet > 0 then
		redis.call('HSET', waitingJobKey, unpack(fieldsToSet))
	end

	for i = 1, #fieldsToSetNX, 2 do
		redis.call('HSETNX', waitingJobKey, fieldsToSetNX[i], fieldsToSetNX[i+1])
	end

	local activeExists = redis.call('EXISTS', activeJobKey)
	local score = runAt
	if activeExists == 1 then
		score = -runAt
	end

	-- Pass updateRunAt directly, with default of true
	local updateRunAt = jobData.updateRunAt
	if updateRunAt == nil then
		updateRunAt = true
	end
	addToWaiting(waitingKey, id, score, updateRunAt)

	return 'OK'
end

--[[
doRetry implementation
]]
local function doRetryImpl(waitingKey, waitingJobKey, activeJobKey, id, nextRunAt)
	local existingScore = redis.call('ZSCORE', waitingKey, id)

	if existingScore then
		existingScore = tonumber(existingScore)
		local blockedRunAt = -existingScore
		local blockedJobData = redis.call('HGETALL', waitingJobKey)

		redis.call('DEL', waitingJobKey)
		redis.call('RENAME', activeJobKey, waitingJobKey)
		redis.call('ZADD', waitingKey, nextRunAt, id)

		if #blockedJobData > 0 then
			local blockedData = {}
			for i = 1, #blockedJobData, 2 do
				blockedData[blockedJobData[i]] = blockedJobData[i+1]
			end

			upsertJobImpl(waitingKey, waitingJobKey, activeJobKey, id, blockedRunAt, blockedData)
		end
	else
		redis.call('RENAME', activeJobKey, waitingJobKey)
		redis.call('ZADD', waitingKey, nextRunAt, id)
	end

	return 'OK'
end

--[[
clearActive implementation
]]
local function clearActiveImpl(activeKey, activeJobKey, waitingKey, waitingJobKey, id, workerId)
	local activeMember = id .. ':' .. workerId

	redis.call('ZREM', activeKey, activeMember)
	redis.call('DEL', activeJobKey)

	local score = redis.call('ZSCORE', waitingKey, id)

	if score then
		score = tonumber(score)
		if score < 0 then
			score = -score
		end

		local updateRunAt = redis.call('HGET', waitingJobKey, 'updateRunAt')
		if not updateRunAt then
			updateRunAt = 'true'
		end

		addToWaiting(waitingKey, id, score, tostring(updateRunAt))
	end

	return 'OK'
end

--[[
onFailure implementation
]]
onFailureImpl = function(activeKey, activeJobKey, waitingKey, waitingJobKey, id, workerId, queueName)
	local failQueueWaitingKey = queueName .. '-fail:waiting'
	local failQueueExists = redis.call('EXISTS', failQueueWaitingKey)

	if failQueueExists == 1 then
		local jobData = redis.call('HGETALL', activeJobKey)

		if #jobData > 0 then
			local jobHash = {}
			for i = 1, #jobData, 2 do
				jobHash[jobData[i]] = jobData[i+1]
			end

			local failJobId = id .. ':failure'
			local failWaitingKey = failQueueWaitingKey
			local failWaitingJobKey = queueName .. '-fail:waiting_job:' .. failJobId
			local failActiveJobKey = queueName .. '-fail:active_job:' .. failJobId

			upsertJobImpl(failWaitingKey, failWaitingJobKey, failActiveJobKey, failJobId, 0, jobHash)
		end
	end

	clearActiveImpl(activeKey, activeJobKey, waitingKey, waitingJobKey, id, workerId)

	return 'PERMANENT_FAILURE'
end

-- Register functions with proper wrappers
redis.register_function{
	function_name='upsertJob',
	callback=function(keys, args)
		local jobData = cjson.decode(args[3])
		return upsertJobImpl(keys[1], keys[2], keys[3], args[1], tonumber(args[2]), jobData)
	end,
	flags={}
}

redis.register_function{
	function_name='cancelWaiting',
	callback=function(keys, args)
		local removed = redis.call('ZREM', keys[1], args[1])
		if removed == 1 then
			redis.call('DEL', keys[2])
		end
		return removed
	end,
	flags={}
}

redis.register_function{
	function_name='dequeue',
	callback=function(keys, args)
		local waitingKey = keys[1]
		local activeKey = keys[2]
		local workerId = args[1]
		local now = tonumber(args[2])
		local heartbeatTimeout = tonumber(args[3])
		local limit = tonumber(args[4])

		local jobs = redis.call('ZRANGEBYSCORE', waitingKey, 0, now, 'LIMIT', 0, limit)
		local dequeuedJobs = {}

		for _, id in ipairs(jobs) do
			local removed = redis.call('ZREM', waitingKey, id)

			if removed == 1 then
				local waitingJobKey = waitingKey:gsub(':waiting$', ':waiting_job:' .. id)
				local activeJobKey = waitingKey:gsub(':waiting$', ':active_job:' .. id)

				local renamed = redis.call('RENAMENX', waitingJobKey, activeJobKey)

				if renamed == 1 then
					local activeMember = id .. ':' .. workerId
					redis.call('ZADD', activeKey, now + heartbeatTimeout, activeMember)
					table.insert(dequeuedJobs, id)
				end
			end
		end

		return dequeuedJobs
	end,
	flags={}
}

redis.register_function{
	function_name='heartbeat',
	callback=function(keys, args)
		local activeMember = args[1] .. ':' .. args[2]
		local now = tonumber(args[3])
		local heartbeatTimeout = tonumber(args[4])

		local result = redis.call('ZADD', keys[1], 'XX', now + heartbeatTimeout, activeMember)
		return result or 0
	end,
	flags={}
}

redis.register_function{
	function_name='clearActive',
	callback=function(keys, args)
		return clearActiveImpl(keys[1], keys[2], keys[3], keys[4], args[1], args[2])
	end,
	flags={}
}

redis.register_function{
	function_name='doRetry',
	callback=function(keys, args)
		return doRetryImpl(keys[1], keys[2], keys[3], args[1], tonumber(args[2]))
	end,
	flags={}
}

redis.register_function{
	function_name='onRetry',
	callback=function(keys, args)
		local activeKey = keys[1]
		local activeJobKey = keys[2]
		local waitingKey = keys[3]
		local waitingJobKey = keys[4]
		local id = args[1]
		local workerId = args[2]
		local nextRunAt = tonumber(args[3])
		local queueName = args[4]

		local activeMember = id .. ':' .. workerId
		redis.call('ZREM', activeKey, activeMember)

		local failureCount = tonumber(redis.call('HGET', activeJobKey, 'failureCount') or 0)
		local maxFailures = tonumber(redis.call('HGET', activeJobKey, 'maxFailures') or 10)

		failureCount = failureCount + 1
		redis.call('HSET', activeJobKey, 'failureCount', failureCount)

		if failureCount >= maxFailures then
			return onFailureImpl(activeKey, activeJobKey, waitingKey, waitingJobKey, id, workerId, queueName)
		else
			return doRetryImpl(waitingKey, waitingJobKey, activeJobKey, id, nextRunAt)
		end
	end,
	flags={}
}

redis.register_function{
	function_name='onStall',
	callback=function(keys, args)
		local activeKey = keys[1]
		local activeJobKey = keys[2]
		local waitingKey = keys[3]
		local waitingJobKey = keys[4]
		local id = args[1]
		local workerId = args[2]
		local nextRunAt = tonumber(args[3])
		local queueName = args[4]

		local activeMember = id .. ':' .. workerId
		redis.call('ZREM', activeKey, activeMember)

		local stallCount = tonumber(redis.call('HGET', activeJobKey, 'stallCount') or 0)
		local maxStalls = tonumber(redis.call('HGET', activeJobKey, 'maxStalls') or 3)

		stallCount = stallCount + 1
		redis.call('HSET', activeJobKey, 'stallCount', stallCount)

		if stallCount >= maxStalls then
			return onFailureImpl(activeKey, activeJobKey, waitingKey, waitingJobKey, id, workerId, queueName)
		else
			return doRetryImpl(waitingKey, waitingJobKey, activeJobKey, id, nextRunAt)
		end
	end,
	flags={}
}

redis.register_function{
	function_name='onFailure',
	callback=function(keys, args)
		return onFailureImpl(keys[1], keys[2], keys[3], keys[4], args[1], args[2], args[3])
	end,
	flags={}
}

redis.register_function{
	function_name='clearStalls',
	callback=function(keys, args)
		local activeKey = keys[1]
		local now = tonumber(args[1])
		local queueName = args[2]
		local nextRunAt = tonumber(args[3])

		local stalledMembers = redis.call('ZRANGEBYSCORE', activeKey, 0, now)
		local processedJobs = {}

		for _, member in ipairs(stalledMembers) do
			local colonPos = member:find(':')
			if colonPos then
				local id = member:sub(1, colonPos - 1)
				local workerId = member:sub(colonPos + 1)

				local activeJobKey = queueName .. ':active_job:' .. id
				local waitingKey = queueName .. ':waiting'
				local waitingJobKey = queueName .. ':waiting_job:' .. id

				local stallCount = tonumber(redis.call('HGET', activeJobKey, 'stallCount') or 0)
				local maxStalls = tonumber(redis.call('HGET', activeJobKey, 'maxStalls') or 3)

				redis.call('ZREM', activeKey, member)
				stallCount = stallCount + 1
				redis.call('HSET', activeJobKey, 'stallCount', stallCount)

				if stallCount >= maxStalls then
					onFailureImpl(activeKey, activeJobKey, waitingKey, waitingJobKey, id, workerId, queueName)
				else
					doRetryImpl(waitingKey, waitingJobKey, activeJobKey, id, nextRunAt)
				end

				table.insert(processedJobs, id)
			end
		end

		return processedJobs
	end,
	flags={}
}
