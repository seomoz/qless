-- Complete(0, id, worker, queue, now, [data, [next, [delay]]])
-- ------------------------------------------------------------
-- Complete a job and optionally put it in another queue, either scheduled or to
-- be considered waiting immediately.
--
-- Args:
--    1) id
--    2) worker
--    3) queue
--    4) now
--    5) [data]
--    6) [next]
--    7) [delay]

if #KEYS > 0 then error('Complete(): No Keys should be provided') end

local id     = assert(ARGV[1]          , 'Complete(): Arg "id" missing.')
local worker = assert(ARGV[2]          , 'Complete(): Arg "worker" missing.')
local queue  = assert(ARGV[3]          , 'Complete(): Arg "queue" missing.')
local now    = assert(tonumber(ARGV[4]), 'Complete(): Arg "now" not a number or missing: ' .. (ARGV[4] or 'nil'))
local data   = ARGV[5]
local nextq  = ARGV[6]
local delay  = assert(tonumber(ARGV[7] or 0), 'Complete(): Arg "delay" not a number: ' .. (ARGV[7] or 'nil'))

-- The bin is midnight of the provided day
-- 24 * 60 * 60 = 86400
local bin = now - (now % 86400)

if data then
	data = cjson.decode(data)
end

-- First things first, we should see if the worker still owns this job
local lastworker, history, state, priority = unpack(redis.call('hmget', 'ql:j:' .. id, 'worker', 'history', 'state', 'priority'))
if (lastworker ~= worker) or (state ~= 'running') then
	return false
end

-- Now we can assume that the worker does own the job. We need to
--    1) Remove the job from the 'locks' from the old queue
--    2) Enqueue it in the next stage if necessary
--    3) Update the data
--    4) Mark the job as completed, remove the worker, remove expires, and update history

-- Unpack the history, and update it
history = cjson.decode(history)
history[#history]['done'] = now

if data then
	redis.call('hset', 'ql:j:' .. id, 'data', cjson.encode(data))
end

-- Remove the job from the previous queue
redis.call('zrem', 'ql:q:' .. queue .. '-work', id)
redis.call('zrem', 'ql:q:' .. queue .. '-locks', id)
redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', id)

----------------------------------------------------------
-- This is the massive stats update that we have to do
----------------------------------------------------------
-- This is how long we've been waiting to get popped
local waiting = now - history[#history]['popped']
-- Now we'll go through the apparently long and arduous process of update
local count, mean, vk = unpack(redis.call('hmget', 'ql:s:run:' .. bin .. ':' .. queue, 'total', 'mean', 'vk'))
count = count or 0
if count == 0 then
	mean  = waiting
	vk    = 0
	count = 1
else
	count = count + 1
	local oldmean = mean
	mean  = mean + (waiting - mean) / count
	vk    = vk + (waiting - mean) * (waiting - oldmean)
end
-- Now, update the histogram
-- - `s1`, `s2`, ..., -- second-resolution histogram counts
-- - `m1`, `m2`, ..., -- minute-resolution
-- - `h1`, `h2`, ..., -- hour-resolution
-- - `d1`, `d2`, ..., -- day-resolution
waiting = math.floor(waiting)
if waiting < 60 then -- seconds
	redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 's' .. waiting, 1)
elseif waiting < 3600 then -- minutes
	redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 'm' .. math.floor(waiting / 60), 1)
elseif waiting < 86400 then -- hours
	redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 'h' .. math.floor(waiting / 3600), 1)
else -- days
	redis.call('hincrby', 'ql:s:run:' .. bin .. ':' .. queue, 'd' .. math.floor(waiting / 86400), 1)
end		
redis.call('hmset', 'ql:s:run:' .. bin .. ':' .. queue, 'total', count, 'mean', mean, 'vk', vk)
----------------------------------------------------------

if nextq then
	-- Enqueue the job
	table.insert(history, {
		q     = nextq,
		put   = now
	})
	
	redis.call('hmset', 'ql:j:' .. id, 'state', 'waiting', 'worker', '',
		'queue', nextq, 'expires', 0, 'history', cjson.encode(history))
	
	if delay > 0 then
	    redis.call('zadd', 'ql:q:' .. nextq .. '-scheduled', now + delay, id)
	else
	    redis.call('zadd', 'ql:q:' .. nextq .. '-work', priority, id)
	end
	return 'waiting'
else
	redis.call('hmset', 'ql:j:' .. id, 'state', 'complete', 'worker', '',
		'queue', '', 'expires', 0, 'history', cjson.encode(history))
	
	-- Do the completion dance
	local count, time = unpack(redis.call('hmget', 'ql:config', 'jobs-history-count', 'jobs-history'))
	
	-- These are the default values
	count = tonumber(count) or 50000
	time  = tonumber(time ) or (7 * 24 * 60 * 60)
	
	-- Schedule this job for destructination eventually
	redis.call('zadd', 'ql:completed', now, id)
	
	-- Now look at the expired job data. First, based on the current time
	local jids = redis.call('zrangebyscore', 'ql:completed', 0, now - time)
	-- Any jobs that need to be expired... delete
	for index, value in ipairs(jids) do
		redis.call('del', 'ql:j:' .. value)
	end
	-- And now remove those from the queued-for-cleanup queue
	redis.call('zremrangebyscore', 'ql:completed', 0, now - time)
	
	-- Now take the all by the most recent 'count' ids
	jids = redis.call('zrange', 'ql:completed', 0, (-1-count))
	for index, jid in ipairs(jids) do
		redis.call('del', 'ql:j:' .. jid)
	end
	redis.call('zremrangebyrank', 'ql:completed', 0, (-1-count))
	return 'complete'
end
