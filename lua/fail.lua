-- Fail(0, id, worker, type, message, now, [data])
-- ---------------------------------------
-- Mark the particular job as failed, with the provided type, and a more specific
-- message. By `type`, we mean some phrase that might be one of several categorical
-- modes of failure. The `message` is something more job-specific, like perhaps
-- a traceback.
-- 
-- This method should __not__ be used to note that a job has been dropped or has 
-- failed in a transient way. This method __should__ be used to note that a job has
-- something really wrong with it that must be remedied.
-- 
-- The motivation behind the `type` is so that similar errors can be grouped together.
-- Optionally, updated data can be provided for the job. A job in any state can be
-- marked as failed. If it has been given to a worker as a job, then its subsequent
-- requests to heartbeat or complete that job will fail. Failed jobs are kept until
-- they are canceled or completed. __Returns__ the id of the failed job if successful,
-- or `False` on failure.
--
-- Args:
--    1) job id
--    2) worker
--    3) failure type
--    4) message
--    5) the current time
--    6) [data]

if #KEYS > 0 then error('Fail(): No Keys should be provided') end

local id      = assert(ARGV[1]          , 'Fail(): Arg "id" missing')
local worker  = assert(ARGV[2]          , 'Fail(): Arg "worker" missing')
local t       = assert(ARGV[3]          , 'Fail(): Arg "type" missing')
local message = assert(ARGV[4]          , 'Fail(): Arg "message" missing')
local now     = assert(tonumber(ARGV[5]), 'Fail(): Arg "now" missing or malformed: ' .. (ARGV[5] or 'nil'))
local data    = ARGV[6]

if data then
	data = cjson.decode(data)
end

-- First things first, we should get the history
local history, queue, state = unpack(redis.call('hmget', 'ql:j:' .. id, 'history', 'queue', 'state'))

-- If the job has been completed, we cannot fail it
if state == 'complete' then
	return false
end

-- Now, take the element of the history for which our provided worker is the worker, and update 'failed'
history = cjson.decode(history or '[]')
if #history > 0 then
	for i=#history,1,-1 do
		if history[i]['worker'] == worker then
			history[i]['failed'] = now
		end
	end
else
	history = {
		{
			worker = worker,
			failed = now
		}
	}
end

-- Now remove the instance from the schedule, and work queues for the queue it's in
redis.call('zrem', 'ql:q:' .. queue .. '-work', id)
redis.call('zrem', 'ql:q:' .. queue .. '-locks', id)
redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', id)

-- The reason that this appears here is that the above will fail if the job doesn't exist
if data then
	redis.call('hset', 'ql:j:' .. id, 'data', cjson.encode(data))
end

redis.call('hmset', 'ql:j:' .. id, 'state', 'failed', 'worker', '', 'queue', '',
	'expires', '', 'history', cjson.encode(history), 'failure', cjson.encode({
		['type']    = t,
		['message'] = message,
		['when']    = now,
		['worker']  = worker
	}))

-- Add this type of failure to the list of failures
redis.call('sadd', 'ql:failures', t)
-- And add this particular instance to the failed types
redis.call('lpush', 'ql:f:' .. t, id)

-- Here is where we'd intcrement stats about the particular stage
-- and possibly the workers

return id