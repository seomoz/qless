-- This scripts conducts a heartbeat for a job, and returns
-- either the new expiration or False if the lock has been
-- given to another node
--
-- Args:
--    1) ID
--    2) worker
--    3) heartbeat time
--    4) [data]

if #KEYS > 0 then error('Heartbeat(): No Keys should be provided') end

local id         = assert(ARGV[1]          , 'Heartbeat(): Arg "id" missing')
local worker     = assert(ARGV[2]          , 'Heartbeat(): Arg "worker" missing')
local expiration = assert(tonumber(ARGV[3]), 'Heartbeat(): Arg "expiration" missing')
local data       = ARGV[4]

if data then
	data = cjson.decode(data)
end

-- First, let's see if the worker still owns this job
if redis.call('hget', 'ql:j:' .. id, 'worker') ~= worker then
    return false
else
    -- Otherwise, optionally update the user data, and the heartbeat
    if data then
        -- I don't know if this is wise, but I'm decoding and encoding
        -- the user data to hopefully ensure its sanity
        redis.call('hmset', 'ql:j:' .. id, 'expires', expiration, 'worker', worker, 'data', cjson.encode(data))
    else
        redis.call('hmset', 'ql:j:' .. id, 'expires', expiration, 'worker', worker)
    end
	
    -- And now we should just update the locks
    local queue = redis.call('hget', 'ql:j:' .. id, 'queue')
    redis.call('zadd', 'ql:q:'.. queue, expiration, id)
    return expiration
end
