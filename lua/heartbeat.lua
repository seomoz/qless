-- This scripts conducts a heartbeat for a job, and returns
-- either the new expiration or False if the lock has been
-- given to another node
--
-- Args:
--    1) ID
--    2) worker
--    3) heartbeat time
--    4) optional data

local heartbeat = tonumber(ARGV[3])

-- First, let's see if the worker still owns this job
if redis.call('hget', 'ql:j:' .. ARGV[1], 'worker') ~= ARGV[2] then
    return false
else
    -- Otherwise, optionally update the user data, and the heartbeat
    if ARGV[4] then
        -- I don't know if this is wise, but I'm decoding and encoding
        -- the user data to hopefully ensure its sanity
        redis.call('hmset', 'ql:j:' .. ARGV[1], 'heartbeat', heartbeat, 'worker', ARGV[2], 'data', cjson.encode(cjson.decode(ARGV[4])))
    else
        redis.call('hmset', 'ql:j:' .. ARGV[1], 'heartbeat', heartbeat, 'worker', ARGV[2])
    end
    -- And now we should just update the locks
    local queue = redis.call('hget', 'ql:j:' .. ARGV[1], 'queue')
    redis.call('zadd', 'ql:q:'.. queue, heartbeat, ARGV[1])
    return heartbeat
end
