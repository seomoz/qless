-- This script takes the name of the queue and then checks
-- for any expired locks, then inserts any scheduled items
-- that are now valid, and lastly returns any work items 
-- that can be handed over.
--
-- Keys:
--    1) queue name
-- Args:
--    1) worker name
--    2) the number of items to return
--    3) the current time
--    4) the heartbeat time

-- Save the current time
local key     = 'ql:q:' .. KEYS[1]
local worker  = ARGV[1]
local count   = tonumber(ARGV[2])
local now     = tonumber(ARGV[3])
local expires = tonumber(ARGV[4])

-- These are the ids that we're going to return
local keys = {}

-- Iterate through all the expired locks and add them to the list
-- of keys that we'll return
for index, id in ipairs(redis.call('zrangebyscore', key .. '-locks', 0, now, 'LIMIT', 0, count)) do
    table.insert(keys, id)
end

-- Now we've checked __all__ the locks for this queue the could
-- have expired, and are no more than the number requested. If
-- we still need values in order to meet the demand, then we 
-- should check if any scheduled items, and if so, we should 
-- insert them to ensure correctness when pulling off the next
-- unit of work.
if #keys < count then    
    -- zadd is a list of arguments that we'll be able to use to
    -- insert into the work queue
    local zadd = {}
    local r = redis.call('zrangebyscore', key .. '-scheduled', 0, now, 'LIMIT', 0, (count - #keys))
    for index, id in ipairs(r) do
        -- With these in hand, we'll have to go out and find the 
        -- priorities of these jobs, and then we'll insert them
        -- into the work queue and then when that's complete, we'll
        -- remove them from the scheduled queue
        table.insert(zadd, tonumber(redis.call('hget', 'ql:j:' .. id, 'priority') or 0))
        table.insert(zadd, id)
    end
    
    -- Now add these to the work list, and then remove them
    -- from the scheduled list
    if #zadd > 0 then
        redis.call('zadd', key .. '-work', unpack(zadd))
        redis.call('zrem', key .. '-scheduled', unpack(r))
    end
    
    -- And now we should get up to the maximum number of requested
    -- work items from the work queue.
    for index, id in ipairs(redis.call('zrange', key .. '-work', 0, (count - #keys) - 1)) do
        table.insert(keys, id)
    end
end

-- Alright, now the `keys` table is filled with all the job
-- ids which we'll be returning. Now we need to get the 
-- metadeata about each of these, update their metadata to
-- reflect which worker they're on, when the lock expires, 
-- etc., add them to the locks queue and then we have to 
-- finally return a list of json blobs

local response = {}
local state
local history
for index, id in ipairs(keys) do
    -- First, we should get the state and history of the item
    state, history = unpack(redis.call('hmget', 'ql:j:' .. id, 'state', 'history'))
    history = cjson.decode(history or '{}')
    if #history > 0 then
        history[#history][5] = worker
        history[#history][3] = now
    end
    
    redis.call(
        'hmset', 'ql:j:' .. id, 'worker', worker, 'expires', expires,
        'state', 'pending', 'history', cjson.encode(history))
    
    redis.call('zadd', key .. '-locks', expires, id)
    local r = redis.call('hmget', 'ql:j:' .. id, 'id', 'priority', 'data', 'tags', 'expires', 'worker', 'state', 'queue')
    table.insert(response, cjson.encode({
        id       = r[1],
        priority = tonumber(r[2]),
        data     = r[3],
        tags     = cjson.decode(r[4]),
        expires  = tonumber(r[5]),
        worker   = r[6],
        state    = r[7],
        queue    = r[8]
    }))
end
redis.call('zrem', key .. '-work', unpack(keys))
return response