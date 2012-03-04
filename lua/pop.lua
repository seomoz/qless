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

-- Save the current time
local t      = os.time()
local key    = 'ql:q:' .. KEYS[1]
local worker = ARGV[1]
local count  = tonumber(ARGV[2])

-- These are the ids that we're going to return
local keys = {}

-- Check to see if any locks are expired
local r = redis.call('zrangebyscore', key .. '-locks', 0, t, 'LIMIT', 0, count)
-- Iterate through all the expired locks and add them to the list
-- of keys that we'll return
for i=0,#r do
    table.insert(keys, r[i][0])
end

-- Now we've checked __all__ the locks for this queue the could
-- have expired, and are no more than the number requested. If
-- we still need values in order to meet the demand, then we 
-- should check if any scheduled items, and if so, we should 
-- insert them to ensure correctness when pulling off the next
-- unit of work.
if #keys < count then
    r = redis.call('zrangebyscore', key .. '-scheduled', 0, t, 'LIMIT', 0, (count - #keys))
    -- With these in hand, we'll have to go out and find the 
    -- priorities of these jobs, and then we'll insert them
    -- into the work queue and then when that's complete, we'll
    -- remove them from the scheduled queue
    for i=0,#r do
        local priority = 
    end
end
redis.call()