-- This script takes the name of the queue and then the 
-- info about the work item, and makes sure that it's 
-- enqueued.
--
-- At some point, I'd like to able to provide functionality
-- that enables this to generate a unique ID for this piece
-- of work. As such, client libraries should not expose 
-- setting the id from the user, as this is an implementation
-- detail that's likely to change and users should not grow
-- to depend on it.
--
-- Keys:
--    1) queue name
-- Args:
--    1) ID*
--    2) priority
--    3) data blob
--    4) tags JSON
--    5) valid after (how many seconds in the future it will be valid)
--
-- The reason that valid-after is provided as seconds into
-- the future rather than a timestamp is that we want to 
-- use the time on the local system, rather than whatever
-- time is provided by the agent adding this item.

-- First, let's save its data
redis.call('hmset', 'ql:j:' .. ARGV[1],
    'id'      , ARGV[1],
    'priority', tonumber(ARGV[2]),
    'data'    , ARGV[3],
    'tags'    , ARGV[4])

-- Now, if a valid-after time was provided, and if it's in
-- the future, then we'll have to schedule it. Otherwise,
-- we're just going to add it to the work queue.
if ARGV[5] and tonumber(ARGV[5]) > 0 then
    redis.call('zadd', 'ql:q:' .. KEYS[1] .. '-scheduled', os.time() + tonumber(ARGV[5]), ARGV[1])
else
    redis.call('zadd', 'ql:q:' .. KEYS[1] .. '-work', tonumber(ARGV[2]), ARGV[1])
end