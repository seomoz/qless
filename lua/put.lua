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
--    2) data blob
--    3) current time
--    4) priority
--    5) tags JSON
--    6) valid after (seconds from now)

-- Let's get the current item's history
history = cjson.decode(redis.call('hget', 'ql:j:' .. ARGV[1], 'history') or '{}')
-- And then append a list about the added queue
table.insert(history, {KEYS[1], tonumber(ARGV[3]), nil, nil, ''})

-- First, let's save its data
redis.call('hmset', 'ql:j:' .. ARGV[1],
    'id'      , ARGV[1],
    'data'    , cjson.encode(cjson.decode(ARGV[2])),
    'priority', tonumber(ARGV[4] or 0),
    'tags'    , cjson.encode(cjson.decode(ARGV[5])),
    'state'   , 'waiting',
    'worker'  , '',
    'queue'   , KEYS[1],
    'history' , cjson.encode(history))

-- Now, if a valid-after time was provided, and if it's in
-- the future, then we'll have to schedule it. Otherwise,
-- we're just going to add it to the work queue.
if ARGV[6] and tonumber(ARGV[6]) > 0 then
    redis.call('zadd', 'ql:q:' .. KEYS[1] .. '-scheduled', tonumber(ARGV[6]) + tonumber(ARGV[3]), ARGV[1])
else
    redis.call('zadd', 'ql:q:' .. KEYS[1] .. '-work', tonumber(ARGV[4] or 0), ARGV[1])
end