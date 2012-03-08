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
--    1) id
--    2) data
--    3) now
--    4) [priority]
--    5) [tags]
--    6) [delay]

if #KEYS ~= 1 then
	if #KEYS < 1 then
		error('Put(): Expected 1 KEYS argument')
	else
		error('Put(): Got ' .. #KEYS .. ', expected 1 KEYS argument')
	end
end

local queue    = assert(KEYS[1]               , 'Put(): Key "queue" missing')
local id       = assert(ARGV[1]               , 'Put(): Arg "id" missing')
local data     = assert(cjson.decode(ARGV[2]) , 'Put(): Arg "data" missing')
local now      = assert(tonumber(ARGV[3])     , 'Put(): Arg "now" missing')
local delay    = assert(tonumber(ARGV[6] or 0), 'Put(): Arg "delay" not a number')

-- Let's see what the old priority, history and tags were
local history, priority, tags, oldqueue, state, failure = unpack(redis.call('hmget', 'ql:j:' .. id, 'history', 'priority', 'tags', 'queue', 'state', 'failure'))

-- Update the history to include this new change
local history = cjson.decode(history or '{}')
table.insert(history, {
	q     = queue,
	put   = now
})

-- And make sure that the tags are either what was provided or the existing
tags     = assert(cjson.decode(ARGV[5] or tags or '[]'), 'Put(): Arg "tags" not JSON')

-- And make sure that the priority is ok
priority = assert(tonumber(ARGV[4] or priority or 0), 'Put(): Arg "priority" not a number')

-- If this item was previously in another queue, then we should remove it from there
if oldqueue then
	redis.call('zrem', 'ql:q:' .. oldqueue .. '-work', id)
	redis.call('zrem', 'ql:q:' .. oldqueue .. '-locks', id)
	redis.call('zrem', 'ql:q:' .. oldqueue .. '-scheduled', id)
end

-- If the job was previously in the 'completed' state, then we should remove
-- it from being enqueued for destructination
if state == 'completed' then
	redis.call('zrem', 'ql:completed', id)
end

-- If we're in the failed state, remove all of our data
if state == 'failed' then
	failure = cjson.decode(failure)
	-- We need to make this remove it from the failed queues
	redis.call('lrem', 'ql:f:' .. failure.type, 0, id)
	if redis.call('llen', 'ql:f:' .. failure.type) == 0 then
		redis.call('srem', 'ql:failures', failure.type)
	end
end

-- First, let's save its data
redis.call('hmset', 'ql:j:' .. id,
    'id'      , id,
    'data'    , cjson.encode(data),
    'priority', priority,
    'tags'    , cjson.encode(tags),
    'state'   , 'waiting',
    'worker'  , '',
	'expires' , 0,
    'queue'   , queue,
    'history' , cjson.encode(history))

-- Now, if a delay was provided, and if it's in the future,
-- then we'll have to schedule it. Otherwise, we're just
-- going to add it to the work queue.
if delay > 0 then
    redis.call('zadd', 'ql:q:' .. queue .. '-scheduled', now + delay, id)
else
    redis.call('zadd', 'ql:q:' .. queue .. '-work', priority, id)
end

return id