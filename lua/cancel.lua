-- Cancel(0, id)
-- -------------
-- Cancel a job from taking place. It will be deleted from the system, and any
-- attempts to renew a heartbeat will fail, and any attempts to complete it
-- will fail. If you try to get the data on the object, you will get nothing.
--
-- Args:
--    1) id

if #KEYS > 0 then error('Cancel(): No Keys should be provided') end

local id     = assert(ARGV[1], 'Cancel(): Arg "id" missing.')

-- Find any stage it's associated with and remove its from that stage
local state, queue, failure = unpack(redis.call('hmget', 'ql:j:' .. id, 'state', 'queue', 'failure'))

if state == 'complete' then
	return False
else
	if queue then
		redis.call('zrem', 'ql:q:' .. queue .. '-work', id)
		redis.call('zrem', 'ql:q:' .. queue .. '-locks', id)
		redis.call('zrem', 'ql:q:' .. queue .. '-scheduled', id)
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
	
	-- Just go ahead and delete our data
	redis.call('del', 'ql:j:' .. id)
end
