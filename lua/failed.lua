-- Failed(0, [type, [start, [limit]]])
-- -----------------------------------
-- If no type is provided, this returns a JSON blob of the counts of the various
-- types of failures known. If a type is provided, it will report up to `limit`
-- from `start` of the jobs affected by that issue. __Returns__ a JSON blob.
-- 
-- 	# If no type, then...
-- 	{
-- 		'type1': 1,
-- 		'type2': 5,
-- 		...
-- 	}
-- 	
-- 	# If a type is provided, then...
-- 	{
--      'total': 20,
-- 		'jobs': [
-- 			{
-- 				# All the normal keys for a job
-- 				'id': ...,
-- 				'data': ...
-- 				# The message for this particular instance
-- 				'message': ...,
-- 				'type': ...,
-- 			}, ...
-- 		]
-- 	}
--
-- Args:
--    1) [type]
--    2) [start]
--    3) [limit]

if #KEYS > 0 then error('Failed(): No Keys should be provided') end

local t     = ARGV[1]
local start = assert(tonumber(ARGV[2] or  0), 'Failed(): Arg "start" is not a number: ' .. (ARGV[2] or 'nil'))
local limit = assert(tonumber(ARGV[3] or 25), 'Failed(): Arg "limit" is not a number: ' .. (ARGV[3] or 'nil'))

if t then
	-- If a type was provided, then we should do paginated lookup into that
	local response = {
		total = redis.call('llen', 'ql:f:' .. t),
		jobs  = {}
	}
	local jids = redis.call('lrange', 'ql:f:' .. t, start, limit)
	for index, jid in ipairs(jids) do
		local job = redis.call(
		    'hmget', 'ql:j:' .. jid, 'id', 'priority', 'data',
		    'tags', 'worker', 'expires', 'state', 'queue', 'history')
		
		table.insert(response.jobs, {
		    id        = job[1],
		    priority  = tonumber(job[2]),
		    data      = cjson.decode(job[3]) or {},
		    tags      = cjson.decode(job[4]) or {},
		    worker    = job[5] or '',
		    expires   = tonumber(job[6]) or 0,
		    state     = job[7],
		    queue     = job[8],
		    history   = cjson.decode(job[9])
		})
	end
	return cjson.encode(response)
else
	-- Otherwise, we should just list all the known failure types we have
	local response = {}
	local types = redis.call('smembers', 'ql:failures')
	for index, value in ipairs(types) do
		response[value] = redis.call('llen', 'ql:f:' .. value)
	end
	return cjson.encode(response)
end