-- This gets all the data associated with the job with the
-- provided id.
--
-- Args:
--    1) ID

if #KEYS > 0 then error('Get(): No Keys should be provided') end

local id = assert(ARGV[1], 'Get(): Arg "id" missing')

-- Let's get all the data we can
local r = redis.call(
    'hmget', 'ql:j:' .. id, 'id', 'priority', 'data', 'tags',
	'worker', 'expires', 'state', 'queue', 'history', 'failure')

if not r[1] then
	return False
end

return cjson.encode({
    id        = r[1],
    priority  = tonumber(r[2]),
    data      = cjson.decode(r[3]) or {},
    tags      = cjson.decode(r[4]) or {},
    worker    = r[5] or '',
    expires   = tonumber(r[6]) or 0,
    state     = r[7],
    queue     = r[8],
    history   = cjson.decode(r[9])
})