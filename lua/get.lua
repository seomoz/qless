-- This gets all the data associated with the job with the
-- provided id.
--
-- Args:
--    1) ID

-- Let's get all the data we can
local r = redis.call(
    'hmget', 'ql:j:' .. ARGV[1], 'id', 'priority', 'data',
    'tags', 'worker', 'expires', 'state', 'queue', 'history')

local _ = cjson.decode(r[9])
local history = {}
for index, list in ipairs(_) do
    table.insert(history, {
        queue     = list[1],
        put       = list[2],
        popped    = list[3],
        completed = list[4],
        worker    = list[5]
    })
end

return cjson.encode({
    id        = r[1],
    priority  = tonumber(r[2]),
    data      = cjson.decode(r[3]),
    tags      = cjson.decode(r[4]),
    worker    = r[5],
    expires   = tonumber(r[6]),
    state     = r[7],
    queue     = r[8],
    history   = history
})