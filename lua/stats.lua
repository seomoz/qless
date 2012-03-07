-- Stats(0, queue, date)
-- ---------------------
-- Return the current statistics for a given queue on a given date. The results 
-- are returned are a JSON blob:
-- 
-- 	{
-- 		'total'    : ...,
-- 		'mean'     : ...,
-- 		'variance' : ...,
-- 		'histogram': [
-- 			...
-- 		]
-- 	}
-- 
-- The histogram's data points are at the second resolution for the first minute,
-- the minute resolution for the first hour, the 15-minute resolution for the first
-- day, the hour resolution for the first 3 days, and then at the day resolution
-- from there on out. The `histogram` key is a list of those values.
--
-- Args:
--    1) queue
--    2) date

if #KEYS > 0 then error('Stats(): No Keys should be provided') end

local queue = assert(ARGS[1], 'Stats(): Arg "queue" missing')
local date  = assert(ARGS[2], 'Stats(): Arg "date" missing')
