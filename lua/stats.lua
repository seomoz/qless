-- Stats(0, queue, date)
-- ---------------------
-- Return the current statistics for a given queue on a given date. The results 
-- are returned are a JSON blob:
--
--
--	{
--		# These are unimplemented as of yet
--		'failed': 3,
--		'retries': 5,
--		'wait' : {
--			'total'    : ...,
--			'mean'     : ...,
--			'variance' : ...,
--			'histogram': [
--				...
--			]
--		}, 'run': {
--			'total'    : ...,
--			'mean'     : ...,
--			'variance' : ...,
--			'histogram': [
--				...
--			]
--		}
--	}
--
-- The histogram's data points are at the second resolution for the first minute,
-- the minute resolution for the first hour, the 15-minute resolution for the first
-- day, the hour resolution for the first 3 days, and then at the day resolution
-- from there on out. The `histogram` key is a list of those values.
--
-- Args:
--    1) queue
--    2) time

if #KEYS > 0 then error('Stats(): No Keys should be provided') end

local queue = assert(ARGV[1], 'Stats(): Arg "queue" missing')
local time  = assert(ARGV[2], 'Stats(): Arg "time" missing')

-- The bin is midnight of the provided day
-- 24 * 60 * 60 = 86400
local bin = time - (time % 86400)

-- This a table of all the keys we want to use in order to produce a histogram
local histokeys = {
	's0','s1','s2','s3','s4','s5','s6','s7','s8','s9','s10','s11','s12','s13','s14','s15','s16','s17','s18','s19','s20','s21','s22','s23','s24','s25','s26','s27','s28','s29','s30','s31','s32','s33','s34','s35','s36','s37','s38','s39','s40','s41','s42','s43','s44','s45','s46','s47','s48','s49','s50','s51','s52','s53','s54','s55','s56','s57','s58','s59',
	'm1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14','m15','m16','m17','m18','m19','m20','m21','m22','m23','m24','m25','m26','m27','m28','m29','m30','m31','m32','m33','m34','m35','m36','m37','m38','m39','m40','m41','m42','m43','m44','m45','m46','m47','m48','m49','m50','m51','m52','m53','m54','m55','m56','m57','m58','m59',
	'h1','h2','h3','h4','h5','h6','h7','h8','h9','h10','h11','h12','h13','h14','h15','h16','h17','h18','h19','h20','h21','h22','h23',
	'd1','d2','d3','d4','d5','d6'
}

mkstats = function(name, bin, queue)
	-- The results we'll be sending back
	local results = {}
	
	local count, mean, vk = unpack(redis.call('hmget', 'ql:s:' .. name .. ':' .. bin .. ':' .. queue, 'total', 'mean', 'vk'))
	
	results.count     = count or 0
	results.mean      = mean  or 0
	results.histogram = {}

	if not count then
		results.std = 0
	else
		if count > 1 then
			results.std = math.sqrt(vk / (count - 1))
		else
			results.std = 0
		end
	end

	local histogram = redis.call('hmget', 'ql:s:' .. name .. ':' .. bin .. ':' .. queue, unpack(histokeys))
	for i=0,#histokeys do
		table.insert(results.histogram, histogram[i] or 0)
	end
	return results
end

return cjson.encode({
	wait = mkstats('wait', bin, queue),
	run  = mkstats('run' , bin, queue)
})