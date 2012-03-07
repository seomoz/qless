-- This scripts gets the requested configuration option, or in the 
-- absence of a requested option, all options.
--
-- NOTE: This serves as at the very least, a reference implementation.
--     It does not require any complex locking operations, or to
--     ensure consistency, and so it is a good candidate for getting
--     absorbed into any client library.
--
-- Args:
--    1) [option]

if #KEYS > 0 then error('GetConfig(): No Keys should be provided') end

if ARGV[1] then
	return redis.call('hget', 'ql:config', ARGV[1])
else
	return redis.call('hgetall', 'ql:config')
end
