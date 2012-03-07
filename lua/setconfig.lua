-- This scripts sets the configuration option for the provided
-- option.
--
-- NOTE: This serves as at the very least, a reference implementation.
--     It does not require any complex locking operations, or to
--     ensure consistency, and so it is a good candidate for getting
--     absorbed into any client library.
--
-- Args:
--    1) option
--    2) value

if #KEYS > 0 then error('SetConfig(): No Keys should be provided') end

if ARGV[2] then
	redis.call('hset', 'ql:config', ARGV[1], ARGV[2])
else
	redis.call('hdel', 'ql:config', ARGV[1])
end