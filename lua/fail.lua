-- Fail(0, id, type, message, now)
-- -------------------------------
-- Mark the particular job as failed, with the provided type, and a more specific
-- message. By `type`, we mean some phrase that might be one of several categorical
-- modes of failure. The `message` is something more job-specific, like perhaps
-- a traceback.
-- 
-- The motivation behind the `type` is so that similar errors can be grouped together.
--
-- Args:
--    1) job id
--    2) failure type
--    3) message
--    4) the current time

if #KEYS > 0 then error('Fail(): No Keys should be provided') end

local id      = assert(ARGV[1]          , 'Fail(): Arg "id" missing')
local t       = assert(ARGV[2]          , 'Fail(): Arg "type" missing')
local message = assert(ARGV[3]          , 'Fail(): Arg "message" missing')
local now     = assert(tonumber(ARGV[4]), 'Fail(): Arg "now" missing or malformed: ' .. (ARGV[4] or nil))
