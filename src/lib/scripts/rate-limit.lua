-- RATE_LIMIT_LUA
-- KEYS[1]: rate_count_key ("qjw:rate:{queue}")
-- KEYS[2]: rate_ts_key    ("qjw:rate:{queue}:ts")
-- ARGV[1]: max (number)
-- ARGV[2]: windowMs (number)
-- ARGV[3]: nowMs (number)
-- ARGV[4]: ttlSec (number)
--
-- Returns:
--   1: Allowed
--   0: Rejected

local ck = KEYS[1]
local tk = KEYS[2]
local max = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local nowMs = tonumber(ARGV[3])
local ttlSec = tonumber(ARGV[4])

local windowStart = redis.call('GET', tk)

if not windowStart or (nowMs - tonumber(windowStart)) >= windowMs then
    redis.call('SET', ck, '1', 'EX', ttlSec)
    redis.call('SET', tk, tostring(nowMs), 'EX', ttlSec)
    return 1
end

local current = tonumber(redis.call('GET', ck) or '0')
if current >= max then
    return 0
end

redis.call('INCR', ck)
redis.call('EXPIRE', ck, ttlSec)
redis.call('EXPIRE', tk, ttlSec)
return 1
