local job_key = KEYS[1]
local lock_id = ARGV[1]
local new_exp = ARGV[2]
local now_iso = ARGV[3]

local fields = redis.call('HMGET', job_key, 'status', 'lockId', 'lockExpiresAt')
local status = fields[1] or ''
local current_lock = fields[2] or ''
local current_exp = fields[3] or ''

if status ~= 'active' then return 0 end
if current_lock ~= lock_id then return 0 end
if current_exp == '' or current_exp <= now_iso then return 0 end

redis.call('HSET', job_key, 'lockExpiresAt', new_exp, 'updatedAt', now_iso)
return 1
