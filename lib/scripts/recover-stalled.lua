local job_key        = KEYS[1]
local active_set     = KEYS[2]
local waiting_zset   = KEYS[3]

local job_id         = ARGV[1]
local expected_exp   = ARGV[2]
local expected_lock  = ARGV[3]
local recovery_ts    = ARGV[4]
local priority_score = tonumber(ARGV[5])

-- Re-read the three guard fields in one atomic HMGET.
local fields = redis.call('HMGET', job_key, 'lockExpiresAt', 'lockId', 'status')
local current_exp    = fields[1] or ''
local current_lock   = fields[2] or ''
local current_status = fields[3] or ''

-- Guard 1: job must still be active.
-- If the worker already completed, failed, or moved to DLQ, skip.
if current_status ~= 'active' then return 0 end

-- Guard 2: lockId must not have changed.
-- A different worker may have claimed the job after the original lock expired
-- and before this script runs.
if current_lock ~= expected_lock then return 0 end

-- Guard 3: lockExpiresAt must be exactly what the caller observed.
-- If the worker renewed its lock the timestamp will be later than observed;
-- the CAS mismatch catches that without needing ISO→epoch conversion in Lua.
if current_exp ~= expected_exp then return 0 end

-- All guards passed — recover atomically.
redis.call('HSET', job_key,
  'status',        'waiting',
  'lockId',        '',
  'lockExpiresAt', '',
  'updatedAt',     recovery_ts
)
redis.call('SREM', active_set,   job_id)
redis.call('ZADD', waiting_zset, priority_score, job_id)
return 1
