local prefix    = ARGV[4]
local now_ms    = tonumber(ARGV[3])

-- Promote due delayed jobs into the waiting set.
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms)
for _, jid in ipairs(due) do
  local pri_raw = redis.call('HGET', prefix .. 'job:' .. jid, 'priority')
  local pri = tonumber(pri_raw) or 0
  redis.call('ZADD',  KEYS[1], -pri, jid)
  redis.call('ZREM',  KEYS[2], jid)
  redis.call('HSET',  prefix .. 'job:' .. jid, 'status', 'waiting')
end

-- Pop the top-priority job.
local items = redis.call('ZPOPMIN', KEYS[1], 1)
if #items == 0 then return '' end
local job_id = items[1]

-- Lock it.
redis.call('SADD', KEYS[3], job_id)
redis.call('HSET', prefix .. 'job:' .. job_id,
  'status',        'active',
  'lockId',        ARGV[1],
  'lockExpiresAt', ARGV[2],
  'updatedAt',     ARGV[2]
)
return job_id
