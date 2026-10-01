--[[
  remove-job.lua
  Atomically delete a job and remove it from every sorted set.

  Replaces three separate round-trips (DEL + ZREM + ZREM) with a single
  server-side script execution.

  KEYS
    [1]  hashKey    — "qjw:job:{jobId}"
    [2]  indexKey   — "qjw:{queue}:index"
    [3]  waitingKey — "qjw:{queue}:waiting"

  ARGV
    [1]  jobId — the job's id

  Returns
    1 if the job existed and was deleted, 0 if it was not found.
--]]

local hashKey    = KEYS[1]
local indexKey   = KEYS[2]
local waitingKey = KEYS[3]

local jobId      = ARGV[1]

-- Check existence first so we can return a meaningful result.
local existed = redis.call("EXISTS", hashKey)

if existed == 1 then
  redis.call("DEL",  hashKey)
  redis.call("ZREM", indexKey,   jobId)
  redis.call("ZREM", waitingKey, jobId)
  return 1
end

-- Still attempt the ZREM calls in case the hash was deleted externally
-- but the sorted-set entries were left behind (defensive cleanup).
redis.call("ZREM", indexKey,   jobId)
redis.call("ZREM", waitingKey, jobId)

return 0
