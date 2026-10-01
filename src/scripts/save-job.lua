--[[
  save-job.lua
  Atomically persist a new job to Redis.

  Replaces three separate round-trips (SET + ZADD + ZADD) with a single
  server-side script execution.

  KEYS
    [1]  hashKey    — "qjw:job:{jobId}"
    [2]  indexKey   — "qjw:{queue}:index"   sorted set, score = priority
    [3]  waitingKey — "qjw:{queue}:waiting" sorted set, score = runAt ms

  ARGV
    [1]  jobJson    — full JSON string of the Job record
    [2]  jobId      — the job's id field
    [3]  priority   — numeric priority (string)
    [4]  runAt      — runAt timestamp ms (string)
    [5]  eligible   — "1" if this job should enter the waiting set now
                      (status is "waiting" or "retrying"), "0" otherwise

  Returns
    1 always.
--]]

local hashKey    = KEYS[1]
local indexKey   = KEYS[2]
local waitingKey = KEYS[3]

local jobJson    = ARGV[1]
local jobId      = ARGV[2]
local priority   = ARGV[3]
local runAt      = ARGV[4]
local eligible   = ARGV[5]

-- Store the full job JSON.
redis.call("SET", hashKey, jobJson)

-- Always add to the priority index so listJobs / countJobs work.
-- BUG FIX: use ZADD NX so re-saving an existing job (e.g. after a crash
-- recovery re-enqueue) does not silently overwrite the priority score that
-- update-job.lua may have set.  saveJob is a new-job operation; priority
-- updates go through updateJob / update-job.lua exclusively.
redis.call("ZADD", indexKey, "NX", priority, jobId)

-- Only add to the waiting set if the job is immediately pick-up-eligible.
-- Use plain ZADD (not NX) so a re-save after a crash can correct the runAt
-- score; the waiting set score must always reflect the true scheduled time.
if eligible == "1" then
  redis.call("ZADD", waitingKey, runAt, jobId)
end

return 1
