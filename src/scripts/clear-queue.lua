--[[
  clear-queue.lua
  Atomically delete every job in a queue and clean up all sorted sets.

  Without this script clearQueue requires:
    1 ZRANGE  — fetch all IDs
    N DEL     — delete each job hash
    1 DEL     — delete index
    1 DEL     — delete waiting set
  = N + 3 round-trips.

  With this script it is a single round-trip regardless of queue size.

  KEYS
    [1]  indexKey   — "qjw:{queue}:index"
    [2]  waitingKey — "qjw:{queue}:waiting"

  ARGV
    (none)

  Returns
    Number of job hashes that were deleted.
--]]

local indexKey   = KEYS[1]
local waitingKey = KEYS[2]

-- Collect every job ID tracked by the priority index.
-- ZRANGE 0 -1 returns all members in score order (ASC).
local ids     = redis.call("ZRANGE", indexKey, 0, -1)
local deleted = 0

for _, jobId in ipairs(ids) do
  local n = redis.call("DEL", "qjw:job:" .. jobId)
  deleted = deleted + n
end

-- Drop both sorted sets.
redis.call("DEL", indexKey)
redis.call("DEL", waitingKey)

return deleted
