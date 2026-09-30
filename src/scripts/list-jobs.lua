--[[
  list-jobs.lua
  Fetch all jobs for a queue in a single server-side round-trip.

  Without this script listJobs requires:
    1 ZRANGE  — fetch all IDs
    N GET     — fetch each job hash individually  ← N+1 problem
  = N + 1 round-trips.

  With this script it is always exactly 1 round-trip.

  KEYS
    [1]  indexKey — "qjw:{queue}:index"  (sorted by priority ASC)

  ARGV
    [1]  statusFilter — status string to filter by, or "" for no filter

  Algorithm
    1. ZRANGE the index to get all job IDs in priority order.
    2. GET each job hash in a loop (all inside the same script execution,
       so it is pipelined on the Redis side with zero network overhead).
    3. Optionally filter by status using a simple string search.
    4. Return the matching JSON strings as a Redis array (multi-bulk reply).

  Returns
    Array of job JSON strings, sorted by priority ASC (then insertion order
    within the same priority level, which matches createdAt ASC because jobs
    are added with ZADD NX and the index preserves insertion order for ties).
--]]

local indexKey    = KEYS[1]
local statusFilter = ARGV[1]   -- e.g. "waiting", "failed", or "" for all

local ids    = redis.call("ZRANGE", indexKey, 0, -1)
local result = {}

for _, jobId in ipairs(ids) do
  local raw = redis.call("GET", "qjw:job:" .. jobId)
  if raw then
    -- Apply status filter if requested.
    -- We do a simple substring search: '"status":"<filter>"'
    -- This is safe because status values are simple ASCII words with no
    -- special characters that would need escaping.
    if statusFilter == "" or raw:find('"status":"' .. statusFilter .. '"', 1, true) then
      result[#result + 1] = raw
    end
  end
end

return result
