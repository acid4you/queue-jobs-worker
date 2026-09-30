--[[
  claim-job.lua
  Atomically claim the next eligible job from a queue.

  This script runs server-side in a single round-trip so no two workers
  can ever pick up the same job, even under high concurrency.

  KEYS
    [1]  index key   — sorted set  "qjw:{queue}:index"   (score = priority)
    [2]  waiting key — sorted set  "qjw:{queue}:waiting" (score = runAt ms)

  ARGV
    [1]  nowMs — current Unix timestamp in milliseconds (string)

  Algorithm
    1. Scan the waiting set for members whose score (runAt) <= nowMs.
    2. Take the first one — it is already sorted by runAt ASC inside the
       waiting set; ties fall back to the global priority order.
    3. Remove it from the waiting set atomically.
    4. Fetch the job JSON from its hash key.
    5. If the JSON is gone (TTL race) skip and try the next candidate.
    6. Decode the JSON, flip status → "active", bump updatedAt, processedAt.
    7. Re-encode and overwrite the hash key.
    8. Return the updated job JSON string.

  Returns
    Bulk string — updated job JSON, or nil if nothing is eligible.
--]]

local indexKey   = KEYS[1]   -- "qjw:{queue}:index"
local waitingKey = KEYS[2]   -- "qjw:{queue}:waiting"
local nowMs      = tonumber(ARGV[1])

-- Fetch all members of the waiting set whose runAt <= nowMs.
-- ZRANGEBYSCORE returns members ordered by score ASC (oldest runAt first).
local candidates = redis.call("ZRANGEBYSCORE", waitingKey, "-inf", nowMs)

for _, jobId in ipairs(candidates) do
  -- Atomically remove from waiting set. If another worker already claimed it
  -- this returns 0 and we skip to the next candidate.
  local removed = redis.call("ZREM", waitingKey, jobId)
  if removed == 1 then
    local hashKey = "qjw:job:" .. jobId
    local raw     = redis.call("GET", hashKey)

    if raw then
      -- Decode, update status fields, re-encode.
      -- Redis does not have a JSON module by default, so we do a targeted
      -- string substitution rather than a full parse. The job JSON is a
      -- single flat object — no nested status/updatedAt/processedAt fields
      -- from sub-objects need to change.
      local nowStr = tostring(nowMs)

      -- Replace "status":"<anything>" → "status":"active"
      raw = raw:gsub('"status":"[^"]*"', '"status":"active"', 1)

      -- Replace "updatedAt":<number> → current timestamp
      raw = raw:gsub('"updatedAt":[%d%.%-]+', '"updatedAt":' .. nowStr, 1)

      -- BUG FIX: the old pattern  "processedAt":[^,}]*  only matched
      -- non-string scalars (numbers, null, booleans).  If processedAt was
      -- stored as a JSON string ("2024-...") the pattern stopped at the
      -- opening quote and left the field partially replaced.
      -- New approach: use two branches — a full JSON-string matcher first,
      -- then a non-string scalar fallback — identical to the null-field
      -- strategy used in update-job.lua.
      if raw:find('"processedAt":', 1, true) then
        raw = raw:gsub(
          '"processedAt":("([^"\\]|\\.)*"|[^,}]+)',
          '"processedAt":' .. nowStr,
          1
        )
      else
        -- Field absent — insert before the closing brace of the top-level
        -- object.  JSON always ends with "}", so this is safe.
        raw = raw:gsub('}$', ',"processedAt":' .. nowStr .. '}')
      end

      redis.call("SET", hashKey, raw)

      -- NOTE: only remove from the *waiting* set, NOT from the index.
      -- The index set tracks ALL jobs regardless of status; only the waiting
      -- set gates which jobs are eligible for pickup.
      -- Index entries are removed only when a job reaches a terminal state
      -- (completed / failed) via update-job.lua or remove-job.lua.

      return raw
    end
    -- JSON missing — job was deleted externally. Continue scanning.
  end
end

return nil
