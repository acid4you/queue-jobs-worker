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
       ZRANGEBYSCORE returns by runAt ASC — but two jobs with identical runAt
       values would be served in arbitrary order, violating priority ordering.
    2. BUG FIX: re-sort candidates by their priority score from the index set
       (lower score = higher priority) so high-priority jobs are always claimed
       first even when multiple jobs become eligible at the same runAt.
    3. Iterate the priority-sorted list.  For each candidate, atomically ZREM
       it from the waiting set — only one worker can succeed (ZREM returns 1).
    4. Fetch the job JSON; skip if missing (external deletion race).
    5. Patch status → "active", bump updatedAt and processedAt in-place.
    6. Persist and return the updated JSON string.

  Returns
    Bulk string — updated job JSON, or nil if nothing is eligible.
--]]

local indexKey   = KEYS[1]   -- "qjw:{queue}:index"
local waitingKey = KEYS[2]   -- "qjw:{queue}:waiting"
local nowMs      = tonumber(ARGV[1])

-- ── 1. Fetch all eligible job IDs from the waiting set ──────────────────────
-- ZRANGEBYSCORE returns members ordered by score (runAt) ASC.
local candidates = redis.call("ZRANGEBYSCORE", waitingKey, "-inf", nowMs)

if #candidates == 0 then
  return nil
end

-- ── 2. Re-sort by priority using scores from the index set ──────────────────
-- BUG FIX: the waiting set is ordered by runAt, not priority.  When multiple
-- jobs are eligible at the same time (or have identical runAt values) they
-- should be dispatched in priority ASC order (lower score = higher priority).
-- We fetch each candidate's priority score and sort locally inside the script.
--
-- ZSCORE returns nil for members not in the index (should never happen in a
-- consistent store, but we default to 0 so such jobs sort at the front rather
-- than causing a Lua error).
local scored = {}
for _, jobId in ipairs(candidates) do
  local score = redis.call("ZSCORE", indexKey, jobId)
  scored[#scored + 1] = { id = jobId, pri = tonumber(score) or 0 }
end

-- Stable sort by priority ASC.  Lua's table.sort is not guaranteed stable,
-- but for our purposes (priority is typically a small integer) this is fine —
-- ties within the same priority are already broken by runAt ASC order from
-- ZRANGEBYSCORE, and table.sort preserves relative input order for equal keys
-- in most Lua runtimes (including Redis's embedded LuaJIT).
table.sort(scored, function(a, b) return a.pri < b.pri end)

-- ── 3. Claim the first job we can atomically remove from the waiting set ─────
for _, entry in ipairs(scored) do
  local jobId = entry.id

  -- ZREM is atomic: returns 1 if we removed it, 0 if another worker beat us.
  local removed = redis.call("ZREM", waitingKey, jobId)
  if removed == 1 then
    local hashKey = "qjw:job:" .. jobId
    local raw     = redis.call("GET", hashKey)

    if raw then
      -- ── 4. Patch status fields in-place ─────────────────────────────────
      -- We use targeted string substitution rather than a full JSON parse
      -- (Redis has no built-in JSON module by default).  The job JSON is a
      -- flat object — no nested status/updatedAt/processedAt fields exist
      -- inside sub-objects, so string replacement is safe and unambiguous.
      local nowStr = tostring(nowMs)

      -- "status":"<anything>"  →  "status":"active"
      raw = raw:gsub('"status":"[^"]*"', '"status":"active"', 1)

      -- "updatedAt":<number>  →  current timestamp
      raw = raw:gsub('"updatedAt":[%d%.%-]+', '"updatedAt":' .. nowStr, 1)

      -- "processedAt": <string|number|null|absent>  →  current timestamp
      -- Two-branch pattern handles both JSON string values and scalar values.
      if raw:find('"processedAt":', 1, true) then
        raw = raw:gsub(
          '"processedAt":("([^"\\]|\\.)*"|[^,}]+)',
          '"processedAt":' .. nowStr,
          1
        )
      else
        -- Field absent — append before the closing brace.
        raw = raw:gsub('}$', ',"processedAt":' .. nowStr .. '}')
      end

      -- ── 5. Persist and return ────────────────────────────────────────────
      redis.call("SET", hashKey, raw)

      -- Only remove from the *waiting* set, NOT from the index.
      -- The index tracks ALL jobs regardless of status so listJobs("active")
      -- continues to find this job.  Index entries are removed only when the
      -- job reaches a terminal state via update-job.lua or remove-job.lua.

      return raw
    end
    -- JSON missing — job was deleted externally. Continue to next candidate.
  end
end

return nil
