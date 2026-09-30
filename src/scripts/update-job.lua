--[[
  update-job.lua  v4
  Atomically read, patch, and re-persist a job — entirely server-side.

  One Redis round-trip: GET + targeted string replacement + SET.

  ── KEYS ──────────────────────────────────────────────────────────────────────
    [1]  hashKey    — "qjw:job:{jobId}"
    [2]  indexKey   — "qjw:{queue}:index"    sorted set, score = priority
    [3]  waitingKey — "qjw:{queue}:waiting"  sorted set, score = runAt ms

  ── ARGV ──────────────────────────────────────────────────────────────────────
    [1]  newStatus   — target status string (always patched)
    [2]  nowMs       — current Unix timestamp ms (always written as updatedAt)
    [3]  runAt       — runAt ms for "retrying" re-queue, or "0"
    [4]  priority    — job priority integer used when re-queuing to index set,
                       or "-1" as sentinel meaning "do not update priority".
    [5]  strFields   — RS-delimited "key\x1evalue" pairs for string fields.
                       RS (ASCII 30) is used instead of pipe to avoid conflicts
                       with error messages that contain "|" characters.
                       Empty string "" means no string fields to patch.
    [6]  numFields   — RS-delimited "key\x1evalue" pairs for numeric fields.
                       Empty string "" means no numeric fields to patch.
    [7]  nullFields  — RS-delimited field names to set to JSON null.
                       Empty string "" means no null fields.

  ── Replacement strategy ──────────────────────────────────────────────────────
    String fields: we replace  "key":"<oldvalue>"  →  "key":"<newvalue>"
      where <oldvalue> is matched with a pattern that handles embedded escape
      sequences:  ([^"\\]|\\.)*   — any non-quote/non-backslash char, or a
      backslash followed by any char (handles \", \\, \n, etc.).

    Numeric fields: we replace  "key":<oldnumber>  →  "key":<newvalue>
      where <oldnumber> matches integers, decimals, and negatives.

    Null fields: we replace the current value (string, number, bool, null,
      or nested object/array) → null.
      We use a two-pass pattern that matches either a full JSON string value
      ("([^"\\]|\\.)*") or a non-string scalar/token ([^,}]+), so nested
      braces inside string values are never misinterpreted.

  ── Returns ───────────────────────────────────────────────────────────────────
    1 on success, 0 if the job does not exist.
--]]

local hashKey    = KEYS[1]
local indexKey   = KEYS[2]
local waitingKey = KEYS[3]

local newStatus  = ARGV[1]
local nowMs      = ARGV[2]
local runAt      = ARGV[3]
local priority   = ARGV[4]   -- "-1" means "don't update priority"
local strFields  = ARGV[5]
local numFields  = ARGV[6]
local nullFields = ARGV[7]

-- RS = ASCII record separator (0x1E) used as our field delimiter.
-- It never appears in job field names or values naturally.
local RS = "\x1e"

-- ── 1. Read current job JSON ──────────────────────────────────────────────────
local raw = redis.call("GET", hashKey)
if not raw then
  return 0
end

-- ── helpers ───────────────────────────────────────────────────────────────────

-- Escape a string for literal use inside a Lua gsub() PATTERN (not replacement).
-- Lua magic chars in patterns: ( ) . % + - * ? [ ] ^ $
local function escPat(s)
  return s:gsub("([%(%)%.%%%+%-%*%?%[%]%^%$])", "%%%1")
end

-- Escape a string for use as a gsub() REPLACEMENT string.
-- Only % needs escaping in Lua replacement strings.
local function escRepl(s)
  return s:gsub("%%", "%%%%")
end

-- ── 2. Patch: status (always) ─────────────────────────────────────────────────
-- status values are simple ASCII words with no special chars — plain replacement.
raw = raw:gsub('"status":"[^"]*"', '"status":"' .. newStatus .. '"', 1)

-- ── 3. Patch: updatedAt (always, numeric) ────────────────────────────────────
raw = raw:gsub('"updatedAt":[%d%.%-]+', '"updatedAt":' .. nowMs, 1)

-- ── 4. Patch: string fields ───────────────────────────────────────────────────
-- Delimiter is RS (0x1E).  Format: "key\x1evalue\x1ekey\x1evalue..."
-- Values are the raw UTF-8 string (already JSON-escaped by the TypeScript caller).
if strFields ~= "" then
  -- Split on RS: iterate over key, value pairs.
  local parts = {}
  for part in (strFields .. RS):gmatch("([^" .. RS .. "]*)" .. RS) do
    parts[#parts + 1] = part
  end

  local i = 1
  while i < #parts do
    local key = parts[i]
    local val = parts[i + 1]
    i = i + 2

    if key ~= "" then
      -- Pattern matches: "key":"<value>" where <value> may contain \" escapes.
      -- ([^"\\]|\\.)* handles any valid JSON string content.
      local pat  = '"' .. escPat(key) .. '":"([^"\\\\]|\\\\.)*"'
      local repl = '"' .. key .. '":"' .. escRepl(val) .. '"'

      if raw:find('"' .. escPat(key) .. '":', 1, true) then
        raw = raw:gsub(pat, repl, 1)
      else
        -- Field absent — insert before the closing brace.
        raw = raw:gsub('}$', ',"' .. key .. '":"' .. escRepl(val) .. '"}')
      end
    end
  end
end

-- ── 5. Patch: numeric fields ──────────────────────────────────────────────────
-- Format same as strFields but values are numbers (integers or decimals).
if numFields ~= "" then
  local parts = {}
  for part in (numFields .. RS):gmatch("([^" .. RS .. "]*)" .. RS) do
    parts[#parts + 1] = part
  end

  local i = 1
  while i < #parts do
    local key = parts[i]
    local val = parts[i + 1]
    i = i + 2

    if key ~= "" then
      local pat  = '"' .. escPat(key) .. '":[%d%.%-]+'
      local repl = '"' .. key .. '":' .. val

      if raw:find('"' .. escPat(key) .. '":', 1, true) then
        raw = raw:gsub(pat, repl, 1)
      else
        raw = raw:gsub('}$', ',"' .. key .. '":' .. val .. '}')
      end
    end
  end
end

-- ── 6. Patch: null fields ─────────────────────────────────────────────────────
-- Format: "key\x1ekey\x1ekey..."
-- The fields we null (error, stacktrace, result, cron) can be strings, numbers,
-- booleans, or null.  We use a two-part pattern:
--   "([^"\\]|\\.)*"   — matches a JSON string value (handles embedded escapes)
--   [^,}]+            — matches a non-string scalar (number, bool, null)
-- This prevents the non-string branch from greedily consuming a nested } or
-- stopping too early inside a string that contains a comma.
if nullFields ~= "" then
  local parts = {}
  for part in (nullFields .. RS):gmatch("([^" .. RS .. "]*)" .. RS) do
    parts[#parts + 1] = part
  end

  for _, key in ipairs(parts) do
    if key ~= "" and raw:find('"' .. escPat(key) .. '":', 1, true) then
      -- Two-branch pattern: JSON string value OR non-string scalar.
      local pat = '"' .. escPat(key) .. '":' .. '("([^"\\\\]|\\\\.)*"|[^,}]+)'
      raw = raw:gsub(pat, '"' .. key .. '":null', 1)
    end
  end
end

-- ── 7. Persist updated JSON ───────────────────────────────────────────────────
redis.call("SET", hashKey, raw)

-- ── 8. Manage sorted-set membership ──────────────────────────────────────────
local jobId = hashKey:match("qjw:job:(.+)")

if newStatus == "retrying" then
  -- Re-enter both sets so the job is re-claimable after the backoff delay.
  redis.call("ZADD", waitingKey, runAt, jobId)
  -- Only update the priority score when the caller explicitly sent one
  -- (priority ~= "-1").  Sending "-1" means "keep existing priority".
  if priority ~= "-1" then
    redis.call("ZADD", indexKey, priority, jobId)
  end

elseif newStatus == "completed" or newStatus == "failed" then
  -- Terminal states: remove from both sets so the job stops being picked up.
  redis.call("ZREM", waitingKey, jobId)
  redis.call("ZREM", indexKey,   jobId)

elseif newStatus == "active" then
  -- BUG FIX (v4): only remove from the *waiting* set, NOT from the index.
  -- Removing from the index caused list-jobs.lua to miss active jobs entirely
  -- because list-jobs.lua iterates the index set to enumerate all jobs.
  redis.call("ZREM", waitingKey, jobId)
  -- Leave the job in indexKey so listJobs("active") finds it.

end

return 1
