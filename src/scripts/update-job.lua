--[[
  update-job.lua  v5
  Atomically read, patch, and re-persist a job — entirely server-side.

  One Redis round-trip: GET + targeted string replacement + SET.

  ── KEYS ──────────────────────────────────────────────────────────────────────
    [1]  hashKey    — "qjw:job:{jobId}"
    [2]  indexKey   — "qjw:{queue}:index"    sorted set, score = priority
    [3]  waitingKey — "qjw:{queue}:waiting"  sorted set, score = runAt ms

  ── ARGV ──────────────────────────────────────────────────────────────────────
    [1]  newStatus   — target status string (always patched)
    [2]  nowMs       — current Unix timestamp ms (always written as updatedAt)
    [3]  runAt       — runAt ms for "retrying" re-queue, or "-1" as sentinel
                       meaning "do not touch runAt / skip ZADD waitingKey".
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
    [8]  boolFields  — RS-delimited "key\x1evalue" pairs for boolean fields.
                       value is "true" or "false" (the JSON literal).
                       Needed because booleans are stored as JSON true/false
                       but cannot be matched by the numeric pattern [%d%.%-]+.
                       Empty string "" means no boolean fields to patch.

  ── Replacement strategy ──────────────────────────────────────────────────────
    String fields:  "key":"<oldvalue>"  →  "key":"<newvalue>"
      <oldvalue> matched with ([^"\\]|\\.)*  (handles embedded escapes).

    Numeric fields: "key":<oldnumber>   →  "key":<newvalue>
      <oldnumber> matches integers, decimals, and negatives: [%d%.%-]+

    Boolean fields: "key":<true|false>  →  "key":<true|false>
      BUG FIX (v5): the previous code sent booleans through numFields as "1"/"0"
      but the existing JSON stores them as the literals true/false.  The numeric
      pattern [%d%.%-]+ never matched those literals, so boolean patches were
      silently dropped.  boolFields now carries the raw JSON literal ("true" /
      "false") and uses an exact-word pattern instead.

    Null fields: current value (string, number, bool, null, object/array) → null.
      Two-branch pattern: full JSON-string value OR non-string scalar/token.

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
local boolFields = ARGV[8] or ""   -- new in v5; default "" for back-compat

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

-- Split an RS-delimited string into an array of parts.
local function splitRS(s)
  local parts = {}
  for part in (s .. RS):gmatch("([^" .. RS .. "]*)" .. RS) do
    parts[#parts + 1] = part
  end
  return parts
end

-- ── 2. Patch: status (always) ─────────────────────────────────────────────────
-- status values are simple ASCII words with no special chars — plain replacement.
raw = raw:gsub('"status":"[^"]*"', '"status":"' .. newStatus .. '"', 1)

-- ── 3. Patch: updatedAt (always, numeric) ────────────────────────────────────
raw = raw:gsub('"updatedAt":[%d%.%-]+', '"updatedAt":' .. nowMs, 1)

-- ── 4. Patch: string fields ───────────────────────────────────────────────────
if strFields ~= "" then
  local parts = splitRS(strFields)
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
if numFields ~= "" then
  local parts = splitRS(numFields)
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

-- ── 6. Patch: boolean fields (new in v5) ─────────────────────────────────────
-- BUG FIX: booleans are stored as JSON literals true/false but were previously
-- sent through numFields as "1"/"0".  The numeric pattern [%d%.%-]+ never
-- matched those literals so boolean patches were silently dropped.
-- boolFields carries the raw JSON literal ("true" or "false") and we match
-- the existing literal with an exact-word alternation pattern.
if boolFields ~= "" then
  local parts = splitRS(boolFields)
  local i = 1
  while i < #parts do
    local key = parts[i]
    local val = parts[i + 1]   -- "true" or "false"
    i = i + 2

    if key ~= "" then
      -- Match either the literal true or false (both are possible stored values).
      local pat  = '"' .. escPat(key) .. '":' .. '(true|false)'
      local repl = '"' .. key .. '":' .. val

      if raw:find('"' .. escPat(key) .. '":', 1, true) then
        raw = raw:gsub(pat, repl, 1)
      else
        raw = raw:gsub('}$', ',"' .. key .. '":' .. val .. '}')
      end
    end
  end
end

-- ── 7. Patch: null fields ─────────────────────────────────────────────────────
if nullFields ~= "" then
  local parts = splitRS(nullFields)
  for _, key in ipairs(parts) do
    if key ~= "" and raw:find('"' .. escPat(key) .. '":', 1, true) then
      -- Two-branch pattern: JSON string value OR non-string scalar.
      local pat = '"' .. escPat(key) .. '":' .. '("([^"\\\\]|\\\\.)*"|[^,}]+)'
      raw = raw:gsub(pat, '"' .. key .. '":null', 1)
    end
  end
end

-- ── 8. Persist updated JSON ───────────────────────────────────────────────────
redis.call("SET", hashKey, raw)

-- ── 9. Manage sorted-set membership ──────────────────────────────────────────
local jobId = hashKey:match("qjw:job:(.+)")

if newStatus == "retrying" then
  -- Re-enter both sets so the job is re-claimable after the backoff delay.
  -- runAt "-1" is the sentinel meaning "no runAt in this patch" — skip ZADD.
  if runAt ~= "-1" then
    redis.call("ZADD", waitingKey, runAt, jobId)
  end
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
  -- Only remove from the *waiting* set, NOT from the index.
  -- The index tracks ALL jobs regardless of status so listJobs("active") works.
  redis.call("ZREM", waitingKey, jobId)

end

return 1
