--[[
  update-job.lua  v6
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
    [4]  priority    — job priority integer, or "-1" sentinel ("keep existing").
    [5]  strFields   — RS-delimited "key\x1evalue" pairs for string fields.
    [6]  numFields   — RS-delimited "key\x1evalue" pairs for numeric fields.
    [7]  nullFields  — RS-delimited field names to set to JSON null.
    [8]  boolFields  — RS-delimited "key\x1evalue" pairs for boolean fields
                       ("true" / "false" JSON literals).

  ── Returns ───────────────────────────────────────────────────────────────────
    1 on success, 0 if the job does not exist.
--]]

local hashKey    = KEYS[1]
local indexKey   = KEYS[2]
local waitingKey = KEYS[3]

local newStatus  = ARGV[1]
local nowMs      = ARGV[2]
local runAt      = ARGV[3]
local priority   = ARGV[4]
local strFields  = ARGV[5]
local numFields  = ARGV[6]
local nullFields = ARGV[7]
local boolFields = ARGV[8] or ""

local RS = "\x1e"

-- ── 1. Read current job JSON ──────────────────────────────────────────────────
local raw = redis.call("GET", hashKey)
if not raw then return 0 end

-- ── helpers ───────────────────────────────────────────────────────────────────

-- Escape for Lua gsub() PATTERN (not replacement).
local function escPat(s)
  return s:gsub("([%(%)%.%%%+%-%*%?%[%]%^%$])", "%%%1")
end

-- Escape for Lua gsub() REPLACEMENT string.
local function escRepl(s)
  return s:gsub("%%", "%%%%")
end

-- Split RS-delimited string into array.
local function splitRS(s)
  local parts = {}
  for part in (s .. RS):gmatch("([^" .. RS .. "]*)" .. RS) do
    parts[#parts + 1] = part
  end
  return parts
end

-- BUG FIX (v6): existence check uses plain literal search (4th arg = true).
-- Previously `find('"' .. escPat(key) .. '":', 1, true)` was used, but the
-- escPat() output contains % escapes that are meaningless in plain-text mode
-- and could corrupt the search string if the key contained literal %.
-- Using the raw key with plain=true is both correct and simpler.
local function hasField(json, key)
  return json:find('"' .. key .. '":', 1, true) ~= nil
end

-- ── 2. Patch: status (always) ─────────────────────────────────────────────────
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
      local pat  = '"' .. escPat(key) .. '":"([^"\\\\]|\\\\.)*"'
      local repl = '"' .. key .. '":"' .. escRepl(val) .. '"'

      if hasField(raw, key) then
        raw = raw:gsub(pat, repl, 1)
      else
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

      if hasField(raw, key) then
        raw = raw:gsub(pat, repl, 1)
      else
        raw = raw:gsub('}$', ',"' .. key .. '":' .. val .. '}')
      end
    end
  end
end

-- ── 6. Patch: boolean fields ──────────────────────────────────────────────────
if boolFields ~= "" then
  local parts = splitRS(boolFields)
  local i = 1
  while i < #parts do
    local key = parts[i]
    local val = parts[i + 1]   -- "true" or "false"
    i = i + 2

    if key ~= "" then
      local pat  = '"' .. escPat(key) .. '":(true|false)'
      local repl = '"' .. key .. '":' .. val

      if hasField(raw, key) then
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
    if key ~= "" and hasField(raw, key) then
      local pat = '"' .. escPat(key) .. '":("([^"\\\\]|\\\\.)*"|[^,}]+)'
      raw = raw:gsub(pat, '"' .. key .. '":null', 1)
    end
  end
end

-- ── 8. Persist updated JSON ───────────────────────────────────────────────────
redis.call("SET", hashKey, raw)

-- ── 9. Manage sorted-set membership ──────────────────────────────────────────
local jobId = hashKey:match("qjw:job:(.+)")

if newStatus == "retrying" then
  if runAt ~= "-1" then
    redis.call("ZADD", waitingKey, runAt, jobId)
  end
  if priority ~= "-1" then
    redis.call("ZADD", indexKey, priority, jobId)
  end

elseif newStatus == "completed" or newStatus == "failed" then
  redis.call("ZREM", waitingKey, jobId)
  redis.call("ZREM", indexKey,   jobId)

elseif newStatus == "active" then
  redis.call("ZREM", waitingKey, jobId)

end

return 1
