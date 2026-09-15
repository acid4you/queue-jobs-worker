# Changelog — tests

Changes to the test suite: unit tests, integration tests, and test infrastructure.

---

## [1.0.5] — 2026-09-15

### Added

- **`lua-scripts.test.ts` — unit tests for Redis Lua scripts**
  - 22 tests covering all four Lua scripts (`CLAIM_LUA`, `RECOVER_STALLED_LUA`, `RENEW_LOCK_LUA`, `RATE_LIMIT_LUA`).
  - Verifies each script loads as a non-empty string from `src/lib/scripts/index.ts`.
  - Asserts presence of critical Redis commands (`ZPOPMIN`, `ZRANGEBYSCORE`, `SADD`, `HSET`, `SREM`, `ZADD`, `INCR`, `EXPIRE`) and CAS/RateLimit guard conditions.
- **`vitest.config.ts` — `rawLuaPlugin` added**
  - Custom Vite transform plugin that loads `.lua` files as raw text strings during tests, mirroring `tsup`'s `loader: { ".lua": "text" }` used at build time.

---
