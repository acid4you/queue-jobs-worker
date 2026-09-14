# Changelog — lib

Changes to the lib module: shared utilities and internal script loaders.

---

## [1.0.5] — 2026-09-15

### Added

- **`lib/scripts/` — Redis Lua scripts extracted to standalone `.lua` files**
  - `claim.lua` — atomic job claim with delayed-job promotion.
  - `recover-stalled.lua` — compare-and-swap stalled job recovery.
  - `renew-lock.lua` — atomic lock renewal with ownership guard.
  - `lib/scripts/index.ts` re-exports all three scripts as named string constants (`CLAIM_LUA`, `RECOVER_STALLED_LUA`, `RENEW_LOCK_LUA`).
  - Scripts are embedded into the CJS/ESM distribution bundles at build time via `tsup`'s `loader: { ".lua": "text" }`.

---
