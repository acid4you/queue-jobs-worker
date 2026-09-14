# Changelog — types

Changes to the types module: all shared TypeScript type definitions and interfaces.

---

## [1.0.5] — 2026-09-15

### Added

- **`lua.d.ts` — ambient module declaration for `.lua` imports**
  - Declares `declare module "*.lua"` so TypeScript recognises `.lua` files as `string`-exporting modules.
  - Required by `src/lib/scripts/index.ts` to import Lua scripts directly without type errors.

---
