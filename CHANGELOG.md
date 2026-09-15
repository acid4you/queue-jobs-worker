# Changelog

All notable changes to **queue-jobs-worker** will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.5] — 2026-09-15

### Core

### Fixed

- **Storage Initialization Enforcement prior to Queue Creation & Execution** ([#14](https://github.com/rafidahmed870/queue-jobs-worker/issues/14))

  Previously, `QueueClient.createQueue()` allowed queues to be created before `await client.init()` was called when using external storage dialects (e.g. Redis, PostgreSQL, MySQL). This caused the created queue to bind to the temporary `InMemoryStorageAdapter` instance. When `client.init()` was subsequently called, the real external storage adapter replaced the internal storage field on `QueueClient`, rendering previously enqueued jobs lost or inaccessible.

  After the fix:

  - `QueueClient.createQueue()` checks `isInitialised` before creating a queue. For external dialects and custom adapters, attempting to create a queue before `await client.init()` throws an explicit error.
  - Queue operations (`enqueue`, `getJob`, `getJobs`, `getJobCounts`, and `createWorker`) enforce initialization status checks before executing, preventing operations on uninitialized storage.
  - In-memory dialect continues to auto-initialize synchronously, preserving convenient single-line setup for tests and local development.

---

### Events

### Added

- `QueueEventEmitter` — strongly-typed lifecycle event bus shared across all components.
- Emits events for the full job lifecycle: enqueued, started, completed, failed, retrying, dead, stalled.
- All event payloads fully typed via `events.types.ts`.

---

<!-- Links -->
[1.0.0]: https://github.com/rafidahmed870/queue-jobs-worker/releases/tag/v1.0.0

### Lib

### Added

- **`lib/scripts/` — Redis Lua scripts extracted to standalone `.lua` files**
  - `claim.lua` — atomic job claim with delayed-job promotion.
  - `recover-stalled.lua` — compare-and-swap stalled job recovery.
  - `renew-lock.lua` — atomic lock renewal with ownership guard.
  - `rate-limit.lua` — atomic rate limit decision, reset, and counter increment.
  - `lib/scripts/index.ts` re-exports scripts as named string constants (`CLAIM_LUA`, `RECOVER_STALLED_LUA`, `RENEW_LOCK_LUA`, `RATE_LIMIT_LUA`).
  - Scripts are embedded into the CJS/ESM distribution bundles at build time via `tsup`'s `loader: { ".lua": "text" }`.

---

### Storage

### Added

- **Consistent timestamps across Lua scripts**
  - Changed `RedisStorageAdapter` to use `now_iso` from `ARGV[3]` for `updatedAt` in `CLAIM_LUA` and `RECOVER_STALLED_LUA`.
  - Previously, `updatedAt` was sometimes derived from `lockExpiresAt`, which could differ from the actual time of the operation.

### Fixed

- **Atomic rate limiting across Redis, PostgreSQL, and MySQL adapters**
  - `RedisStorageAdapter`: Implemented `rate-limit.lua` (`RATE_LIMIT_LUA`) script to perform window check, expiry reset, counter evaluation, increment, and TTL renewal atomically inside Redis.
  - `PostgreSQLStorageAdapter`: Wrapped `checkAndIncrementRateLimit` in a pool client transaction (`BEGIN ... COMMIT`) utilizing `INSERT ... ON CONFLICT DO NOTHING` and `SELECT ... FOR UPDATE` row locking.
  - `MySQLStorageAdapter`: Wrapped `checkAndIncrementRateLimit` in a connection transaction (`beginTransaction ... commit`) utilizing `INSERT ... ON DUPLICATE KEY UPDATE` and `SELECT ... FOR UPDATE` row locking.


---

### Types

### Added

- **`lua.d.ts` — ambient module declaration for `.lua` imports**
  - Declares `declare module "*.lua"` so TypeScript recognises `.lua` files as `string`-exporting modules.
  - Required by `src/lib/scripts/index.ts` to import Lua scripts directly without type errors.

---

### Tests

### Added

- **`lua-scripts.test.ts` — unit tests for Redis Lua scripts**
  - 22 tests covering all four Lua scripts (`CLAIM_LUA`, `RECOVER_STALLED_LUA`, `RENEW_LOCK_LUA`, `RATE_LIMIT_LUA`).
  - Verifies each script loads as a non-empty string from `src/lib/scripts/index.ts`.
  - Asserts presence of critical Redis commands (`ZPOPMIN`, `ZRANGEBYSCORE`, `SADD`, `HSET`, `SREM`, `ZADD`, `INCR`, `EXPIRE`) and CAS/RateLimit guard conditions.
- **`vitest.config.ts` — `rawLuaPlugin` added**
  - Custom Vite transform plugin that loads `.lua` files as raw text strings during tests, mirroring `tsup`'s `loader: { ".lua": "text" }` used at build time.

---

## [1.0.4] — 2026-09-13

### Core

### Fixed

- **`Worker` — Job timeout cooperative cancellation via `AbortSignal`** ([#12](https://github.com/rafidahmed870/queue-jobs-worker/issues/12))

  Previously, when a job attempt reached its configured `timeout`, the worker rejected the internal execution promise and marked the attempt as failed (or scheduled a retry), but the underlying processor `Promise` continued running in the background. This could lead to duplicate side effects when retries overlapped with timed-out attempts.

  After the fix:

  - `Processor` type signature is updated: `type Processor<TPayload = unknown> = (job: Job<TPayload>, signal: AbortSignal) => Promise<void>`.
  - An `AbortController` is created for each job attempt.
  - When job execution times out, the worker aborts the `AbortSignal` with a timeout error before rejecting the wrapper promise.
  - User processors can monitor `signal.aborted` or pass `signal` to async operations (e.g. `fetch`, database queries, timers) for cooperative cancellation.

---

### Events

### Added

- `QueueEventEmitter` — strongly-typed lifecycle event bus shared across all components.
- Emits events for the full job lifecycle: enqueued, started, completed, failed, retrying, dead, stalled.
- All event payloads fully typed via `events.types.ts`.

---

<!-- Links -->

[1.0.5]: https://github.com/rafidahmed870/queue-jobs-worker/compare/v1.0.0...v1.0.5
[1.0.0]: https://github.com/rafidahmed870/queue-jobs-worker/releases/tag/v1.0.0
[1.0.4]: https://github.com/rafidahmed870/queue-jobs-worker/compare/v1.0.0...v1.0.4
[1.0.0]: https://github.com/rafidahmed870/queue-jobs-worker/releases/tag/v1.0.0
[1.0.3]: https://github.com/rafidahmed870/queue-jobs-worker/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/rafidahmed870/queue-jobs-worker/compare/v1.0.1...v1.0.2
[1.0.0]: https://github.com/rafidahmed870/queue-jobs-worker/releases/tag/v1.0.0
[1.0.1]: https://github.com/rafidahmed870/queue-jobs-worker/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/rafidahmed870/queue-jobs-worker/releases/tag/v1.0.0
