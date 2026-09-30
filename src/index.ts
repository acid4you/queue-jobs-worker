// ── Core classes ──────────────────────────────────────────────────────────────
export { QueueClient } from "./classes/client.js";
export { Queue } from "./classes/queue.js";
export { Worker } from "./classes/worker.js";

// ── Worker types ──────────────────────────────────────────────────────────────
export type { WorkerHandler, WorkerEvents } from "./classes/worker.js";

// ── All public types ──────────────────────────────────────────────────────────
export type {
  // Client
  StorageDialect,
  BackoffStrategy,
  RateLimitOptions,
  QueueClientConfigOptions,
  QueueClientOptions,
  // Legacy lowercase aliases (backward compat)
  storageDialect,
  rateLimitOptions,
  // Queue / Worker
  QueueOptions,
  WorkerOptions,
  // Job
  Job,
  JobOptions,
  JobStatus,
} from "./types/index.js";

// ── Storage interface (for custom backend authors) ────────────────────────────
export type { IStorage } from "./storage/storage.interface.js";

// ── Package version ───────────────────────────────────────────────────────────
export { VERSION } from "./version.js";
