import type { RateLimitOptions } from "./client.types.js";

/**
 * Options for `new Queue(name, client, options)`.
 *
 * Queue is a pure producer — it only manages job storage.
 * Concurrency, polling, and execution belong to the Worker.
 *
 * @property rateLimit      - Optional rate-limit config (max jobs per duration).
 * @property defaultJobOpts - Per-queue job defaults (override client-level defaults).
 */
export interface QueueOptions {
  rateLimit?: RateLimitOptions;
  defaultJobOpts?: {
    attempts?: number;
    delay?: number;
    priority?: number;
    removeOnComplete?: boolean;
    removeOnFail?: boolean;
  };
}

/**
 * Options for `new Worker(queue, handler, options)`.
 *
 * Worker is a pure consumer — it only processes jobs from a Queue.
 *
 * @property concurrency  - Max parallel jobs. Default: 1.
 * @property pollInterval - Ms between queue polls when idle. Default: 500.
 */
export interface WorkerOptions {
  concurrency?: number;
  pollInterval?: number;
}

// Re-export for convenience.
export type { RateLimitOptions };
