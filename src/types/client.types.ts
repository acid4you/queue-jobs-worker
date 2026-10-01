/**
 * The storage backend to use.
 *
 * - memory   — In-process Map; no external dependencies. Good for dev and testing.
 * - redis    — Redis via the `redis` npm package (peer dependency).
 * - postgres — PostgreSQL via the `pg` npm package (peer dependency).
 * - mysql    — MySQL/MariaDB via the `mysql2` npm package (peer dependency).
 */
export type StorageDialect = "memory" | "redis" | "mysql" | "postgres";

/**
 * The retry backoff strategy applied between successive attempts.
 *
 * - fixed       — Always wait `retryDelay` ms.
 * - linear      — Wait `retryDelay * attemptNumber` ms.
 * - exponential — Wait `retryDelay * 2^(attemptNumber - 1)` ms.
 */
export type BackoffStrategy = "fixed" | "linear" | "exponential";

/**
 * Global defaults applied to every job unless overridden at the queue or job level.
 *
 * @property attempts    - Max retry attempts per job (0 = unlimited). Default: 3.
 * @property retryDelay  - Base delay in ms before the first retry. Default: 1000.
 * @property backoff     - Backoff strategy. Default: "exponential".
 * @property timeout     - Max ms a job can run before being marked failed. Default: 30000.
 */
export interface QueueClientConfigOptions {
  attempts?: number;
  retryDelay?: number;
  backoff?: BackoffStrategy;
  timeout?: number;
}

/**
 * Options passed to `new QueueClient(...)`.
 *
 * @property dialect          - Which storage backend to use.
 * @property connectionString - DSN/URL for Redis, PostgreSQL, or MySQL.
 *                              Not required for `memory` dialect.
 * @property debug            - When true, the client logs internal operations to console.
 * @property options          - Global job execution defaults.
 */
export interface QueueClientOptions {
  dialect: StorageDialect;
  connectionString?: string;
  debug?: boolean;
  options?: QueueClientConfigOptions;
}

// Legacy alias kept so existing user code that already imports `storageDialect`
// (lowercase) continues to compile without changes.
export type storageDialect = StorageDialect;

// Legacy alias kept for backward compatibility.
export type rateLimitOptions = RateLimitOptions;

/**
 * Rate-limiting configuration for a queue.
 *
 * @property max      - Maximum number of jobs to start within the `duration` window.
 * @property duration - Length of the rate-limit window in milliseconds.
 */
export interface RateLimitOptions {
  max: number;
  duration: number;
}
