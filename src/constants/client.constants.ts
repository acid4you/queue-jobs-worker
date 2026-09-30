import type { QueueClientConfigOptions } from "../types/client.types.js";

/**
 * Sensible defaults applied to every job unless the user overrides them
 * at the client, queue, or individual job level.
 */
export const DEFAULT_QUEUE_CONFIG: Required<QueueClientConfigOptions> = {
  attempts: 3,
  retryDelay: 1000, // 1 second base delay
  backoff: "exponential",
  timeout: 30_000, // 30 seconds
};
