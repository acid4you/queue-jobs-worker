/**
 * All possible lifecycle states a job can be in.
 *
 * waiting   — Job has been added to the queue and is waiting to be picked up.
 * delayed   — Job is scheduled to run in the future (delay or cron).
 * active    — Job is currently being processed by a worker.
 * completed — Job finished successfully.
 * failed    — Job exhausted all retry attempts and is permanently failed.
 * retrying  — Job failed once but still has remaining attempts; waiting for next retry.
 */
export type JobStatus = "waiting" | "delayed" | "active" | "completed" | "failed" | "retrying";

/**
 * Options you can pass when adding a job to the queue.
 *
 * @property attempts   - Max retry attempts for this specific job (overrides queue default).
 * @property delay      - Milliseconds to wait before the job becomes eligible to run.
 * @property priority   - Lower number = higher priority (default: 0).
 * @property jobId      - Custom job ID. Auto-generated (crypto UUID) if not provided.
 * @property cron       - A cron expression to schedule the job on a recurring schedule.
 *                        Uses the `croner` library syntax, e.g. "0 * * * *" (every hour).
 * @property removeOnComplete - Auto-remove the job from storage after it completes.
 * @property removeOnFail     - Auto-remove the job from storage after it permanently fails.
 */
export interface JobOptions {
  attempts?: number;
  delay?: number;
  priority?: number;
  jobId?: string;
  cron?: string;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
}

/**
 * The full job record stored in the queue.
 *
 * @property id          - Unique identifier (UUID v4 via node:crypto).
 * @property name        - Logical job name, e.g. "send-welcome-email".
 * @property data        - Arbitrary payload passed to the worker handler.
 * @property status      - Current lifecycle state.
 * @property opts        - Original options this job was created with.
 * @property attempts    - Max number of attempts allowed.
 * @property attemptsMade - Number of attempts that have been made so far.
 * @property delay       - Milliseconds to wait before first run.
 * @property runAt       - Absolute timestamp (ms) when the job becomes eligible.
 * @property priority    - Scheduling priority; lower = runs first.
 * @property cron        - Cron expression for recurring jobs.
 * @property result      - Return value from the handler on success.
 * @property error       - Error message from the last failed attempt.
 * @property stacktrace  - Error stack trace from the last failed attempt.
 * @property createdAt   - Unix timestamp (ms) when the job was created.
 * @property updatedAt   - Unix timestamp (ms) of the last status change.
 * @property processedAt - Unix timestamp (ms) when processing started.
 * @property finishedAt  - Unix timestamp (ms) when the job completed or permanently failed.
 */
export interface Job<TData = unknown, TResult = unknown> {
  id: string;
  name: string;
  data: TData;
  status: JobStatus;
  opts: JobOptions;
  attempts: number;
  attemptsMade: number;
  delay: number;
  runAt: number;
  priority: number;
  cron?: string;
  result?: TResult;
  error?: string;
  stacktrace?: string;
  createdAt: number;
  updatedAt: number;
  processedAt?: number;
  finishedAt?: number;
}
