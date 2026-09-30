import type { Job, JobStatus } from "../types/job.types.js";

/**
 * Every storage backend must implement this interface.
 * The Queue and Worker classes talk exclusively to this contract —
 * they never know whether the data is in a Map, Redis, Postgres, or MySQL.
 */
export interface IStorage {
  /**
   * Open the connection / initialise the storage.
   * Called once by QueueClient.init().
   */
  connect(): Promise<void>;

  /**
   * Close the connection and release all resources.
   * Called once by QueueClient.close().
   */
  disconnect(): Promise<void>;

  // ─── Job CRUD ────────────────────────────────────────────────────────────

  /** Persist a new job. */
  saveJob<TData = unknown, TResult = unknown>(
    queueName: string,
    job: Job<TData, TResult>,
  ): Promise<void>;

  /** Fetch a single job by its ID. Returns undefined if not found. */
  getJob<TData = unknown, TResult = unknown>(
    queueName: string,
    jobId: string,
  ): Promise<Job<TData, TResult> | undefined>;

  /** Update an existing job (partial or full). */
  updateJob<TData = unknown, TResult = unknown>(
    queueName: string,
    jobId: string,
    patch: Partial<Job<TData, TResult>>,
  ): Promise<void>;

  /** Permanently remove a job from storage. */
  removeJob(queueName: string, jobId: string): Promise<void>;

  // ─── Querying ─────────────────────────────────────────────────────────────

  /**
   * Return all jobs for a queue, optionally filtered by status.
   * Results are sorted: lower priority number first, then older createdAt first.
   */
  listJobs<TData = unknown, TResult = unknown>(
    queueName: string,
    status?: JobStatus,
  ): Promise<Job<TData, TResult>[]>;

  /**
   * Pick the next job that is eligible to run right now.
   *
   * Eligibility rules:
   *  - status === "waiting" AND runAt <= Date.now()
   *  - OR status === "retrying" AND runAt <= Date.now()
   *
   * Returns undefined when the queue has nothing ready.
   */
  getNextJob<TData = unknown, TResult = unknown>(
    queueName: string,
  ): Promise<Job<TData, TResult> | undefined>;

  /** Remove every job in a queue. */
  clearQueue(queueName: string): Promise<void>;

  /** Return the total number of jobs in a queue (all statuses). */
  countJobs(queueName: string): Promise<number>;
}
