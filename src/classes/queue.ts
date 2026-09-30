import { randomUUID } from "node:crypto";
import { Cron } from "croner";
import { QueueClient } from "./client.js";
import type { IStorage } from "../storage/storage.interface.js";
import type { Job, JobOptions, JobStatus } from "../types/job.types.js";
import type { QueueOptions } from "../types/queue.types.js";

export type { Job, JobOptions, JobStatus };

// ─── Queue name validation ────────────────────────────────────────────────────

const VALID_QUEUE_NAME = /^[a-zA-Z0-9_\-:.]+$/;

function assertQueueName(name: string): void {
  if (!name || !VALID_QUEUE_NAME.test(name)) {
    throw new Error(
      `[queue-jobs-worker] Invalid queue name "${name}". ` +
      "Queue names must be non-empty and may only contain: " +
      "letters, digits, hyphens (-), underscores (_), colons (:), and dots (.).",
    );
  }
}

/**
 * Queue — pure job producer.
 *
 * Responsible for one thing: managing a named collection of jobs in storage.
 * It does not poll, process, or execute anything. A Worker consumes from it.
 *
 * Usage
 * ─────
 * const client = new QueueClient({ dialect: "redis", connectionString: "…" });
 * await client.init();
 *
 * const queue = new Queue("emails", client);
 *
 * // Add jobs
 * const job = await queue.add("welcome", { to: "user@example.com" });
 * await queue.add("reminder", { userId: 42 }, { delay: 60_000 });
 * await queue.add("report",   {},             { cron: "0 9 * * *" });
 *
 * // Inspect
 * const job  = await queue.get(id);
 * const all  = await queue.list();
 * const n    = await queue.count();
 *
 * // Remove
 * await queue.remove(id);
 * await queue.clear();
 */
export class Queue<TData = unknown, TResult = unknown> {
  public readonly name: string;
  public readonly client: QueueClient;
  public readonly options: Required<QueueOptions>;

  /** Active croner handles keyed by jobId — used to cancel scheduled jobs. */
  private readonly _cronHandles = new Map<string, Cron>();

  constructor(
    name: string,
    client?: QueueClient,
    options: QueueOptions = {},
  ) {
    const activeClient = client ?? QueueClient.getDefaultClient();

    if (!activeClient?.isInitialized()) {
      throw new Error(
        "[queue-jobs-worker] QueueClient must be initialized before creating a Queue. " +
        "Call await client.init() first.",
      );
    }

    // Validate the queue name early — an invalid name used as a Redis key or
    // SQL column value could corrupt the key layout or enable injection.
    assertQueueName(name);

    this.name = name;
    this.client = activeClient;
    this.options = {
      rateLimit:      options.rateLimit      ?? { max: 0, duration: 0 },
      defaultJobOpts: options.defaultJobOpts ?? {},
    };
  }

  // ── Storage shortcut ──────────────────────────────────────────────────────

  private get storage(): IStorage {
    return this.client.getStorage();
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Add a job to the queue.
   *
   * The job is immediately persisted to storage with status `"waiting"`.
   * Cron jobs start as `"delayed"` and are moved to `"waiting"` on each tick.
   *
   * @param name - Job type identifier, e.g. `"send-welcome-email"`.
   * @param data - Arbitrary payload forwarded to the Worker handler.
   * @param opts - Per-job overrides for attempts, delay, priority, cron, etc.
   * @returns    The fully populated Job record (with auto-generated UUID id).
   */
  public async add(
    name: string,
    data: TData,
    opts: JobOptions = {},
  ): Promise<Job<TData, TResult>> {
    const clientCfg    = this.client.getConfig();
    const queueDefs    = this.options.defaultJobOpts;
    const delay        = opts.delay ?? queueDefs.delay ?? 0;
    const now          = Date.now();

    const job: Job<TData, TResult> = {
      id:           opts.jobId ?? randomUUID(),
      name,
      data,
      // Cron jobs begin as "delayed"; the croner callback flips them to
      // "waiting" on each scheduled tick so a Worker can pick them up.
      // Plain delayed jobs use "waiting" straight away — runAt gates pickup.
      status:       opts.cron ? "delayed" : "waiting",
      opts,
      attempts:     opts.attempts ?? queueDefs.attempts ?? clientCfg.attempts,
      attemptsMade: 0,
      delay,
      runAt:        now + delay,
      priority:     opts.priority ?? queueDefs.priority ?? 0,
      // exactOptionalPropertyTypes: only include the optional `cron` field
      // when it is actually defined — never assign undefined to it explicitly.
      ...(opts.cron !== undefined && { cron: opts.cron }),
      createdAt:    now,
      updatedAt:    now,
    };

    await this.storage.saveJob<TData, TResult>(this.name, job);
    this.client._log(`Queue[${this.name}] added "${name}" id=${job.id}`);

    if (opts.cron) {
      this._scheduleCron(job);
    }

    return job;
  }

  /**
   * Fetch a single job by ID.
   * Returns `undefined` when the job does not exist or has been removed.
   */
  public async get(jobId: string): Promise<Job<TData, TResult> | undefined> {
    return this.storage.getJob<TData, TResult>(this.name, jobId);
  }

  /**
   * Permanently remove a job from the queue.
   * If the job has an active cron schedule, it is also cancelled.
   */
  public async remove(jobId: string): Promise<void> {
    this._cancelCron(jobId);
    await this.storage.removeJob(this.name, jobId);
    this.client._log(`Queue[${this.name}] removed id=${jobId}`);
  }

  /**
   * List jobs, optionally filtered by status.
   * Results are sorted: priority ASC, createdAt ASC.
   *
   * @param status - `"waiting" | "delayed" | "active" | "completed" | "failed" | "retrying"`
   */
  public async list(status?: JobStatus): Promise<Job<TData, TResult>[]> {
    return this.storage.listJobs<TData, TResult>(this.name, status);
  }

  /**
   * Remove every job in this queue (all statuses).
   * All active cron schedules for this queue are also cancelled.
   */
  public async clear(): Promise<void> {
    for (const [id] of this._cronHandles) {
      this._cancelCron(id);
    }
    await this.storage.clearQueue(this.name);
    this.client._log(`Queue[${this.name}] cleared.`);
  }

  /**
   * Return the total number of jobs in this queue (all statuses combined).
   */
  public async count(): Promise<number> {
    return this.storage.countJobs(this.name);
  }

  // ── Internal — used by Worker only ───────────────────────────────────────

  /**
   * Atomically claim the next eligible job.
   * Eligibility: `status IN ('waiting','retrying') AND runAt <= now`.
   * Returns `undefined` when nothing is ready.
   * @internal
   */
  public async _nextJob(): Promise<Job<TData, TResult> | undefined> {
    return this.storage.getNextJob<TData, TResult>(this.name);
  }

  /**
   * Persist a partial status update to a job.
   * @internal
   */
  public async _updateJob(
    jobId: string,
    patch: Partial<Job<TData, TResult>>,
  ): Promise<void> {
    await this.storage.updateJob<TData, TResult>(this.name, jobId, patch);
  }

  // ── Cron helpers ──────────────────────────────────────────────────────────

  private _scheduleCron(job: Job<TData, TResult>): void {
    if (!job.cron) return;

    const handle = new Cron(job.cron, async () => {
      const current = await this.storage.getJob<TData, TResult>(this.name, job.id);
      if (!current) {
        this._cancelCron(job.id);
        return;
      }

      // Re-queue once the previous run has reached a terminal or idle state.
      // "completed"  — last run succeeded; start a fresh one.
      // "delayed"    — initial state before first run; start the first run.
      // "failed"     — last run exhausted retries; reset so cron keeps firing.
      //
      // Intentionally excluded:
      // "retrying"   — job failed once but still has remaining attempts and
      //                is scheduled to retry soon.  Resetting it here would
      //                wipe attemptsMade and runAt, causing it to skip its
      //                retry backoff and re-run prematurely.  The worker will
      //                pick it up naturally when runAt elapses.
      // "active"     — job is currently running; leave it alone.
      // "waiting"    — already queued for this cron tick; nothing to do.
      if (
        current.status === "completed" ||
        current.status === "delayed"   ||
        current.status === "failed"
      ) {
        await this.storage.updateJob<TData, TResult>(this.name, job.id, {
          status:       "waiting",
          attemptsMade: 0,
          runAt:        Date.now(),
          // Clear residual values from the previous run. Passing null is
          // intentional — storage backends treat it as JSON null (cleared),
          // and the Lua update-job.lua handles null fields explicitly.
          // We cast to satisfy exactOptionalPropertyTypes without using the
          // forbidden `undefined` assignment to an optional property.
          error:      null as unknown as string,
          stacktrace: null as unknown as string,
          result:     null as unknown as TResult,
        });
        this.client._log(
          `Queue[${this.name}] cron tick — "${job.name}" id=${job.id} re-queued`,
        );
      }
    });

    this._cronHandles.set(job.id, handle);
  }

  private _cancelCron(jobId: string): void {
    const handle = this._cronHandles.get(jobId);
    if (handle) {
      handle.stop();
      this._cronHandles.delete(jobId);
    }
  }
}
