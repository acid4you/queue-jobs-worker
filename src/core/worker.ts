/**
 * Worker
 *
 * Claims jobs from a queue and executes registered processors concurrently.
 *
 * Key guarantees:
 *  - At-most `concurrency` jobs run simultaneously.
 *  - Job claiming is delegated to the storage adapter's atomic `claim()` call.
 *  - Failed jobs are requeued (with backoff) or moved to the DLQ.
 *  - Stalled jobs (expired locks from crashed workers) are recovered
 *    periodically.
 *  - Graceful shutdown: stop claiming, wait for active jobs, release any
 *    locks that could not be finished within shutdownTimeout.
 *  - Cron jobs: re-enqueued immediately after each successful execution.
 */

import { randomUUID } from "node:crypto";
import { Cron } from "croner";
import type { StorageAdapter } from "../types/storage.types.js";
import type { WorkerOptions, WorkerStatus, Processor } from "../types/worker.types.js";
import type { QueueOptions } from "../types/queue.types.js";
import type { ClientDefaults } from "../types/client.types.js";
import { Job } from "./job.js";
import { calculateBackoff, nextRunAt } from "./backoff.js";
import type { QueueEventEmitter } from "../events/emitter.js";

type ResolvedDefaults = Required<ClientDefaults>;

/**
 * Helper to generate a deterministic job ID for the next occurrence of a cron job.
 * Caps ID length and prevents nested `cron:cron:...` prefixes over multiple runs.
 */
export function generateCronNextJobId(currentJobId: string, nextDate: Date): string {
  const match = /^cron:(.+):(\d+)$/.exec(currentJobId);
  const rootId = match ? match[1]! : currentJobId;
  return `cron:${rootId}:${nextDate.getTime()}`;
}

// ---------------------------------------------------------------------------
// Resolved config helper
// ---------------------------------------------------------------------------

function resolveConfig(
  workerOptions: WorkerOptions,
  queueOptions: QueueOptions,
  defaults: ResolvedDefaults,
) {
  return {
    concurrency: workerOptions.concurrency ?? queueOptions.concurrency ?? defaults.concurrency,
    shutdownTimeout: workerOptions.shutdownTimeout ?? 30_000,
    pollInterval: queueOptions.pollInterval ?? defaults.pollInterval,
    stalledInterval: queueOptions.stalledInterval ?? defaults.stalledInterval,
    lockDuration: queueOptions.lockDuration ?? defaults.lockDuration,
    rateLimit: queueOptions.rateLimit ?? defaults.rateLimit,
  };
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

export class Worker {
  /** Unique identifier for this worker instance. */
  readonly id: string;

  private readonly queueName: string;
  private readonly storage: StorageAdapter;
  private readonly emitter: QueueEventEmitter;
  private readonly processors: Map<string, Processor<unknown>>;
  private readonly config: ReturnType<typeof resolveConfig>;

  private _status: WorkerStatus = "idle";
  private activeCount = 0;

  /**
   * Tracks job IDs currently being processed so we can release their locks
   * when graceful shutdown times out before they finish.
   */
  private readonly activeJobIds = new Set<string>();

  private pollTimer: NodeJS.Timeout | null = null;
  private stalledTimer: NodeJS.Timeout | null = null;

  /** Resolves when all active jobs finish during shutdown. */
  private drainResolve: (() => void) | null = null;

  constructor(
    queueName: string,
    storage: StorageAdapter,
    emitter: QueueEventEmitter,
    processors: Map<string, Processor<unknown>>,
    workerOptions: WorkerOptions,
    queueOptions: QueueOptions,
    defaults: ResolvedDefaults,
  ) {
    this.id = `worker:${queueName}:${randomUUID()}`;
    this.queueName = queueName;
    this.storage = storage;
    this.emitter = emitter;
    this.processors = processors;
    this.config = resolveConfig(workerOptions, queueOptions, defaults);
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  get status(): WorkerStatus {
    return this._status;
  }

  /** Start polling for jobs. */
  start(): void {
    if (this._status !== "idle" && this._status !== "stopped") {
      return;
    }

    this._status = "running";
    this.emitter.emit("worker:started", this.id);
    this.emitter.emit("worker:status", this.id, this._status);

    this.schedulePoll();
    this.scheduleStallCheck();
    void this.recoverCronJobs();
  }

  /**
   * Gracefully stop the worker.
   *
   * 1. Stop accepting new jobs.
   * 2. Wait up to `shutdownTimeout` ms for active jobs to finish.
   * 3. Release locks on any jobs that did not finish in time so another
   *    worker can reclaim them.
   * 4. Emit stopped event.
   */
  async stop(): Promise<void> {
    if (this._status === "stopped" || this._status === "stopping") {
      return;
    }

    this._status = "stopping";
    this.emitter.emit("worker:status", this.id, this._status);

    // Cancel timers.
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.stalledTimer) {
      clearTimeout(this.stalledTimer);
      this.stalledTimer = null;
    }

    // Wait for active jobs to finish, but no longer than shutdownTimeout.
    if (this.activeCount > 0) {
      await Promise.race([
        new Promise<void>((resolve) => {
          this.drainResolve = resolve;
        }),
        new Promise<void>((resolve) => setTimeout(resolve, this.config.shutdownTimeout)),
      ]);
    }

    // Release locks for any jobs that did not finish in time.
    // releaseLock() sets lockExpiresAt to an already-expired timestamp so that
    // recoverStalledJobs() on any worker will immediately pick them up and
    // return them to "waiting" — rather than leaving them stuck in "active"
    // forever (which would happen if lockExpiresAt were cleared to null/empty,
    // since every adapter's stalled-job check requires a non-null expired value).
    if (this.activeJobIds.size > 0) {
      await Promise.all(
        Array.from(this.activeJobIds).map((jobId) =>
          this.storage.releaseLock(jobId, this.id).catch(() => {
            // Best-effort — storage may already be unavailable during shutdown.
          }),
        ),
      );
    }

    this._status = "stopped";
    this.emitter.emit("worker:stopped", this.id);
    this.emitter.emit("worker:status", this.id, this._status);
  }

  // -------------------------------------------------------------------------
  // Poll loop
  // -------------------------------------------------------------------------

  private schedulePoll(): void {
    if (this._status !== "running") return;

    this.pollTimer = setTimeout(() => {
      void this.poll();
    }, this.config.pollInterval);
  }

  private async poll(): Promise<void> {
    if (this._status !== "running") return;

    try {
      // Fill up to concurrency limit.
      while (this._status === "running" && this.activeCount < this.config.concurrency) {
        const claimed = await this.claimNext();
        if (!claimed) break; // No eligible jobs right now.
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.emitter.emit("queue:error", this.queueName, error);
      this.emitter.emit("worker:error", this.id, error);
    }

    this.schedulePoll();
  }

  // -------------------------------------------------------------------------
  // Claim & execute
  // -------------------------------------------------------------------------

  private async claimNext(): Promise<boolean> {
    const now = new Date().toISOString();

    // Attempt to claim a job before touching the rate-limit counter.
    // The counter must only be consumed when a job is actually claimed for
    // processing — polling an empty queue must not burn quota (issue #4).
    const raw = await this.storage.claim({
      queue: this.queueName,
      lockId: this.id,
      lockDuration: this.config.lockDuration,
      now,
    });

    if (!raw) return false;

    // A job was claimed. Now enforce the rate limit.  If the limit has been
    // reached we immediately release the lock so the job is recoverable and
    // return false — the poll loop will stop trying until the next cycle.
    if (this.config.rateLimit) {
      const allowed = await this.storage.checkAndIncrementRateLimit(
        this.queueName,
        this.config.rateLimit.max,
        this.config.rateLimit.duration,
        now,
      );
      if (!allowed) {
        await this.storage.releaseLock(raw.id, this.id);
        return false;
      }
    }

    const job = new Job(raw);
    this.activeCount += 1;
    this.activeJobIds.add(job.id);

    // Fire-and-forget — errors are caught inside executeJob.
    void this.executeJob(job);

    return true;
  }

  private async executeJob(job: Job<unknown>): Promise<void> {
    this.emitter.emit("job:started", job._data);

    const processor = this.processors.get(job.type);

    if (!processor) {
      // No processor registered → treat as a permanent failure.
      const error = new Error(
        `No processor registered for job type "${job.type}" in queue "${this.queueName}"`,
      );
      await this.handleFailure(job, error);
      return;
    }

    const abortController = new AbortController();
    let timeoutHandle: NodeJS.Timeout | null = null;
    let lockRenewTimer: NodeJS.Timeout | null = null;

    const lockRenewInterval = Math.max(100, Math.floor(this.config.lockDuration / 2));
    lockRenewTimer = setInterval(async () => {
      try {
        const renewed = await this.storage.renewLock(job.id, this.id, this.config.lockDuration);
        if (!renewed && lockRenewTimer) {
          clearInterval(lockRenewTimer);
          lockRenewTimer = null;
        }
      } catch {
        // ignore transient renewal errors
      }
    }, lockRenewInterval);

    try {
      await new Promise<void>((resolve, reject) => {
        // Enforce per-attempt timeout.
        timeoutHandle = setTimeout(() => {
          const timeoutError = new Error(`Job timed out after ${job.timeout}ms`);
          abortController.abort(timeoutError);
          reject(timeoutError);
        }, job.timeout);

        Promise.resolve(processor(job, abortController.signal)).then(resolve, reject);
      });

      // Success path.
      if (timeoutHandle) clearTimeout(timeoutHandle);
      if (lockRenewTimer) {
        clearInterval(lockRenewTimer);
        lockRenewTimer = null;
      }

      // Re-enqueue cron job for its next run BEFORE completing current job.
      if (job.cron) {
        try {
          await this.enqueueCronNext(job);
        } catch {
          // If in-band retries fail, worker:error was emitted.
          // We complete the job as its processor succeeded, and out-of-band
          // recoverCronJobs() will pick up and recreate the missing next occurrence.
        }
      }

      await this.storage.complete(job.id, this.id);

      const completedData = (await this.storage.getJob(job.id)) ?? job._data;
      this.emitter.emit("job:completed", completedData);
    } catch (err) {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      if (lockRenewTimer) {
        clearInterval(lockRenewTimer);
        lockRenewTimer = null;
      }
      const error = err instanceof Error ? err : new Error(String(err));
      await this.handleFailure(job, error);
    } finally {
      if (lockRenewTimer) {
        clearInterval(lockRenewTimer);
        lockRenewTimer = null;
      }
      this.activeJobIds.delete(job.id);
      this.activeCount -= 1;
      // Signal drain waiter if we've reached zero active jobs.
      if (this.activeCount === 0 && this.drainResolve) {
        this.drainResolve();
        this.drainResolve = null;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Cron — re-enqueue the next occurrence after a successful run
  // -------------------------------------------------------------------------

  private async enqueueCronNext(job: Job<unknown>): Promise<void> {
    let nextDate: Date | null;

    try {
      const cronInstance = new Cron(job.cron as string);
      const refDate = job.runAt ? new Date(job.runAt) : undefined;
      nextDate = cronInstance.nextRun(refDate);
    } catch (cronErr) {
      // Cron expression is invalid or croner itself threw.
      const error =
        cronErr instanceof Error
          ? cronErr
          : new Error(
              `croner failed to initialize for expression "${job.cron as string}": ${String(cronErr)}`,
            );
      this.emitter.emit("worker:error", this.id, error);
      return; // Do not re-enqueue — invalid expression should not produce a job.
    }

    if (nextDate === null) {
      // The cron schedule has no future occurrences (e.g. a bounded expression
      // that has already elapsed). Emitting an error lets operators know the
      // job will not recur rather than silently dropping it.
      const error = new Error(
        `Cron expression "${job.cron as string}" has no future occurrences — job "${job.id}" will not be re-enqueued`,
      );
      this.emitter.emit("worker:error", this.id, error);
      return;
    }

    const runAt = nextDate.toISOString();
    const nextJobId = generateCronNextJobId(job.id, nextDate);
    const payload = job._data
      ? job._data.payload
      : (job as unknown as { payload: unknown }).payload;

    let attempt = 0;
    const maxRetries = 3;

    while (true) {
      attempt++;
      try {
        await this.storage.enqueue({
          id: nextJobId,
          queue: this.queueName,
          type: job.type,
          payload,
          maxAttempts: job.maxAttempts,
          retryDelay: job.retryDelay,
          backoff: job.backoff,
          timeout: job.timeout,
          priority: job.priority,
          runAt,
          cron: job.cron,
        });
        break; // Successfully enqueued next occurrence
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        this.emitter.emit("worker:error", this.id, error);
        if (attempt >= maxRetries) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
      }
    }
  }

  // -------------------------------------------------------------------------
  // Failure handling — requeue or DLQ
  // -------------------------------------------------------------------------

  private async handleFailure(job: Job<unknown>, error: Error): Promise<void> {
    const attemptNumber = job.attemptsMade + 1; // the attempt that just failed
    const hasMore = attemptNumber < job.maxAttempts;

    this.emitter.emit("job:failed", job._data, error);

    if (hasMore) {
      const delayMs = calculateBackoff(job.backoff, job.retryDelay, attemptNumber);
      const runAt = nextRunAt(delayMs);

      await this.storage.requeue({
        jobId: job.id,
        runAt,
        error: error.message,
        attemptNumber,
        lockId: this.id,
        ...(error.stack !== undefined && { stack: error.stack }),
      });

      const updated = await this.storage.getJob(job.id);
      if (updated) {
        this.emitter.emit("job:retrying", updated, error, runAt);
      }
    } else {
      // No attempts remaining → Dead Letter Queue.
      await this.storage.moveToDlq({
        jobId: job.id,
        error: error.message,
        attemptNumber,
        lockId: this.id,
        ...(error.stack !== undefined && { stack: error.stack }),
      });

      const dead = await this.storage.getJob(job.id);
      if (dead) {
        this.emitter.emit("job:dead", dead, error);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Stalled-job recovery
  // -------------------------------------------------------------------------

  private scheduleStallCheck(): void {
    if (this._status !== "running") return;

    this.stalledTimer = setTimeout(() => {
      void this.recoverStalledJobs();
    }, this.config.stalledInterval);
  }

  private async recoverStalledJobs(): Promise<void> {
    if (this._status !== "running") return;

    try {
      const now = new Date().toISOString();
      const recovered = await this.storage.recoverStalledJobs(this.queueName, now);

      for (const jobId of recovered) {
        this.emitter.emit("job:stalled", jobId);
        this.emitter.emit("job:recovered", jobId);
      }

      await this.recoverCronJobs();
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.emitter.emit("worker:error", this.id, error);
    }

    this.scheduleStallCheck();
  }

  /**
   * Cron job recovery — scans completed cron jobs in this worker's queue and ensures
   * that their next scheduled occurrence exists in storage.
   */
  private async recoverCronJobs(): Promise<void> {
    if (this._status !== "running") return;

    try {
      const completedJobs = await this.storage.getJobs({
        queue: this.queueName,
        status: "completed",
        limit: 100,
      });

      for (const job of completedJobs) {
        if (!job.cron) continue;

        let nextDate: Date | null;
        try {
          const cronInstance = new Cron(job.cron);
          const refDate = job.runAt ? new Date(job.runAt) : undefined;
          nextDate = cronInstance.nextRun(refDate);
        } catch {
          // Ignore invalid cron expressions during recovery scan
          continue;
        }

        if (nextDate === null) continue;

        const nextJobId = generateCronNextJobId(job.id, nextDate);
        const existing = await this.storage.getJob(nextJobId);

        if (!existing) {
          await this.storage.enqueue({
            id: nextJobId,
            queue: this.queueName,
            type: job.type,
            payload: job.payload,
            maxAttempts: job.maxAttempts,
            retryDelay: job.retryDelay,
            backoff: job.backoff,
            timeout: job.timeout,
            priority: job.priority,
            runAt: nextDate.toISOString(),
            cron: job.cron,
          });
        }
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.emitter.emit("worker:error", this.id, error);
    }
  }
}
