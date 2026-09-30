import { EventEmitter } from "node:events";
import type { Queue } from "./queue.js";
import type { Job } from "../types/job.types.js";
import type { WorkerOptions } from "../types/queue.types.js";
import type { BackoffStrategy } from "../types/client.types.js";
import type { QueueClient } from "./client.js";

// ─── Handler type ─────────────────────────────────────────────────────────────

/**
 * The function you write to process a single job.
 *
 * - Return any value — it is stored in `job.result` on success.
 * - Throw any error — the Worker handles retries, backoff, and status updates.
 */
export type WorkerHandler<TData = unknown, TResult = unknown> = (
  job: Job<TData, TResult>,
) => TResult | Promise<TResult>;

// ─── Event map ────────────────────────────────────────────────────────────────

/**
 * Typed events emitted by a Worker instance.
 */
export interface WorkerEvents<TData, TResult> {
  /** A job has been picked up and is now processing. */
  active: [job: Job<TData, TResult>];
  /** A job finished successfully. result is the handler's return value. */
  completed: [job: Job<TData, TResult>, result: TResult];
  /** One attempt failed — job may still retry. */
  error: [job: Job<TData, TResult>, error: Error];
  /** All attempts exhausted — job is permanently failed. */
  failed: [job: Job<TData, TResult>, error: Error];
  /** Polling loop started. */
  started: [];
  /** Polling loop stopped and all in-flight jobs drained. */
  stopped: [];
}

// ─── Worker ───────────────────────────────────────────────────────────────────

/**
 * Worker — pure job consumer.
 *
 * A Worker takes a Queue and a handler function. It polls the Queue for
 * eligible jobs, executes them through the handler, and manages the full
 * job lifecycle: status transitions, timeouts, retries, backoff, and events.
 *
 * It does NOT add, list, or remove jobs — those operations belong on Queue.
 *
 * Usage
 * ─────
 * const queue  = new Queue("emails", client);
 * const worker = new Worker(queue, async (job) => {
 *   await sendEmail(job.data);
 * });
 *
 * worker.start();
 *
 * // Lifecycle events
 * worker.on("completed", (job, result) => console.log(job.id, result));
 * worker.on("failed",    (job, err)    => console.error(job.id, err));
 *
 * // Graceful shutdown — waits for in-flight jobs before resolving
 * await worker.close();
 */
export class Worker<TData = unknown, TResult = unknown> extends EventEmitter {
  /** The Queue this worker consumes from. Read-only after construction. */
  public readonly queue: Queue<TData, TResult>;

  private readonly handler: WorkerHandler<TData, TResult>;
  private readonly concurrency: number;
  private readonly pollInterval: number;

  private running = false;
  private loopTimer: ReturnType<typeof setTimeout> | null = null;
  private activeCount = 0;

  /**
   * @param queue   - The Queue to consume jobs from.
   * @param handler - Async function that processes one job at a time.
   * @param options - `concurrency` (default 1) and `pollInterval` ms (default 500).
   */
  constructor(
    queue: Queue<TData, TResult>,
    handler: WorkerHandler<TData, TResult>,
    options: WorkerOptions = {},
  ) {
    super();

    this.queue = queue;
    this.handler = handler;
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.pollInterval = Math.max(0, options.pollInterval ?? 500);

    // Register with the queue so queue.close() can auto-close this worker.
    this.queue._registerWorker(this);

    // FIX: Do NOT attach a blank "error" listener here — that silently swallows
    // errors even when the caller attaches their own listener later, because
    // EventEmitter walks ALL listeners including the blank no-op.
    // Instead, override emit() so we only apply the guard at emit time when
    // no real "error" listener exists (see emit() override below).
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Start the polling loop.
   * Calling start() on an already-running worker is a no-op.
   */
  public start(): this {
    if (this.running) return this;

    this.running = true;
    this.emit("started");
    this._log(`Worker[${this.queue.name}] started (concurrency=${this.concurrency}).`);
    this.scheduleLoop();
    return this;
  }

  /**
   * Gracefully stop the worker.
   *
   * - No new jobs are picked up after this call.
   * - Already in-flight jobs are allowed to finish.
   * - Resolves once the poll loop has stopped and active count reaches 0.
   *
   * Safe to call even if start() was never called.
   */
  public async close(): Promise<void> {
    // BUG FIX: always unregister from the queue, even when the worker was
    // never started. Previously, `if (!this.running) return` short-circuited
    // before _unregisterWorker was reached, leaving a reference in the queue's
    // _workers set forever and causing queue.close() to call close() on an
    // already-closed (or never-started) worker again.
    if (!this.running) {
      this.queue._unregisterWorker(this);
      return;
    }

    this.running = false;
    if (this.loopTimer) {
      clearTimeout(this.loopTimer);
      this.loopTimer = null;
    }

    await this.drainActive();

    // Deregister from the queue — prevents a double-close if queue.close()
    // is called after this worker was already closed manually.
    this.queue._unregisterWorker(this);

    this.emit("stopped");
    this._log(`Worker[${this.queue.name}] stopped.`);
  }

  /** `true` while the polling loop is active. */
  public isRunning(): boolean {
    return this.running;
  }

  // ── Typed EventEmitter overloads ──────────────────────────────────────────

  public on<K extends keyof WorkerEvents<TData, TResult>>(
    event: K,
    listener: (...args: WorkerEvents<TData, TResult>[K]) => void,
  ): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  public once<K extends keyof WorkerEvents<TData, TResult>>(
    event: K,
    listener: (...args: WorkerEvents<TData, TResult>[K]) => void,
  ): this {
    return super.once(event, listener as (...args: unknown[]) => void);
  }

  /**
   * FIX: Override emit() to guard against ERR_UNHANDLED_ERROR only when no
   * real "error" listener is registered. This avoids the blank-listener
   * anti-pattern that would have swallowed errors in tests / callers that DO
   * register their own listener (since all listeners are always called).
   */
  public emit<K extends keyof WorkerEvents<TData, TResult>>(
    event: K,
    ...args: WorkerEvents<TData, TResult>[K]
  ): boolean {
    // Node.js throws ERR_UNHANDLED_ERROR only for the "error" event with zero
    // listeners. If the caller has at least one listener, let it through normally.
    if (event === "error" && this.listenerCount("error") === 0) {
      // No listener registered — swallow silently so the process doesn't crash.
      // The error information is already written to storage by handleFailure();
      // the "failed" event will be emitted right after for permanent failures.
      return false;
    }
    return super.emit(event, ...args);
  }

  // ── Poll loop ─────────────────────────────────────────────────────────────

  private scheduleLoop(): void {
    if (!this.running) return;
    this.loopTimer = setTimeout(
      () => void this.tick().finally(() => this.scheduleLoop()),
      this.pollInterval,
    );
  }

  /**
   * One iteration of the poll loop.
   * Starts as many jobs as concurrency allows, then returns.
   */
  private async tick(): Promise<void> {
    while (this.running && this.activeCount < this.concurrency) {
      // Bail out if the client was closed mid-tick.
      if (!this.client.isInitialized()) break;

      let job: Job<TData, TResult> | undefined;
      try {
        job = await this.queue._nextJob();
      } catch {
        // Storage unavailable (e.g. closed between ticks) — stop quietly.
        break;
      }

      // FIX: re-check running after the await — close() may have been called
      // while we were waiting for _nextJob() to resolve.
      if (!this.running) break;

      if (!job) break; // queue empty or nothing eligible

      // Increment before the async chain so the concurrency ceiling is
      // respected even while multiple processJob() calls are in flight.
      this.activeCount++;
      void this.processJob(job).finally(() => {
        // Clamp to 0 to guard against any unexpected synchronous throw path
        // that could otherwise decrement past zero and break drainActive().
        this.activeCount = Math.max(0, this.activeCount - 1);
      });
    }
  }

  // ── Job execution ─────────────────────────────────────────────────────────

  private async processJob(job: Job<TData, TResult>): Promise<void> {
    const config = this.client.getConfig();
    const timeout = (job.opts as { timeout?: number }).timeout ?? config.timeout;

    // The job was already flipped to "active" atomically inside getNextJob()
    // (all storage backends do this). We only need to re-fetch here so the
    // emitted "active" event carries the latest fields (processedAt, updatedAt)
    // that were set server-side. Writing status/processedAt again would create
    // a second, slightly-later timestamp that overwrites the authoritative one.
    const active = (await this.queue.get(job.id)) ?? job;
    this.emit("active", active);
    this._log(
      `Worker[${this.queue.name}] processing "${job.name}" id=${job.id} ` +
        // FIX: log the correct current attempt number (attemptsMade is 0-based,
        // add 1 for the human-readable "attempt N of M" display).
        `attempt=${active.attemptsMade + 1}/${active.attempts === 0 ? "∞" : active.attempts}`,
    );

    let result: TResult;
    try {
      result = await this.runWithTimeout(active, timeout);
    } catch (raw) {
      await this.handleFailure(active, raw);
      return;
    }

    // ── Success ────────────────────────────────────────────────────────────
    const removeOnComplete =
      (job.opts as { removeOnComplete?: boolean }).removeOnComplete ??
      this.queue.options.defaultJobOpts.removeOnComplete ??
      false;

    // Build the completed snapshot before potentially removing the job from
    // storage. If removeOnComplete is true the job will be gone after remove(),
    // so we must not call queue.get() afterwards.
    const finishedAt = Date.now();
    const completedSnapshot: Job<TData, TResult> = {
      ...active,
      status: "completed" as const,
      result,
      finishedAt,
    };

    if (removeOnComplete) {
      await this.queue.remove(job.id);
    } else {
      // BUG FIX: `result` can be any TResult — object, array, string, number,
      // boolean, or null. The redis.storage.ts field encoder only handles scalar
      // types directly; composite values (object/array) were silently skipped,
      // leaving `result` unchanged in Redis storage.
      // Fix: JSON-stringify any non-scalar result so it travels through the
      // string encoder path. All storage backends (Postgres, MySQL, memory)
      // already handle `result` as an opaque value inside the full payload
      // object, so stringifying it here is correct for Redis and a no-op
      // overhead for the others (they receive the patch object, not the
      // raw encoded fields).
      const encodedResult =
        result === null ||
        result === undefined ||
        typeof result === "string" ||
        typeof result === "number" ||
        typeof result === "boolean"
          ? result
          : (JSON.stringify(result) as unknown as TResult);

      await this.queue._updateJob(job.id, {
        status: "completed",
        result: encodedResult,
        finishedAt,
      });
    }

    this.emit("completed", completedSnapshot, result);
    this._log(`Worker[${this.queue.name}] completed "${job.name}" id=${job.id}`);
  }

  /**
   * Race the handler against a hard timeout.
   * FIX: clear the deadline timer when the handler wins the race — without
   * this, the setTimeout handle keeps the event loop alive and Node.js will
   * not exit cleanly in tests or short-lived scripts.
   */
  private runWithTimeout(job: Job<TData, TResult>, timeoutMs: number): Promise<TResult> {
    const work = Promise.resolve(this.handler(job));
    if (timeoutMs <= 0) return work;

    return new Promise<TResult>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`Job timed out after ${timeoutMs} ms`)),
        timeoutMs,
      );

      work.then(
        (val) => {
          clearTimeout(timer);
          resolve(val);
        },
        (err: unknown) => {
          clearTimeout(timer);
          reject(err);
        },
      );
    });
  }

  /** Decide between a retry and a permanent failure. */
  private async handleFailure(job: Job<TData, TResult>, rawErr: unknown): Promise<void> {
    const err = rawErr instanceof Error ? rawErr : new Error(String(rawErr));

    // Re-fetch so attemptsMade is always accurate (avoids stale closure).
    // BUG FIX: the previous fallback was `?? job` where `job` is the snapshot
    // from BEFORE the active transition (attemptsMade = N-1). If the re-fetch
    // fails we should still use the correct in-memory value we just computed
    // rather than the pre-active snapshot.  We optimistically increment
    // attemptsMade first, then use the re-fetched value if available.
    const fetched = await this.queue.get(job.id).catch(() => undefined);
    const current = fetched ?? job;
    const attemptsMade = (fetched?.attemptsMade ?? job.attemptsMade) + 1;
    const maxAttempts = current.attempts; // 0 = unlimited
    const canRetry = maxAttempts === 0 || attemptsMade < maxAttempts;

    // Emit "error" on every failed attempt (retry or final).
    this.emit("error", { ...current, attemptsMade } as Job<TData, TResult>, err);
    this._log(
      `Worker[${this.queue.name}] attempt ${attemptsMade}/${maxAttempts === 0 ? "∞" : maxAttempts} failed — ` +
        `"${current.name}" id=${current.id}: ${err.message}`,
    );

    if (canRetry) {
      const delay = this.calcBackoff(attemptsMade, current);
      const retryAt = Date.now() + delay;

      await this.queue._updateJob(current.id, {
        status: "retrying",
        attemptsMade,
        runAt: retryAt,
        error: err.message,
        ...(err.stack !== undefined && { stacktrace: err.stack }),
      });

      this._log(
        `Worker[${this.queue.name}] retry in ${delay}ms — ` +
          `attempt ${attemptsMade + 1}/${maxAttempts === 0 ? "∞" : maxAttempts}`,
      );
      return;
    }

    // ── Permanent failure ──────────────────────────────────────────────────
    const removeOnFail =
      (current.opts as { removeOnFail?: boolean }).removeOnFail ??
      this.queue.options.defaultJobOpts.removeOnFail ??
      false;

    if (removeOnFail) {
      await this.queue.remove(current.id);
    } else {
      await this.queue._updateJob(current.id, {
        status: "failed",
        attemptsMade,
        error: err.message,
        ...(err.stack !== undefined && { stacktrace: err.stack }),
        finishedAt: Date.now(),
      });
    }

    const failed = (await this.queue.get(current.id)) ?? {
      ...current,
      status: "failed" as const,
      attemptsMade,
    };

    this.emit("failed", failed, err);
    this._log(`Worker[${this.queue.name}] permanently failed — "${current.name}" id=${current.id}`);
  }

  /**
   * Calculate the delay before the next retry.
   *
   * fixed       → base
   * linear      → base × attemptsMade
   * exponential → base × 2^(attemptsMade − 1)   capped at 30 min
   */
  private calcBackoff(attemptsMade: number, job: Job<TData, TResult>): number {
    const cfg = this.client.getConfig();
    const base = cfg.retryDelay;
    const strategy = cfg.backoff as BackoffStrategy;
    const MAX = 30 * 60 * 1_000;

    let delay: number;
    switch (strategy) {
      case "fixed":
        delay = base;
        break;
      case "linear":
        delay = base * attemptsMade;
        break;
      case "exponential":
        delay = base * Math.pow(2, attemptsMade - 1);
        break;
    }

    // Allow per-job retryDelay override stored in opts.
    const perJobBase = (job.opts as { retryDelay?: number }).retryDelay;
    if (perJobBase !== undefined) {
      switch (strategy) {
        case "fixed":
          delay = perJobBase;
          break;
        case "linear":
          delay = perJobBase * attemptsMade;
          break;
        case "exponential":
          delay = perJobBase * Math.pow(2, attemptsMade - 1);
          break;
        default:
          delay = perJobBase;
          break;
      }
    }

    return Math.min(delay, MAX);
  }

  /** Poll every 50 ms until no jobs are actively processing, with a 30 s safety timeout. */
  private drainActive(): Promise<void> {
    return new Promise((resolve, reject) => {
      // BUG FIX: the previous implementation polled forever with no upper
      // bound.  If a processJob() call somehow never decrements activeCount
      // (e.g. an unhandled rejection escaping the finally block) close() would
      // hang indefinitely, blocking graceful shutdown.
      // A 30-second hard timeout rejects the promise so the caller can surface
      // the issue rather than silently blocking the process exit.
      const DRAIN_TIMEOUT_MS = 30_000;
      const deadline = setTimeout(() => {
        reject(
          new Error(
            `[queue-jobs-worker] Worker[${this.queue.name}] drain timed out after ` +
              `${DRAIN_TIMEOUT_MS}ms — ${this.activeCount} job(s) still active. ` +
              "This may indicate an unhandled error in a job handler.",
          ),
        );
      }, DRAIN_TIMEOUT_MS);

      const poll = () => {
        if (this.activeCount === 0) {
          clearTimeout(deadline);
          resolve();
        } else {
          setTimeout(poll, 50);
        }
      };
      poll();
    });
  }

  // ── Convenience accessors ─────────────────────────────────────────────────

  private get client(): QueueClient {
    return this.queue.client;
  }

  private _log(msg: string): void {
    this.client._log(msg);
  }
}
