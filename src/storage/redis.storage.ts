import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";
import type { IStorage } from "./storage.interface.js";
import type { Job, JobStatus } from "../types/job.types.js";

// ─── Round-trip budget per operation ─────────────────────────────────────────
//
//  saveJob      → 1  (save-job.lua)
//  getJob       → 1  (GET)
//  updateJob    → 1  (update-job.lua  — GET+patch+SET server-side)
//  removeJob    → 1  (remove-job.lua)
//  listJobs     → 1  (list-jobs.lua   — ZRANGE+N×GET server-side)
//  getNextJob   → 1  (claim-job.lua   — atomic claim)
//  clearQueue   → 1  (clear-queue.lua — ZRANGE+N×DEL server-side)
//  countJobs    → 1  (ZCARD)
//
// Every mutating operation is atomic (single Lua execution on Redis).
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Redis storage backend.
 *
 * Requires the `redis` npm package (>=4.0.0) as a peer dependency.
 * Install: npm install redis
 *
 * Key layout
 * ──────────
 *   qjw:job:{jobId}          — JSON string  (full Job record)
 *   qjw:{queue}:index        — sorted set   score = priority   value = jobId
 *   qjw:{queue}:waiting      — sorted set   score = runAt ms   value = jobId
 *                              contains jobs with status "waiting" | "retrying"
 *
 * Lua scripts (SCRIPT LOAD on connect → EVALSHA on every call)
 * ─────────────────────────────────────────────────────────────
 *   save-job.lua    atomic saveJob
 *   update-job.lua  atomic read+patch+write  (no TS pre-read)
 *   remove-job.lua  atomic removeJob
 *   clear-queue.lua atomic clearQueue
 *   list-jobs.lua   server-side listJobs     (no N+1)
 *   claim-job.lua   atomic getNextJob
 */
export class RedisStorage implements IStorage {
  private client: RedisClientLike | null = null;
  private readonly connectionString: string;

  /** SHA1 digests returned by SCRIPT LOAD, keyed by script name. */
  private sha: Record<string, string> = {};

  constructor(connectionString: string) {
    this.connectionString = connectionString;
  }

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    let mod: { createClient(opts: { url: string }): RedisClientLike };
    try {
      mod = (await import("redis")) as unknown as typeof mod;
    } catch {
      throw new Error(
        "[queue-jobs-worker] Redis dialect requires the `redis` package. " +
          "Run: npm install redis",
      );
    }

    const c = mod.createClient({ url: this.connectionString });
    c.on("error", (err: unknown) => console.error("[queue-jobs-worker] Redis error:", err));
    await c.connect();
    this.client = c;

    await this.loadScripts();
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.quit();
      this.client = null;
    }
  }

  // ─── IStorage implementation ──────────────────────────────────────────────

  /**
   * Persist a new job.
   * 1 round-trip (save-job.lua: SET + ZADD index + conditional ZADD waiting).
   */
  async saveJob<TData, TResult>(queueName: string, job: Job<TData, TResult>): Promise<void> {
    const eligible = job.status === "waiting" || job.status === "retrying" ? "1" : "0";

    await this.evalSha("save-job", {
      keys: [this.hashKey(job.id), this.indexKey(queueName), this.waitingKey(queueName)],
      arguments: [JSON.stringify(job), job.id, String(job.priority), String(job.runAt), eligible],
    });
  }

  /**
   * Fetch a single job by ID.
   * 1 round-trip (GET).
   */
  async getJob<TData, TResult>(
    _queueName: string,
    jobId: string,
  ): Promise<Job<TData, TResult> | undefined> {
    const raw = await this.assertClient().get(this.hashKey(jobId));
    if (!raw) return undefined;
    return JSON.parse(raw) as Job<TData, TResult>;
  }

  /**
   * Atomically patch a job and update sorted-set membership.
   * 1 round-trip (update-job.lua: GET + patch + SET server-side).
   *
   * Fields are decomposed into three RS-delimited (ASCII 0x1E) lists:
   *   strFields  — "key\x1evalue\x1ekey\x1evalue…"  for string fields
   *   numFields  — "key\x1evalue\x1ekey\x1evalue…"  for numeric fields
   *   nullFields — "key\x1ekey\x1e…"                fields to set to null
   *
   * RS (record separator, 0x1E) is used instead of pipe so that error
   * messages containing "|" characters are never mistaken for delimiters.
   */
  async updateJob<TData, TResult>(
    queueName: string,
    jobId: string,
    patch: Partial<Job<TData, TResult>>,
  ): Promise<void> {
    const newStatus = (patch.status ?? "") as string;
    const nowMs = String(Date.now());

    // BUG FIX: `patch.runAt ?? 0` defaulted to "0" whenever runAt was not in
    // the patch (e.g. a simple status→completed update).  The Lua script then
    // did ZADD waitingKey 0 jobId for "retrying" state, scheduling the retry
    // in the distant past and making it immediately claimable — bypassing the
    // backoff delay entirely.  Use "-1" as a sentinel ("don't touch runAt") so
    // the Lua script only writes to the waiting set's score when a real runAt
    // was supplied.
    const runAt = patch.runAt !== undefined ? String(patch.runAt) : "-1";

    // Only send a real priority when it was explicitly included in the patch.
    // The Lua script skips the ZADD when priority is the sentinel "-1".
    const priority = patch.priority !== undefined ? String(patch.priority) : "-1";

    // RS = ASCII record separator — safe delimiter that cannot appear in
    // job field names or in any value we ever store (error messages, stack
    // traces, cron expressions, UUIDs, etc.).
    const RS = "\x1e";

    const strParts: string[] = [];
    const numParts: string[] = [];
    const nullParts: string[] = [];
    const boolParts: string[] = [];

    for (const [key, val] of Object.entries(patch) as [string, unknown][]) {
      // status, updatedAt, and runAt are handled directly by the Lua script.
      if (key === "status" || key === "updatedAt" || key === "runAt") continue;

      if (val === null || val === undefined) {
        nullParts.push(key);
      } else if (typeof val === "number") {
        numParts.push(key, String(val));
      } else if (typeof val === "boolean") {
        // BUG FIX: booleans must be sent as the JSON literals "true"/"false",
        // NOT as "1"/"0".  The Lua numeric pattern [%d%.%-]+ cannot match the
        // stored JSON literals true/false, so bool patches were silently lost.
        // The new boolFields ARGV in update-job.lua v5 handles this correctly.
        boolParts.push(key, val ? "true" : "false");
      } else if (typeof val === "string") {
        strParts.push(key, val);
      }
      // Composite fields (data, opts, result) are intentionally skipped —
      // only scalar fields are ever patched through updateJob.
    }

    await this.evalSha("update-job", {
      keys: [this.hashKey(jobId), this.indexKey(queueName), this.waitingKey(queueName)],
      arguments: [
        newStatus,
        nowMs,
        runAt,
        priority,
        strParts.join(RS),
        numParts.join(RS),
        nullParts.join(RS),
        boolParts.join(RS), // ARGV[8] — new in update-job.lua v5
      ],
    });
  }

  /**
   * Permanently delete a job and clean up sorted sets.
   * 1 round-trip (remove-job.lua: EXISTS + DEL + ZREM × 2).
   */
  async removeJob(queueName: string, jobId: string): Promise<void> {
    await this.evalSha("remove-job", {
      keys: [this.hashKey(jobId), this.indexKey(queueName), this.waitingKey(queueName)],
      arguments: [jobId],
    });
  }

  /**
   * List all jobs, optionally filtered by status.
   * 1 round-trip (list-jobs.lua: ZRANGE + N×GET server-side).
   * Results are already sorted by priority ASC (index set order).
   */
  async listJobs<TData, TResult>(
    queueName: string,
    status?: JobStatus,
  ): Promise<Job<TData, TResult>[]> {
    const raw = await this.evalSha("list-jobs", {
      keys: [this.indexKey(queueName)],
      arguments: [status ?? ""],
    });

    if (!Array.isArray(raw) || raw.length === 0) return [];

    return (raw as string[]).map((r) => JSON.parse(r) as Job<TData, TResult>);
  }

  /**
   * Atomically claim the next eligible job and mark it active.
   * 1 round-trip (claim-job.lua).
   */
  async getNextJob<TData, TResult>(queueName: string): Promise<Job<TData, TResult> | undefined> {
    const raw = await this.evalSha("claim-job", {
      keys: [this.indexKey(queueName), this.waitingKey(queueName)],
      arguments: [String(Date.now())],
    });

    if (!raw || typeof raw !== "string") return undefined;
    return JSON.parse(raw) as Job<TData, TResult>;
  }

  /**
   * Delete every job in a queue and drop both sorted sets.
   * 1 round-trip (clear-queue.lua: ZRANGE + N×DEL + 2×DEL server-side).
   */
  async clearQueue(queueName: string): Promise<void> {
    await this.evalSha("clear-queue", {
      keys: [this.indexKey(queueName), this.waitingKey(queueName)],
      arguments: [],
    });
  }

  /**
   * Return the total number of jobs in a queue (all statuses).
   * 1 round-trip (ZCARD on the index set).
   */
  async countJobs(queueName: string): Promise<number> {
    return this.assertClient().zCard(this.indexKey(queueName));
  }

  // ─── Script loading ───────────────────────────────────────────────────────

  /**
   * Read every Lua script from disk and SCRIPT LOAD it into Redis.
   * Uses async readFile so the event loop is never blocked.
   * Called once on connect, and again automatically on NOSCRIPT errors.
   */
  private async loadScripts(): Promise<void> {
    const c = this.assertClient();
    const dir = join(dirname(fileURLToPath(import.meta.url)), "..", "scripts");

    const names = [
      "save-job",
      "update-job",
      "remove-job",
      "clear-queue",
      "list-jobs",
      "claim-job",
    ] as const;

    // Read all files concurrently (async — does not block the event loop).
    const sources = await Promise.all(
      names.map((name) => readFile(join(dir, `${name}.lua`), "utf8")),
    );

    // Load all scripts into Redis concurrently.
    const shas = await Promise.all(sources.map((src) => c.scriptLoad(src)));

    // Store with a safe assignment — no non-null assertion needed.
    for (let i = 0; i < names.length; i++) {
      const sha = shas[i];
      if (sha !== undefined) {
        this.sha[names[i]] = sha;
      }
    }
  }

  // ─── Private helpers ──────────────────────────────────────────────────────

  /**
   * Call EVALSHA and automatically reload scripts on NOSCRIPT errors
   * (e.g. after a Redis server restart that flushed the script cache).
   */
  private async evalSha(
    name: string,
    opts: { keys: string[]; arguments: string[] },
  ): Promise<unknown> {
    const c = this.assertClient();

    // FIX: safe lookup — throw a clear error instead of non-null asserting.
    const sha = this.sha[name];
    if (!sha) {
      throw new Error(
        `[queue-jobs-worker] Lua script "${name}" has not been loaded. ` +
          "This is a bug — please open an issue.",
      );
    }

    try {
      return await c.evalSha(sha, opts);
    } catch (err) {
      if (isNoscriptError(err)) {
        // Redis flushed its script cache (e.g. after a restart) — reload and retry.
        await this.loadScripts();
        const reloadedSha = this.sha[name];
        if (!reloadedSha) throw err;
        return c.evalSha(reloadedSha, opts);
      }
      throw err;
    }
  }

  private assertClient(): RedisClientLike {
    if (!this.client) {
      throw new Error(
        "[queue-jobs-worker] RedisStorage is not connected. " + "Call client.init() first.",
      );
    }
    return this.client;
  }

  private hashKey(jobId: string): string {
    return `qjw:job:${jobId}`;
  }

  private indexKey(queueName: string): string {
    return `qjw:${queueName}:index`;
  }

  private waitingKey(queueName: string): string {
    return `qjw:${queueName}:waiting`;
  }
}

// ─── NOSCRIPT detection ───────────────────────────────────────────────────────

function isNoscriptError(err: unknown): boolean {
  return err instanceof Error && err.message.toUpperCase().includes("NOSCRIPT");
}

// ─── Minimal Redis client shim ────────────────────────────────────────────────
// Only the methods this module actually calls are typed.

interface RedisClientLike {
  connect(): Promise<void>;
  quit(): Promise<void>;
  on(event: string, listener: (err: unknown) => void): void;
  get(key: string): Promise<string | null>;
  zCard(key: string): Promise<number>;
  scriptLoad(script: string): Promise<string>;
  evalSha(sha: string, opts: { keys: string[]; arguments: string[] }): Promise<unknown>;
}
