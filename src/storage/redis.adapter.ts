/**
 * RedisStorageAdapter  (node-redis v4/v5)
 *
 * Key layout  — prefix: "qjw:"
 * ─────────────────────────────────────────────────────────────────────────
 *  qjw:job:{id}                → Hash   — all job fields (flat strings)
 *  qjw:queue:{name}:waiting    → Sorted Set  score = -priority
 *                                            (ZPOPMIN → highest priority first)
 *  qjw:queue:{name}:delayed    → Sorted Set  score = runAt epoch-ms
 *  qjw:queue:{name}:active     → Set
 *  qjw:queue:{name}:completed  → Set
 *  qjw:queue:{name}:dead       → Set
 *  qjw:rate:{name}             → String  sliding-window counter
 *  qjw:rate:{name}:ts          → String  window-start epoch-ms
 * ─────────────────────────────────────────────────────────────────────────
 *
 * Atomic claim
 * ─────────────────────────────────────────────────────────────────────────
 *  A Lua script executed by Redis guarantees that two concurrent workers
 *  cannot claim the same job.
 * ─────────────────────────────────────────────────────────────────────────
 */

import type { StorageAdapter } from "../types/storage.types.js";
import type {
  EnqueueInput,
  ClaimInput,
  RequeueInput,
  MoveToDlqInput,
  GetJobsFilter,
} from "../types/storage.types.js";
import type { JobData, JobStatus, JobAttempt } from "../types/job.types.js";
import {
  CLAIM_LUA,
  RECOVER_STALLED_LUA,
  RENEW_LOCK_LUA,
  RATE_LIMIT_LUA,
} from "../lib/scripts/index.js";

// ---------------------------------------------------------------------------
// Lazy import — redis is an optional peer dependency
// ---------------------------------------------------------------------------

// Type-only import — runtime import is deferred inside loadRedis().
import type { RedisClientType } from "redis";
import type * as RedisModule from "redis";

async function loadRedis(): Promise<typeof RedisModule> {
  try {
    return await import("redis");
  } catch {
    throw new Error(
      'RedisStorageAdapter requires the "redis" package (node-redis v4+).\n' +
        "Install it: npm install redis",
    );
  }
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

const PREFIX = "qjw:";

const k = {
  job: (id: string) => `${PREFIX}job:${id}`,
  waiting: (q: string) => `${PREFIX}queue:${q}:waiting`,
  delayed: (q: string) => `${PREFIX}queue:${q}:delayed`,
  active: (q: string) => `${PREFIX}queue:${q}:active`,
  completed: (q: string) => `${PREFIX}queue:${q}:completed`,
  dead: (q: string) => `${PREFIX}queue:${q}:dead`,
  rateCount: (q: string) => `${PREFIX}rate:${q}`,
  rateTs: (q: string) => `${PREFIX}rate:${q}:ts`,
};

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

type FlatHash = Record<string, string>;

function jobToHash(job: JobData<unknown>): FlatHash {
  return {
    id: job.id,
    queue: job.queue,
    type: job.type,
    payload: JSON.stringify(job.payload),
    status: job.status,
    attemptsMade: String(job.attemptsMade),
    maxAttempts: String(job.maxAttempts),
    retryDelay: String(job.retryDelay),
    backoff: job.backoff,
    timeout: String(job.timeout),
    priority: String(job.priority),
    runAt: job.runAt,
    ...(job.cron !== undefined ? { cron: job.cron } : {}),
    attempts: JSON.stringify(job.attempts),
    lockId: job.lockId ?? "",
    lockExpiresAt: job.lockExpiresAt ?? "",
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    completedAt: job.completedAt ?? "",
    failedAt: job.failedAt ?? "",
  };
}

function hashToJob<TPayload>(h: FlatHash): JobData<TPayload> {
  const job: JobData<TPayload> = {
    id: h["id"] ?? "",
    queue: h["queue"] ?? "",
    type: h["type"] ?? "",
    payload: JSON.parse(h["payload"] ?? "null") as TPayload,
    status: (h["status"] ?? "waiting") as JobStatus,
    attemptsMade: Number(h["attemptsMade"] ?? 0),
    maxAttempts: Number(h["maxAttempts"] ?? 1),
    retryDelay: Number(h["retryDelay"] ?? 1000),
    backoff: (h["backoff"] ?? "exponential") as JobData<TPayload>["backoff"],
    timeout: Number(h["timeout"] ?? 30000),
    priority: Number(h["priority"] ?? 0),
    runAt: h["runAt"] ?? new Date().toISOString(),
    attempts: JSON.parse(h["attempts"] ?? "[]") as JobAttempt[],
    lockId: h["lockId"] || null,
    lockExpiresAt: h["lockExpiresAt"] || null,
    createdAt: h["createdAt"] ?? new Date().toISOString(),
    updatedAt: h["updatedAt"] ?? new Date().toISOString(),
    completedAt: h["completedAt"] || null,
    failedAt: h["failedAt"] || null,
  };
  if (h["cron"]) job.cron = h["cron"];
  return job;
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

export class RedisStorageAdapter implements StorageAdapter {
  private client!: RedisClientType;
  private readonly url: string;

  constructor(connectionString: string) {
    this.url = connectionString;
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  async initialize(): Promise<void> {
    const { createClient } = await loadRedis();

    this.client = createClient({ url: this.url }) as RedisClientType;

    // Surface connection errors as exceptions (node-redis v4 hides them otherwise).
    this.client.on("error", () => {
      // Handled by the connect() rejection below on first connect.
      // Subsequent errors are emitted on the client; callers should listen if needed.
    });

    await this.client.connect();

    // Verify the connection is healthy.
    const pong = await this.client.ping();
    if (pong !== "PONG") {
      throw new Error(
        "RedisStorageAdapter: PING returned unexpected response. Connection may be unhealthy.",
      );
    }
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.quit();
    }
  }

  // -------------------------------------------------------------------------
  // Enqueue
  // -------------------------------------------------------------------------

  async enqueue<TPayload = unknown>(input: EnqueueInput<TPayload>): Promise<JobData<TPayload>> {
    const now = new Date().toISOString();
    const runAtMs = new Date(input.runAt).getTime();
    const isDelayed = runAtMs > Date.now();

    const job: JobData<TPayload> = {
      id: input.id,
      queue: input.queue,
      type: input.type,
      payload: input.payload,
      status: isDelayed ? "delayed" : "waiting",
      attemptsMade: 0,
      maxAttempts: input.maxAttempts,
      retryDelay: input.retryDelay,
      backoff: input.backoff,
      timeout: input.timeout,
      priority: input.priority,
      runAt: input.runAt,
      ...(input.cron !== undefined ? { cron: input.cron } : {}),
      attempts: [],
      lockId: null,
      lockExpiresAt: null,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      failedAt: null,
    };

    const key = k.job(input.id);

    // Idempotent — return existing job if already present.
    const exists = await this.client.exists(key);
    if (exists) {
      const hash = await this.client.hGetAll(key);
      return hashToJob<TPayload>(hash);
    }

    // Persist in a pipeline.
    const multi = this.client.multi();
    multi.hSet(key, jobToHash(job as JobData<unknown>));

    if (isDelayed) {
      multi.zAdd(k.delayed(input.queue), { score: runAtMs, value: input.id });
    } else {
      // Negative priority score → ZPOPMIN yields highest priority first.
      multi.zAdd(k.waiting(input.queue), { score: -input.priority, value: input.id });
    }

    await multi.exec();
    return job;
  }

  // -------------------------------------------------------------------------
  // Claim  (atomic Lua)
  // -------------------------------------------------------------------------

  async claim<TPayload = unknown>(input: ClaimInput): Promise<JobData<TPayload> | null> {
    const { queue, lockId, lockDuration, now } = input;
    const nowMs = new Date(now).getTime();
    const lockExpiresAt = new Date(nowMs + lockDuration).toISOString();

    // node-redis v4 eval signature:
    //   client.eval(script, { keys, arguments })
    const jobId = (await this.client.eval(CLAIM_LUA, {
      keys: [k.waiting(queue), k.delayed(queue), k.active(queue)],
      arguments: [lockId, lockExpiresAt, String(nowMs), PREFIX],
    })) as string;

    if (!jobId) return null;

    const hash = await this.client.hGetAll(k.job(jobId));
    if (!hash || Object.keys(hash).length === 0) return null;

    return hashToJob<TPayload>(hash);
  }

  // -------------------------------------------------------------------------
  // Renew lock
  // -------------------------------------------------------------------------

  async renewLock(jobId: string, lockId: string, lockDuration: number): Promise<boolean> {
    const nowMs = Date.now();
    const nowIso = new Date(nowMs).toISOString();
    const newExpiresAt = new Date(nowMs + lockDuration).toISOString();

    const res = await this.client.eval(RENEW_LOCK_LUA, {
      keys: [k.job(jobId)],
      arguments: [lockId, newExpiresAt, nowIso],
    });

    return Number(res) === 1;
  }

  // -------------------------------------------------------------------------
  // Complete
  // -------------------------------------------------------------------------

  async complete(jobId: string, lockId?: string): Promise<void> {
    const now = new Date().toISOString();
    const hash = await this.client.hGetAll(k.job(jobId));
    if (!hash || Object.keys(hash).length === 0) return;

    if (lockId !== undefined) {
      if (hash["status"] !== "active" || hash["lockId"] !== lockId) return;
    }

    const queue = hash["queue"] ?? "";
    const multi = this.client.multi();
    multi.hSet(k.job(jobId), {
      status: "completed",
      lockId: "",
      lockExpiresAt: "",
      completedAt: now,
      updatedAt: now,
    });
    multi.sRem(k.active(queue), jobId);
    multi.sAdd(k.completed(queue), jobId);
    await multi.exec();
  }

  // -------------------------------------------------------------------------
  // Requeue
  // -------------------------------------------------------------------------

  async requeue(input: RequeueInput): Promise<void> {
    const now = new Date().toISOString();
    const hash = await this.client.hGetAll(k.job(input.jobId));
    if (!hash || Object.keys(hash).length === 0) return;

    if (input.lockId !== undefined) {
      if (hash["status"] !== "active" || hash["lockId"] !== input.lockId) return;
    }

    const queue = hash["queue"] ?? "";
    const attempts: JobAttempt[] = JSON.parse(hash["attempts"] ?? "[]") as JobAttempt[];

    attempts.push({
      attempt: input.attemptNumber,
      startedAt: hash["updatedAt"] ?? now,
      finishedAt: now,
      error: input.error,
      ...(input.stack !== undefined ? { stack: input.stack } : {}),
    });

    const multi = this.client.multi();
    multi.hSet(k.job(input.jobId), {
      status: "waiting",
      attemptsMade: String(input.attemptNumber),
      attempts: JSON.stringify(attempts),
      runAt: input.runAt,
      lockId: "",
      lockExpiresAt: "",
      updatedAt: now,
    });
    multi.sRem(k.active(queue), input.jobId);
    // Put back in delayed set so the claim script can promote it when due.
    multi.zAdd(k.delayed(queue), {
      score: new Date(input.runAt).getTime(),
      value: input.jobId,
    });
    await multi.exec();
  }

  // -------------------------------------------------------------------------
  // Move to DLQ
  // -------------------------------------------------------------------------

  async moveToDlq(input: MoveToDlqInput): Promise<void> {
    const now = new Date().toISOString();
    const hash = await this.client.hGetAll(k.job(input.jobId));
    if (!hash || Object.keys(hash).length === 0) return;

    if (input.lockId !== undefined) {
      if (hash["status"] !== "active" || hash["lockId"] !== input.lockId) return;
    }

    const queue = hash["queue"] ?? "";
    const attempts: JobAttempt[] = JSON.parse(hash["attempts"] ?? "[]") as JobAttempt[];

    attempts.push({
      attempt: input.attemptNumber,
      startedAt: hash["updatedAt"] ?? now,
      finishedAt: now,
      error: input.error,
      ...(input.stack !== undefined ? { stack: input.stack } : {}),
    });

    const multi = this.client.multi();
    multi.hSet(k.job(input.jobId), {
      status: "dead",
      attemptsMade: String(input.attemptNumber),
      attempts: JSON.stringify(attempts),
      lockId: "",
      lockExpiresAt: "",
      failedAt: now,
      updatedAt: now,
    });
    multi.sRem(k.active(queue), input.jobId);
    multi.sAdd(k.dead(queue), input.jobId);
    await multi.exec();
  }

  // -------------------------------------------------------------------------
  // Release lock
  // -------------------------------------------------------------------------

  async releaseLock(jobId: string, lockId?: string): Promise<void> {
    const hash = await this.client.hGetAll(k.job(jobId));
    if (!hash || Object.keys(hash).length === 0) return;

    if (lockId !== undefined) {
      if (hash["status"] !== "active" || hash["lockId"] !== lockId) return;
    }

    const now = new Date().toISOString();
    // Set lockExpiresAt to now (already-expired) rather than clearing it to an
    // empty string. recoverStalledJobs() skips entries where lockExpiresAt is
    // falsy, so an empty string would leave the job permanently stuck.
    await this.client.hSet(k.job(jobId), {
      lockId: "",
      lockExpiresAt: now,
      updatedAt: now,
    });
  }

  // -------------------------------------------------------------------------
  // Recover stalled jobs  (atomic compare-and-swap per job via Lua)
  //
  // Previous implementation: two-phase read pipeline → write pipeline.
  // Race condition: a worker could complete or renew its lock between the two
  // phases, causing recovery to overwrite a legitimately-active job.
  //
  // Fix (issue #6): for each candidate job the RECOVER_STALLED_LUA script
  // re-reads lockExpiresAt, lockId, and status atomically and only applies
  // the recovery if all three still match what was observed in the read phase
  // (compare-and-swap).  If the worker renewed or completed the job in the
  // window between the read and the Lua call, the CAS mismatch causes the
  // script to return 0 and the job is left untouched.
  // -------------------------------------------------------------------------

  async recoverStalledJobs(queue: string, now: string): Promise<string[]> {
    const nowMs = new Date(now).getTime();
    const activeIds = await this.client.sMembers(k.active(queue));
    if (activeIds.length === 0) return [];

    // Phase 1 — read candidate fields.
    // We fetch lockExpiresAt, lockId, and priority for all active jobs in one
    // pipeline.  These values become the "expected" snapshot passed to the Lua
    // CAS script.  If any of these values change before the Lua executes, the
    // script will detect the mismatch and skip that job.
    const fetchPipeline = this.client.multi();
    for (const jobId of activeIds) {
      fetchPipeline.hmGet(k.job(jobId), ["lockExpiresAt", "lockId", "priority"]);
    }
    const fetchResults = await fetchPipeline.exec();

    // Phase 2 — per-job atomic CAS via Lua.
    // Each eval call is an independent atomic unit in Redis; no pipeline is
    // needed here because the script itself guarantees atomicity per job.
    const recovered: string[] = [];
    const evalPromises: Promise<unknown>[] = [];

    for (let i = 0; i < activeIds.length; i++) {
      const jobId = activeIds[i] as string;
      const fields = fetchResults[i] as unknown as
        [string | null, string | null, string | null] | null;
      if (!fields) continue;

      const [lockExpiresAt, lockId, priorityStr] = fields;

      // Pre-filter: skip jobs whose lock has not yet expired according to the
      // snapshot.  This avoids unnecessary Lua round-trips for healthy jobs.
      if (!lockExpiresAt) continue;
      if (new Date(lockExpiresAt).getTime() > nowMs) continue;

      const priority = Number(priorityStr ?? "0");
      const priorityScore = String(-priority); // negative → ZPOPMIN yields highest first

      // The Lua script performs a CAS: it re-reads lockExpiresAt, lockId, and
      // status atomically, aborts if anything has changed, and only then
      // applies the recovery.  Returns 1 on recovery, 0 on stale skip.
      const p = this.client
        .eval(RECOVER_STALLED_LUA, {
          keys: [k.job(jobId), k.active(queue), k.waiting(queue)],
          arguments: [jobId, lockExpiresAt, lockId ?? "", now, priorityScore],
        })
        .then((result) => {
          if (result === 1) recovered.push(jobId);
        });

      evalPromises.push(p);
    }

    // Wait for all CAS scripts to finish before returning.
    await Promise.all(evalPromises);

    return recovered;
  }

  // -------------------------------------------------------------------------
  // Reads
  // -------------------------------------------------------------------------

  async getJob<TPayload = unknown>(jobId: string): Promise<JobData<TPayload> | null> {
    const hash = await this.client.hGetAll(k.job(jobId));
    if (!hash || Object.keys(hash).length === 0) return null;
    return hashToJob<TPayload>(hash);
  }

  async getJobs<TPayload = unknown>(filter: GetJobsFilter): Promise<JobData<TPayload>[]> {
    const { queue, status, limit = 100, offset = 0 } = filter;

    let ids: string[] = [];

    if (queue && status) {
      switch (status) {
        case "waiting":
          ids = await this.client.zRange(k.waiting(queue), 0, -1);
          break;
        case "delayed":
          ids = await this.client.zRange(k.delayed(queue), 0, -1);
          break;
        case "active":
          ids = await this.client.sMembers(k.active(queue));
          break;
        case "completed":
          ids = await this.client.sMembers(k.completed(queue));
          break;
        case "dead":
          ids = await this.client.sMembers(k.dead(queue));
          break;
        default:
          ids = [];
      }
    } else {
      // Scan all job keys — suitable for dev/small datasets only.
      for await (const key of this.client.scanIterator({
        MATCH: `${PREFIX}job:*`,
        COUNT: 500,
      })) {
        const keyStr: string = Array.isArray(key) ? String(key[0]) : String(key);
        ids.push(keyStr.replace(`${PREFIX}job:`, ""));
      }
    }

    const results: JobData<TPayload>[] = [];
    const page = ids.slice(offset, offset + limit);

    for (const id of page) {
      const job = await this.getJob<TPayload>(id);
      if (!job) continue;
      if (queue && job.queue !== queue) continue;
      if (status && job.status !== status) continue;
      results.push(job);
    }

    return results;
  }

  async getJobCounts(queue: string): Promise<Record<JobStatus, number>> {
    const [waiting, delayed, active, completed, dead] = await Promise.all([
      this.client.zCard(k.waiting(queue)),
      this.client.zCard(k.delayed(queue)),
      this.client.sCard(k.active(queue)),
      this.client.sCard(k.completed(queue)),
      this.client.sCard(k.dead(queue)),
    ]);

    return { waiting, delayed, active, completed, dead };
  }

  // -------------------------------------------------------------------------
  // Rate limiting
  // -------------------------------------------------------------------------

  async checkAndIncrementRateLimit(
    queue: string,
    max: number,
    windowMs: number,
    now: string,
  ): Promise<boolean> {
    const nowMs = new Date(now).getTime();
    const ck = k.rateCount(queue);
    const tk = k.rateTs(queue);
    const ttlSec = Math.max(1, Math.ceil(windowMs / 1000));

    const res = await this.client.eval(RATE_LIMIT_LUA, {
      keys: [ck, tk],
      arguments: [String(max), String(windowMs), String(nowMs), String(ttlSec)],
    });

    return Number(res) === 1;
  }
}
