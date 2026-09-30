import type { IStorage } from "./storage.interface.js";
import type { Job, JobStatus } from "../types/job.types.js";

/**
 * PostgreSQL storage backend.
 *
 * Requires the `pg` npm package (>=8.0.0) as a peer dependency.
 * Install it in your project:  npm install pg
 *
 * Table schema (auto-created on first connect)
 * ─────────────────────────────────────────────
 * CREATE TABLE IF NOT EXISTS qjw_jobs (
 *   id          TEXT        NOT NULL,
 *   queue_name  TEXT        NOT NULL,
 *   payload     JSONB       NOT NULL,    -- full Job object
 *   status      TEXT        NOT NULL,
 *   priority    INTEGER     NOT NULL DEFAULT 0,
 *   run_at      BIGINT      NOT NULL,
 *   created_at  BIGINT      NOT NULL,
 *   PRIMARY KEY (queue_name, id)
 * );
 */
export class PostgresStorage implements IStorage {
  private pool: PgPoolLike | null = null;
  private readonly connectionString: string;

  constructor(connectionString: string) {
    this.connectionString = connectionString;
  }

  // ─── IStorage ─────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    let pgModule: { Pool: new (opts: { connectionString: string }) => PgPoolLike };
    try {
      pgModule = await import("pg") as typeof pgModule;
    } catch {
      throw new Error(
        "[queue-jobs-worker] Postgres dialect requires the `pg` package. " +
        "Run: npm install pg",
      );
    }

    this.pool = new pgModule.Pool({ connectionString: this.connectionString });
    await this.ensureTable();
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
  }

  async saveJob<TData, TResult>(
    queueName: string,
    job: Job<TData, TResult>,
  ): Promise<void> {
    const p = this.assertPool();
    await p.query(
      `INSERT INTO qjw_jobs (id, queue_name, payload, status, priority, run_at, created_at)
       VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
       ON CONFLICT (queue_name, id) DO UPDATE SET payload = EXCLUDED.payload,
         status = EXCLUDED.status, priority = EXCLUDED.priority, run_at = EXCLUDED.run_at`,
      [
        job.id,
        queueName,
        JSON.stringify(job),
        job.status,
        job.priority,
        job.runAt,
        job.createdAt,
      ],
    );
  }

  async getJob<TData, TResult>(
    queueName: string,
    jobId: string,
  ): Promise<Job<TData, TResult> | undefined> {
    const p = this.assertPool();
    const { rows } = await p.query<{ payload: Job<TData, TResult> }>(
      `SELECT payload FROM qjw_jobs WHERE queue_name = $1 AND id = $2`,
      [queueName, jobId],
    );
    return rows[0]?.payload;
  }

  async updateJob<TData, TResult>(
    queueName: string,
    jobId: string,
    patch: Partial<Job<TData, TResult>>,
  ): Promise<void> {
    const existing = await this.getJob<TData, TResult>(queueName, jobId);
    if (!existing) return;

    const updated: Job<TData, TResult> = {
      ...existing,
      ...patch,
      updatedAt: Date.now(),
    };

    const p = this.assertPool();
    await p.query(
      `UPDATE qjw_jobs
       SET payload    = $1::jsonb,
           status     = $2,
           priority   = $3,
           run_at     = $4
       WHERE queue_name = $5 AND id = $6`,
      [
        JSON.stringify(updated),
        updated.status,
        updated.priority,
        updated.runAt,
        queueName,
        jobId,
      ],
    );
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    const p = this.assertPool();
    await p.query(
      `DELETE FROM qjw_jobs WHERE queue_name = $1 AND id = $2`,
      [queueName, jobId],
    );
  }

  async listJobs<TData, TResult>(
    queueName: string,
    status?: JobStatus,
  ): Promise<Job<TData, TResult>[]> {
    const p = this.assertPool();
    let sql = `SELECT payload FROM qjw_jobs WHERE queue_name = $1`;
    const params: unknown[] = [queueName];

    if (status !== undefined) {
      sql += ` AND status = $2`;
      params.push(status);
    }

    sql += ` ORDER BY priority ASC, created_at ASC`;

    const { rows } = await p.query<{ payload: Job<TData, TResult> }>(sql, params);
    return rows.map((r) => r.payload);
  }

  async getNextJob<TData, TResult>(
    queueName: string,
  ): Promise<Job<TData, TResult> | undefined> {
    const p = this.assertPool();
    const now = Date.now();

    // Use a CTE that atomically selects AND updates in one statement.
    // FOR UPDATE SKIP LOCKED ensures each row is claimed by exactly one worker
    // under concurrent multi-process access.
    //
    // BUG FIX: also update the `status` column so it stays in sync with the
    // payload's status field — without this the scalar column and the JSONB
    // payload diverged, which broke any query that filtered on the column
    // (e.g. future list/count queries added by callers).
    // We RETURNING the merged payload expression directly (not j.payload)
    // to guarantee the returned value is always the fully post-UPDATE object.
    const { rows } = await p.query<{ payload: Job<TData, TResult> }>(
      `WITH claimed AS (
         SELECT id FROM qjw_jobs
         WHERE queue_name = $1
           AND status IN ('waiting', 'retrying')
           AND run_at <= $2
         ORDER BY priority ASC, created_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       ),
       updated AS (
         UPDATE qjw_jobs j
         SET status  = 'active',
             payload = j.payload || jsonb_build_object(
                         'status',      'active',
                         'updatedAt',   $2::bigint,
                         'processedAt', $2::bigint
                       )
         FROM claimed
         WHERE j.queue_name = $1 AND j.id = claimed.id
         RETURNING j.payload || jsonb_build_object(
                     'status',      'active',
                     'updatedAt',   $2::bigint,
                     'processedAt', $2::bigint
                   ) AS payload
       )
       SELECT payload FROM updated`,
      [queueName, now],
    );

    return rows[0]?.payload;
  }

  async clearQueue(queueName: string): Promise<void> {
    const p = this.assertPool();
    await p.query(`DELETE FROM qjw_jobs WHERE queue_name = $1`, [queueName]);
  }

  async countJobs(queueName: string): Promise<number> {
    const p = this.assertPool();
    const { rows } = await p.query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM qjw_jobs WHERE queue_name = $1`,
      [queueName],
    );
    return parseInt(rows[0]?.count ?? "0", 10);
  }

  // ─── Private helpers ──────────────────────────────────────────────────────

  private assertPool(): PgPoolLike {
    if (!this.pool) {
      throw new Error(
        "[queue-jobs-worker] PostgresStorage is not connected. Did you call client.init()?",
      );
    }
    return this.pool;
  }

  private async ensureTable(): Promise<void> {
    const p = this.assertPool();
    await p.query(`
      CREATE TABLE IF NOT EXISTS qjw_jobs (
        id          TEXT    NOT NULL,
        queue_name  TEXT    NOT NULL,
        payload     JSONB   NOT NULL,
        status      TEXT    NOT NULL,
        priority    INTEGER NOT NULL DEFAULT 0,
        run_at      BIGINT  NOT NULL,
        created_at  BIGINT  NOT NULL,
        PRIMARY KEY (queue_name, id)
      );
      CREATE INDEX IF NOT EXISTS qjw_jobs_next_idx
        ON qjw_jobs (queue_name, status, priority ASC, created_at ASC);
    `);
  }
}

// ─── Minimal type shim ────────────────────────────────────────────────────────

interface PgPoolLike {
  query<T = unknown>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
  end(): Promise<void>;
}
