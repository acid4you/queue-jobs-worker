import type { IStorage } from "./storage.interface.js";
import type { Job, JobStatus } from "../types/job.types.js";

/**
 * MySQL / MariaDB storage backend.
 *
 * Requires the `mysql2` npm package (>=3.0.0) as a peer dependency.
 * Install it in your project:  npm install mysql2
 *
 * Table schema (auto-created on first connect)
 * ─────────────────────────────────────────────
 * CREATE TABLE IF NOT EXISTS qjw_jobs (
 *   id          VARCHAR(36)  NOT NULL,
 *   queue_name  VARCHAR(255) NOT NULL,
 *   payload     JSON         NOT NULL,   -- full Job object
 *   status      VARCHAR(20)  NOT NULL,
 *   priority    INT          NOT NULL DEFAULT 0,
 *   run_at      BIGINT       NOT NULL,
 *   created_at  BIGINT       NOT NULL,
 *   PRIMARY KEY (queue_name, id)
 * );
 */
export class MysqlStorage implements IStorage {
  private pool: MysqlPoolLike | null = null;
  private readonly connectionString: string;

  constructor(connectionString: string) {
    this.connectionString = connectionString;
  }

  // ─── IStorage ─────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    let mysql2Module: {
      createPool: (opts: {
        uri: string;
        waitForConnections: boolean;
        connectionLimit: number;
      }) => MysqlPoolLike;
    };
    try {
      mysql2Module = (await import("mysql2/promise")) as typeof mysql2Module;
    } catch {
      throw new Error(
        "[queue-jobs-worker] MySQL dialect requires the `mysql2` package. " +
          "Run: npm install mysql2",
      );
    }

    this.pool = mysql2Module.createPool({
      uri: this.connectionString,
      waitForConnections: true,
      connectionLimit: 10,
    });

    await this.ensureTable();
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
  }

  async saveJob<TData, TResult>(queueName: string, job: Job<TData, TResult>): Promise<void> {
    const p = this.assertPool();
    await p.execute(
      `INSERT INTO qjw_jobs (id, queue_name, payload, status, priority, run_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         payload    = VALUES(payload),
         status     = VALUES(status),
         priority   = VALUES(priority),
         run_at     = VALUES(run_at)`,
      [job.id, queueName, JSON.stringify(job), job.status, job.priority, job.runAt, job.createdAt],
    );
  }

  async getJob<TData, TResult>(
    queueName: string,
    jobId: string,
  ): Promise<Job<TData, TResult> | undefined> {
    const p = this.assertPool();
    const [rows] = await p.execute<MysqlRow[]>(
      `SELECT payload FROM qjw_jobs WHERE queue_name = ? AND id = ? LIMIT 1`,
      [queueName, jobId],
    );

    const row = (rows as MysqlRow[])[0];
    if (!row) return undefined;

    // mysql2 parses JSON columns automatically; handle both cases.
    const raw = typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload;
    return raw as Job<TData, TResult>;
  }

  async updateJob<TData, TResult>(
    queueName: string,
    jobId: string,
    patch: Partial<Job<TData, TResult>>,
  ): Promise<void> {
    // BUG FIX: the previous implementation did getJob() + UPDATE in two
    // separate round-trips with no transaction.  Between the read and the
    // write another process could update the same job, and our write would
    // silently overwrite the newer data (lost-update / ABA race).
    //
    // Fix: use a single UPDATE with JSON_MERGE_PATCH so the merge happens
    // atomically inside MySQL.  We pass updatedAt explicitly so the server
    // always records the correct timestamp regardless of clock skew.
    const p = this.assertPool();

    // Build a minimal patch object (only the fields actually changing) and
    // add updatedAt so the merged payload stays consistent.
    const patchWithTs = { ...patch, updatedAt: Date.now() } as Record<string, unknown>;

    // JSON_MERGE_PATCH(col, ?) merges our patch into the stored JSONB object
    // atomically inside MySQL.  Scalar columns (status, priority, run_at) are
    // updated from the patch when present, otherwise kept as-is.
    const newStatus = patch.status !== undefined ? patch.status : null;
    const newPriority = patch.priority !== undefined ? patch.priority : null;
    const newRunAt = patch.runAt !== undefined ? patch.runAt : null;

    await p.execute(
      `UPDATE qjw_jobs
       SET payload   = JSON_MERGE_PATCH(payload, ?),
           status    = COALESCE(?, status),
           priority  = COALESCE(?, priority),
           run_at    = COALESCE(?, run_at)
       WHERE queue_name = ? AND id = ?`,
      [JSON.stringify(patchWithTs), newStatus, newPriority, newRunAt, queueName, jobId],
    );
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    const p = this.assertPool();
    await p.execute(`DELETE FROM qjw_jobs WHERE queue_name = ? AND id = ?`, [queueName, jobId]);
  }

  async listJobs<TData, TResult>(
    queueName: string,
    status?: JobStatus,
  ): Promise<Job<TData, TResult>[]> {
    const p = this.assertPool();
    let sql = `SELECT payload FROM qjw_jobs WHERE queue_name = ?`;
    const params: unknown[] = [queueName];

    if (status !== undefined) {
      sql += ` AND status = ?`;
      params.push(status);
    }

    sql += ` ORDER BY priority ASC, created_at ASC`;

    const [rows] = await p.execute<MysqlRow[]>(sql, params);
    return (rows as MysqlRow[]).map((r) => {
      const raw = typeof r.payload === "string" ? JSON.parse(r.payload) : r.payload;
      return raw as Job<TData, TResult>;
    });
  }

  async getNextJob<TData, TResult>(queueName: string): Promise<Job<TData, TResult> | undefined> {
    const p = this.assertPool();
    const now = Date.now();

    // FIX: use SELECT ... FOR UPDATE SKIP LOCKED inside an explicit
    // transaction so concurrent multi-process workers never double-claim.
    // mysql2 promise pool gives us a connection we can BEGIN/COMMIT on.
    const conn = await p.getConnection();
    try {
      // BUG FIX: conn.execute("START TRANSACTION") works at the SQL level but
      // bypasses mysql2's internal transaction-state tracking, which can confuse
      // pool health checks and connection-reuse logic.  Use the dedicated API
      // method instead so mysql2 knows the connection is in a transaction.
      await conn.beginTransaction();

      const [rows] = await conn.execute<MysqlRow[]>(
        `SELECT id, payload FROM qjw_jobs
         WHERE queue_name = ?
           AND status IN ('waiting', 'retrying')
           AND run_at <= ?
         ORDER BY priority ASC, created_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED`,
        [queueName, now],
      );

      const row = (rows as MysqlRow[])[0];
      if (!row) {
        await conn.commit();
        return undefined;
      }

      const jobId = row.id as string;
      const raw = typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload;
      const job = raw as Job<TData, TResult>;

      // Atomically flip status to active in the same transaction.
      const updated: Job<TData, TResult> = {
        ...job,
        status: "active",
        updatedAt: now,
        processedAt: now,
      };

      await conn.execute(
        `UPDATE qjw_jobs
         SET payload = ?, status = 'active', run_at = run_at
         WHERE queue_name = ? AND id = ?`,
        [JSON.stringify(updated), queueName, jobId],
      );

      await conn.commit();
      return updated;
    } catch (err) {
      await conn.rollback().catch(() => undefined);
      throw err;
    } finally {
      conn.release();
    }
  }

  async clearQueue(queueName: string): Promise<void> {
    const p = this.assertPool();
    await p.execute(`DELETE FROM qjw_jobs WHERE queue_name = ?`, [queueName]);
  }

  async countJobs(queueName: string): Promise<number> {
    const p = this.assertPool();
    const [rows] = await p.execute<MysqlRow[]>(
      `SELECT COUNT(*) AS count FROM qjw_jobs WHERE queue_name = ?`,
      [queueName],
    );
    const row = (rows as MysqlRow[])[0];
    if (!row) return 0;
    // FIX: mysql2 returns COUNT(*) as BigInt in some configurations.
    // Convert via String first to safely handle both Number and BigInt.
    return parseInt(String(row.count ?? 0), 10);
  }

  // ─── Private helpers ──────────────────────────────────────────────────────

  private assertPool(): MysqlPoolLike {
    if (!this.pool) {
      throw new Error(
        "[queue-jobs-worker] MysqlStorage is not connected. Did you call client.init()?",
      );
    }
    return this.pool;
  }

  private async ensureTable(): Promise<void> {
    const p = this.assertPool();
    await p.execute(`
      CREATE TABLE IF NOT EXISTS qjw_jobs (
        id          VARCHAR(36)  NOT NULL,
        queue_name  VARCHAR(255) NOT NULL,
        payload     JSON         NOT NULL,
        status      VARCHAR(20)  NOT NULL,
        priority    INT          NOT NULL DEFAULT 0,
        run_at      BIGINT       NOT NULL,
        created_at  BIGINT       NOT NULL,
        PRIMARY KEY (queue_name, id),
        INDEX qjw_jobs_next_idx (queue_name, status, priority ASC, created_at ASC)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
  }
}

// ─── Minimal type shim ────────────────────────────────────────────────────────

interface MysqlRow {
  id?: string;
  payload: string | Record<string, unknown>;
  count?: number | string | bigint;
}

interface MysqlConnectionLike {
  execute<T = unknown>(sql: string, params?: unknown[]): Promise<[T, unknown]>;
  beginTransaction(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  release(): void;
}

interface MysqlPoolLike {
  execute<T = unknown>(sql: string, params?: unknown[]): Promise<[T, unknown]>;
  getConnection(): Promise<MysqlConnectionLike>;
  end(): Promise<void>;
}
