import type { IStorage } from "./storage.interface.js";
import type { Job, JobStatus } from "../types/job.types.js";

/**
 * In-memory storage backend.
 *
 * Uses a plain `Map<queueName, Map<jobId, Job>>` — zero external dependencies.
 * Data is lost when the process exits, making this ideal for:
 *   - Local development
 *   - Unit / integration tests
 *   - Lightweight single-process applications
 */
export class MemoryStorage implements IStorage {
  /**
   * Top-level map: queueName → (jobId → Job).
   * Each queue gets its own inner Map so lookups stay O(1).
   */
  private readonly store = new Map<string, Map<string, Job>>();

  // ─── IStorage ─────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    // Nothing to open for an in-memory store.
  }

  async disconnect(): Promise<void> {
    // Clear all data on shutdown so memory is released cleanly.
    this.store.clear();
  }

  async saveJob<TData, TResult>(queueName: string, job: Job<TData, TResult>): Promise<void> {
    const queue = this.getOrCreateQueue(queueName);
    queue.set(job.id, job as unknown as Job);
  }

  async getJob<TData, TResult>(
    queueName: string,
    jobId: string,
  ): Promise<Job<TData, TResult> | undefined> {
    const queue = this.store.get(queueName);
    if (!queue) return undefined;
    const job = queue.get(jobId);
    return job as Job<TData, TResult> | undefined;
  }

  async updateJob<TData, TResult>(
    queueName: string,
    jobId: string,
    patch: Partial<Job<TData, TResult>>,
  ): Promise<void> {
    const queue = this.store.get(queueName);
    if (!queue) return;

    const existing = queue.get(jobId);
    if (!existing) return;

    // Merge the patch into the stored job and refresh updatedAt.
    const updated: Job = {
      ...existing,
      ...(patch as Partial<Job>),
      updatedAt: Date.now(),
    };
    queue.set(jobId, updated);
  }

  async removeJob(queueName: string, jobId: string): Promise<void> {
    this.store.get(queueName)?.delete(jobId);
  }

  async listJobs<TData, TResult>(
    queueName: string,
    status?: JobStatus,
  ): Promise<Job<TData, TResult>[]> {
    const queue = this.store.get(queueName);
    if (!queue) return [];

    let jobs = Array.from(queue.values()) as Job<TData, TResult>[];
    if (status !== undefined) {
      jobs = jobs.filter((j) => j.status === status);
    }

    return this.sortJobs(jobs);
  }

  async getNextJob<TData, TResult>(queueName: string): Promise<Job<TData, TResult> | undefined> {
    const queue = this.store.get(queueName);
    if (!queue) return undefined;

    const now = Date.now();

    const candidates = (Array.from(queue.values()) as Job<TData, TResult>[]).filter(
      (j) => (j.status === "waiting" || j.status === "retrying") && j.runAt <= now,
    );

    if (candidates.length === 0) return undefined;

    const next = this.sortJobs(candidates)[0]!;

    // FIX: atomically flip status → "active" inside the same synchronous Map
    // operation so a second concurrent worker iterating in the same tick sees
    // the updated status and will not claim the same job.
    // Node.js is single-threaded, so a synchronous Map write here is enough
    // to prevent double-claims within a single process.
    const claimed: Job = {
      ...(next as unknown as Job),
      status: "active",
      processedAt: now,
      updatedAt: now,
    };
    queue.set(next.id, claimed);

    return claimed as unknown as Job<TData, TResult>;
  }

  async clearQueue(queueName: string): Promise<void> {
    this.store.get(queueName)?.clear();
  }

  async countJobs(queueName: string): Promise<number> {
    return this.store.get(queueName)?.size ?? 0;
  }

  // ─── Private helpers ──────────────────────────────────────────────────────

  private getOrCreateQueue(queueName: string): Map<string, Job> {
    let queue = this.store.get(queueName);
    if (!queue) {
      queue = new Map<string, Job>();
      this.store.set(queueName, queue);
    }
    return queue;
  }

  /**
   * Sort jobs by priority ascending (lower number = higher priority),
   * then by createdAt ascending (older jobs run first).
   */
  private sortJobs<TData, TResult>(jobs: Job<TData, TResult>[]): Job<TData, TResult>[] {
    return jobs.slice().sort((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      return a.createdAt - b.createdAt;
    });
  }
}
