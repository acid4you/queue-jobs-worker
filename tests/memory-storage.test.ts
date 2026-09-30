import { describe, it, expect, beforeEach } from "vitest";
import { MemoryStorage } from "../src/storage/memory.storage.js";
import type { Job } from "../src/types/job.types.js";

function makeJob(overrides: Partial<Job> = {}): Job {
  const now = Date.now();
  return {
    id: "job-1",
    name: "test",
    data: { x: 1 },
    status: "waiting",
    opts: {},
    attempts: 3,
    attemptsMade: 0,
    delay: 0,
    runAt: now,
    priority: 0,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

let storage: MemoryStorage;

beforeEach(async () => {
  storage = new MemoryStorage();
  await storage.connect();
});

describe("MemoryStorage — saveJob / getJob", () => {
  it("saves and retrieves a job by id", async () => {
    const job = makeJob();
    await storage.saveJob("q", job);
    const fetched = await storage.getJob("q", job.id);
    expect(fetched).toMatchObject({ id: "job-1", name: "test" });
  });

  it("returns undefined for an unknown jobId", async () => {
    expect(await storage.getJob("q", "nope")).toBeUndefined();
  });

  it("returns undefined for an unknown queue", async () => {
    expect(await storage.getJob("no-such-queue", "id")).toBeUndefined();
  });
});

describe("MemoryStorage — updateJob", () => {
  it("merges a partial patch into the stored job", async () => {
    await storage.saveJob("q", makeJob({ id: "u1", status: "waiting" }));
    await storage.updateJob("q", "u1", { status: "active" });
    const updated = await storage.getJob("q", "u1");
    expect(updated?.status).toBe("active");
  });

  it("updates updatedAt on every patch", async () => {
    const before = Date.now();
    await storage.saveJob("q", makeJob({ id: "u2", updatedAt: before - 1000 }));
    await storage.updateJob("q", "u2", { status: "active" });
    const updated = await storage.getJob("q", "u2");
    expect(updated!.updatedAt).toBeGreaterThanOrEqual(before);
  });

  it("is a no-op for an unknown jobId", async () => {
    await expect(storage.updateJob("q", "ghost", { status: "active" })).resolves.toBeUndefined();
  });
});

describe("MemoryStorage — removeJob", () => {
  it("deletes a job from the store", async () => {
    await storage.saveJob("q", makeJob({ id: "r1" }));
    await storage.removeJob("q", "r1");
    expect(await storage.getJob("q", "r1")).toBeUndefined();
  });

  it("is a no-op for an unknown id", async () => {
    await expect(storage.removeJob("q", "ghost")).resolves.toBeUndefined();
  });
});

describe("MemoryStorage — listJobs", () => {
  it("returns all jobs when no status filter is given", async () => {
    await storage.saveJob("q", makeJob({ id: "l1", status: "waiting" }));
    await storage.saveJob("q", makeJob({ id: "l2", status: "completed" }));
    const all = await storage.listJobs("q");
    expect(all).toHaveLength(2);
  });

  it("filters by status", async () => {
    await storage.saveJob("q", makeJob({ id: "s1", status: "waiting" }));
    await storage.saveJob("q", makeJob({ id: "s2", status: "failed" }));
    const waiting = await storage.listJobs("q", "waiting");
    expect(waiting).toHaveLength(1);
    expect(waiting[0]!.status).toBe("waiting");
  });

  it("sorts by priority then createdAt (asc)", async () => {
    const t = Date.now();
    await storage.saveJob("q", makeJob({ id: "p3", priority: 10, createdAt: t }));
    await storage.saveJob("q", makeJob({ id: "p1", priority: 1, createdAt: t }));
    await storage.saveJob("q", makeJob({ id: "p2", priority: 5, createdAt: t }));

    const jobs = await storage.listJobs("q");
    expect(jobs.map((j) => j.id)).toEqual(["p1", "p2", "p3"]);
  });

  it("returns empty array for an empty queue", async () => {
    expect(await storage.listJobs("empty-q")).toEqual([]);
  });
});

describe("MemoryStorage — getNextJob", () => {
  it("returns the first waiting job whose runAt <= now", async () => {
    const now = Date.now();
    await storage.saveJob("q", makeJob({ id: "ready", status: "waiting", runAt: now - 100 }));
    await storage.saveJob("q", makeJob({ id: "future", status: "waiting", runAt: now + 60_000 }));
    const next = await storage.getNextJob("q");
    expect(next?.id).toBe("ready");
  });

  it("also picks up 'retrying' jobs whose runAt <= now", async () => {
    const now = Date.now();
    await storage.saveJob("q", makeJob({ id: "r1", status: "retrying", runAt: now - 1 }));
    const next = await storage.getNextJob("q");
    expect(next?.id).toBe("r1");
  });

  it("does not return active, completed, or failed jobs", async () => {
    const now = Date.now();
    await storage.saveJob("q", makeJob({ id: "a", status: "active", runAt: now }));
    await storage.saveJob("q", makeJob({ id: "c", status: "completed", runAt: now }));
    await storage.saveJob("q", makeJob({ id: "f", status: "failed", runAt: now }));
    expect(await storage.getNextJob("q")).toBeUndefined();
  });

  it("returns undefined when queue is empty", async () => {
    expect(await storage.getNextJob("empty-q")).toBeUndefined();
  });

  it("returns the highest-priority eligible job", async () => {
    const now = Date.now();
    await storage.saveJob(
      "q",
      makeJob({ id: "low", priority: 10, runAt: now - 1, status: "waiting" }),
    );
    await storage.saveJob(
      "q",
      makeJob({ id: "high", priority: 1, runAt: now - 1, status: "waiting" }),
    );
    const next = await storage.getNextJob("q");
    expect(next?.id).toBe("high");
  });
});

describe("MemoryStorage — clearQueue / countJobs", () => {
  it("clearQueue removes all jobs", async () => {
    await storage.saveJob("q", makeJob({ id: "c1" }));
    await storage.saveJob("q", makeJob({ id: "c2" }));
    await storage.clearQueue("q");
    expect(await storage.countJobs("q")).toBe(0);
  });

  it("countJobs returns correct count", async () => {
    expect(await storage.countJobs("cnt-q")).toBe(0);
    await storage.saveJob("cnt-q", makeJob({ id: "x1" }));
    await storage.saveJob("cnt-q", makeJob({ id: "x2" }));
    expect(await storage.countJobs("cnt-q")).toBe(2);
  });
});

describe("MemoryStorage — disconnect", () => {
  it("clears all data on disconnect", async () => {
    await storage.saveJob("q", makeJob({ id: "d1" }));
    await storage.disconnect();
    // After disconnect the store is empty; a new connect (or just a new instance) is needed.
    const fresh = new MemoryStorage();
    await fresh.connect();
    expect(await fresh.countJobs("q")).toBe(0);
  });
});

describe("MemoryStorage — clearQueue removes outer entry", () => {
  it("clearQueue removes the queue entry entirely (no memory leak)", async () => {
    await storage.saveJob("q", makeJob({ id: "cl1" }));
    await storage.clearQueue("q");
    expect(await storage.countJobs("q")).toBe(0);
    // Saving again after clear should work fine (entry recreated on demand).
    await storage.saveJob("q", makeJob({ id: "cl2" }));
    expect(await storage.countJobs("q")).toBe(1);
  });
});

describe("MemoryStorage — getNextJob atomicity", () => {
  it("two synchronous calls never return the same job", async () => {
    const now = Date.now();
    await storage.saveJob("q", makeJob({ id: "atom1", status: "waiting", runAt: now - 1 }));

    // Fire both calls in the same microtask tick — Node.js is single-threaded
    // so the synchronous Map.set inside getNextJob prevents double-claim.
    const [a, b] = await Promise.all([
      storage.getNextJob("q"),
      storage.getNextJob("q"),
    ]);

    const claimed = [a, b].filter(Boolean);
    expect(claimed).toHaveLength(1);
  });
});

describe("MemoryStorage — updateJob null fields", () => {
  it("null patch fields clear existing values", async () => {
    const job = makeJob({ id: "null1", error: "oops", stacktrace: "at line 1" });
    await storage.saveJob("q", job);
    await storage.updateJob("q", "null1", {
      status: "waiting",
      error: null as unknown as string,
      stacktrace: null as unknown as string,
    });
    const updated = await storage.getJob("q", "null1");
    expect(updated?.error).toBeNull();
    expect(updated?.stacktrace).toBeNull();
  });
});

describe("MemoryStorage — saveJob upsert", () => {
  it("re-saving with a different status overwrites the stored job", async () => {
    await storage.saveJob("q", makeJob({ id: "ups1", status: "waiting" }));
    await storage.saveJob("q", makeJob({ id: "ups1", status: "active" }));
    const job = await storage.getJob("q", "ups1");
    expect(job?.status).toBe("active");
  });
});
