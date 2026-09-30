import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { QueueClient } from "../src/classes/client.js";
import { Queue } from "../src/classes/queue.js";
import { Worker } from "../src/classes/worker.js";
import type { Job } from "../src/types/job.types.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
  interval  = 20,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = async () => {
      if (await predicate()) return resolve();
      if (Date.now() - start >= timeoutMs) return reject(new Error("waitFor timed out"));
      setTimeout(check, interval);
    };
    void check();
  });
}

// ─── setup ────────────────────────────────────────────────────────────────────

let client: QueueClient;

beforeEach(async () => {
  QueueClient.clearDefaultClient();
  client = new QueueClient({
    dialect: "memory",
    options: { attempts: 3, retryDelay: 50, backoff: "fixed", timeout: 2000 },
  });
  await client.init();
});

afterEach(async () => {
  if (client.isInitialized()) await client.close();
  QueueClient.clearDefaultClient();
});

// ─── construction ─────────────────────────────────────────────────────────────

describe("Worker — construction", () => {
  it("takes a Queue and handler", () => {
    const queue  = new Queue("ctor", client);
    const worker = new Worker(queue, async () => "ok");
    expect(worker.queue).toBe(queue);
    expect(worker.isRunning()).toBe(false);
  });

  it("does NOT expose add/get/remove/list/count (those live on Queue)", () => {
    const queue  = new Queue("api", client);
    const worker = new Worker(queue, async () => "ok");
    expect((worker as Record<string, unknown>).add).toBeUndefined();
    expect((worker as Record<string, unknown>).get).toBeUndefined();
    expect((worker as Record<string, unknown>).remove).toBeUndefined();
    expect((worker as Record<string, unknown>).list).toBeUndefined();
    expect((worker as Record<string, unknown>).count).toBeUndefined();
  });

  it("concurrency option is stored (Worker-only concern)", () => {
    const queue  = new Queue("conc", client);
    const worker = new Worker(queue, async () => "ok", { concurrency: 4 });
    // concurrency is private — just verify it doesn't throw and worker works
    expect(worker.isRunning()).toBe(false);
  });
});

// ─── success path ─────────────────────────────────────────────────────────────

describe("Worker — successful processing", () => {
  it("processes a job and marks it completed", async () => {
    const queue  = new Queue<{ n: number }, number>("success", client);
    const worker = new Worker(queue, async (job) => job.data.n * 2);
    worker.start();

    const job = await queue.add("double", { n: 21 });
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    const done = await queue.get(job.id);
    expect(done?.status).toBe("completed");
    expect(done?.result).toBe(42);
    expect(done?.finishedAt).toBeDefined();

    await worker.close();
  });

  it("emits 'active' then 'completed'", async () => {
    const queue  = new Queue<{ x: number }, number>("events", client);
    const worker = new Worker(queue, async (job) => job.data.x + 1);

    const events: string[] = [];
    worker.on("active",    () => events.push("active"));
    worker.on("completed", () => events.push("completed"));
    worker.start();

    const job = await queue.add("inc", { x: 9 });
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    expect(events).toEqual(["active", "completed"]);
    await worker.close();
  });

  it("stores the handler return value in job.result", async () => {
    const queue  = new Queue<{ name: string }, string>("result", client);
    const worker = new Worker(queue, async (job) => `Hello, ${job.data.name}!`);
    worker.start();

    const job = await queue.add("greet", { name: "World" });
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    expect((await queue.get(job.id))?.result).toBe("Hello, World!");
    await worker.close();
  });

  it("emits 'started' and 'stopped'", async () => {
    const queue  = new Queue("lifecycle", client);
    const worker = new Worker(queue, async () => "ok");
    const events: string[] = [];
    worker.on("started", () => events.push("started"));
    worker.on("stopped", () => events.push("stopped"));
    worker.start();
    await worker.close();
    expect(events).toContain("started");
    expect(events).toContain("stopped");
  });

  it("removeOnComplete deletes the job from storage", async () => {
    const queue  = new Queue("rm-ok", client);
    const worker = new Worker(queue, async () => "done");
    worker.start();

    const job = await queue.add("rm", {}, { removeOnComplete: true });
    await waitFor(async () => (await queue.get(job.id)) === undefined);

    expect(await queue.get(job.id)).toBeUndefined();
    await worker.close();
  });
});

// ─── retry / failure ──────────────────────────────────────────────────────────

describe("Worker — retries and failure", () => {
  it("retries up to the attempt limit then marks failed", async () => {
    const queue  = new Queue("retry", client);
    const worker = new Worker(queue, async () => {
      throw new Error("always fails");
    });
    worker.start();

    const job = await queue.add("bad", {}, { attempts: 3 });
    await waitFor(
      async () => (await queue.get(job.id))?.status === "failed",
      15_000,
    );

    const failed = await queue.get(job.id);
    expect(failed?.status).toBe("failed");
    expect(failed?.attemptsMade).toBe(3);
    expect(failed?.error).toMatch("always fails");
    expect(failed?.finishedAt).toBeDefined();

    await worker.close();
  });

  it("emits 'error' per attempt and 'failed' once", async () => {
    const queue    = new Queue("err-events", client);
    const worker   = new Worker(queue, async () => { throw new Error("boom"); });
    const errors:  Error[] = [];
    const failedJ: Job[]   = [];
    worker.on("error",  (_, e) => errors.push(e));
    worker.on("failed", (j)    => failedJ.push(j as Job));
    worker.start();

    const job = await queue.add("boom", {}, { attempts: 2 });
    await waitFor(
      async () => (await queue.get(job.id))?.status === "failed",
      10_000,
    );

    expect(errors).toHaveLength(2);
    expect(failedJ).toHaveLength(1);
    expect(failedJ[0]!.status).toBe("failed");
    await worker.close();
  });

  it("stores error message and stacktrace on the job", async () => {
    const queue  = new Queue("stack", client);
    const worker = new Worker(queue, async () => { throw new Error("oh no"); });
    worker.start();

    const job = await queue.add("bad", {}, { attempts: 1 });
    await waitFor(
      async () => (await queue.get(job.id))?.status === "failed",
      5_000,
    );

    const failed = await queue.get(job.id);
    expect(failed?.error).toBe("oh no");
    expect(failed?.stacktrace).toContain("Error: oh no");
    await worker.close();
  });

  it("removeOnFail deletes the job from storage", async () => {
    const queue  = new Queue("rm-fail", client);
    const worker = new Worker(queue, async () => { throw new Error("gone"); });
    worker.start();

    const job = await queue.add("rm", {}, { attempts: 1, removeOnFail: true });
    await waitFor(async () => (await queue.get(job.id)) === undefined, 5_000);

    expect(await queue.get(job.id)).toBeUndefined();
    await worker.close();
  });

  it("attempts: 0 means unlimited — retries until success", async () => {
    let calls = 0;
    const queue  = new Queue("unlimited", client);
    const worker = new Worker(queue, async () => {
      calls++;
      if (calls < 4) throw new Error("not yet");
      return "finally";
    });
    worker.start();

    const job = await queue.add("eventually", {}, { attempts: 0 });
    await waitFor(
      async () => (await queue.get(job.id))?.status === "completed",
      15_000,
    );

    expect((await queue.get(job.id))?.status).toBe("completed");
    expect(calls).toBe(4);
    await worker.close();
  });
});

// ─── timeout ──────────────────────────────────────────────────────────────────

describe("Worker — timeout", () => {
  it("fails a job that exceeds the timeout", async () => {
    const shortClient = new QueueClient({
      dialect: "memory",
      options:  { attempts: 1, timeout: 100 },
    });
    await shortClient.init();

    const queue  = new Queue("timeout-q", shortClient);
    const worker = new Worker(queue, async () => {
      await sleep(500);
      return "too slow";
    });
    worker.start();

    const job = await queue.add("slow", {});
    await waitFor(
      async () => (await queue.get(job.id))?.status === "failed",
      5_000,
    );

    const failed = await queue.get(job.id);
    expect(failed?.status).toBe("failed");
    expect(failed?.error).toMatch("timed out");

    await worker.close();
    await shortClient.close();
  });
});

// ─── concurrency ──────────────────────────────────────────────────────────────

describe("Worker — concurrency", () => {
  it("respects the concurrency ceiling", async () => {
    let concurrent    = 0;
    let maxConcurrent = 0;

    const queue  = new Queue("conc-q", client);
    const worker = new Worker(
      queue,
      async () => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await sleep(80);
        concurrent--;
        return "ok";
      },
      { concurrency: 3 },
    );
    worker.start();

    await Promise.all(
      Array.from({ length: 6 }, (_, i) => queue.add(`j${i}`, {})),
    );

    await waitFor(
      async () => (await queue.list("completed")).length === 6,
      10_000,
    );
    await worker.close();

    expect(maxConcurrent).toBeGreaterThanOrEqual(2);
    expect(maxConcurrent).toBeLessThanOrEqual(3);
  });
});

// ─── delayed jobs ─────────────────────────────────────────────────────────────

describe("Worker — delayed jobs", () => {
  it("does not process a job before its runAt", async () => {
    const queue  = new Queue("delay-q", client);
    const worker = new Worker(queue, async () => "done");
    worker.start();

    const job = await queue.add("future", {}, { delay: 500 });

    await sleep(100);
    const still = await queue.get(job.id);
    expect(still?.status).not.toBe("completed");

    await waitFor(
      async () => (await queue.get(job.id))?.status === "completed",
      5_000,
    );
    await worker.close();
  });
});

// ─── priority ordering ────────────────────────────────────────────────────────

describe("Worker — priority ordering", () => {
  it("processes lower-number priority first (concurrency 1)", async () => {
    const processed: string[] = [];
    const queue  = new Queue<{ name: string }>("prio-q", client);
    const worker = new Worker(
      queue,
      async (job) => { processed.push(job.data.name); await sleep(20); },
      { concurrency: 1 },
    );

    // Add all three before starting so Worker picks them in order.
    await queue.add("low",  { name: "low"  }, { priority: 10 });
    await queue.add("high", { name: "high" }, { priority: 1  });
    await queue.add("mid",  { name: "mid"  }, { priority: 5  });

    worker.start();
    await waitFor(
      async () => (await queue.list("completed")).length === 3,
      10_000,
    );
    await worker.close();

    expect(processed[0]).toBe("high");
    expect(processed[1]).toBe("mid");
    expect(processed[2]).toBe("low");
  });
});

// ─── graceful close ───────────────────────────────────────────────────────────

describe("Worker — graceful close", () => {
  it("close() on a never-started worker is a no-op", async () => {
    const queue  = new Queue("noop", client);
    const worker = new Worker(queue, async () => "ok");
    await expect(worker.close()).resolves.toBeUndefined();
  });

  it("start() is idempotent", async () => {
    const queue  = new Queue("idempotent", client);
    const worker = new Worker(queue, async () => "ok");
    worker.start();
    worker.start(); // second call is a no-op
    expect(worker.isRunning()).toBe(true);
    await worker.close();
  });

  it("waits for in-flight jobs before resolving", async () => {
    let finished = 0;
    const queue  = new Queue("drain", client);
    const worker = new Worker(queue, async () => {
      await sleep(60);
      finished++;
      return "ok";
    });
    worker.start();

    await queue.add("t1", {});
    await queue.add("t2", {});

    // Wait until at least one job is active before triggering close.
    await waitFor(async () => (await queue.list("active")).length > 0);

    await worker.close();
    expect(finished).toBeGreaterThanOrEqual(1);
  });
});
