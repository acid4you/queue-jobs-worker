import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { QueueClient } from "../src/classes/client.js";
import { Queue } from "../src/classes/queue.js";

let client: QueueClient;

beforeEach(async () => {
  QueueClient.clearDefaultClient();
  client = new QueueClient({ dialect: "memory" });
  await client.init();
});

afterEach(async () => {
  if (client.isInitialized()) await client.close();
  QueueClient.clearDefaultClient();
});

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeQueue(name = "test-q") {
  return new Queue<{ msg: string }>(name, client);
}

// ─── construction ─────────────────────────────────────────────────────────────

describe("Queue — construction", () => {
  it("stores name and client", () => {
    const q = makeQueue();
    expect(q.name).toBe("test-q");
    expect(q.client).toBe(client);
  });

  it("uses the default client when none is passed", () => {
    const q = new Queue("implicit");
    expect(q.client).toBe(client);
  });

  it("throws if no initialized client exists", async () => {
    QueueClient.clearDefaultClient();
    expect(() => new Queue("orphan")).toThrow("must be initialized");
  });

  it("has no concurrency option (that belongs to Worker)", () => {
    const q = makeQueue();
    // QueueOptions should not contain concurrency
    expect((q.options as Record<string, unknown>).concurrency).toBeUndefined();
  });
});

// ─── add ──────────────────────────────────────────────────────────────────────

describe("Queue — add()", () => {
  it("returns a job with a UUID v4 id", async () => {
    const q = makeQueue();
    const job = await q.add("greet", { msg: "hello" });
    expect(job.id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
  });

  it("sets name and data", async () => {
    const q = makeQueue();
    const job = await q.add("greet", { msg: "hello" });
    expect(job.name).toBe("greet");
    expect(job.data).toEqual({ msg: "hello" });
  });

  it("plain job starts as 'waiting'", async () => {
    const q = makeQueue();
    const job = await q.add("plain", { msg: "x" });
    expect(job.status).toBe("waiting");
  });

  it("delayed job starts as 'waiting' — runAt gates pickup", async () => {
    const q = makeQueue();
    const job = await q.add("delayed", { msg: "x" }, { delay: 5000 });
    expect(job.status).toBe("waiting");
    expect(job.runAt).toBeGreaterThan(Date.now());
  });

  it("cron job starts as 'delayed'", async () => {
    const q = makeQueue();
    const job = await q.add("cron", { msg: "x" }, { cron: "0 * * * *" });
    expect(job.status).toBe("delayed");
    expect(job.cron).toBe("0 * * * *");
    await q.remove(job.id); // cancel croner handle
  });

  it("accepts a custom jobId", async () => {
    const q = makeQueue();
    const job = await q.add("custom", { msg: "x" }, { jobId: "my-id" });
    expect(job.id).toBe("my-id");
  });

  it("sets priority from opts", async () => {
    const q = makeQueue();
    const job = await q.add("prio", { msg: "x" }, { priority: 7 });
    expect(job.priority).toBe(7);
  });

  it("inherits attempts from client config", async () => {
    const q = makeQueue();
    const job = await q.add("inherit", { msg: "x" });
    expect(job.attempts).toBe(client.getConfig().attempts);
  });

  it("per-job attempts override", async () => {
    const q = makeQueue();
    const job = await q.add("override", { msg: "x" }, { attempts: 7 });
    expect(job.attempts).toBe(7);
  });

  it("sets createdAt / updatedAt close to now", async () => {
    const before = Date.now();
    const q = makeQueue();
    const job = await q.add("time", { msg: "x" });
    expect(job.createdAt).toBeGreaterThanOrEqual(before);
    expect(job.updatedAt).toBeGreaterThanOrEqual(before);
  });
});

// ─── get ──────────────────────────────────────────────────────────────────────

describe("Queue — get()", () => {
  it("retrieves an existing job", async () => {
    const q = makeQueue();
    const job = await q.add("fetch", { msg: "hi" });
    const got = await q.get(job.id);
    expect(got?.id).toBe(job.id);
    expect(got?.name).toBe("fetch");
  });

  it("returns undefined for unknown id", async () => {
    const q = makeQueue();
    expect(await q.get("ghost")).toBeUndefined();
  });
});

// ─── remove ───────────────────────────────────────────────────────────────────

describe("Queue — remove()", () => {
  it("removes a job so get() returns undefined", async () => {
    const q = makeQueue();
    const job = await q.add("rm", { msg: "x" });
    await q.remove(job.id);
    expect(await q.get(job.id)).toBeUndefined();
  });

  it("removing a non-existent id does not throw", async () => {
    const q = makeQueue();
    await expect(q.remove("ghost")).resolves.toBeUndefined();
  });
});

// ─── list ─────────────────────────────────────────────────────────────────────

describe("Queue — list()", () => {
  it("returns all jobs with no filter", async () => {
    const q = makeQueue();
    await q.add("a", { msg: "1" });
    await q.add("b", { msg: "2" });
    await q.add("c", { msg: "3" }, { delay: 5000 });
    expect(await q.list()).toHaveLength(3);
  });

  it("filters by status", async () => {
    const q = makeQueue();
    await q.add("w1", { msg: "1" });
    await q.add("w2", { msg: "2" });
    const cron = await q.add("cr", { msg: "3" }, { cron: "0 * * * *" });

    const waiting = await q.list("waiting");
    expect(waiting).toHaveLength(2);
    expect(waiting.every((j) => j.status === "waiting")).toBe(true);

    const delayed = await q.list("delayed");
    expect(delayed).toHaveLength(1);

    await q.remove(cron.id);
  });

  it("sorts by priority ASC then createdAt ASC", async () => {
    const q = makeQueue();
    await q.add("low", { msg: "l" }, { priority: 10 });
    await q.add("high", { msg: "h" }, { priority: 1 });
    await q.add("mid", { msg: "m" }, { priority: 5 });

    const jobs = await q.list();
    expect(jobs[0]!.name).toBe("high");
    expect(jobs[1]!.name).toBe("mid");
    expect(jobs[2]!.name).toBe("low");
  });

  it("returns empty array for empty queue", async () => {
    const q = makeQueue("empty");
    expect(await q.list()).toEqual([]);
  });
});

// ─── clear / count ────────────────────────────────────────────────────────────

describe("Queue — clear() / count()", () => {
  it("count() reflects the number of jobs", async () => {
    const q = makeQueue();
    expect(await q.count()).toBe(0);
    await q.add("a", { msg: "1" });
    await q.add("b", { msg: "2" });
    expect(await q.count()).toBe(2);
  });

  it("clear() removes all jobs", async () => {
    const q = makeQueue();
    await q.add("a", { msg: "1" });
    await q.add("b", { msg: "2" });
    await q.clear();
    expect(await q.list()).toHaveLength(0);
    expect(await q.count()).toBe(0);
  });
});
