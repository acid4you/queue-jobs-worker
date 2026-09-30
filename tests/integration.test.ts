/**
 * Integration tests — full vertical slice through QueueClient → Queue → Worker.
 * Mirrors how real Express and NestJS apps would use the library.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { QueueClient } from "../src/classes/client.js";
import { Queue } from "../src/classes/queue.js";
import { Worker } from "../src/classes/worker.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 8_000,
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

let client: QueueClient;

beforeEach(async () => {
  QueueClient.clearDefaultClient();
  client = new QueueClient({
    dialect: "memory",
    options: { retryDelay: 50, timeout: 3000 },
  });
  await client.init();
});

afterEach(async () => {
  if (client.isInitialized()) await client.close();
  QueueClient.clearDefaultClient();
});

// ─── Queue and Worker are clearly separated ───────────────────────────────────

describe("Integration — separation of concerns", () => {
  it("Queue is the producer, Worker is the consumer — they live independently", async () => {
    // Producer side — anyone in the app can hold a Queue reference and add jobs.
    const queue = new Queue<{ email: string }>("mailer", client);

    const results: string[] = [];

    // Consumer side — only the worker service holds this.
    const worker = new Worker(queue, async (job) => {
      results.push(job.data.email);
      return "sent";
    });

    // Jobs can be enqueued BEFORE the worker starts.
    const j1 = await queue.add("welcome", { email: "alice@example.com" });
    const j2 = await queue.add("welcome", { email: "bob@example.com" });

    worker.start();

    await waitFor(async () => {
      return (
        (await queue.get(j1.id))?.status === "completed" &&
        (await queue.get(j2.id))?.status === "completed"
      );
    });

    await worker.close();

    expect(results).toContain("alice@example.com");
    expect(results).toContain("bob@example.com");
  });

  it("multiple workers can consume from the same queue concurrently", async () => {
    const queue = new Queue("shared", client);
    let total   = 0;

    const w1 = new Worker(queue, async () => { await sleep(20); total++; return "w1"; });
    const w2 = new Worker(queue, async () => { await sleep(20); total++; return "w2"; });

    await Promise.all(
      Array.from({ length: 4 }, (_, i) => queue.add(`job-${i}`, {})),
    );

    w1.start();
    w2.start();

    await waitFor(async () => total >= 4, 10_000);

    await w1.close();
    await w2.close();

    expect(total).toBe(4);
  });
});

// ─── Express-style bootstrap ──────────────────────────────────────────────────

describe("Integration — Express-style bootstrap", () => {
  it("init → add → process → close full lifecycle", async () => {
    const results: string[] = [];
    const queue  = new Queue<{ email: string }>("express-mailer", client);
    const worker = new Worker(queue, async (job) => {
      results.push(job.data.email);
      return "sent";
    });

    worker.start();

    const j1 = await queue.add("send", { email: "alice@example.com" });
    const j2 = await queue.add("send", { email: "bob@example.com" });

    await waitFor(async () => {
      return (
        (await queue.get(j1.id))?.status === "completed" &&
        (await queue.get(j2.id))?.status === "completed"
      );
    });

    // SIGTERM pattern: stop worker first, then close client.
    await worker.close();
    await client.close();

    expect(results).toContain("alice@example.com");
    expect(results).toContain("bob@example.com");
    expect(client.isClosed()).toBe(true);
  });
});

// ─── NestJS-style onModuleInit / onModuleDestroy ──────────────────────────────

describe("Integration — NestJS module lifecycle", () => {
  it("simulates onModuleInit and onModuleDestroy", async () => {
    class EmailService {
      private nestClient!: QueueClient;
      public queue!: Queue<{ to: string }>;
      private worker!: Worker<{ to: string }, string>;
      public processed: string[] = [];

      async onModuleInit() {
        this.nestClient = new QueueClient({ dialect: "memory" });
        await this.nestClient.init();
        this.queue  = new Queue("nest-emails", this.nestClient);
        this.worker = new Worker(this.queue, async (job) => {
          this.processed.push(job.data.to);
          return "delivered";
        });
        this.worker.start();
      }

      async onModuleDestroy() {
        await this.worker.close();
        await this.nestClient.close();
      }
    }

    const svc = new EmailService();
    await svc.onModuleInit();

    const j = await svc.queue.add("send", { to: "test@example.com" });
    await waitFor(async () => (await svc.queue.get(j.id))?.status === "completed");

    await svc.onModuleDestroy();

    expect(svc.processed).toContain("test@example.com");
  });
});

// ─── implicit default client ──────────────────────────────────────────────────

describe("Integration — implicit default client", () => {
  it("Queue uses the default client automatically", async () => {
    // client was init'd in beforeEach → it is the default.
    const queue  = new Queue("implicit"); // no client arg
    const worker = new Worker(queue, async (job) => `done:${job.name}`);
    worker.start();

    const job = await queue.add("task", {});
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    expect((await queue.get(job.id))?.result).toBe("done:task");
    await worker.close();
  });
});

// ─── multiple queues on the same client ──────────────────────────────────────

describe("Integration — multiple queues", () => {
  it("two independent queues process separately", async () => {
    const emailResults:  string[] = [];
    const reportResults: number[] = [];

    const emailQueue  = new Queue<{ to: string }>("emails",  client);
    const reportQueue = new Queue<{ id: number }>("reports", client);

    const emailWorker  = new Worker(emailQueue,  async (j) => { emailResults.push(j.data.to); return "sent"; });
    const reportWorker = new Worker(reportQueue, async (j) => { reportResults.push(j.data.id); return "done"; });

    emailWorker.start();
    reportWorker.start();

    const e1 = await emailQueue.add("send",  { to: "alice@example.com" });
    const e2 = await emailQueue.add("send",  { to: "bob@example.com"   });
    const r1 = await reportQueue.add("build", { id: 99 });

    await waitFor(async () => {
      return (
        (await emailQueue.get(e1.id))?.status  === "completed" &&
        (await emailQueue.get(e2.id))?.status  === "completed" &&
        (await reportQueue.get(r1.id))?.status === "completed"
      );
    });

    await emailWorker.close();
    await reportWorker.close();

    expect(emailResults).toHaveLength(2);
    expect(reportResults).toContain(99);
  });
});

// ─── event observability ──────────────────────────────────────────────────────

describe("Integration — event observability", () => {
  it("collects completed and failed events across mixed jobs", async () => {
    const completedIds: string[] = [];
    const failedIds:    string[] = [];

    const queue  = new Queue<{ fail: boolean }>("observable", client);
    const worker = new Worker(queue, async (job) => {
      if (job.data.fail) throw new Error("intentional");
      return "ok";
    });

    worker.on("completed", (job) => completedIds.push(job.id));
    worker.on("failed",    (job) => failedIds.push(job.id));
    worker.start();

    const ok1 = await queue.add("ok-1",  { fail: false });
    const ok2 = await queue.add("ok-2",  { fail: false });
    const bad = await queue.add("bad-1", { fail: true  }, { attempts: 1 });

    await waitFor(async () => {
      return (
        (await queue.get(ok1.id))?.status === "completed" &&
        (await queue.get(ok2.id))?.status === "completed" &&
        (await queue.get(bad.id))?.status === "failed"
      );
    });

    await worker.close();

    expect(completedIds).toContain(ok1.id);
    expect(completedIds).toContain(ok2.id);
    expect(failedIds).toContain(bad.id);
  });
});
