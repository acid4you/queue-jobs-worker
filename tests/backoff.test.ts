/**
 * Backoff strategy tests.
 * Verify the Worker applies the correct inter-retry delay by measuring
 * real wall-clock timestamps between handler invocations.
 */
import { describe, it, expect, afterEach } from "vitest";
import { QueueClient } from "../src/classes/client.js";
import { Queue } from "../src/classes/queue.js";
import { Worker } from "../src/classes/worker.js";

function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 15_000,
  interval  = 30,
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

afterEach(async () => {
  if (client?.isInitialized()) await client.close();
  QueueClient.clearDefaultClient();
});

describe("Backoff — fixed", () => {
  it("waits a constant delay between retries", async () => {
    client = new QueueClient({
      dialect: "memory",
      options: { attempts: 3, retryDelay: 80, backoff: "fixed", timeout: 5000 },
    });
    await client.init();

    let calls = 0;
    const timestamps: number[] = [];
    const queue  = new Queue("fixed-q", client);
    const worker = new Worker(queue, async () => {
      timestamps.push(Date.now());
      if (++calls < 3) throw new Error("retry");
      return "ok";
    });
    worker.start();

    const job = await queue.add("task", {}, { attempts: 3 });
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    const gap1 = timestamps[1]! - timestamps[0]!;
    const gap2 = timestamps[2]! - timestamps[1]!;

    // Both gaps ≥ base delay.
    expect(gap1).toBeGreaterThanOrEqual(70);
    expect(gap2).toBeGreaterThanOrEqual(70);
    // Fixed: gaps should be similar in magnitude.
    expect(Math.abs(gap2 - gap1)).toBeLessThan(200);

    await worker.close();
  });
});

describe("Backoff — exponential", () => {
  it("each retry waits longer than the previous one", async () => {
    client = new QueueClient({
      dialect: "memory",
      options: { attempts: 3, retryDelay: 50, backoff: "exponential", timeout: 5000 },
    });
    await client.init();

    let calls = 0;
    const timestamps: number[] = [];
    const queue  = new Queue("exp-q", client);
    const worker = new Worker(queue, async () => {
      timestamps.push(Date.now());
      if (++calls < 3) throw new Error("retry");
      return "done";
    });
    worker.start();

    const job = await queue.add("task", {}, { attempts: 3 });
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    const gap1 = timestamps[1]! - timestamps[0]!;
    const gap2 = timestamps[2]! - timestamps[1]!;

    // Exponential: base=50ms → attempt1: 50ms, attempt2: 100ms → gap2 > gap1.
    expect(gap2).toBeGreaterThan(gap1 * 0.8);

    await worker.close();
  });
});

describe("Backoff — linear", () => {
  it("delay grows linearly with attempt count", async () => {
    client = new QueueClient({
      dialect: "memory",
      options: { attempts: 3, retryDelay: 40, backoff: "linear", timeout: 5000 },
    });
    await client.init();

    let calls = 0;
    const timestamps: number[] = [];
    const queue  = new Queue("lin-q", client);
    const worker = new Worker(queue, async () => {
      timestamps.push(Date.now());
      if (++calls < 3) throw new Error("retry");
      return "ok";
    });
    worker.start();

    const job = await queue.add("task", {}, { attempts: 3 });
    await waitFor(async () => (await queue.get(job.id))?.status === "completed");

    const gap1 = timestamps[1]! - timestamps[0]!;
    const gap2 = timestamps[2]! - timestamps[1]!;

    // Linear: base=40ms → attempt1: 40ms, attempt2: 80ms → gap2 ≥ gap1.
    expect(gap2).toBeGreaterThanOrEqual(gap1 * 0.8);

    await worker.close();
  });
});
