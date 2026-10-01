import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { QueueClient } from "../src/classes/client.js";

beforeEach(() => QueueClient.clearDefaultClient());
afterEach(() => QueueClient.clearDefaultClient());

// ─── lifecycle ────────────────────────────────────────────────────────────────

describe("QueueClient — lifecycle", () => {
  it("starts uninitialized", () => {
    const c = new QueueClient({ dialect: "memory" });
    expect(c.isInitialized()).toBe(false);
    expect(c.isClosed()).toBe(false);
  });

  it("init() marks it initialized and returns this", async () => {
    const c = new QueueClient({ dialect: "memory" });
    const ret = await c.init();
    expect(ret).toBe(c);
    expect(c.isInitialized()).toBe(true);
    await c.close();
  });

  it("init() throws on second call", async () => {
    const c = new QueueClient({ dialect: "memory" });
    await c.init();
    await expect(c.init()).rejects.toThrow("already initialized");
    await c.close();
  });

  it("close() marks it closed", async () => {
    const c = new QueueClient({ dialect: "memory" });
    await c.init();
    await c.close();
    expect(c.isClosed()).toBe(true);
    expect(c.isInitialized()).toBe(false);
  });

  it("close() is idempotent — second call is silent", async () => {
    const c = new QueueClient({ dialect: "memory" });
    await c.init();
    await c.close();
    await expect(c.close()).resolves.toBeUndefined();
  });

  it("close() before init() throws", async () => {
    const c = new QueueClient({ dialect: "memory" });
    await expect(c.close()).rejects.toThrow("never initialized");
  });

  it("init() after close() throws", async () => {
    const c = new QueueClient({ dialect: "memory" });
    await c.init();
    await c.close();
    await expect(c.init()).rejects.toThrow("cannot be re-initialized");
  });

  it("getStorage() before init() throws", () => {
    const c = new QueueClient({ dialect: "memory" });
    expect(() => c.getStorage()).toThrow("must be initialized");
  });
});

// ─── default client ───────────────────────────────────────────────────────────

describe("QueueClient — default client", () => {
  it("first init() registers itself as default", async () => {
    expect(QueueClient.getDefaultClient()).toBeUndefined();
    const c = new QueueClient({ dialect: "memory" });
    await c.init();
    expect(QueueClient.getDefaultClient()).toBe(c);
    await c.close();
  });

  it("close() removes it from default slot", async () => {
    const c = new QueueClient({ dialect: "memory" });
    await c.init();
    await c.close();
    expect(QueueClient.getDefaultClient()).toBeUndefined();
  });

  it("second init() does not steal the default", async () => {
    const c1 = new QueueClient({ dialect: "memory" });
    const c2 = new QueueClient({ dialect: "memory" });
    await c1.init();
    await c2.init();
    expect(QueueClient.getDefaultClient()).toBe(c1);
    await c1.close();
    await c2.close();
  });

  it("setDefaultClient() overrides the slot", async () => {
    const c1 = new QueueClient({ dialect: "memory" });
    const c2 = new QueueClient({ dialect: "memory" });
    await c1.init();
    await c2.init();
    QueueClient.setDefaultClient(c2);
    expect(QueueClient.getDefaultClient()).toBe(c2);
    await c1.close();
    await c2.close();
  });
});

// ─── config ───────────────────────────────────────────────────────────────────

describe("QueueClient — config", () => {
  it("merges user options over defaults", async () => {
    const c = new QueueClient({
      dialect: "memory",
      options: { attempts: 10, retryDelay: 2000, backoff: "linear" },
    });
    await c.init();
    const cfg = c.getConfig();
    expect(cfg.attempts).toBe(10);
    expect(cfg.retryDelay).toBe(2000);
    expect(cfg.backoff).toBe("linear");
    expect(cfg.timeout).toBe(30_000); // default preserved
    await c.close();
  });

  it("exposes dialect + connectionString + debug accessors", async () => {
    const c = new QueueClient({ dialect: "memory", debug: false });
    await c.init();
    expect(c.getDialect()).toBe("memory");
    expect(c.getConnectionString()).toBeUndefined();
    expect(c.isDebugMode()).toBe(false);
    await c.close();
  });
});
