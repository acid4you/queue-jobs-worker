import { describe, it, expect } from "vitest";
import { resolveStorage } from "../src/utils/resolve-storage.js";
import { MemoryStorage } from "../src/storage/memory.storage.js";

describe("resolveStorage", () => {
  it("returns a MemoryStorage for dialect 'memory'", () => {
    const storage = resolveStorage("memory");
    expect(storage).toBeInstanceOf(MemoryStorage);
  });

  it("returns a MemoryStorage for 'memory' even if a connectionString is supplied", () => {
    const storage = resolveStorage("memory", "unused://connection");
    expect(storage).toBeInstanceOf(MemoryStorage);
  });

  it("throws if 'redis' is used without a connectionString", () => {
    expect(() => resolveStorage("redis")).toThrow("connectionString");
  });

  it("throws if 'postgres' is used without a connectionString", () => {
    expect(() => resolveStorage("postgres")).toThrow("connectionString");
  });

  it("throws if 'mysql' is used without a connectionString", () => {
    expect(() => resolveStorage("mysql")).toThrow("connectionString");
  });

  it("creates a RedisStorage instance when dialect is 'redis'", async () => {
    // We only check instantiation — we don't connect (no real Redis in unit tests).
    const { RedisStorage } = await import("../src/storage/redis.storage.js");
    const storage = resolveStorage("redis", "redis://localhost:6379");
    expect(storage).toBeInstanceOf(RedisStorage);
  });

  it("creates a PostgresStorage instance when dialect is 'postgres'", async () => {
    const { PostgresStorage } = await import("../src/storage/postgres.storage.js");
    const storage = resolveStorage("postgres", "postgresql://user:pass@localhost/db");
    expect(storage).toBeInstanceOf(PostgresStorage);
  });

  it("creates a MysqlStorage instance when dialect is 'mysql'", async () => {
    const { MysqlStorage } = await import("../src/storage/mysql.storage.js");
    const storage = resolveStorage("mysql", "mysql://user:pass@localhost/db");
    expect(storage).toBeInstanceOf(MysqlStorage);
  });

  it("throws a descriptive error for an unknown dialect", () => {
    expect(() => resolveStorage("unknown" as "memory")).toThrow("Unknown storage dialect");
  });
});
