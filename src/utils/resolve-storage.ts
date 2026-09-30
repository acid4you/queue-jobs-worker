import type { IStorage } from "../storage/storage.interface.js";
import type { StorageDialect } from "../types/client.types.js";
import { MemoryStorage } from "../storage/memory.storage.js";
import { RedisStorage } from "../storage/redis.storage.js";
import { PostgresStorage } from "../storage/postgres.storage.js";
import { MysqlStorage } from "../storage/mysql.storage.js";

/**
 * Factory that maps a dialect string to the correct IStorage implementation.
 *
 * Called once inside QueueClient.init() — the result is stored on the client
 * and shared across every Queue/Worker that belongs to it.
 *
 * @param dialect          - One of "memory" | "redis" | "postgres" | "mysql".
 * @param connectionString - Required for every dialect except "memory".
 *                           Pass undefined for in-memory usage.
 * @throws If a non-memory dialect is requested without a connectionString.
 * @throws If an unknown dialect is provided.
 */
export function resolveStorage(
  dialect: StorageDialect,
  connectionString?: string,
): IStorage {
  switch (dialect) {
    case "memory":
      return new MemoryStorage();

    case "redis":
      assertConnectionString(dialect, connectionString);
      return new RedisStorage(connectionString);

    case "postgres":
      assertConnectionString(dialect, connectionString);
      return new PostgresStorage(connectionString);

    case "mysql":
      assertConnectionString(dialect, connectionString);
      return new MysqlStorage(connectionString);

    default: {
      // TypeScript exhaustiveness check — this branch is unreachable at
      // runtime if the type system is respected, but gives a clear message
      // if someone passes an invalid string from plain JS.
      const _unreachable: never = dialect;
      throw new Error(
        `[queue-jobs-worker] Unknown storage dialect: "${String(_unreachable)}". ` +
        `Valid options are: "memory", "redis", "postgres", "mysql".`,
      );
    }
  }
}

function assertConnectionString(
  dialect: StorageDialect,
  connectionString: string | undefined,
): asserts connectionString is string {
  if (!connectionString) {
    throw new Error(
      `[queue-jobs-worker] Dialect "${dialect}" requires a connectionString. ` +
      `Example: { dialect: "${dialect}", connectionString: "${exampleDsn(dialect)}" }`,
    );
  }
}

function exampleDsn(dialect: StorageDialect): string {
  switch (dialect) {
    case "redis":    return "redis://localhost:6379";
    case "postgres": return "postgresql://user:pass@localhost:5432/mydb";
    case "mysql":    return "mysql://user:pass@localhost:3306/mydb";
    default:         return "";
  }
}
