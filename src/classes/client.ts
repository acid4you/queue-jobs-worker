import { resolveStorage } from "../utils/resolve-storage.js";
import { DEFAULT_QUEUE_CONFIG } from "../constants/client.constants.js";
import type { IStorage } from "../storage/storage.interface.js";
import type {
  QueueClientOptions,
  QueueClientConfigOptions,
  StorageDialect,
} from "../types/client.types.js";

/**
 * QueueClient — the entry point for the entire queue system.
 *
 * Every Queue and Worker you create is tied to a QueueClient instance.
 * One client holds one storage connection and one set of global defaults.
 *
 * Typical usage
 * ─────────────
 * // 1. Create and initialise the client (do this once at app startup)
 * const client = new QueueClient({ dialect: "memory" });
 * await client.init();
 *
 * // 2. Create queues and workers … (see Queue / Worker docs)
 *
 * // 3. Gracefully shut everything down (call on app exit / SIGTERM)
 * await client.close();
 *
 * Singleton default client
 * ────────────────────────
 * The first client you call .init() on is automatically stored as the
 * process-wide default.  Any Queue or Worker constructed without an explicit
 * client argument will pick it up automatically.
 *
 * You can also set the default explicitly:
 *   QueueClient.setDefaultClient(client);
 */
export class QueueClient {
  // ── Singleton default ───────────────────────────────────────────────────
  private static _default: QueueClient | undefined;

  // ── Instance state ───────────────────────────────────────────────────────
  private readonly _dialect: StorageDialect;
  private readonly _connectionString?: string;
  private readonly _debug: boolean;
  private readonly _config: Required<QueueClientConfigOptions>;

  private _storage: IStorage | null = null;
  private _initialized = false;
  private _closed = false;

  constructor(opts: QueueClientOptions) {
    this._dialect = opts.dialect;
    this._debug = opts.debug ?? false;
    this._config = { ...DEFAULT_QUEUE_CONFIG, ...opts.options };
    // exactOptionalPropertyTypes: only assign the optional field when defined.
    if (opts.connectionString !== undefined) {
      this._connectionString = opts.connectionString;
    }
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  /**
   * Open the storage connection and mark this client as ready.
   *
   * Must be called before creating any Queue or Worker.
   * Calling init() more than once throws to prevent accidental double-init.
   *
   * @returns `this` so you can chain:  await new QueueClient(…).init()
   */
  public async init(): Promise<this> {
    if (this._initialized) {
      throw new Error("[queue-jobs-worker] QueueClient is already initialized.");
    }
    if (this._closed) {
      throw new Error(
        "[queue-jobs-worker] QueueClient has been closed and cannot be re-initialized. " +
          "Create a new instance instead.",
      );
    }

    this._storage = resolveStorage(this._dialect, this._connectionString);
    await this._storage.connect();
    this._initialized = true;

    // Auto-register as the process-wide default if none is set yet.
    if (!QueueClient._default) {
      QueueClient._default = this;
    }

    this._log(`QueueClient initialized (dialect: ${this._dialect})`);
    return this;
  }

  /**
   * Close the storage connection and release all resources.
   *
   * Call this on application shutdown (e.g. SIGTERM handler, NestJS onModuleDestroy,
   * Express app.close()).  Any in-flight worker processing will finish its current
   * job before the worker's own close() stops the polling loop.
   */
  public async close(): Promise<void> {
    if (this._closed) return; // idempotent — second call is a no-op
    if (!this._initialized) {
      throw new Error("[queue-jobs-worker] Cannot close a QueueClient that was never initialized.");
    }

    await this._storage!.disconnect();
    this._closed = true;
    this._initialized = false;

    // Remove from the default slot if this was the default.
    if (QueueClient._default === this) {
      QueueClient._default = undefined;
    }

    this._log("QueueClient closed.");
  }

  // ── Static helpers ────────────────────────────────────────────────────────

  /** Returns the process-wide default client, or undefined if none is set. */
  public static getDefaultClient(): QueueClient | undefined {
    return QueueClient._default;
  }

  /**
   * Explicitly set (or replace) the process-wide default client.
   * Useful in tests or multi-client setups.
   */
  public static setDefaultClient(client: QueueClient): void {
    QueueClient._default = client;
  }

  /** Clear the default client reference (useful between tests). */
  public static clearDefaultClient(): void {
    QueueClient._default = undefined;
  }

  // ── Accessors (read-only to the outside world) ───────────────────────────

  /** True after init() succeeds and before close() is called. */
  public isInitialized(): boolean {
    return this._initialized;
  }

  /** True after close() has been called. */
  public isClosed(): boolean {
    return this._closed;
  }

  /** The merged global job execution defaults. */
  public getConfig(): Required<QueueClientConfigOptions> {
    return { ...this._config };
  }

  /** The storage dialect this client was created with. */
  public getDialect(): StorageDialect {
    return this._dialect;
  }

  /** The connection string (may be undefined for the memory dialect). */
  public getConnectionString(): string | undefined {
    return this._connectionString;
  }

  /** Whether debug logging is enabled. */
  public isDebugMode(): boolean {
    return this._debug;
  }

  /**
   * The underlying IStorage instance.
   *
   * Exposed so Queue / Worker can call storage methods directly.
   * Throws if the client has not been initialized yet.
   *
   * @internal
   */
  public getStorage(): IStorage {
    if (!this._storage || !this._initialized) {
      throw new Error(
        "[queue-jobs-worker] QueueClient must be initialized before accessing storage. " +
          "Did you forget to call await client.init()?",
      );
    }
    return this._storage;
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  /**
   * Write a debug-mode log line.
   *
   * FIX: the old signature accepted `...args: unknown[]` which let callers
   * accidentally spread raw objects into the console output, potentially
   * leaking internal state (job payloads, connection strings, stack traces)
   * into log aggregators.  The new signature accepts only a pre-formatted
   * string so callers must stringify anything sensitive before passing it.
   *
   * @internal
   */
  public _log(message: string): void {
    if (this._debug) {
      console.log(`[queue-jobs-worker] ${message}`);
    }
  }
}
