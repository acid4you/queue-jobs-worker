# queue-jobs-worker

Reliable background job queue and worker system for Node.js.

Supports **in-memory**, **Redis**, **PostgreSQL**, and **MySQL**. Pick the storage that fits your project — the API stays identical regardless of which backend you choose.

```
npm install queue-jobs-worker
```

---

## Core concept

The library is built around a clean separation of responsibilities:

```
QueueClient  →  opens and holds the storage connection
Queue        →  adds, inspects, and removes jobs  (producer)
Worker       →  picks up and processes jobs        (consumer)
```

`Queue` and `Worker` are completely independent. Any part of your application can hold a `Queue` reference and add jobs. Only the service that processes jobs needs a `Worker`.

```ts
import { QueueClient, Queue, Worker } from "queue-jobs-worker";

// 1. Configure storage — once at startup
const client = new QueueClient({ dialect: "memory" });
await client.init();

// 2. Producer — add jobs from anywhere
const queue = new Queue("emails", client);
await queue.add("welcome", { to: "alice@example.com" });

// 3. Consumer — process jobs in one place
const worker = new Worker(queue, async (job) => {
  await sendEmail(job.data.to);
});
worker.start();

// 4. Shutdown
await worker.close();
await client.close();
```

---

## Contents

- [Storage backends](#storage-backends)
- [QueueClient](#queueclient)
- [Queue](#queue)
- [Worker](#worker)
- [Job options](#job-options)
- [Delayed jobs](#delayed-jobs)
- [Scheduled (cron) jobs](#scheduled-cron-jobs)
- [Retries & backoff](#retries--backoff)
- [Concurrency](#concurrency)
- [Priority](#priority)
- [Events](#events)
- [Express integration](#express-integration)
- [NestJS integration](#nestjs-integration)
- [Custom storage backend](#custom-storage-backend)
- [TypeScript](#typescript)
- [API reference](#api-reference)
- [Job lifecycle](#job-lifecycle)

---

## Storage backends

### In-memory (no extra dependencies)

```ts
const client = new QueueClient({ dialect: "memory" });
await client.init();
```

Data lives in the Node.js process. Suitable for local development, testing, and single-process apps.

### Redis

```
npm install redis
```

```ts
const client = new QueueClient({
  dialect: "redis",
  connectionString: "redis://localhost:6379",
});
await client.init();
```

The Redis backend uses Lua scripts loaded on connect (`SCRIPT LOAD` / `EVALSHA`) for atomic job claiming and status updates — no double-pickup under concurrency.

### PostgreSQL

```
npm install pg
```

```ts
const client = new QueueClient({
  dialect: "postgres",
  connectionString: "postgresql://user:pass@localhost:5432/mydb",
});
await client.init();
```

The required table (`qjw_jobs`) is created automatically on the first `init()` call.

### MySQL / MariaDB

```
npm install mysql2
```

```ts
const client = new QueueClient({
  dialect: "mysql",
  connectionString: "mysql://user:pass@localhost:3306/mydb",
});
await client.init();
```

The required table (`qjw_jobs`) is created automatically on the first `init()` call.

---

## QueueClient

`QueueClient` holds the storage connection and the global job execution defaults that every `Queue` and `Worker` inherits.

```ts
const client = new QueueClient({
  dialect: "memory", // "memory" | "redis" | "postgres" | "mysql"
  connectionString: "…", // required for redis / postgres / mysql
  debug: true, // log internal operations to console
  options: {
    attempts: 3, // max retry attempts per job (0 = unlimited)
    retryDelay: 1000, // base delay in ms between retries
    backoff: "exponential", // "fixed" | "linear" | "exponential"
    timeout: 30_000, // ms a job may run before it is killed
  },
});

await client.init(); // call once at app startup
await client.close(); // call on shutdown — waits for storage to disconnect
```

### Default client

The first `init()` call registers this client as the **process-wide default**. Any `Queue` or `Worker` created without an explicit client argument picks it up automatically:

```ts
await new QueueClient({ dialect: "memory" }).init();

// No client argument needed — uses the default.
const queue = new Queue("emails");
```

---

## Queue

`Queue` is the **producer**. It manages a named collection of jobs in storage. It has no polling loop and no execution logic — that is the Worker's job.

```ts
const queue = new Queue("emails", client, {
  defaultJobOpts: {
    attempts: 5,
    removeOnComplete: true,
  },
});
```

### Add a job

```ts
const job = await queue.add("welcome", { to: "alice@example.com" });
```

### Get a job by ID

```ts
const job = await queue.get(jobId); // Job | undefined
```

### Remove a job

```ts
await queue.remove(jobId);
```

### List jobs

```ts
const all = await queue.list();
const waiting = await queue.list("waiting");
const active = await queue.list("active");
const completed = await queue.list("completed");
const failed = await queue.list("failed");
const retrying = await queue.list("retrying");
```

Results are sorted by priority (ascending) then `createdAt` (ascending).

### Clear all jobs

```ts
await queue.clear();
```

### Count jobs

```ts
const n = await queue.count();
```

---

## Worker

`Worker` is the **consumer**. It takes a `Queue` and a handler function, polls for eligible jobs, and manages the full job lifecycle.

```ts
const worker = new Worker(
  queue, // Queue to consume from
  async (job) => {
    // handler — throw to fail, return to complete
    await processJob(job.data);
    return "done"; // stored in job.result on success
  },
  {
    concurrency: 3, // max parallel jobs (default: 1)
    pollInterval: 500, // ms between polls when idle (default: 500)
  },
);
```

### Start and stop

```ts
worker.start(); // begin polling — non-blocking
await worker.close(); // graceful shutdown — waits for in-flight jobs
```

`close()` guarantees that any job currently being processed will finish before the worker stops. Safe to call before `client.close()`.

Calling `start()` after `close()` throws — create a new `Worker` instance instead.

### Shutting down a queue and all its workers at once

`queue.close()` is the recommended shutdown pattern. It automatically stops every `Worker` that was created from this queue, waits for in-flight jobs to drain, then cancels all cron schedules:

```ts
// Instead of closing each worker individually:
const queue = new Queue("emails", client);
const w1 = new Worker(queue, handler, { concurrency: 2 });
const w2 = new Worker(queue, handler, { concurrency: 2 });
w1.start();
w2.start();

// One call closes everything tied to this queue:
await queue.close();
await client.close();
```

`queue.close()` is idempotent — calling it more than once is safe.

### Worker does not manage jobs

All job management methods (`add`, `get`, `remove`, `list`, `count`) live on `Queue`. The Worker's only responsibility is execution.

```ts
// Add from the queue (producer side)
const job = await queue.add("task", { payload: "…" });

// Inspect from the queue (anywhere in your app)
const found = await queue.get(job.id);
const jobs = await queue.list("completed");
const n = await queue.count();

// Remove from the queue
await queue.remove(job.id);
```

---

## Job options

Pass options as the third argument to `queue.add()`:

```ts
await queue.add("task", data, {
  attempts: 5, // max retry attempts for this job
  delay: 5_000, // run 5 s from now
  priority: 1, // lower = runs first (default: 0)
  jobId: "my-id", // custom ID — auto UUID if omitted
  cron: "0 9 * * *", // run every day at 09:00 (croner syntax)
  removeOnComplete: true, // delete from storage after success
  removeOnFail: true, // delete from storage after permanent failure
});
```

---

## Delayed jobs

```ts
// Run in 30 seconds
await queue.add("reminder", { userId: 42 }, { delay: 30_000 });

// Run in 1 hour
await queue.add("follow-up", { orderId: "x" }, { delay: 60 * 60 * 1_000 });
```

A delayed job has `status: "waiting"` immediately. The Worker checks `runAt <= Date.now()` before picking it up — no separate scheduler required.

---

## Scheduled (cron) jobs

Pass any [croner](https://github.com/hexagon/croner)-compatible cron expression:

```ts
// Every day at 9 AM
await queue.add("daily-report", {}, { cron: "0 9 * * *" });

// Every hour
await queue.add("hourly-sync", {}, { cron: "0 * * * *" });

// Every 5 minutes
await queue.add("health-check", {}, { cron: "*/5 * * * *" });
```

The job starts as `"delayed"`. After each cron tick it is reset to `"waiting"` and the Worker picks it up like any other job. Remove the job to cancel the schedule:

```ts
await queue.remove(cronJobId);
```

---

## Retries & backoff

Configure globally on the client or override per job:

```ts
// Client-level defaults
const client = new QueueClient({
  dialect: "memory",
  options: {
    attempts: 5,
    retryDelay: 1_000,
    backoff: "exponential",
  },
});

// Per-job override
await queue.add("risky", data, { attempts: 10 });
```

### Backoff strategies

| Strategy      | Formula                | Example (base = 1 s)  |
| ------------- | ---------------------- | --------------------- |
| `fixed`       | `base`                 | 1 s, 1 s, 1 s, …      |
| `linear`      | `base × attempt`       | 1 s, 2 s, 3 s, …      |
| `exponential` | `base × 2^(attempt−1)` | 1 s, 2 s, 4 s, 8 s, … |

Exponential strategy is capped at **30 minutes**.

Set `attempts: 0` for unlimited retries.

---

## Concurrency

Concurrency is a **Worker** option — not a Queue option. The Queue itself is storage-only.

```ts
const worker = new Worker(queue, handler, { concurrency: 5 });
```

This means you can also scale by running multiple Workers against the same Queue:

```ts
const w1 = new Worker(queue, handler, { concurrency: 3 });
const w2 = new Worker(queue, handler, { concurrency: 3 });
w1.start();
w2.start();
```

The Redis backend uses Lua scripts to ensure atomic job claiming — two workers will never pick up the same job.

---

## Priority

Lower number = higher priority (default: `0`).

```ts
await queue.add("urgent", data, { priority: 1 });
await queue.add("normal", data, { priority: 5 });
await queue.add("bulk", data, { priority: 10 });
```

Jobs with equal priority are processed in FIFO order.

---

## Events

`Worker` extends `EventEmitter` with typed events:

```ts
worker.on("active", (job) => console.log("processing", job.id));
worker.on("completed", (job, result) => console.log("done", job.id, result));
worker.on("error", (job, err) => console.warn("attempt failed", err.message));
worker.on("failed", (job, err) => console.error("permanent fail", job.id));
worker.on("started", () => console.log("worker polling"));
worker.on("stopped", () => console.log("worker stopped"));
```

| Event       | Arguments     | When                                 |
| ----------- | ------------- | ------------------------------------ |
| `active`    | `job`         | Job picked up, processing started    |
| `completed` | `job, result` | Job finished successfully            |
| `error`     | `job, error`  | One attempt failed (may still retry) |
| `failed`    | `job, error`  | All attempts exhausted               |
| `started`   | —             | `worker.start()` called              |
| `stopped`   | —             | `worker.close()` resolved            |

---

## Express integration

```ts
import express from "express";
import { QueueClient, Queue, Worker } from "queue-jobs-worker";

const app = express();

// ── Startup ───────────────────────────────────────────────────────────────────
const client = new QueueClient({
  dialect: "redis",
  connectionString: process.env.REDIS_URL,
});
await client.init();

// Producer — anyone can import emailQueue and call add()
const emailQueue = new Queue<{ to: string; subject: string }>("emails", client);

// Consumer — only this module cares about the worker
const emailWorker = new Worker(
  emailQueue,
  async (job) => {
    await mailer.send(job.data);
    return "sent";
  },
  { concurrency: 5 },
);

emailWorker.on("failed", (job, err) => {
  console.error(`Job ${job.id} permanently failed:`, err.message);
});
emailWorker.start();

// ── Route ─────────────────────────────────────────────────────────────────────
app.post("/register", async (req, res) => {
  await emailQueue.add("welcome", { to: req.body.email, subject: "Welcome!" });
  res.status(202).json({ message: "accepted" });
});

// ── Shutdown ──────────────────────────────────────────────────────────────────
process.on("SIGTERM", async () => {
  await emailWorker.close(); // drain in-flight jobs
  await client.close(); // then close storage
  process.exit(0);
});

app.listen(3000);
```

---

## NestJS integration

```ts
// queue.module.ts
import { Module } from "@nestjs/common";
import { QueueService } from "./queue.service.js";

@Module({ providers: [QueueService], exports: [QueueService] })
export class QueueModule {}

// queue.service.ts
import { Injectable, OnModuleInit, OnModuleDestroy } from "@nestjs/common";
import { QueueClient, Queue, Worker } from "queue-jobs-worker";

@Injectable()
export class QueueService implements OnModuleInit, OnModuleDestroy {
  private client!: QueueClient;

  // Export the Queue so other modules can add jobs without touching the Worker.
  public emailQueue!: Queue<{ to: string }>;
  private emailWorker!: Worker<{ to: string }, string>;

  async onModuleInit() {
    this.client = new QueueClient({
      dialect: "postgres",
      connectionString: process.env.DATABASE_URL,
    });
    await this.client.init();

    this.emailQueue = new Queue("emails", this.client);
    this.emailWorker = new Worker(
      this.emailQueue,
      async (job) => {
        await mailer.send(job.data.to);
        return "sent";
      },
      { concurrency: 3 },
    );
    this.emailWorker.start();
  }

  async onModuleDestroy() {
    await this.emailWorker.close();
    await this.client.close();
  }
}

// users.controller.ts
@Controller("users")
export class UsersController {
  constructor(private readonly queue: QueueService) {}

  @Post("register")
  async register(@Body() dto: RegisterDto) {
    // Only the Queue is needed here — no Worker import required.
    await this.queue.emailQueue.add("welcome", { to: dto.email });
    return { message: "accepted" };
  }
}
```

---

## Custom storage backend

Implement `IStorage` to plug in any database or service:

```ts
import type { IStorage, Job, JobStatus } from "queue-jobs-worker";

export class MongoStorage implements IStorage {
  async connect() {
    /* open client */
  }
  async disconnect() {
    /* close client */
  }

  async saveJob(queueName, job) {
    /* upsert */
  }
  async getJob(queueName, jobId) {
    /* findOne → Job | undefined */
  }
  async updateJob(queueName, jobId, patch) {
    /* findOneAndUpdate */
  }
  async removeJob(queueName, jobId) {
    /* deleteOne */
  }

  async listJobs(queueName, status?) {
    /* find, sorted by priority+createdAt */
  }
  async getNextJob(queueName) {
    /* atomic claim: status IN (waiting,retrying) AND runAt <= now */
  }

  async clearQueue(queueName) {
    /* deleteMany */
  }
  async countJobs(queueName) {
    /* countDocuments */
  }
}
```

The `getNextJob` implementation must be **atomic** in multi-process environments — use a transaction, `findOneAndUpdate`, or a server-side script to prevent two workers claiming the same job.

---

## TypeScript

The library is written in TypeScript and ships full `.d.ts` declarations.

Type your job data and result for end-to-end safety:

```ts
interface EmailData {
  to: string;
  subject: string;
  body: string;
}
interface EmailResult {
  messageId: string;
}

const queue = new Queue<EmailData, EmailResult>("emails", client);
const worker = new Worker<EmailData, EmailResult>(queue, async (job) => {
  const id = await mailer.send(job.data); // job.data → EmailData
  return { messageId: id }; // return type checked as EmailResult
});

worker.on("completed", (job, result) => {
  console.log(result.messageId); // result → EmailResult ✓
});
```

---

## API reference

### `QueueClient`

| Method                             | Returns                    | Description                         |
| ---------------------------------- | -------------------------- | ----------------------------------- |
| `new QueueClient(opts)`            | —                          | Create a client                     |
| `init()`                           | `Promise<this>`            | Open storage, register as default   |
| `close()`                          | `Promise<void>`            | Close storage (idempotent)          |
| `isInitialized()`                  | `boolean`                  | True between `init()` and `close()` |
| `isClosed()`                       | `boolean`                  | True after `close()`                |
| `getConfig()`                      | `QueueClientConfigOptions` | Merged global defaults              |
| `getDialect()`                     | `StorageDialect`           | Configured dialect                  |
| `getStorage()`                     | `IStorage`                 | Underlying storage instance         |
| `QueueClient.getDefaultClient()`   | `QueueClient \| undefined` | Process-wide default                |
| `QueueClient.setDefaultClient(c)`  | `void`                     | Override the default                |
| `QueueClient.clearDefaultClient()` | `void`                     | Clear the default                   |

### `Queue`

| Method                            | Returns                     | Description                       |
| --------------------------------- | --------------------------- | --------------------------------- |
| `new Queue(name, client?, opts?)` | —                           | Create a queue                    |
| `add(name, data, opts?)`          | `Promise<Job>`              | Persist a new job                 |
| `get(jobId)`                      | `Promise<Job \| undefined>` | Fetch a job by ID                 |
| `remove(jobId)`                   | `Promise<void>`             | Delete a job                      |
| `list(status?)`                   | `Promise<Job[]>`            | List jobs, optional status filter |
| `clear()`                         | `Promise<void>`             | Remove all jobs                   |
| `count()`                         | `Promise<number>`           | Total job count                   |
| `close()`                         | `Promise<void>`             | Close all workers + cron (idempotent) |

### `Worker`

| Method                              | Returns         | Description                    |
| ----------------------------------- | --------------- | ------------------------------ |
| `new Worker(queue, handler, opts?)` | —               | Create a worker                |
| `start()`                           | `this`          | Start polling                  |
| `close()`                           | `Promise<void>` | Graceful shutdown              |
| `isRunning()`                       | `boolean`       | True while polling             |
| `isClosed()`                        | `boolean`       | True after close()             |
| `on(event, fn)`                     | `this`          | Subscribe to a lifecycle event |

### `Job`

| Field          | Type        | Description                     |
| -------------- | ----------- | ------------------------------- |
| `id`           | `string`    | UUID v4                         |
| `name`         | `string`    | Job type name                   |
| `data`         | `TData`     | Payload                         |
| `status`       | `JobStatus` | Current lifecycle state         |
| `attempts`     | `number`    | Max attempts allowed            |
| `attemptsMade` | `number`    | Attempts made so far            |
| `delay`        | `number`    | Initial delay in ms             |
| `runAt`        | `number`    | Timestamp when eligible to run  |
| `priority`     | `number`    | Scheduling priority             |
| `cron`         | `string?`   | Cron expression                 |
| `result`       | `TResult?`  | Handler return value on success |
| `error`        | `string?`   | Last error message              |
| `stacktrace`   | `string?`   | Last error stack trace          |
| `createdAt`    | `number`    | Unix ms — created               |
| `updatedAt`    | `number`    | Unix ms — last status change    |
| `processedAt`  | `number?`   | Unix ms — processing started    |
| `finishedAt`   | `number?`   | Unix ms — completed or failed   |

### `JobStatus`

```
"waiting"   — eligible to be picked up (runAt <= now)
"delayed"   — cron job waiting for its first tick
"active"    — currently being processed by a Worker
"completed" — handler returned successfully
"failed"    — all attempts exhausted
"retrying"  — last attempt failed; waiting for retry delay
```

---

## Job lifecycle

```
queue.add()
     │
     ▼
 "waiting" ──────────────────────────────────────────────────────────┐
     │  Worker picks up (runAt <= now)                               │
     ▼                                                               │
 "active"                                                            │
     │                                                               │
     ├─ handler returns ──► "completed"                              │
     │                                                               │
     └─ handler throws                                               │
           │                                                         │
           ├─ attempts remaining ──► "retrying" ──(delay)──► ───────┘
           │
           └─ no attempts left ──► "failed"


queue.add({ cron: "…" })
     │
     ▼
 "delayed"
     │  croner tick fires
     ▼
 "waiting" ──► (same flow above) ──► "completed"
                                          │
                      next tick resets ───┘
```

---

## Peer dependencies

| Package  | Version   | Dialect      |
| -------- | --------- | ------------ |
| `redis`  | `>=4.0.0` | `"redis"`    |
| `pg`     | `>=8.0.0` | `"postgres"` |
| `mysql2` | `>=3.0.0` | `"mysql"`    |

Install only the package you need. `croner` is a direct dependency — it is always installed automatically.

---

## License

MIT © Rafid Ahmed
