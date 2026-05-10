# rbmq (Node.js)

**Redis Better Message Queue** — a small, atomic Redis-backed queue for Node.js. Same Lua scripts as the Rust implementation. A Rust producer and a Node consumer (or vice versa) share queues with no translation layer.

## Quick start

```sh
npm install rbmq ioredis
```

```js
import Redis from "ioredis";
import { createClient } from "rbmq";

const redis = new Redis(); // your connection, your lifecycle
const rbmq = createClient(redis, { ns: "rbmq" });

await rbmq.createQueue("jobs");
await rbmq.sendMessage("jobs", "hello");

const msg = await rbmq.receiveMessage("jobs");
if (msg) {
    console.log(msg.message.toString());
    await rbmq.deleteMessage("jobs", msg.id);
}
```

## Design rules (read these before extending)

- **No singletons.** `createClient(redis, opts)` returns a plain object; no module-level state. Run as many in parallel as you like.
- **No hot-patching.** We don't monkey-patch ioredis or your code. Lua scripts are registered via `defineCommand` on the redis instance you pass in.
- **No `AsyncLocalStorage` or other implicit-dependency tricks.** Pass the redis instance and the rbmq client explicitly to whoever needs them.
- **Plain JavaScript with JSDoc.** No TypeScript, no build step. Run with Node 20+.
- **Ioredis is the only runtime dep.** Vitest is the only dev dep.

## API

```js
import { createClient, createWorker } from "rbmq";
```

### `createClient(redis, options)`

Returns an rbmq client object. `options.ns` (default `"rbmq"`), `options.realtime` (default `false`).

| method | notes |
|---|---|
| `createQueue(qname, { vt?, delay?, maxsize? })` | `vt` and `delay` are ms (number, BigInt, or `null`). `maxsize` is bytes (`-1` for unlimited). |
| `deleteQueue(qname)` | Removes config + bodies + index entry. |
| `listQueues()` | `Promise<string[]>` |
| `getQueueAttributes(qname)` | All numeric fields are `BigInt`. |
| `setQueueAttributes(qname, { vt?, delay?, maxsize? })` | Pass only what you want to change. |
| `sendMessage(qname, body, { delay? })` | `body` accepts `Buffer`, `Uint8Array`, or `string`. Returns the assigned id. |
| `sendMessageBatch(qname, [body, ...], { delay? })` | One PUBLISH per call when `realtime` is on. Atomic — all or none. |
| `receiveMessage(qname, { vt? })` | Returns `null` if empty, `{ id, message: Buffer, rc, fr, sent }` otherwise. |
| `receiveMessageBatch(qname, max, { vt? })` | Up to `max` in one call. |
| `popMessage(qname)` | Receive + delete in one atomic step. |
| `deleteMessage(qname, id)` | `Promise<boolean>`. |
| `changeMessageVisibility(qname, id, hiddenMs)` | Extends or shortens an in-flight message's hidden window. |
| `moveMessage(src, id, dst)` | Atomic move; preserves rc and fr. |
| `receiveMessageOrDlq(qname, dlq, maxReceives, { vt? })` | Routes over-budget messages to `dlq` and tries the next one. |

### `createWorker(options)`

A polling helper with built-in heartbeat, optional DLQ, and clean shutdown.

```js
import { createWorker } from "rbmq";

const worker = createWorker({
    redis,                              // OR client: <existing rbmq client>
    ns: "rbmq",
    routes: {
        emails: async (msg) => { /* ... */ },
        billing: async (msg) => { /* ... */ },
    },
    pollIntervalMs: 1000,
    heartbeatIntervalMs: 10_000,
    visibilityExtensionMs: 30_000,
    dlq: { queue: "jobs_dead", maxFailures: 3 },           // optional
    routeDlqs: { billing: { queue: "billing_dead", maxFailures: 0 } },  // optional override
});

const ac = new AbortController();
process.on("SIGTERM", () => ac.abort());
await worker.runUntil(ac.signal);
```

`max_failures = 0` ⇒ DLQ on the first error. `max_failures = N` ⇒ DLQ on the (N+1)-th error.

## Cross-language compatibility

Same Lua scripts run on the Rust side (`rbmq` crate, v1.0+). Same key layout:

- `{ns}:{queue}` — sorted set (id → visibility timestamp, microseconds)
- `{ns}:{queue}:cfg` — hash (vt, delay, maxsize, totalrecv, totalsent, created, modified)
- `{ns}:{queue}:msg` — hash (id → packed `<rc>\n<fr>\n<sent>\n<body>`)
- `{ns}:QUEUES` — set of queue names

Not wire-compatible with the original `smrchy/rsmq` package or `rsmq_async` v17/v18.

## Errors

```js
import {
    RbmqError,
    QueueNotFoundError,
    QueueExistsError,
    MessageTooLongError,
    InvalidFormatError,
    InvalidValueError,
} from "rbmq";
```

Lua-side errors (`QueueNotFound`, `QueueExists`, `MessageTooLong`) are mapped to typed errors on the Node side. Other Redis errors propagate as the underlying `Error` from ioredis.

## Tests

```sh
npm install
npm test
```

Requires a Redis on `127.0.0.1:6379` (or set `REDIS_HOST` / `REDIS_PORT`).

## License

MIT.
