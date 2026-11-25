# RBMQ

[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A Redis-backed job queue for Rust and Node.js. Give a queue a name, send a
payload, and run a worker. The worker renews the job's visibility and
acknowledges it when the handler succeeds. Both clients use the same Lua
scripts, so Rust producers and Node workers can share queues.

RBMQ is the successor to
[`rsmq_async`](https://crates.io/crates/rsmq_async/18.0.0). Use a fresh namespace
for RBMQ and drain existing queues with the old client before switching. The
[`rsmq_async` 18.0.0 source](https://github.com/DavidBM/rsmq-async-rs/tree/8f423e36e81ea13f562213c0eac4817db4ed7c5a)
uses a different storage format, compatible with the original JS RSMQ.

## Rust quick start

Add these dependencies.

```toml
[dependencies]
rbmq = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

```rust
use rbmq::{Rbmq, RbmqConnection, RbmqMessage, Worker};
use std::convert::Infallible;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let mut queue = Rbmq::connect(&url).await?;
    queue.ensure_queue("jobs").await?;
    queue.send_message("jobs", "generate-report", None).await?;

    Worker::from_url(&url)
        .route("jobs", |job: RbmqMessage<String>| async move {
            println!("Processing {}", job.message);
            Ok::<(), Infallible>(())
        })
        .build()
        .await?
        .run_until(async { let _ = tokio::signal::ctrl_c().await; })
        .await?;
    Ok(())
}
```

`ensure_queue` is safe to run on every startup. Existing queues retain their
settings and pending jobs. In production, run the producer and worker wherever
they can reach the same Redis URL.

## Node.js quick start

Pack the Node client from this checkout, then install it into your application.

```sh
npm pack --pack-destination /tmp ./node
# From your application directory
npm install /tmp/davidbm-rbmq-2.0.0.tgz ioredis
```

```js
import Redis from "ioredis";
import { createClient, createWorker } from "@davidbm/rbmq";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1/");
const queue = createClient(redis);
await queue.ensureQueue("jobs");
await queue.sendMessage("jobs", "generate-report");

const worker = createWorker({
    client: queue,
    routes: {
        jobs: async (job) => console.log("Processing", job.message.toString()),
    },
});

const shutdown = new AbortController();
process.once("SIGINT", () => shutdown.abort());
try {
    await worker.runUntil(shutdown.signal);
} finally {
    await redis.quit();
}
```

## Delivery and throughput

- Workers reserve a job before calling the handler. A default 30-second processing
  lease is renewed every 10 seconds. Acknowledgment, renewal, and dead-letter
  moves are conditional on the attempt still owning the delivery.
- A handler failure uses the queue's visibility timeout as its retry delay.
  Configure a dead-letter queue when failed jobs should stop retrying.
- Delivery is at least once. Make handlers idempotent when they perform external
  side effects.
- With the Lua scripts cached, a send, receive, or acknowledgment each takes one
  round trip. Batches amortize round trips across jobs. New jobs with the same
  visibility timestamp retain enqueue order. Optional pub/sub can
  wake Rust workers earlier than their polling interval.
- Use the low-level client for direct polling, custom message types, bulk
  operations, or pop-and-delete. See the [Rust examples](examples/) and
  [Node API](node/README.md).

## Backend compatibility

The Rust, Node, and cross-language CI matrix includes Redis 7, Redis 8,
Valkey 9, and Dragonfly 2.1. The clients declare each key a Lua script accesses,
so Dragonfly can use its default script checks. No server extension is needed.
The matrix covers single-primary deployments using TCP.

## Production deployment

- Use `tokio-tls` or `smol-tls` with a `rediss://` URL for encrypted Rust
  connections. The system trust store verifies the server certificate. With
  Node, pass the corresponding TLS settings to ioredis.
- Use a dedicated Redis account. Store connection URLs in secret configuration,
  and restrict application access to its queue namespace.
- Choose the backend's persistence and failover settings before enqueueing
  durable jobs. Queue data uses multiple keys, so use a non-evicting backend
  sized for pending jobs and payloads.
- Stop workers gracefully to let handlers finish. Use idempotency keys for
  external side effects, and monitor queue depth, retries, and dead-letter
  depth. Plan for redelivery after a process or network failure.

## Storage layout

```
{ns}:{queue}        ZSET   id -> visibility timestamp (microseconds)
{ns}:{queue}:cfg    HASH   vt, delay, maxsize, totalrecv, totalsent, created, modified
{ns}:{queue}:msg    HASH   id -> "<rc>\n<fr>\n<sent>\n<body>"
{ns}:rt:{queue}     PUBSUB channel for realtime notifications (when enabled)
{ns}:QUEUES         SET    of queue names in the namespace
```

## License

MIT. See [LICENSE](LICENSE).
