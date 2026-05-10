# rsmq-async

[![Crates.io](https://img.shields.io/crates/v/rsmq_async)](https://crates.io/crates/rsmq_async)
[![Docs.rs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square)](https://docs.rs/rsmq_async)
[![Crates.io](https://img.shields.io/crates/l/rsmq_async)](https://choosealicense.com/licenses/mit/)
[![dependency status](https://deps.rs/crate/rsmq_async/latest/status.svg)](https://deps.rs/crate/rsmq_async)

An async Rust port of [RSMQ](https://github.com/smrchy/rsmq) — a lightweight message queue built entirely on Redis. No extra infrastructure, no brokers, just Redis.

Wire-compatible with the original JavaScript implementation: messages enqueued by a JS server can be consumed by a Rust worker and vice versa.

---

## How it works

RSMQ stores queues as Redis sorted sets. Each message gets a score based on its delivery time, so delayed and hidden messages are naturally ordered. All queue operations are executed as atomic Lua scripts, making them safe under concurrent access.

- **At-least-once delivery** — receive a message, process it, then delete it. If your worker crashes mid-flight, the message becomes visible again after its visibility timeout.
- **At-most-once delivery** — use `pop_message` to receive and delete in one atomic step.
- **Delayed messages** — held back for a configurable duration before becoming visible.
- **Per-message visibility timeouts** — received messages stay hidden for a configurable window, giving your worker time to process them.
- **Atomic batch send / receive** — push or pull N messages in a single Lua-script round trip.
- **Atomic dead-letter routing** — `move_message` and `receive_message_or_dlq` primitives, plus a high-level `Worker` helper that wires everything up.

---

## Quick start

```toml
[dependencies]
rsmq_async = "18"
```

```rust
use rsmq_async::{Rsmq, RsmqConnection, RsmqError};

#[tokio::main]
async fn main() -> Result<(), RsmqError> {
    let mut rsmq = Rsmq::new(Default::default()).await?;

    rsmq.create_queue("jobs", None, None, None).await?;
    rsmq.send_message("jobs", "hello from Rust", None).await?;

    if let Some(msg) = rsmq.receive_message::<String>("jobs", None).await? {
        println!("Got: {}", msg.message);
        rsmq.delete_message("jobs", &msg.id).await?;
    }

    Ok(())
}
```

> Always `delete_message` after successfully processing — this is what confirms delivery.

---

## Implementations

Three implementations are provided, all behind the same [`RsmqConnection`] trait.

| Type | Use when |
|---|---|
| `Rsmq` | **Start here.** A single multiplexed connection handles concurrent operations efficiently — no pool overhead, no contention. Right for the vast majority of workloads. |
| `PooledRsmq` | You're sending large payloads (images, documents, big blobs) and one slow operation blocking the shared connection becomes a problem. |
| `RsmqSync` | You're in a sync context. Wraps `Rsmq` in a Tokio runtime. Requires the `sync` feature (default-on). |

Write code against the trait to stay implementation-agnostic:

```rust
use rsmq_async::RsmqConnection;

async fn process(rsmq: &mut impl RsmqConnection) {
    // works with Rsmq, PooledRsmq, or RsmqSync
}
```

### Connection pool

```rust
use rsmq_async::{PooledRsmq, PoolOptions, RsmqConnection};

let pool_opts = PoolOptions { max_size: Some(20), min_idle: Some(5) };
let mut rsmq = PooledRsmq::new(Default::default(), pool_opts).await?;
```

### Sync wrapper

```rust
use rsmq_async::{RsmqSync, RsmqConnectionSync};

let mut rsmq = RsmqSync::new(Default::default()).await?;
rsmq.send_message("myqueue", "hello", None)?;
```

---

## Cargo features

| Feature | Default | Description |
|---|---|---|
| `tokio-comp` | yes | Tokio async runtime support. |
| `smol-comp` | no | smol async runtime support. |
| `sync` | yes | Enables `RsmqSync` and `RsmqConnectionSync`. |
| `serde` | yes | Enables [`Json<T>`], [`RsmqJsonExt`], and the `RsmqError::JsonError` variant. Pulls in `serde` and `serde_json`. |
| `worker` | yes | Enables the [`Worker`] helper. Tokio-only. Pulls in `futures-util` and `log`. |
| `break-js-comp` | no | Microsecond-precision scores and IDs (see below). |

To use the smol runtime instead of Tokio:

```toml
rsmq_async = { version = "18", default-features = false, features = ["smol-comp"] }
```

To strip everything optional:

```toml
rsmq_async = { version = "18", default-features = false, features = ["tokio-comp"] }
```

### `break-js-comp`

By default, rsmq-async is wire-compatible with the JS library: scores are stored in milliseconds. Enabling `break-js-comp` switches scores to full microsecond precision — finer ordering for high-throughput queues, but messages written by a JS server and a Rust server with `break-js-comp` will have mismatched score units. **Don't mix the two on the same queue.**

---

## Message types

`send_message`, `receive_message`, and `pop_message` are generic over your message type.

### Built-in: strings and bytes

```rust
rsmq.send_message("q", "hello", None).await?;
let msg = rsmq.receive_message::<String>("q", None).await?;

rsmq.send_message("q", vec![0u8, 1, 2], None).await?;
let msg = rsmq.receive_message::<Vec<u8>>("q", None).await?;
```

### Typed JSON via `serde` (default feature)

Two equivalent APIs — pick whichever fits the call site.

```rust
use rsmq_async::{Json, RsmqJsonExt};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct Job { id: u64, payload: String }

// Style 1: Json<T> wrapper composes with the existing API.
rsmq.send_message("jobs", Json(Job { id: 1, payload: "x".into() }), None).await?;
let msg = rsmq.receive_message::<Json<Job>>("jobs", None).await?;
// msg.unwrap().message.0  is your Job

// Style 2: extension trait — `send_json` / `receive_json` / `pop_json`.
rsmq.send_json("jobs", &Job { id: 2, payload: "y".into() }, None).await?;
let msg = rsmq.receive_json::<Job>("jobs", None).await?;
// msg.unwrap().message  is your Job (no `.0`)
```

The two differ only in error handling: the wrapper drops serde errors (returns the raw bytes via `RsmqError::CannotDecodeMessage`), while the extension trait surfaces them as `RsmqError::JsonError(serde_json::Error)`.

### Custom types

For types you can't make `Serialize`, implement the conversions directly:

```rust
use rsmq_async::RedisBytes;

struct MyPayload(/* ... */);

impl From<MyPayload> for RedisBytes {
    fn from(p: MyPayload) -> RedisBytes { /* serialize to bytes */ }
}

impl TryFrom<RedisBytes> for MyPayload {
    type Error = Vec<u8>; // must be Vec<u8>

    fn try_from(b: RedisBytes) -> Result<Self, Vec<u8>> {
        /* deserialize, return original bytes on failure */
    }
}
```

---

## Queue configuration

```rust
use std::time::Duration;

rsmq.create_queue(
    "jobs",
    Some(Duration::from_secs(30)),  // visibility timeout (default: 30s)
    Some(Duration::from_secs(0)),   // delivery delay    (default: 0)
    Some(65536),                    // max message size in bytes, -1 for unlimited
).await?;
```

Update attributes after creation:

```rust
rsmq.set_queue_attributes("jobs", Some(Duration::from_secs(60)), None, None).await?;
```

Inspect stats:

```rust
let attrs = rsmq.get_queue_attributes("jobs").await?;
println!("messages: {}, hidden: {}", attrs.msgs, attrs.hiddenmsgs);
```

---

## Batching

Send or receive up to N messages in a single Lua-script round trip. Atomic on the implementations that override the default impl (`Rsmq`, `PooledRsmq`, `RsmqSync`):

```rust
let ids = rsmq
    .send_message_batch("jobs", vec!["a".to_string(), "b".to_string(), "c".to_string()], None)
    .await?;

let msgs = rsmq.receive_message_batch::<String>("jobs", None, 100).await?;
for msg in msgs {
    // ...process...
    rsmq.delete_message("jobs", &msg.id).await?;
}
```

`send_message_batch` is fully atomic: either all messages land in Redis or none do (script error). With realtime enabled, exactly one PUBLISH fires per batch (carrying the post-batch queue size).

---

## Realtime notifications

Set `realtime: true` in `RsmqOptions` to have RSMQ publish to `{ns}:rt:{qname}` on every `send_message` (and one PUBLISH per `send_message_batch`). Subscribe with `redis-rs` to wake workers immediately instead of polling.

```rust
use rsmq_async::RsmqOptions;

let mut rsmq = Rsmq::new(RsmqOptions { realtime: true, ..Default::default() }).await?;
```

> Use a single subscriber per queue — multiple workers listening on the same SUBSCRIBE channel and racing to call `receive_message` is a common mistake.

The [`Worker`] helper (below) handles realtime wake-up for you via the `use_realtime(true)` builder method.

---

## Worker helper

The `worker` feature (default-on, tokio-only) provides a `Worker` that polls one or more queues, dispatches each message to a per-queue handler, runs the handler with automatic visibility-heartbeat (so a slow handler can outlive the queue's `vt` without redelivery), and optionally subscribes to realtime PUBLISHes.

```rust
use rsmq_async::{RsmqMessage, RsmqOptions, Worker};
use std::convert::Infallible;

let worker = Worker::builder(RsmqOptions::default())
    .route("emails", |msg: RsmqMessage<String>| async move {
        send_email(&msg.message).await?;
        Ok::<(), Infallible>(())
    })
    .route("billing", |msg: RsmqMessage<Vec<u8>>| async move {
        charge_card(&msg.message).await?;
        Ok::<(), Infallible>(())
    })
    .heartbeat_interval(Duration::from_secs(10))
    .visibility_extension(Duration::from_secs(60))
    .use_realtime(true)         // requires senders to use RsmqOptions { realtime: true, .. }
    .build()
    .await?;

worker.run().await?;            // runs forever
// — or —
worker.run_until(shutdown_signal).await?;  // waits for in-flight handler to finish
```

Handlers take `RsmqMessage<T>` for any `T: TryFrom<RedisBytes>` and return `Result<(), E>` for any `E: std::error::Error`. On `Ok`, the worker deletes the message; on `Err`, it leaves it for redelivery (or routes to DLQ — see below).

The worker uses the `log` crate for handler/heartbeat diagnostics — wire up `env_logger`, `tracing-log`, etc. to control verbosity. A single worker is single-task by design; for parallelism, instantiate multiple workers.

### Dead-letter routing in the worker

```rust
let worker = Worker::builder(RsmqOptions::default())
    .dlq("jobs_dead", 3)                         // global DLQ after 3 failures
    .dlq_for("billing", "billing_dead", 0)       // per-route override: DLQ on first failure
    .route("emails",  |msg: RsmqMessage<String>| async { Ok::<(), MyErr>(()) })
    .route("billing", |msg: RsmqMessage<String>| async { Ok::<(), MyErr>(()) })
    .build()
    .await?;
```

`max_failures = 0` ⇒ DLQ on first failure (no retries). `max_failures = N` ⇒ DLQ on the (N+1)-th failure (compares against the message's `rc`). The DLQ queue must already exist — call `create_queue` on it before building the worker. Self-loops (a route's DLQ pointing at itself) are rejected at build time.

---

## Dead-letter primitives

Two atomic primitives on `RsmqConnection`. Use the worker for the typical case; reach for these directly when you need custom flows.

```rust
// Atomic move: preserves body, rc, fr.
rsmq.move_message("src", &msg_id, "dst").await?;

// Atomic receive that auto-DLQs over-budget messages and tries the next.
let msg = rsmq.receive_message_or_dlq::<String>("src", None, "dlq", 3).await?;
```

`receive_message_or_dlq` differs from the worker's DLQ flow: it routes purely on receive count (regardless of handler outcome), so it's useful when you want a strict "deliver at most N times" policy without involving a handler.

---

## Delivery guarantees

| Pattern | How |
|---|---|
| **At least once** | `receive_message` + `delete_message` after success. Failures re-deliver after visibility timeout. |
| **At most once** | `pop_message` atomically dequeues and deletes. |

---

## Development

Start a local Redis with Docker:

```sh
docker run -d --name redis-test -p 6379:6379 redis:latest
```

Run the tests (sequential is required — tests share the same Redis DB):

```sh
cargo test
# (test-threads=1 is enforced via .cargo/config.toml)
```

To target a different Redis instance:

```sh
REDIS_URL=127.0.0.1:6380 cargo test
```

If Redis isn't reachable, the test harness fails with a clear error message and three remediation paths (Docker, apt, `REDIS_URL`).
