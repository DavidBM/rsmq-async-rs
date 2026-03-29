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
- **Delayed messages** — messages can be held back for a configurable duration before becoming visible.
- **Per-message visibility timeouts** — received messages stay hidden for a configurable window, giving your worker time to process them.

---

## Quick start

```toml
[dependencies]
rsmq_async = "16"
```

```rust
use rsmq_async::{Rsmq, RsmqConnection, RsmqError};

#[tokio::main]
async fn main() -> Result<(), RsmqError> {
    let mut rsmq = Rsmq::new(Default::default()).await?;

    rsmq.create_queue("jobs", None, None, None).await?;

    // Send a message
    rsmq.send_message("jobs", "hello from Rust", None).await?;

    // Receive and process it
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

Three implementations are provided, all behind the same `RsmqConnection` trait.

| Type | Use when |
|---|---|
| `Rsmq` | **Start here.** A single multiplexed connection handles concurrent operations efficiently — no pool overhead, no contention. Right for the vast majority of workloads. |
| `PooledRsmq` | You're sending large payloads (images, documents, big blobs) and one slow operation blocking the shared connection becomes a problem. |
| `RsmqSync` | You're in a sync context. Wraps `Rsmq` in a Tokio runtime. Requires the `sync` feature. |

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

## Message types

`send_message`, `receive_message`, and `pop_message` are generic over your message type.

Built-in implementations cover the common cases:

```rust
// Send and receive strings
rsmq.send_message("q", "hello", None).await?;
let msg = rsmq.receive_message::<String>("q", None).await?;

// Or raw bytes
rsmq.send_message("q", vec![0u8, 1, 2], None).await?;
let msg = rsmq.receive_message::<Vec<u8>>("q", None).await?;
```

For custom types, implement `TryFrom<RedisBytes>` to receive and `Into<RedisBytes>` to send:

```rust
use rsmq_async::RedisBytes;

struct MyPayload { /* ... */ }

impl TryFrom<RedisBytes> for MyPayload {
    type Error = Vec<u8>; // must be Vec<u8>

    fn try_from(b: RedisBytes) -> Result<Self, Vec<u8>> {
        // deserialize from b.0
    }
}

impl From<MyPayload> for RedisBytes {
    fn from(p: MyPayload) -> RedisBytes {
        RedisBytes(/* serialize to bytes */)
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
    Some(65536),                    // max message size in bytes, -1 for unlimited (default: 65536)
).await?;
```

Queue attributes can be updated after creation:

```rust
rsmq.set_queue_attributes("jobs", Some(Duration::from_secs(60)), None, None).await?;
```

You can inspect queue stats at any time:

```rust
let attrs = rsmq.get_queue_attributes("jobs").await?;
println!("messages: {}, hidden: {}", attrs.msgs, attrs.hiddenmsgs);
```

---

## Realtime notifications

Set `realtime: true` in `RsmqOptions` to have RSMQ publish to `{ns}:rt:{qname}` on every `send_message`. Subscribe with `redis-rs` to wake workers immediately instead of polling.

```rust
use rsmq_async::RsmqOptions;

let mut rsmq = Rsmq::new(RsmqOptions { realtime: true, ..Default::default() }).await?;
```

> Use a single subscriber per queue — multiple workers listening on the same SUBSCRIBE channel and racing to call `receive_message` is a common mistake.

---

## Features

| Feature | Default | Description |
|---|---|---|
| `tokio-comp` | yes | Tokio async runtime support |
| `smol-comp` | no | smol async runtime support |
| `sync` | yes | Enables `RsmqSync` and `RsmqConnectionSync` |
| `break-js-comp` | no | Microsecond-precision scores and IDs (see below) |

To use the smol runtime instead of Tokio:

```toml
rsmq_async = { version = "16", default-features = false, features = ["smol-comp"] }
```

### `break-js-comp`

By default, rsmq-async is wire-compatible with the JS library: message IDs encode microseconds in base36 (matching JS) and queue scores are stored in milliseconds.

Enabling `break-js-comp` switches scores to full microsecond precision. This gives finer ordering for high-throughput queues but means messages written by a JS server and a Rust server with `break-js-comp` will have mismatched score units — **don't mix the two on the same queue**.

```toml
rsmq_async = { version = "16", features = ["break-js-comp"] }
```

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
cargo test -- --test-threads=1
```

To target a different Redis instance:

```sh
REDIS_URL=127.0.0.1:6380 cargo test -- --test-threads=1
```
