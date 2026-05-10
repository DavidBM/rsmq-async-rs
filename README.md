# rsmq-async

[![Crates.io](https://img.shields.io/crates/v/rsmq_async)](https://crates.io/crates/rsmq_async)
[![Docs.rs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square)](https://docs.rs/rsmq_async)
[![Crates.io](https://img.shields.io/crates/l/rsmq_async)](https://choosealicense.com/licenses/mit/)
[![dependency status](https://deps.rs/crate/rsmq_async/latest/status.svg)](https://deps.rs/crate/rsmq_async)

**A small, atomic, Redis-backed message queue for Rust.** If you already run Redis and you want a decent queue without standing up a second broker, this is for you.

It's an async Rust port of [RSMQ](https://github.com/smrchy/rsmq), wire-compatible with the original JavaScript implementation — so a Node.js producer and a Rust worker can share a queue without anyone changing serialization formats.

---

## Why this, why Redis

**If you need a simple queue without complications, this is for you.**

If you already run Redis, you have most of a queue sitting right there. This crate is the rest of it.

That's the whole pitch. You take the Redis you're already operating — already monitoring, already backing up, already restarting on autopilot — add a small Rust dependency, and you have a working queue with at-least-once delivery, visibility timeouts, batches, and a DLQ. No new broker to deploy, no new failure modes to learn, no new dashboards, no new on-call rotation.

Yes, Redis itself is a broker — that's the point. It's just one you very likely already have.

**What you get from Redis:**

- **Speed.** In-memory, sub-millisecond per-operation latency. The library hands Redis a short Lua script per call and gets out of the way.
- **Durability if you want it.** AOF (append-only file) gives you crash-consistent persistence; RDB snapshots if your tolerance for loss is wider. Tune it to your needs — this crate doesn't care.
- **Atomic Lua scripts.** Every multi-key operation here is one short script, so the races a queue normally has to engineer around just don't exist. Two workers can't claim the same message; a `move_message` can't half-apply; a batch send is all-or-nothing.
- **Familiar ops.** Backup, restart, monitor, alert — all the runbooks you already have keep working.

**Be honest about throughput.** Each call is one or two Redis round trips, so a single worker against a local Redis processes on the order of a few hundred messages per second. Add more workers and it scales close-to-linearly until Redis itself becomes the bottleneck. If you're sustaining many thousands of messages per second per queue with strict latency targets, you probably want a dedicated broker. If you're not — emails, billing jobs, webhooks, indexing tasks, the bread-and-butter background work that most services actually have — this is plenty fast and several orders of magnitude simpler to operate.

This crate doesn't try to be clever. It hands Redis a handful of well-tested Lua scripts and lets the database do what it's already good at.

### What you get

- **At-least-once delivery** with visibility timeouts — receive, process, delete. If your worker dies mid-flight, the message reappears for someone else.
- **At-most-once delivery** when you want it — `pop_message` dequeues and deletes in a single atomic step.
- **Delayed messages** — schedule work to become visible later.
- **Per-message visibility heartbeats** — extend a message's hidden window while you're still working on it, so a 5-minute job doesn't get redelivered every 30 seconds.
- **Atomic batches** — push or pull N messages in a single round trip and a single Lua script.
- **Atomic dead-letter routing** — kick poison messages out to a DLQ without ever risking a duplicate.
- **Optional realtime pub/sub** — wake workers immediately on enqueue instead of polling.
- **Cross-language interop** — share queues with the original JavaScript RSMQ.
- **A high-level `Worker` helper** — queue-name router, automatic heartbeat, graceful shutdown, optional DLQ. For most users this is the only API they need.
- **Three client flavors behind one trait** — multiplexed (default), pooled, and synchronous.
- **Both Tokio and smol** as async runtimes.
- **Optional `serde` JSON support**, two ways.
- **`#![forbid(unsafe_code)]`**, ~2k lines of Rust + ~150 lines of Lua you can read in an afternoon.

---

## Quick start

```toml
[dependencies]
rsmq_async = "18"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use rsmq_async::{Rsmq, RsmqConnection, RsmqError};

#[tokio::main]
async fn main() -> Result<(), RsmqError> {
    let mut rsmq = Rsmq::new(Default::default()).await?;

    rsmq.create_queue("jobs", None, None, None).await?;
    rsmq.send_message("jobs", "hello from Rust", None).await?;

    if let Some(msg) = rsmq.receive_message::<String>("jobs", None).await? {
        println!("got: {}", msg.message);
        rsmq.delete_message("jobs", &msg.id).await?;
    }

    Ok(())
}
```

That's the whole loop. Receive a message, do your work, delete the message. If your code panics or the box dies before `delete_message`, the message becomes visible again after its visibility timeout and another worker picks it up. That's at-least-once delivery, and it's the foundation everything else builds on.

> **The single most important rule:** call `delete_message` only after you've successfully processed the message. Skipping or deferring it is what gives you redelivery. Doing it too early gives you data loss on crash.

---

## How it actually works

Each queue is a Redis sorted set. Each message gets a score equal to "the timestamp at which this message becomes visible." Receiving a message means asking Redis for the lowest-score entry whose score is in the past, then bumping that score forward by the visibility timeout — all inside a single Lua script, so no two workers can ever read the same message at the same time.

Because every operation that touches multiple keys is one Lua script, the queue's invariants don't depend on luck or careful ordering of separate Redis commands. They're enforced by the database. There are no half-applied moves, no race windows where a message exists in two places, no "we tried to delete but the visibility extension already fired."

You can read the scripts yourself — they live in [`src/redis-scripts/`](src/redis-scripts/) and they're short.

---

## Pick your client

All three implementations sit behind the same [`RsmqConnection`] trait, so you can write generic code and swap them.

| Client | When to use |
|---|---|
| **`Rsmq`** | **Start here.** A single multiplexed connection that interleaves concurrent operations. No pool overhead, no contention, no surprises. Right for the vast majority of workloads. |
| **`PooledRsmq`** | You're sending large payloads (images, documents, big blobs) and you don't want a single slow op holding up the line. Backed by [bb8](https://docs.rs/bb8). |
| **`RsmqSync`** | You're in a synchronous codebase and don't want to deal with `.await`. Wraps `Rsmq` in a Tokio runtime under the hood. |

Generic over the trait:

```rust
use rsmq_async::RsmqConnection;

async fn process(rsmq: &mut impl RsmqConnection) -> Result<(), rsmq_async::RsmqError> {
    // works with Rsmq, PooledRsmq, or RsmqSync
    rsmq.send_message("jobs", "anything", None).await?;
    Ok(())
}
```

### Connection pool

```rust
use rsmq_async::{PooledRsmq, PoolOptions};

let pool_opts = PoolOptions { max_size: Some(20), min_idle: Some(5) };
let mut rsmq = PooledRsmq::new(Default::default(), pool_opts).await?;
```

### Sync wrapper

```rust
use rsmq_async::{RsmqSync, RsmqConnectionSync};

let mut rsmq = RsmqSync::new(Default::default()).await?;
rsmq.send_message("jobs", "hello", None)?;
```

---

## The `Worker` helper

For the common case — "I want one process that polls some queues and dispatches each message to a handler" — you don't need to write the loop yourself. The `worker` feature (default-on) gives you a router with built-in heartbeat, optional realtime wake-up, and optional DLQ routing.

```rust
use rsmq_async::{RsmqMessage, RsmqOptions, Worker};
use std::convert::Infallible;
use std::time::Duration;

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
    .use_realtime(true)
    .build()
    .await?;

worker.run().await?;                        // forever
// or
worker.run_until(shutdown_signal).await?;   // graceful
```

What the worker does for you:

- **Routes** each message by queue name to the matching handler.
- **Decodes** the message bytes into your handler's parameter type.
- **Heartbeats** — while a handler runs, the worker periodically extends the message's visibility so a slow handler doesn't get its message redelivered behind its back.
- **Acknowledges** automatically: `Ok` deletes the message, `Err` leaves it for redelivery (or routes it to DLQ; see below).
- **Wakes on realtime** if you opted in — no polling latency between enqueue and pickup.
- **Shuts down cleanly** — in-flight handlers run to completion before `run_until` returns.

A single worker is single-task by design. For parallelism, run multiple workers — they coordinate naturally through Redis.

### DLQ routing in the worker

```rust
let worker = Worker::builder(RsmqOptions::default())
    .dlq("jobs_dead", 3)                         // global DLQ after 3 failures
    .dlq_for("billing", "billing_dead", 0)       // override: DLQ on first failure
    .route("emails",  handler_emails)
    .route("billing", handler_billing)
    .build()
    .await?;
```

`max_failures = 0` means the very first error sends the message to the DLQ. `max_failures = N` means the worker tolerates up to N failures and routes to DLQ on the next one. The DLQ queue must exist (`create_queue` it ahead of time). A self-loop (a route's DLQ pointing at itself) is rejected when you call `build()`.

---

## Dead-letter primitives

The worker is the right tool for most people. If you need custom flows, two primitives are exposed directly on `RsmqConnection`:

```rust
// Atomic move: preserves body, receive count, first-received timestamp.
rsmq.move_message("src", &msg_id, "dst").await?;

// Atomic receive that DLQs over-budget messages and tries the next one.
let msg = rsmq.receive_message_or_dlq::<String>("src", None, "dlq", 3).await?;
```

`receive_message_or_dlq` routes purely on receive count, regardless of handler outcome — useful when you want a strict "deliver at most N times" policy without involving a handler at all.

---

## Batching

Send or receive up to N messages in a single round trip, atomically, on `Rsmq` / `PooledRsmq` / `RsmqSync`:

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

`send_message_batch` is fully atomic — either every message lands or none do. With realtime enabled, exactly one PUBLISH fires per batch, carrying the post-batch queue size. Use this when you have many small messages and the round-trip cost is dominating throughput.

---

## Realtime notifications

Set `realtime: true` in `RsmqOptions` and RSMQ will `PUBLISH` to `{ns}:rt:{qname}` on every send. Subscribe with `redis-rs` to wake workers immediately instead of polling.

```rust
use rsmq_async::RsmqOptions;

let mut rsmq = Rsmq::new(RsmqOptions { realtime: true, ..Default::default() }).await?;
```

> Use **one** subscriber per queue. Multiple workers all calling `SUBSCRIBE` on the same channel and racing to `receive_message` is a common foot-gun and produces unnecessary work. The built-in `Worker` does this correctly when you call `.use_realtime(true)`.

---

## Message types

`send_message`, `receive_message`, and `pop_message` are generic over the payload type.

### Built-in: strings and bytes

```rust
rsmq.send_message("q", "hello", None).await?;
let msg = rsmq.receive_message::<String>("q", None).await?;

rsmq.send_message("q", vec![0u8, 1, 2], None).await?;
let msg = rsmq.receive_message::<Vec<u8>>("q", None).await?;
```

### Typed JSON (default `serde` feature)

Two equivalent APIs — pick whichever feels more natural at the call site:

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

The two differ only in error handling: the `Json<T>` wrapper's `TryFrom` returns the raw bytes on a parse failure (surfaces as `RsmqError::CannotDecodeMessage`), while the extension trait surfaces serde errors directly as `RsmqError::JsonError`.

### Custom types

For anything that isn't `Serialize`, implement the conversions yourself:

```rust
use rsmq_async::RedisBytes;

struct MyPayload(/* ... */);

impl From<MyPayload> for RedisBytes {
    fn from(p: MyPayload) -> RedisBytes { /* serialize to bytes */ unimplemented!() }
}

impl TryFrom<RedisBytes> for MyPayload {
    type Error = Vec<u8>;  // must be Vec<u8>; gives the original bytes back on failure
    fn try_from(b: RedisBytes) -> Result<Self, Vec<u8>> { /* deserialize */ unimplemented!() }
}
```

See [`examples/custom_type.rs`](examples/custom_type.rs) for a working version.

---

## Queue configuration

```rust
use std::time::Duration;

rsmq.create_queue(
    "jobs",
    Some(Duration::from_secs(30)),  // visibility timeout (default: 30s)
    Some(Duration::from_secs(0)),   // delivery delay    (default: 0)
    Some(65536),                    // max message size in bytes; -1 = unlimited
).await?;
```

Update later:

```rust
rsmq.set_queue_attributes("jobs", Some(Duration::from_secs(60)), None, None).await?;
```

Inspect:

```rust
let attrs = rsmq.get_queue_attributes("jobs").await?;
println!("messages: {}, hidden: {}", attrs.msgs, attrs.hiddenmsgs);
```

---

## Delivery guarantees

| Pattern | How |
|---|---|
| **At least once** | `receive_message` + `delete_message` after success. Failures redeliver after the visibility timeout. |
| **At most once** | `pop_message` atomically dequeues and deletes. The message is gone whether you process it or not. |
| **Bounded redelivery** | `receive_message_or_dlq(... max_receives)` — strict cap on receives, regardless of handler outcome. |
| **Bounded failures** | `Worker` with `.dlq(...)` — DLQ kicks in only after handler errors. |

There is no exactly-once. Nobody has it. Anyone who tells you they do is selling you something. With `delete_message` and idempotent handlers you get the practical equivalent.

---

## Atomicity, in plain terms

A few specific guarantees worth calling out, because they're the things that bite you in queue libraries that *don't* use Lua scripts:

- **Two workers can never receive the same message.** Receive is a single Lua script that atomically picks the next due message and pushes its visibility forward. There is no window between "find" and "claim."
- **A message in flight is never simultaneously in two queues.** `move_message` does the source remove and destination insert in one script, with the receive count and first-received timestamp preserved.
- **A batch send is all-or-nothing.** `send_message_batch` uses one script — either every message is appended or the whole call fails.
- **DLQ routing can't drop or duplicate.** The worker uses `move_message` for DLQ transfers, so the message either ends up in the DLQ (with metadata intact) or stays where it was.
- **Realtime PUBLISHes are bounded.** One PUBLISH per `send_message`, one PUBLISH per `send_message_batch` (not one per message in the batch).

---

## Cross-language compatibility

By default, message scores are stored in milliseconds — wire-compatible with the original Node.js [RSMQ](https://github.com/smrchy/rsmq). A JS producer and a Rust consumer can share a queue, and vice versa, with no translation layer.

If you don't need that compat, the `break-js-comp` feature switches to microsecond-precision scores. **Don't mix the two on the same queue** — Rust producers running with `break-js-comp` will write scores in different units than a JS server, and ordering will fall apart.

---

## Cargo features

| Feature | Default | Description |
|---|---|---|
| `tokio-comp` | yes | Tokio runtime support. |
| `smol-comp` | no | smol runtime support. Use `default-features = false` to switch. |
| `sync` | yes | Enables `RsmqSync` and `RsmqConnectionSync`. |
| `serde` | yes | Enables `Json<T>`, `RsmqJsonExt`, and `RsmqError::JsonError`. |
| `worker` | yes | Enables the `Worker` helper. Tokio-only. |
| `break-js-comp` | no | Microsecond-precision scores. Breaks compatibility with the JS library — see above. |

To use smol instead of Tokio:

```toml
rsmq_async = { version = "18", default-features = false, features = ["smol-comp"] }
```

To strip everything optional:

```toml
rsmq_async = { version = "18", default-features = false, features = ["tokio-comp"] }
```

---

## Tested

- **66 integration tests** across 6 files, all running against a real Redis. No mocks — the test harness brings up a real connection and the suite covers send/receive/delete/pop, visibility behavior, batching, JSON paths, queue attributes, name validation, message-size limits, DLQ flows, the worker's heartbeat, and shutdown semantics.
- **CI matrix** runs the full suite under three feature combinations (`serde` × `break-js-comp` × baseline), plus a separate smol build, plus a lint job that runs `cargo fmt --check` and `cargo clippy --all-targets --all-features -D warnings`.
- **`#![forbid(unsafe_code)]`** at the crate root.
- **Examples are built in CI** (`cargo build --examples --all-features`), so the public-facing samples in [`examples/`](examples/) actually compile against every release.

---

## When *not* to use this

Be honest with yourself.

- You need **many thousands of msg/s sustained per queue** with hard latency budgets. The round-trip overhead per call puts a realistic ceiling well below dedicated brokers on bare metal. Add workers until Redis itself is the bottleneck — if you hit that and need more, switch.
- You need **partitioned consumer groups** with strict ordering across keys (Kafka-style). RSMQ has FIFO-by-score within a queue but no native partition story. You can shard across multiple queues, but if you need this natively, use the right tool.
- You need **persistent log replay** — "give me everything from offset 0." This is a queue, not a log. Once you delete, the message is gone.
- You're already running Kafka, Pulsar, or SQS, and routing through them is a one-liner. Don't add Redis just to use this.

For everything else — the long tail of services that just need to hand work to a worker reliably and move on — this is enough, and *much* less to operate.

---

## Examples

Working programs in [`examples/`](examples/), all runnable with `cargo run --example <name>`:

- [`worker_loop.rs`](examples/worker_loop.rs) — the minimal `receive`/`delete` loop.
- [`worker_helper.rs`](examples/worker_helper.rs) — the high-level `Worker` with multiple routes.
- [`retry_with_backoff.rs`](examples/retry_with_backoff.rs) — exponential backoff using `rc` and `change_message_visibility`.
- [`long_running_heartbeat.rs`](examples/long_running_heartbeat.rs) — keeping a slow handler safe with manual heartbeats.
- [`serde_message.rs`](examples/serde_message.rs) — both JSON APIs side by side.
- [`custom_type.rs`](examples/custom_type.rs) — implementing `Into<RedisBytes>` / `TryFrom<RedisBytes>` for your own type.

---

## Development

Start a local Redis with Docker:

```sh
docker run -d --name redis-test -p 6379:6379 redis:latest
```

Run the tests (they share the Redis DB and so are forced to run sequentially via `.cargo/config.toml`):

```sh
cargo test
```

Target a different Redis:

```sh
REDIS_URL=127.0.0.1:6380 cargo test
```

If Redis isn't reachable, the test harness fails fast with three remediation paths (Docker, apt, `REDIS_URL`) instead of stalling.

---

## License

MIT. See [LICENSE](LICENSE).

Original RSMQ © Patrick Liess. Rust port © David Bonet and contributors.
