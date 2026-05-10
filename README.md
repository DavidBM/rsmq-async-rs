# rbmq — Redis Better Message Queue

[![Crates.io](https://img.shields.io/crates/v/rbmq?label=rust)](https://crates.io/crates/rbmq)
[![npm](https://img.shields.io/npm/v/rbmq?label=node)](https://www.npmjs.com/package/rbmq)
[![License](https://img.shields.io/crates/l/rbmq)](LICENSE)

**A small, atomic, Redis-backed message queue. The same Lua scripts power the Rust crate and the Node.js package — a Rust producer and a Node consumer share queues with no translation layer.**

> Successor to [`rsmq_async`](https://crates.io/crates/rsmq_async). **v1 is a clean break — not wire-compatible** with the original [`smrchy/rsmq`](https://github.com/smrchy/rsmq) Node package or with `rsmq_async` v17/v18. If you need that compatibility, stay on `rsmq_async`.

## What's in here

```
scripts/    Lua scripts — the source of truth for queue semantics
src/        Rust crate (rbmq)
node/       Node.js package (rbmq, ioredis-based)
examples/   Rust examples (worker, retry, JSON, custom types, interop helper)
tests/      Rust integration tests (~206)
node/test/  Node integration tests (~95, including 20 cross-language)
```

## Quick start

### Rust

```toml
[dependencies]
rbmq = "1.0.0-alpha.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use rbmq::{Rbmq, RbmqConnection, RbmqError};

#[tokio::main]
async fn main() -> Result<(), RbmqError> {
    let mut rbmq = Rbmq::new(Default::default()).await?;
    rbmq.create_queue("jobs", None, None, None).await?;
    rbmq.send_message("jobs", "hello from Rust", None).await?;

    if let Some(msg) = rbmq.receive_message::<String>("jobs", None).await? {
        println!("got: {}", msg.message);
        rbmq.delete_message("jobs", &msg.id).await?;
    }
    Ok(())
}
```

### Node.js

```sh
npm install rbmq ioredis
```

```js
import Redis from "ioredis";
import { createClient } from "rbmq";

const redis = new Redis();
const rbmq = createClient(redis);

await rbmq.createQueue("jobs");
await rbmq.sendMessage("jobs", "hello from Node");

const msg = await rbmq.receiveMessage("jobs");
if (msg) {
    console.log(msg.message.toString());
    await rbmq.deleteMessage("jobs", msg.id);
}
```

## Design (the why)

This is a Redis client that gives you queue semantics without a second broker. The atomicity guarantees you'd normally have to engineer around — two workers can't claim the same message; a `move_message` can't half-apply; a batch send is all-or-nothing — are enforced by Redis itself, because every multi-key operation is one Lua script.

Architectural choices in v1:

- **One round trip per call.** Every public method dispatches to a single Lua script. The script reads queue config (vt/delay/maxsize), calls `redis.call("TIME")`, mutates state, and (when realtime is on) `PUBLISH`es — all in one atomic invocation. No pre-fetch round trips.
- **Logic lives in Lua, not in Rust or JS.** The Rust and Node libraries are thin wrappers over the same scripts. Cross-language wire compat is structural, not promised.
- **Storage layout split for clarity.** Three keys per queue: a sorted set (`{ns}:{q}` for visibility ordering), a config hash (`{ns}:{q}:cfg`), and a per-message hash (`{ns}:{q}:msg`).
- **Packed message values.** Each message is one hash entry: `"<rc>\n<fr>\n<sent>\n<body>"`. One `HGET` per receive instead of three; bodies can contain anything (newlines, NULs, binary).
- **Microsecond scores.** No millisecond mode, no JS-rsmq compatibility branches.
- **Hex IDs.** 32 lowercase hex chars (16 random bytes). No time prefix; ordering comes from the sorted-set score.

## What you get

- **At-least-once delivery** with visibility timeouts.
- **At-most-once delivery** (`pop_message` / `popMessage`).
- **Delayed messages** — schedule work for later.
- **Per-message visibility heartbeats** — extend an in-flight message's hidden window from your worker.
- **Atomic batches** — push or pull N messages in one round trip.
- **Atomic dead-letter routing** — `move_message` and `receive_message_or_dlq`.
- **Optional realtime pub/sub** — wake workers on enqueue.
- **Cross-language interop** — Rust ↔ Node share queues by structure.
- **A `Worker` helper on each side** — heartbeat, DLQ, graceful shutdown.

## When *not* to use this

- **Many thousands of msg/s sustained per queue with hard latency budgets.** Each call is one round trip; multiple workers scale linearly until Redis itself bottlenecks. If you need more than that, use a dedicated broker.
- **Kafka-style partitioned consumer groups with strict cross-key ordering.**
- **Persistent log replay.** Once a message is deleted, it's gone.

## Tested

- **Rust:** ~206 integration tests + doc tests, against a real Redis. Coverage: send/receive/delete/pop, batches, JSON, visibility, DLQ flows, worker semantics, concurrency, encoding edge cases, queue-name validation, lifecycle, properties.
- **Node:** ~95 integration tests including the same scenarios on the Node side.
- **Cross-language:** 20 interop tests where a Rust process and a Node process operate on the same queue and observe each other's writes — proven by CI.

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
