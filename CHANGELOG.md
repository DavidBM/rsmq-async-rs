# Changelog

## 17.4.0 - 2026-05-10

### Added
- Atomic `receive_message_or_dlq` primitive on [`Rsmq`], [`PooledRsmq`], and [`RsmqSync`]. Identical to `receive_message`, but transparently routes any message whose post-increment `rc` exceeds `max_receives` to a configurable DLQ (preserving `:rc` and `:fr`) and tries the next visible message in the same Lua script invocation. Returns the first eligible message, or `None` if no eligible message was found within the script's per-call iteration cap (currently 100). Rejects `qname == dlq` self-loops.
  - `max_receives = 0` ⇒ never deliver, always DLQ.
  - `max_receives = N` ⇒ deliver at most N times before routing to DLQ.
- 6 new integration tests in `tests/receive_or_dlq.rs`.

### Fixed
- `moveMessage.lua` (and the new `receiveMessageOrDlq.lua`): when moving a message that has `:rc > 0` but no `:fr` (e.g. a not-yet-delivered message moved by `move_message`, or a message DLQ'd on its very first receive), the destination's `:fr` is now always set — defaulting to the current timestamp — so a subsequent `receive_message` from the destination doesn't fail trying to `HGET` a missing `:fr` field.

## 17.3.0 - 2026-05-10

### Added
- Atomic `move_message(src, msg_id, dst)` primitive on [`Rsmq`], [`PooledRsmq`], and [`RsmqSync`]. Implemented as a single Lua script (`moveMessage.lua`) that preserves the message body and the `:rc` / `:fr` metadata, sets the destination score to the current time (so the message is visible immediately in the DLQ), and bumps the destination's `totalsent` counter. Returns `false` if the message no longer exists in the source. Rejects self-loops (`src == dst`).
- Worker DLQ integration:
  - `WorkerBuilder::dlq(queue, max_failures)` — global default DLQ for all routes.
  - `WorkerBuilder::dlq_for(route, queue, max_failures)` — per-route override.
  - `max_failures = 0` ⇒ DLQ on first failure; `max_failures = N` ⇒ DLQ on the (N+1)-th failure (compares against the message's `rc`).
  - Self-loop validation at build time (`InvalidFormat` if a route's DLQ is itself).
  - Worker uses `Rsmq::move_message` so the DLQ transfer is atomic, never producing duplicate or lost messages.
  - The DLQ must already exist (`create_queue` it before building the worker); this is documented but not validated at build time.
- Tests `tests/move_message.rs` (4) and 5 new worker DLQ tests in `tests/worker.rs`.

### Note on the trait surface
- `move_message` is **not** added to the `RsmqConnection` trait in this release — only as inherent methods on the concrete facades. Promoting it to the trait requires either a default impl (no clean atomic option) or a major bump. Deferred to a future release.

## 17.2.0 - 2026-05-10

### Added
- New `worker` Cargo feature (**default**, tokio-only) with [`Worker`] and [`WorkerBuilder`]:
  - Builder + `run` / `run_until(shutdown_future)` API. Queue-name router (`.route("emails", handler).route("billing", handler)`).
  - Each handler is a `Fn(RsmqMessage<T>) -> impl Future<Output = Result<(), E>>` where `T: TryFrom<RedisBytes>` and `E: std::error::Error`. Decode and handler errors are surfaced and the message is left in the queue for redelivery.
  - Automatic visibility heartbeat: while a handler runs, the worker periodically calls `change_message_visibility` so slow handlers don't get redelivered. Configurable interval and extension amount.
  - Optional `use_realtime` mode that subscribes to `{ns}:rt:*` PUBLISHes for low-latency wake-up instead of strict polling.
  - Single-task by design — for parallelism, run multiple `Worker` instances.
- New example `examples/worker_helper.rs`.
- New integration tests `tests/worker.rs` covering successful processing, handler errors leaving messages for redelivery, queue-name routing, empty-routes rejection, and the heartbeat keeping a slow handler safe.
- New direct dependency on `futures-util` (optional, gated on `worker`) for the pubsub stream.

## 17.1.0 - 2026-05-10

### Added
- `RsmqConnection::send_message_batch` and `RsmqConnection::receive_message_batch` (and their `RsmqConnectionSync` counterparts), with default impls that loop the singular methods. Implementations on `Rsmq`, `PooledRsmq`, and `RsmqSync` override them with new Lua scripts (`sendMessageBatch.lua`, `receiveMessageBatch.lua`) for full atomicity:
  - Batch send: all messages land or none do; one realtime `PUBLISH` per call (with the post-batch queue size) instead of one per message.
  - Batch receive: up to `max_count` messages picked in one `ZRANGE BYSCORE` and updated in a single script. Phantom entries (sorted-set members with no body) are skipped silently, mirroring the singular receive logic.
- New integration tests in `tests/batch.rs` covering ID minting, empty inputs, max-size enforcement, partial fills, and end-to-end receive+delete.

## 17.0.0 - 2026-05-10

### Breaking
- New `RsmqError::JsonError(Different<serde_json::Error>)` variant, gated on the `serde` feature (which is now a default feature). Exhaustive `match` arms on `RsmqError` will need an additional branch (or `_` arm). Disable with `default-features = false` to opt out of `serde` / `serde_json`.

### Added
- New `serde` Cargo feature (**default**) with two coexisting JSON-message APIs:
  - `Json<T>` wrapper, plugged into the existing `send_message` / `receive_message` / `pop_message`. `From<Json<T>> for RedisBytes` panics on serialization failure (see docs); `TryFrom<RedisBytes> for Json<T>` returns the original bytes on parse failure (surfaces as `RsmqError::CannotDecodeMessage`).
  - `RsmqJsonExt` extension trait with `send_json` / `receive_json` / `pop_json`, blanket-implemented for any `RsmqConnection`. Surfaces serde errors as `RsmqError::JsonError`.
  - Sync counterpart `RsmqJsonExtSync` available with `serde` + `sync` features.
- `examples/` directory with `worker_loop`, `retry_with_backoff`, `custom_type`, `long_running_heartbeat`, and `serde_message`.
- New integration tests `tests/serde.rs` covering both JSON APIs and their error paths.

### Changed
- `RsmqMessage<T>`: dropped the `T: TryFrom<RedisBytes>` bound from the struct definition itself. The bound is still enforced at receive call sites; relaxing it lets `RsmqMessage<T>` carry types like JSON-decoded structs that don't directly implement the conversion. Non-breaking on its own (relaxes a constraint).
- Crate-level rustdoc rewritten: removed duplicated examples and stale references to `async-std-comp` and pre-v16 versions, fixed the broken `deps.rs` badge, tightened doctest examples.
- `Cargo.toml`: fixed `homepage` and `repository` to point at `DavidBM/rsmq-async-rs` (were pointing at the original `Couragium` fork).

### Fixed
- Test harness (`tests/support/mod.rs`) bounds the connect-retry to ~3 seconds and panics with actionable guidance (Docker / apt / `REDIS_URL` instructions) instead of stalling silently when Redis is unreachable.

### Internal
- CI rewritten: Redis service container, feature matrix (`serde` × `break-js-comp`, plus a no-`serde` row), separate `smol-comp` build job, dedicated lint job running `cargo fmt --check` and `cargo clippy --all-targets --all-features -D warnings`.
- `RsmqConnectionSync` trait declaration is now `#[cfg(feature = "sync")]` to match its already-gated re-export (no public surface change).
- `cargo fmt` sweep across the repo.

## 16.0.0 - 2026-03-29

### Fixed
- Queue name validation: corrected boolean logic (`&&` → `||`) and added missing character check.
- Unsafe slice indexing in `pop_message` / `receive_message` replaced with safe `get()` + `unwrap_or`.
- `InvalidValue` error format string used the same positional argument three times.
- `create_queue` is now atomic: the `SADD` for the queue index is included in the same `MULTI`/`EXEC` pipeline as the `HSETNX` operations.
- `receiveMessage.lua`: replaced deprecated `ZRANGEBYSCORE` with `ZRANGE BYSCORE`, and added a nil-body guard before any side effects.
- Typo fixes: `BugCreatingRandonValue` → `BugCreatingRandomValue`.

### Added
- `getQueueAttributes.lua`: atomic `TIME` + `HMGET` + `ZCARD` + `ZCOUNT` in a single Lua script.
- 16 new integration tests covering name validation, `maxsize` boundaries, message size limits, attribute counters, partial updates, the phantom-message guard, and error cases.

### Changed
- `maxsize` is now `i64` (was `i32`) for consistency with Lua integer returns. **Breaking** for callers that named the type explicitly.

## 15.0.0 - 2026-03-29

### Added
- `RsmqConnectionSync` trait and re-export, behind the existing `sync` feature.
- `smol-comp` feature flag for the smol runtime.

### Changed
- **Breaking:** `redis` upgraded from `^0.28` to `^1`. Uses the new `ConnectionInfo` / `RedisConnectionInfo` builder API.
- **Breaking:** `async-std-comp` feature replaced by `smol-comp`.
- `rand` upgraded to `^0.10`.

### Removed
- `keep-alive` feature (removed in `redis` 1.x).

## 14.0.0

Internal cleanup and formatting.

## 13.0.0

### Changed
- **Breaking:** `redis` upgraded to `^0.27`.
- Dependency updates.

## 12.0.0

### Changed
- Lua scripts are now loaded once via `SCRIPT LOAD` instead of being sent on every call.
- Optional millisecond-precision time mode added behind the `break-js-comp` feature flag (disabled by default to preserve JS wire compatibility).

## 11.2.0

### Added
- `RsmqSync` synchronous facade.

## 10.0.0 - 2024-05-08

### Changed
- **Breaking:** Queue name format aligned with the original Node.js RSMQ for wire compatibility ([PR #20](https://github.com/DavidBM/rsmq-async-rs/pull/20)). Empty your previous queues before upgrading — earlier-format queues won't be readable by v10+.
