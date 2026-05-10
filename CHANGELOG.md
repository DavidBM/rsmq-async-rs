# Changelog

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
