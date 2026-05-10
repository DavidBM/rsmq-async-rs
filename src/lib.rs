//! # RSMQ in async Rust
//!
//! Async Rust port of [RSMQ](https://github.com/smrchy/rsmq) — a lightweight message queue
//! built on Redis sorted sets and atomic Lua scripts. No extra brokers, just Redis. Wire-compatible
//! with the original JavaScript implementation, so producers and consumers can mix languages.
//!
//! See the [README](https://github.com/DavidBM/rsmq-async-rs) for the full guide and examples.
//!
//! ## Quick start
//!
//! ```no_run
//! use rsmq_async::{Rsmq, RsmqConnection, RsmqError};
//!
//! # async fn _example() -> Result<(), RsmqError> {
//! let mut rsmq = Rsmq::new(Default::default()).await?;
//!
//! rsmq.create_queue("jobs", None, None, None).await?;
//! rsmq.send_message("jobs", "hello", None).await?;
//!
//! if let Some(msg) = rsmq.receive_message::<String>("jobs", None).await? {
//!     // ...process msg.message...
//!     rsmq.delete_message("jobs", &msg.id).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Always call [`RsmqConnection::delete_message`] after a successful receive — that's what
//! confirms delivery. Otherwise the message becomes visible again after the queue's `vt`.
//!
//! ## Implementations
//!
//! All three implement the same [`RsmqConnection`] trait, so write code against the trait
//! to stay implementation-agnostic.
//!
//! - [`Rsmq`] — multiplexed connection. **Start here.** Right for almost all workloads.
//! - [`PooledRsmq`] — connection pool. Use for large payloads where a slow op would block others.
//! - [`RsmqSync`] — sync wrapper for non-async contexts (requires the `sync` feature).
//!
//! ## Realtime notifications
//!
//! Set `realtime: true` in [`RsmqOptions`] and RSMQ will `PUBLISH` to `{ns}:rt:{qname}` on every
//! `send_message`. Subscribe with `redis-rs` to wake workers immediately instead of polling.
//! Use a single subscriber per queue — multiple workers racing on `SUBSCRIBE` is a common mistake.
//!
//! ## Custom message types
//!
//! [`Rsmq::send_message`] takes any `Into<RedisBytes>`; [`Rsmq::receive_message`] / [`Rsmq::pop_message`]
//! take any `TryFrom<RedisBytes, Error = Vec<u8>>`. Built-in impls cover `String`, `&str`, `Vec<u8>`,
//! and `&[u8]`. For your own types, implement the conversions:
//!
//! ```no_run
//! use rsmq_async::RedisBytes;
//!
//! struct MyPayload(Vec<u8>);
//!
//! impl From<MyPayload> for RedisBytes {
//!     fn from(p: MyPayload) -> RedisBytes { RedisBytes::from(p.0) }
//! }
//!
//! impl TryFrom<RedisBytes> for MyPayload {
//!     type Error = Vec<u8>;
//!     fn try_from(b: RedisBytes) -> Result<Self, Vec<u8>> {
//!         Ok(MyPayload(b.into_bytes()))
//!     }
//! }
//! ```
//!
//! ## Cargo features
//!
//! - `tokio-comp` *(default)* — Tokio runtime support.
//! - `smol-comp` — smol runtime support. Disable defaults to use it standalone.
//! - `sync` *(default)* — enables [`RsmqSync`] and [`RsmqConnectionSync`].
//! - `serde` *(default)* — enables [`Json<T>`](Json), [`RsmqJsonExt`], and the
//!   `RsmqError::JsonError` variant. Pulls in `serde` and `serde_json`.
//! - `worker` *(default)* — enables [`Worker`], a polling/heartbeat/realtime-aware async
//!   worker helper with a queue-name router. Tokio-only.
//! - `break-js-comp` — full microsecond-precision scores. Off by default to stay wire-compatible
//!   with the JS library; **don't mix** a `break-js-comp` Rust producer with a JS server on the
//!   same queue.
//!

#![forbid(unsafe_code)]

mod error;
mod functions;
#[cfg(feature = "serde")]
mod json;
mod multiplexed_facade;
mod pooled_facade;
#[cfg(feature = "sync")]
mod sync_facade;
mod r#trait;
mod types;
#[cfg(feature = "worker")]
mod worker;

pub use error::RsmqError;
pub use error::RsmqResult;
#[cfg(all(feature = "serde", feature = "sync"))]
pub use json::RsmqJsonExtSync;
#[cfg(feature = "serde")]
pub use json::{Json, RsmqJsonExt};
pub use multiplexed_facade::Rsmq;
pub use pooled_facade::{PoolOptions, PooledRsmq, RedisConnectionManager};
pub use r#trait::RsmqConnection;
#[cfg(feature = "sync")]
pub use r#trait::RsmqConnectionSync;
#[cfg(feature = "sync")]
pub use sync_facade::RsmqSync;
pub use types::RedisBytes;
pub use types::RsmqMessage;
pub use types::RsmqOptions;
pub use types::RsmqQueueAttributes;
#[cfg(feature = "worker")]
pub use worker::{DecodeError, Worker, WorkerBuilder};
