//! # rbmq — Redis Better Message Queue
//!
//! A small, atomic, Redis-backed message queue. All semantics live in Lua scripts shared with the
//! Node.js client of the same name, so a Rust producer and a Node consumer (or vice versa) can
//! share a queue with no translation layer. Successor to `rsmq_async`; not wire-compatible with
//! the original `smrchy/rsmq` (use `rsmq_async` v17/v18 if you need that).
//!
//! See the [README](https://github.com/DavidBM/rsmq-async-rs) for the full guide and examples.
//!
//! ## Quick start
//!
//! ```no_run
//! use rbmq::{Rbmq, RbmqConnection, RbmqError};
//!
//! # async fn _example() -> Result<(), RbmqError> {
//! let mut rbmq = Rbmq::new(Default::default()).await?;
//!
//! rbmq.create_queue("jobs", None, None, None).await?;
//! rbmq.send_message("jobs", "hello", None).await?;
//!
//! if let Some(msg) = rbmq.receive_message::<String>("jobs", None).await? {
//!     // ...process msg.message...
//!     rbmq.delete_message("jobs", &msg.id).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Always call [`RbmqConnection::delete_message`] after a successful receive — that's what
//! confirms delivery. Otherwise the message becomes visible again after the queue's `vt`.
//!
//! ## Implementations
//!
//! All three implement the same [`RbmqConnection`] trait, so write code against the trait
//! to stay implementation-agnostic.
//!
//! - [`Rbmq`] — multiplexed connection. **Start here.** Right for almost all workloads.
//! - [`PooledRbmq`] — connection pool. Use for large payloads where a slow op would block others.
//! - [`RbmqSync`] — sync wrapper for non-async contexts (requires the `sync` feature).
//!
//! ## Realtime notifications
//!
//! Set `realtime: true` in [`RbmqOptions`] and rbmq will `PUBLISH` to `{ns}:rt:{qname}` on every
//! `send_message`. Subscribe with `redis-rs` to wake workers immediately instead of polling.
//! Use a single subscriber per queue — multiple workers racing on `SUBSCRIBE` is a common mistake.
//!
//! ## Custom message types
//!
//! [`Rbmq::send_message`] takes any `Into<RedisBytes>`; [`Rbmq::receive_message`] / [`Rbmq::pop_message`]
//! take any `TryFrom<RedisBytes, Error = Vec<u8>>`. Built-in impls cover `String`, `&str`, `Vec<u8>`,
//! and `&[u8]`. For your own types, implement the conversions:
//!
//! ```no_run
//! use rbmq::RedisBytes;
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
//! - `sync` *(default)* — enables [`RbmqSync`] and [`RbmqConnectionSync`].
//! - `serde` *(default)* — enables [`Json<T>`](Json), [`RbmqJsonExt`], and the
//!   `RbmqError::JsonError` variant. Pulls in `serde` and `serde_json`.
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

pub use error::RbmqError;
pub use error::RbmqResult;
#[cfg(all(feature = "serde", feature = "sync"))]
pub use json::RbmqJsonExtSync;
#[cfg(feature = "serde")]
pub use json::{Json, RbmqJsonExt};
pub use multiplexed_facade::Rbmq;
pub use pooled_facade::{PoolOptions, PooledRbmq, RedisConnectionManager};
pub use r#trait::RbmqConnection;
#[cfg(feature = "sync")]
pub use r#trait::RbmqConnectionSync;
#[cfg(feature = "sync")]
pub use sync_facade::RbmqSync;
pub use types::RedisBytes;
pub use types::RbmqMessage;
pub use types::RbmqOptions;
pub use types::RbmqQueueAttributes;
#[cfg(feature = "worker")]
pub use worker::{DecodeError, Worker, WorkerBuilder};
