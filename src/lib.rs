//! # RSMQ in async Rust
//!
//! RSMQ port to async rust. RSMQ is a simple redis queue system that works in any redis v2.6+. It contains the same
//! methods as the original one in [https://github.com/smrchy/rsmq](https://github.com/smrchy/rsmq)
//!
//! [![Crates.io](https://img.shields.io/crates/v/rsmq_async)](https://crates.io/crates/rsmq_async)
//! [![Crates.io](https://img.shields.io/crates/l/rsmq_async)](https://choosealicense.com/licenses/mit/)
//! [![dependency status](https://deps.rs/crate/rsmq_async/4.0.0/status.svg)](https://deps.rs/crate/rsmq_async)
//! [![Docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square)](https://docs.rs/rsmq_async)
//!
//! ## Example
//!
//! ```rust
//! # use rsmq_async::RsmqError; use rsmq_async::{Rsmq, RsmqConnection};
//!
//! # async fn it_works() -> Result<(), RsmqError> { let mut rsmq = Rsmq::new(Default::default()).await?;
//!
//! let message = rsmq.receive_message::<String>("myqueue", None).await?;
//!
//! if let Some(message) = message { rsmq.delete_message("myqueue", &message.id).await?; }
//!
//! # Ok(())
//! # }
//!
//! ```
//!
//! Main object documentation are in: [`Rsmq`] and[`PooledRsmq`] and they both implement the trait
//! [`RsmqConnection`] where you can see all the RSMQ methods. Make sure you always import the trait
//! [`RsmqConnection`].
//!
//! ## Installation
//!
//! Check [https://crates.io/crates/rsmq_async](https://crates.io/crates/rsmq_async)
//!
//! ## Example
//!
//! ```rust
//!
//! use rsmq_async::{Rsmq, RsmqConnection};
//!
//! async fn it_works() { let mut rsmq = Rsmq::new(Default::default())
//!         .await
//!         .expect("connection failed");
//!
//!     rsmq.create_queue("myqueue", None, None, None)
//!         .await
//!         .expect("failed to create queue");
//!
//!     rsmq.send_message("myqueue", "testmessage", None)
//!         .await
//!         .expect("failed to send message");
//!
//!     let message = rsmq
//!         .receive_message::<String>("myqueue", None)
//!         .await
//!         .expect("cannot receive message");
//!
//!     if let Some(message) = message { rsmq.delete_message("myqueue", &message.id).await; } }
//!
//! ```
//!
//! ## Realtime
//!
//! When initializing RSMQ you can enable the realtime PUBLISH for new messages. On every new message that gets sent to
//! RSQM via `sendMessage` a Redis PUBLISH will be issued to `{rsmq.ns}:rt:{qname}`. So, you can subscribe to it using
//! redis-rs library directly.
//!
//! ### How to use the realtime option
//!
//! Besides the PUBLISH redis command when a new message is sent to RSMQ nothing else will happen. Your app could use
//! the Redis SUBSCRIBE command to be notified of new messages and issue a `receiveMessage` then. However make sure not
//! to listen with multiple workers for new messages with SUBSCRIBE to prevent multiple simultaneous `receiveMessage`
//! calls.
//!
//! ## Guarantees
//!
//! If you want to implement "at least one delivery" guarantee, you need to receive the messages using "receive_message"
//! and then, once the message is successfully processed, delete it with "delete_message".
//!
//! ## Connection Pool
//!
//! If you want to use a connection pool, just use [`PooledRsmq`] instad of Rsmq. It implements the RsmqConnection trait
//! as the normal Rsmq.
//!
//! If you want to accept any of both implementation, just accept the trait [`RsmqConnection`]
//!
//! ## Executor compatibility
//!
//! By default it will intruct redis-rs library to enable async-std and tokio compatibility and choose Tokio
//! if Tokio is avaialble, async-std if not. If you want to choose, you can change the `Cargo.toml` definition to
//!
//! ```toml
//!
//! rsmq_async = { version = "9", default-features = false, features = ["tokio-comp"] }
//!
//! ```
//!
//! Where `"tokio-comp"` can also be `"async-std-comp"`.
//!
//! ## `Rsmq` vs `PooledRsmq`
//!
//! In almost all workloads you might prefer the `Rsmq` object, as it works with a multiplexed connection.
//!
//! For specific workloads, where you might be sending a lof of data (images, documents, big blobs) you might prefer to
//! use the `PooledRsmq` and configure it with `PoolOptions`.
//!
//! They both use the `redis::aio::MultiplexedConnection`, but the pooled connection can be configured to spawn several
//! of those, so one operation won't block the other.
//!
//! ## Response types
//!
//! There are 3 functions that take generic types:
//!
//! - `pop_message` and `receive_message`: Where the type for the received message is `RsmqMessage<E>` where `E:
//!   TryFrom<RedisBytes, Error = Vec<u8>>`. So, If you have custom type, you can implement the trait
//!   `TryFrom<RedisBytes>` for `YourCustomType` and use it like: `rsmq.receive_message::<YourCustomType>
//!   ("myqueue", None)`. Implementations are provided for `String` and `Vec<u8>`.
//! - `send_message` where the message to send needs to implement `Into<RedisBytes> + Send`. So you will need to
//!   implement the trait for your type. You can check the implementations for the type RedisBytes and see how we did
//!   it. Implementations are provided for `String`, `&str` and `Vec<u8>`.
//!
//! All this is because strings in Rust are very convenient to use for json messages, so always returning a `Vec<u8>`
//! may not be the most ergonomic solution. But at the same time, we can just add some already made implementations for
//! it and you can just use it with your type or, if you are sending, let's say, images, just use the method like:
//! `rsmq.receive_message::<Vec<u8>>("myqueue", None)` and transform it later to your type. (Or just implement the
//! `TryFrom<RedisBytes>` for your type and the transformation will be automatic.)
//!
//! ### Example for implementing a custom type
//!
//! ```rust,ignore
//!
//! impl TryFrom<RedisBytes> for String {
//!
//!     type Error = Vec<u8>; // Always set Error as Vec<u8>;
//!
//!     fn try_from(bytes: RedisBytes) -> Result<Self, Self::Error> {
//!         String::from_utf8(bytes.0).map_err(|e| e.into_bytes())
//!     }
//!
//! }
//!
//! ```
//!

#![forbid(unsafe_code)]

mod error;
mod functions;
mod multiplexed_facade;
mod pooled_facade;
mod r#trait;
mod types;

pub use error::RsmqError;
pub use error::RsmqResult;
pub use multiplexed_facade::Rsmq;
pub use pooled_facade::{PoolOptions, PooledRsmq, RedisConnectionManager};
pub use r#trait::RsmqConnection;
pub use types::RedisBytes;
pub use types::RsmqMessage;
pub use types::RsmqOptions;
pub use types::RsmqQueueAttributes;
