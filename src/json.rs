//! Optional `serde`/`serde_json` integration. Two ways to ship typed messages:
//!
//! - [`Json<T>`] — wrapper that JSON-encodes on send and decodes on receive, plugged into the
//!   existing [`RbmqConnection::send_message`] / [`RbmqConnection::receive_message`] APIs.
//! - [`RbmqJsonExt`] — extension trait adding `send_json` / `receive_json` / `pop_json`
//!   methods directly on any [`RbmqConnection`]. Returns proper `Result` on serde errors.
//!
//! Both require the `serde` Cargo feature.

use crate::types::RedisBytes;
use crate::{RbmqConnection, RbmqMessage, RbmqResult};
use core::convert::TryFrom;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::time::Duration;

/// Newtype wrapping a `T` so it can be sent and received as JSON through the existing
/// [`RbmqConnection`] API.
///
/// ```no_run
/// # use rbmq::{Json, Rbmq, RbmqConnection, RbmqError};
/// # use serde::{Serialize, Deserialize};
/// #[derive(Serialize, Deserialize)]
/// struct Job { name: String }
///
/// # async fn _example() -> Result<(), RbmqError> {
/// let mut rbmq = Rbmq::new(Default::default()).await?;
/// rbmq.send_message("jobs", Json(Job { name: "hi".into() }), None).await?;
///
/// if let Some(msg) = rbmq.receive_message::<Json<Job>>("jobs", None).await? {
///     println!("{}", msg.message.0.name);
///     rbmq.delete_message("jobs", &msg.id).await?;
/// }
/// # Ok(()) }
/// ```
///
/// # Panics
///
/// `From<Json<T>> for RedisBytes` panics if `serde_json::to_vec` fails — only possible for
/// values that aren't representable as JSON (maps with non-string keys, NaN floats, custom
/// `Serialize` impls that error). For fallible serialization use [`RbmqJsonExt::send_json`]
/// instead, which surfaces the error as `Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Json<T>(pub T);

impl<T: Serialize> From<Json<T>> for RedisBytes {
    fn from(json: Json<T>) -> RedisBytes {
        let bytes = serde_json::to_vec(&json.0).expect(
            "Json<T> -> RedisBytes failed; use RbmqJsonExt::send_json for fallible serialization",
        );
        RedisBytes::from(bytes)
    }
}

impl<T: DeserializeOwned> TryFrom<RedisBytes> for Json<T> {
    type Error = Vec<u8>;

    fn try_from(bytes: RedisBytes) -> Result<Self, Self::Error> {
        let raw = bytes.into_bytes();
        match serde_json::from_slice(&raw) {
            Ok(v) => Ok(Json(v)),
            Err(_) => Err(raw),
        }
    }
}

/// Extension trait adding `send_json` / `receive_json` / `pop_json` to any [`RbmqConnection`].
///
/// Surfaces serde errors as [`RbmqError::JsonError`](crate::RbmqError::JsonError), unlike the
/// [`Json<T>`] wrapper whose `TryFrom` impl drops the error in favor of returning the raw bytes.
///
/// ```no_run
/// # use rbmq::{Rbmq, RbmqConnection, RbmqError, RbmqJsonExt};
/// # use serde::{Serialize, Deserialize};
/// #[derive(Serialize, Deserialize)]
/// struct Job { id: u64 }
///
/// # async fn _example() -> Result<(), RbmqError> {
/// let mut rbmq = Rbmq::new(Default::default()).await?;
/// rbmq.send_json("jobs", &Job { id: 1 }, None).await?;
/// if let Some(msg) = rbmq.receive_json::<Job>("jobs", None).await? {
///     rbmq.delete_message("jobs", &msg.id).await?;
/// }
/// # Ok(()) }
/// ```
pub trait RbmqJsonExt: RbmqConnection {
    fn send_json<T: Serialize + ?Sized>(
        &mut self,
        qname: &str,
        message: &T,
        delay: Option<Duration>,
    ) -> impl Future<Output = RbmqResult<String>> + Send;

    fn receive_json<T: DeserializeOwned>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> impl Future<Output = RbmqResult<Option<RbmqMessage<T>>>> + Send;

    fn pop_json<T: DeserializeOwned>(
        &mut self,
        qname: &str,
    ) -> impl Future<Output = RbmqResult<Option<RbmqMessage<T>>>> + Send;
}

impl<C: RbmqConnection + Send> RbmqJsonExt for C {
    fn send_json<T: Serialize + ?Sized>(
        &mut self,
        qname: &str,
        message: &T,
        delay: Option<Duration>,
    ) -> impl Future<Output = RbmqResult<String>> + Send {
        let serialized = serde_json::to_vec(message);
        async move {
            let bytes = serialized?;
            self.send_message(qname, bytes, delay).await
        }
    }

    async fn receive_json<T: DeserializeOwned>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RbmqResult<Option<RbmqMessage<T>>> {
        decode_json(self.receive_message::<Vec<u8>>(qname, hidden).await?)
    }

    async fn pop_json<T: DeserializeOwned>(
        &mut self,
        qname: &str,
    ) -> RbmqResult<Option<RbmqMessage<T>>> {
        decode_json(self.pop_message::<Vec<u8>>(qname).await?)
    }
}

fn decode_json<T: DeserializeOwned>(
    raw: Option<RbmqMessage<Vec<u8>>>,
) -> RbmqResult<Option<RbmqMessage<T>>> {
    match raw {
        None => Ok(None),
        Some(msg) => {
            let message: T = serde_json::from_slice(&msg.message)?;
            Ok(Some(RbmqMessage {
                id: msg.id,
                message,
                rc: msg.rc,
                fr: msg.fr,
                sent: msg.sent,
            }))
        }
    }
}

#[cfg(feature = "sync")]
mod sync {
    use super::decode_json;
    use crate::r#trait::RbmqConnectionSync;
    use crate::{RbmqMessage, RbmqResult};
    use serde::{de::DeserializeOwned, Serialize};
    use std::time::Duration;

    /// Sync counterpart of [`RbmqJsonExt`](super::RbmqJsonExt). Available with both `serde` and
    /// `sync` features.
    pub trait RbmqJsonExtSync: RbmqConnectionSync {
        fn send_json<T: Serialize + ?Sized>(
            &mut self,
            qname: &str,
            message: &T,
            delay: Option<Duration>,
        ) -> RbmqResult<String>;

        fn receive_json<T: DeserializeOwned>(
            &mut self,
            qname: &str,
            hidden: Option<Duration>,
        ) -> RbmqResult<Option<RbmqMessage<T>>>;

        fn pop_json<T: DeserializeOwned>(
            &mut self,
            qname: &str,
        ) -> RbmqResult<Option<RbmqMessage<T>>>;
    }

    impl<C: RbmqConnectionSync> RbmqJsonExtSync for C {
        fn send_json<T: Serialize + ?Sized>(
            &mut self,
            qname: &str,
            message: &T,
            delay: Option<Duration>,
        ) -> RbmqResult<String> {
            let bytes = serde_json::to_vec(message)?;
            self.send_message(qname, bytes, delay)
        }

        fn receive_json<T: DeserializeOwned>(
            &mut self,
            qname: &str,
            hidden: Option<Duration>,
        ) -> RbmqResult<Option<RbmqMessage<T>>> {
            decode_json(self.receive_message::<Vec<u8>>(qname, hidden)?)
        }

        fn pop_json<T: DeserializeOwned>(
            &mut self,
            qname: &str,
        ) -> RbmqResult<Option<RbmqMessage<T>>> {
            decode_json(self.pop_message::<Vec<u8>>(qname)?)
        }
    }
}

#[cfg(feature = "sync")]
pub use sync::RbmqJsonExtSync;
