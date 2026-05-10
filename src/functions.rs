use crate::types::RedisBytes;
use crate::{
    types::{QueueDescriptor, RbmqMessage, RbmqQueueAttributes},
    RbmqError, RbmqResult,
};
use core::convert::TryFrom;
use radix_fmt::radix_36;
use rand::seq::IteratorRandom;
use redis::{aio::ConnectionLike, pipe};
use std::convert::TryInto;
use std::time::Duration;

const JS_COMPAT_MAX_TIME_MILLIS: u64 = 9_999_999_000;

// With break-js-comp, scores are in microseconds; scale ms durations before adding to ts.
#[cfg(feature = "break-js-comp")]
const DURATION_SCALE: u64 = 1000;
#[cfg(not(feature = "break-js-comp"))]
const DURATION_SCALE: u64 = 1;

// Flag passed to getQueueAttributes Lua: 1 = microsecond scores, 0 = millisecond scores.
#[cfg(feature = "break-js-comp")]
const USE_MICROSECONDS: u64 = 1;
#[cfg(not(feature = "break-js-comp"))]
const USE_MICROSECONDS: u64 = 0;

/// The main object of this library. Creates/Handles the redis connection and contains all the methods
#[derive(Clone)]
pub struct RbmqFunctions<T: ConnectionLike> {
    pub(crate) ns: String,
    pub(crate) realtime: bool,
    pub(crate) conn: std::marker::PhantomData<T>,
}

impl<T: ConnectionLike> std::fmt::Debug for RbmqFunctions<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RbmqFunctions")
    }
}

#[derive(Debug, Clone)]
pub struct CachedScript {
    change_message_visibility_sha1: String,
    receive_message_sha1: String,
    get_queue_attributes_sha1: String,
    send_message_batch_sha1: String,
    receive_message_batch_sha1: String,
    move_message_sha1: String,
    receive_message_or_dlq_sha1: String,
}

impl CachedScript {
    async fn init<T: ConnectionLike>(conn: &mut T) -> RbmqResult<Self> {
        let change_message_visibility_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/changeMessageVisibility.lua"))
            .query_async(conn)
            .await?;
        let receive_message_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/receiveMessage.lua"))
            .query_async(conn)
            .await?;
        let get_queue_attributes_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/getQueueAttributes.lua"))
            .query_async(conn)
            .await?;
        let send_message_batch_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/sendMessageBatch.lua"))
            .query_async(conn)
            .await?;
        let receive_message_batch_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/receiveMessageBatch.lua"))
            .query_async(conn)
            .await?;
        let move_message_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/moveMessage.lua"))
            .query_async(conn)
            .await?;
        let receive_message_or_dlq_sha1: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(include_str!("../scripts/receiveMessageOrDlq.lua"))
            .query_async(conn)
            .await?;
        Ok(Self {
            change_message_visibility_sha1,
            receive_message_sha1,
            get_queue_attributes_sha1,
            send_message_batch_sha1,
            receive_message_batch_sha1,
            move_message_sha1,
            receive_message_or_dlq_sha1,
        })
    }

    async fn invoke_change_message_visibility<R, T: ConnectionLike>(
        &self,
        conn: &mut T,
        key1: String,
        key2: String,
        key3: String,
    ) -> RbmqResult<R>
    where
        R: redis::FromRedisValue,
    {
        redis::cmd("EVALSHA")
            .arg(&self.change_message_visibility_sha1)
            .arg(3)
            .arg(key1)
            .arg(key2)
            .arg(key3)
            .query_async(conn)
            .await
            .map_err(Into::into)
    }

    async fn invoke_receive_message<R, T: ConnectionLike>(
        &self,
        conn: &mut T,
        key1: String,
        key2: String,
        key3: String,
        should_delete: String,
    ) -> RbmqResult<R>
    where
        R: redis::FromRedisValue,
    {
        redis::cmd("EVALSHA")
            .arg(&self.receive_message_sha1)
            .arg(3)
            .arg(key1)
            .arg(key2)
            .arg(key3)
            .arg(should_delete)
            .query_async(conn)
            .await
            .map_err(Into::into)
    }

    async fn invoke_get_queue_attributes<R, T: ConnectionLike>(
        &self,
        conn: &mut T,
        key_hash: String,
        key_set: String,
        time_multiplier: u64,
    ) -> RbmqResult<R>
    where
        R: redis::FromRedisValue,
    {
        redis::cmd("EVALSHA")
            .arg(&self.get_queue_attributes_sha1)
            .arg(2)
            .arg(key_hash)
            .arg(key_set)
            .arg(time_multiplier)
            .query_async(conn)
            .await
            .map_err(Into::into)
    }

    async fn invoke_send_message_batch<T: ConnectionLike>(
        &self,
        conn: &mut T,
        queue_key: String,
        realtime: bool,
        items: Vec<(String, u64, Vec<u8>)>,
    ) -> RbmqResult<i64> {
        let mut cmd = redis::cmd("EVALSHA");
        cmd.arg(&self.send_message_batch_sha1)
            .arg(1)
            .arg(queue_key)
            .arg(if realtime { "1" } else { "0" });
        for (id, score, body) in items {
            cmd.arg(id).arg(score.to_string()).arg(body);
        }
        cmd.query_async(conn).await.map_err(Into::into)
    }

    async fn invoke_receive_message_batch<R, T: ConnectionLike>(
        &self,
        conn: &mut T,
        queue_key: String,
        now_ts: String,
        new_visibility_ts: String,
        should_delete: String,
        max_count: u32,
    ) -> RbmqResult<R>
    where
        R: redis::FromRedisValue,
    {
        redis::cmd("EVALSHA")
            .arg(&self.receive_message_batch_sha1)
            .arg(3)
            .arg(queue_key)
            .arg(now_ts)
            .arg(new_visibility_ts)
            .arg(should_delete)
            .arg(max_count)
            .query_async(conn)
            .await
            .map_err(Into::into)
    }

    async fn invoke_move_message<T: ConnectionLike>(
        &self,
        conn: &mut T,
        src_key: String,
        dst_key: String,
        msg_id: String,
        microseconds: bool,
    ) -> RbmqResult<i64> {
        redis::cmd("EVALSHA")
            .arg(&self.move_message_sha1)
            .arg(2)
            .arg(src_key)
            .arg(dst_key)
            .arg(msg_id)
            .arg(if microseconds { "1" } else { "0" })
            .query_async(conn)
            .await
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    async fn invoke_receive_message_or_dlq<R, T: ConnectionLike>(
        &self,
        conn: &mut T,
        src_key: String,
        now_ts: String,
        new_visibility_ts: String,
        dlq_key: String,
        max_receives: u64,
        microseconds: bool,
    ) -> RbmqResult<R>
    where
        R: redis::FromRedisValue,
    {
        redis::cmd("EVALSHA")
            .arg(&self.receive_message_or_dlq_sha1)
            .arg(4)
            .arg(src_key)
            .arg(now_ts)
            .arg(new_visibility_ts)
            .arg(dlq_key)
            .arg(max_receives)
            .arg(if microseconds { "1" } else { "0" })
            .query_async(conn)
            .await
            .map_err(Into::into)
    }
}

impl<T: ConnectionLike> RbmqFunctions<T> {
    /// Change the hidden time of a already sent message.
    pub async fn change_message_visibility(
        &self,
        conn: &mut T,
        qname: &str,
        message_id: &str,
        hidden: Duration,
        cached_script: &CachedScript,
    ) -> RbmqResult<()> {
        let hidden = get_redis_duration(Some(hidden), &Duration::from_secs(30));

        let queue = self.get_queue(conn, qname, false).await?;

        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        cached_script
            .invoke_change_message_visibility::<(), T>(
                conn,
                format!("{}:{}", self.ns, qname),
                message_id.to_string(),
                (queue.ts + hidden * DURATION_SCALE).to_string(),
            )
            .await?;

        Ok(())
    }

    pub async fn load_scripts(&self, conn: &mut T) -> RbmqResult<CachedScript> {
        CachedScript::init(conn).await
    }

    /// Creates a new queue. Attributes can be later modified with "set_queue_attributes" method
    ///
    /// hidden: Time the messages will be hidden when they are received with the "receive_message" method.
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited size)
    pub async fn create_queue(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<()> {
        valid_name_format(qname)?;

        let key = format!("{}:{}:cfg", self.ns, qname);
        let hidden = get_redis_duration(hidden, &Duration::from_secs(30));
        let delay = get_redis_duration(delay, &Duration::ZERO);
        let maxsize = maxsize.unwrap_or(65536);

        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
        number_in_range(delay, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
        if let Err(error) = number_in_range(maxsize, 1024, 65536) {
            if maxsize != -1 {
                // TODO: Create another error in order to explain that -1 is allowed
                return Err(error);
            }
        }

        let time: (u64, u64) = redis::cmd("TIME").query_async(conn).await?;

        let results: Vec<i64> = pipe()
            .atomic()
            .cmd("HSETNX")
            .arg(&key)
            .arg("vt")
            .arg(hidden)
            .cmd("HSETNX")
            .arg(&key)
            .arg("delay")
            .arg(delay)
            .cmd("HSETNX")
            .arg(&key)
            .arg("maxsize")
            .arg(maxsize)
            .cmd("HSETNX")
            .arg(&key)
            .arg("created")
            .arg(time.0)
            .cmd("HSETNX")
            .arg(&key)
            .arg("modified")
            .arg(time.0)
            .cmd("HSETNX")
            .arg(&key)
            .arg("totalrecv")
            .arg(0_i32)
            .cmd("HSETNX")
            .arg(&key)
            .arg("totalsent")
            .arg(0_i32)
            .cmd("SADD")
            .arg(format!("{}:QUEUES", self.ns))
            .arg(qname)
            .query_async(conn)
            .await?;

        if results[0] == 0 {
            return Err(RbmqError::QueueExists);
        }

        Ok(())
    }

    /// Deletes a message from the queue.
    ///
    /// Important to use when you are using receive_message.
    pub async fn delete_message(&self, conn: &mut T, qname: &str, id: &str) -> RbmqResult<bool> {
        let key = format!("{}:{}", self.ns, qname);

        let results: (u16, u16) = pipe()
            .atomic()
            .cmd("ZREM")
            .arg(&key)
            .arg(id)
            .cmd("HDEL")
            .arg(format!("{}:msg", &key))
            .arg(id)
            .arg(format!("{}:rc", id))
            .arg(format!("{}:fr", id))
            .query_async(conn)
            .await?;

        if results.0 == 1 && results.1 > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    /// Deletes the queue and all the messages on it
    pub async fn delete_queue(&self, conn: &mut T, qname: &str) -> RbmqResult<()> {
        let key = format!("{}:{}", self.ns, qname);

        let results: (u16, u16) = pipe()
            .atomic()
            .cmd("DEL")
            .arg(format!("{}:cfg", &key))
            .arg(format!("{}:msg", &key))
            .arg(&key)
            .cmd("SREM")
            .arg(format!("{}:QUEUES", self.ns))
            .arg(qname)
            .query_async(conn)
            .await?;

        if results.0 == 0 {
            return Err(RbmqError::QueueNotFound);
        }

        Ok(())
    }

    /// Returns the queue attributes and statistics
    pub async fn get_queue_attributes(
        &self,
        conn: &mut T,
        qname: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<RbmqQueueAttributes> {
        let key = format!("{}:{}", self.ns, qname);

        #[allow(clippy::type_complexity)]
        let result: (
            u64,
            u64,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            u64,
            u64,
        ) = cached_script
            .invoke_get_queue_attributes(conn, format!("{}:cfg", key), key, USE_MICROSECONDS)
            .await?;

        let (
            _time_sec,
            _time_usec,
            vt,
            delay,
            maxsize,
            totalrecv,
            totalsent,
            created,
            modified,
            msgs,
            hiddenmsgs,
        ) = result;

        if vt.is_none() {
            return Err(RbmqError::QueueNotFound);
        }

        Ok(RbmqQueueAttributes {
            vt: vt
                .map(|dur| Duration::from_millis(dur.try_into().unwrap_or(0)))
                .unwrap_or(Duration::ZERO),
            delay: delay
                .map(|dur| Duration::from_millis(dur.try_into().unwrap_or(0)))
                .unwrap_or(Duration::ZERO),
            maxsize: maxsize.unwrap_or(0),
            totalrecv: totalrecv.and_then(|v| v.try_into().ok()).unwrap_or(0),
            totalsent: totalsent.and_then(|v| v.try_into().ok()).unwrap_or(0),
            created: created.and_then(|v| v.try_into().ok()).unwrap_or(0),
            modified: modified.and_then(|v| v.try_into().ok()).unwrap_or(0),
            msgs,
            hiddenmsgs,
        })
    }

    /// Returns a list of queues in the namespace
    pub async fn list_queues(&self, conn: &mut T) -> RbmqResult<Vec<String>> {
        let queues = redis::cmd("SMEMBERS")
            .arg(format!("{}:QUEUES", self.ns))
            .query_async(conn)
            .await?;

        Ok(queues)
    }

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    pub async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        let queue = self.get_queue(conn, qname, false).await?;

        let result: (bool, String, Vec<u8>, u64, u64) = cached_script
            .invoke_receive_message(
                conn,
                format!("{}:{}", self.ns, qname),
                queue.ts.to_string(),
                queue.ts.to_string(),
                "true".to_string(),
            )
            .await?;

        if !result.0 {
            return Ok(None);
        }

        let message = E::try_from(RedisBytes(result.2)).map_err(RbmqError::CannotDecodeMessage)?;

        Ok(Some(RbmqMessage {
            id: result.1.clone(),
            message,
            rc: result.3,
            fr: result.4,
            sent: result
                .1
                .get(0..10)
                .and_then(|s| u64::from_str_radix(s, 36).ok())
                .unwrap_or(0),
        }))
    }

    /// Returns a message. The message stays hidden for some time (defined by "hidden"
    /// argument or the queue settings). After that time, the message will be redelivered.
    /// In order to avoid the redelivery, you need to use the "delete_message" after this function.
    pub async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        cached_script: &CachedScript,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        let queue = self.get_queue(conn, qname, false).await?;

        let hidden = get_redis_duration(hidden, &queue.vt);
        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let result: (bool, String, Vec<u8>, u64, u64) = cached_script
            .invoke_receive_message(
                conn,
                format!("{}:{}", self.ns, qname),
                queue.ts.to_string(),
                (queue.ts + hidden * DURATION_SCALE).to_string(),
                "false".to_string(),
            )
            .await?;

        if !result.0 {
            return Ok(None);
        }

        let message = E::try_from(RedisBytes(result.2)).map_err(RbmqError::CannotDecodeMessage)?;

        Ok(Some(RbmqMessage {
            id: result.1.clone(),
            message,
            rc: result.3,
            fr: result.4,
            sent: result
                .1
                .get(0..10)
                .and_then(|s| u64::from_str_radix(s, 36).ok())
                .unwrap_or(0),
        }))
    }

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or the queue settings) before being delivered to a client.
    pub async fn send_message<E: Into<RedisBytes>>(
        &self,
        conn: &mut T,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RbmqResult<String> {
        let queue = self.get_queue(conn, qname, true).await?;

        let delay = get_redis_duration(delay, &queue.delay);
        let key = format!("{}:{}", self.ns, qname);

        number_in_range(delay, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let message: RedisBytes = message.into();

        let msg_len: i64 = message
            .0
            .len()
            .try_into()
            .map_err(|_| RbmqError::MessageTooLong)?;

        if queue.maxsize != -1 && msg_len > queue.maxsize {
            return Err(RbmqError::MessageTooLong);
        }

        let queue_uid = match queue.uid {
            Some(uid) => uid,
            None => return Err(RbmqError::QueueNotFound),
        };

        let cfg_key = format!("{}:cfg", key);
        let msg_key = format!("{}:msg", key);

        let mut piping = pipe();

        let mut commands = piping
            .atomic()
            .cmd("ZADD")
            .arg(&key)
            .arg(queue.ts + delay * DURATION_SCALE)
            .arg(&queue_uid)
            .cmd("HSET")
            .arg(&msg_key)
            .arg(&queue_uid)
            .arg(message.0)
            .cmd("HINCRBY")
            .arg(&cfg_key)
            .arg("totalsent")
            .arg(1_u64);

        if self.realtime {
            commands = commands.cmd("ZCARD").arg(&key);
        }

        let result: Vec<i64> = commands.query_async(conn).await?;

        if self.realtime {
            redis::cmd("PUBLISH")
                .arg(format!("{}:rt:{}", self.ns, qname))
                .arg(result[3])
                .query_async::<()>(conn)
                .await?;
        }

        Ok(queue_uid)
    }

    /// Atomically inserts a batch of messages into the queue. Returns the assigned message IDs
    /// in input order. If realtime is enabled, fires a single PUBLISH with the new queue size
    /// after the script completes.
    pub async fn send_message_batch<E: Into<RedisBytes>>(
        &self,
        conn: &mut T,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
        cached_script: &CachedScript,
    ) -> RbmqResult<Vec<String>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let queue = self.get_queue(conn, qname, false).await?;

        let delay = get_redis_duration(delay, &queue.delay);
        number_in_range(delay, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let key = format!("{}:{}", self.ns, qname);
        let score = queue.ts + delay * DURATION_SCALE;

        // Convert messages, validate sizes, and mint per-message IDs. We mint IDs in Rust
        // (rather than Lua) so the random-suffix logic stays consistent with single send_message.
        let (sec, usec): (u64, u64) = redis::cmd("TIME").query_async(conn).await?;
        let time_us = sec * 1_000_000 + usec;
        let id_prefix = radix_36(time_us).to_string();

        let mut items: Vec<(String, u64, Vec<u8>)> = Vec::with_capacity(messages.len());
        let mut ids: Vec<String> = Vec::with_capacity(messages.len());
        for message in messages {
            let bytes: RedisBytes = message.into();
            let msg_len: i64 = bytes
                .0
                .len()
                .try_into()
                .map_err(|_| RbmqError::MessageTooLong)?;
            if queue.maxsize != -1 && msg_len > queue.maxsize {
                return Err(RbmqError::MessageTooLong);
            }
            let uid = format!("{}{}", id_prefix, RbmqFunctions::<T>::make_id(22)?);
            ids.push(uid.clone());
            items.push((uid, score, bytes.0));
        }

        let new_size = cached_script
            .invoke_send_message_batch(conn, key, self.realtime, items)
            .await?;

        if self.realtime {
            redis::cmd("PUBLISH")
                .arg(format!("{}:rt:{}", self.ns, qname))
                .arg(new_size)
                .query_async::<()>(conn)
                .await?;
        }

        Ok(ids)
    }

    /// Atomically receives up to `max_count` visible messages, sharing the same new
    /// visibility timestamp. Phantom entries (sorted-set members with no body) are skipped
    /// silently, mirroring the single-message receive logic. Returns at most `max_count`
    /// messages but may return fewer if not enough are visible.
    pub async fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
        cached_script: &CachedScript,
    ) -> RbmqResult<Vec<RbmqMessage<E>>> {
        if max_count == 0 {
            return Ok(Vec::new());
        }

        let queue = self.get_queue(conn, qname, false).await?;

        let hidden = get_redis_duration(hidden, &queue.vt);
        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let raw: Vec<(String, Vec<u8>, u64, u64)> = cached_script
            .invoke_receive_message_batch(
                conn,
                format!("{}:{}", self.ns, qname),
                queue.ts.to_string(),
                (queue.ts + hidden * DURATION_SCALE).to_string(),
                "false".to_string(),
                max_count,
            )
            .await?;

        let mut out = Vec::with_capacity(raw.len());
        for (id, body, rc, fr) in raw {
            let message = E::try_from(RedisBytes(body)).map_err(RbmqError::CannotDecodeMessage)?;
            let sent = id
                .get(0..10)
                .and_then(|s| u64::from_str_radix(s, 36).ok())
                .unwrap_or(0);
            out.push(RbmqMessage {
                id,
                message,
                rc,
                fr,
                sent,
            });
        }
        Ok(out)
    }

    /// Receives the next visible message, but transparently routes any message whose
    /// post-increment `rc` exceeds `max_receives` to `dlq` (preserving `:rc` and `:fr`)
    /// and tries the next one. Returns the first message whose `rc` is still within
    /// budget, or `None` if no eligible message was found within the script's safety
    /// iteration limit (currently 100 per call).
    ///
    /// `max_receives = 0` means "always route to DLQ" — the function will never deliver.
    /// `max_receives = N` means "deliver at most N times before routing to DLQ".
    ///
    /// `dlq` must be a queue that already exists; this function does not initialize the
    /// destination queue's configuration.
    pub async fn receive_message_or_dlq<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        dlq: &str,
        max_receives: u64,
        cached_script: &CachedScript,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        valid_name_format(qname)?;
        valid_name_format(dlq)?;
        if qname == dlq {
            return Err(RbmqError::InvalidFormat(format!(
                "qname and dlq must be different (got {qname:?})"
            )));
        }

        let queue = self.get_queue(conn, qname, false).await?;

        let hidden = get_redis_duration(hidden, &queue.vt);
        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let result: (bool, String, Vec<u8>, u64, u64) = cached_script
            .invoke_receive_message_or_dlq(
                conn,
                format!("{}:{}", self.ns, qname),
                queue.ts.to_string(),
                (queue.ts + hidden * DURATION_SCALE).to_string(),
                format!("{}:{}", self.ns, dlq),
                max_receives,
                USE_MICROSECONDS == 1,
            )
            .await?;

        if !result.0 {
            return Ok(None);
        }

        let message = E::try_from(RedisBytes(result.2)).map_err(RbmqError::CannotDecodeMessage)?;

        Ok(Some(RbmqMessage {
            id: result.1.clone(),
            message,
            rc: result.3,
            fr: result.4,
            sent: result
                .1
                .get(0..10)
                .and_then(|s| u64::from_str_radix(s, 36).ok())
                .unwrap_or(0),
        }))
    }

    /// Atomically moves a message from `src` to `dst`, preserving its body and the
    /// `:rc` / `:fr` metadata. The message becomes visible in `dst` immediately
    /// (score = current time). Returns `true` if the message was found in `src`,
    /// `false` if it didn't exist there.
    ///
    /// `dst` must be a queue that already exists (created via `create_queue`); this
    /// method does not initialize the destination queue's configuration, only inserts
    /// the message and bumps `totalsent`.
    pub async fn move_message(
        &self,
        conn: &mut T,
        src: &str,
        msg_id: &str,
        dst: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<bool> {
        valid_name_format(src)?;
        valid_name_format(dst)?;
        if src == dst {
            return Err(RbmqError::InvalidFormat(format!(
                "src and dst must be different (got {src:?})"
            )));
        }
        let src_key = format!("{}:{}", self.ns, src);
        let dst_key = format!("{}:{}", self.ns, dst);
        let moved: i64 = cached_script
            .invoke_move_message(
                conn,
                src_key,
                dst_key,
                msg_id.to_string(),
                USE_MICROSECONDS == 1,
            )
            .await?;
        Ok(moved == 1)
    }

    /// Modify the queue attributes. Keep in mind that "hidden" and "delay" can be overwritten when the message is sent. "hidden" can be changed by the method "change_message_visibility"
    ///
    /// hidden: Time the messages will be hidden when they are received with the "receive_message" method.
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited size)
    pub async fn set_queue_attributes(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
        cached_script: &CachedScript,
    ) -> RbmqResult<RbmqQueueAttributes> {
        self.get_queue(conn, qname, false).await?;

        let queue_name = format!("{}:{}:cfg", self.ns, qname);

        let time: (u64, u64) = redis::cmd("TIME").query_async(conn).await?;

        let mut pipe = pipe();
        pipe.atomic()
            .cmd("HSET")
            .arg(&queue_name)
            .arg("modified")
            .arg(time.0);

        if let Some(hidden) = hidden {
            let duration = get_redis_duration(Some(hidden), &Duration::from_secs(30));
            number_in_range(duration, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
            pipe.cmd("HSET").arg(&queue_name).arg("vt").arg(duration);
        }

        if let Some(delay) = delay {
            let delay = get_redis_duration(Some(delay), &Duration::ZERO);
            number_in_range(delay, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
            pipe.cmd("HSET").arg(&queue_name).arg("delay").arg(delay);
        }

        if let Some(maxsize) = maxsize {
            if let Err(error) = number_in_range(maxsize, 1024, 65536) {
                if maxsize != -1 {
                    // TODO: Create another error in order to explain that -1 is allowed
                    return Err(error);
                }
            }
            pipe.cmd("HSET")
                .arg(&queue_name)
                .arg("maxsize")
                .arg(maxsize);
        }

        pipe.query_async::<()>(conn).await?;

        self.get_queue_attributes(conn, qname, cached_script).await
    }

    async fn get_queue(&self, conn: &mut T, qname: &str, uid: bool) -> RbmqResult<QueueDescriptor> {
        let result: (Vec<Option<String>>, (u64, u64)) = pipe()
            .atomic()
            .cmd("HMGET")
            .arg(format!("{}:{}:cfg", self.ns, qname))
            .arg("vt")
            .arg("delay")
            .arg("maxsize")
            .cmd("TIME")
            .query_async(conn)
            .await?;

        let sec = (result.1).0;
        let usec = (result.1).1;
        // Message IDs always encode microseconds (matching JS rbmq).
        let time_us = sec * 1_000_000 + usec;
        // ts is the score unit: microseconds with break-js-comp, milliseconds otherwise.
        #[cfg(feature = "break-js-comp")]
        let ts = time_us;
        #[cfg(not(feature = "break-js-comp"))]
        let ts = sec * 1000 + usec / 1000;

        let (hmget_first, hmget_second, hmget_third) =
            match (result.0.first(), result.0.get(1), result.0.get(2)) {
                (Some(Some(v0)), Some(Some(v1)), Some(Some(v2))) => (v0, v1, v2),
                _ => return Err(RbmqError::QueueNotFound),
            };

        let quid = if uid {
            Some(radix_36(time_us).to_string() + &RbmqFunctions::<T>::make_id(22)?)
        } else {
            None
        };

        Ok(QueueDescriptor {
            vt: Duration::from_millis(hmget_first.parse().map_err(|_| RbmqError::CannotParseVT)?),
            delay: Duration::from_millis(
                hmget_second
                    .parse()
                    .map_err(|_| RbmqError::CannotParseDelay)?,
            ),
            maxsize: hmget_third
                .parse()
                .map_err(|_| RbmqError::CannotParseMaxsize)?,
            ts,
            uid: quid,
        })
    }

    fn make_id(len: usize) -> RbmqResult<String> {
        const POSSIBLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::rng();
        let mut id = String::with_capacity(len);
        for _ in 0..len {
            let idx = (0..POSSIBLE.len())
                .choose(&mut rng)
                .ok_or(RbmqError::BugCreatingRandomValue)?;
            id.push(POSSIBLE[idx] as char);
        }
        Ok(id)
    }
}

fn number_in_range<T: std::cmp::PartialOrd + std::fmt::Display>(
    value: T,
    min: T,
    max: T,
) -> RbmqResult<()> {
    if value >= min && value <= max {
        Ok(())
    } else {
        Err(RbmqError::InvalidValue(
            format!("{}", value),
            format!("{}", min),
            format!("{}", max),
        ))
    }
}

fn valid_name_format(name: &str) -> RbmqResult<()> {
    if name.is_empty() || name.len() > 160 {
        return Err(RbmqError::InvalidFormat(name.to_string()));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(RbmqError::InvalidFormat(name.to_string()));
    }
    Ok(())
}

fn get_redis_duration(d: Option<Duration>, default: &Duration) -> u64 {
    d.as_ref()
        .map(Duration::as_millis)
        .map(u64::try_from)
        .and_then(Result::ok)
        .unwrap_or_else(|| u64::try_from(default.as_millis()).ok().unwrap_or(30_000))
}
