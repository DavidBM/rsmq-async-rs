use crate::types::RedisBytes;
use crate::{
    types::{RbmqMessage, RbmqQueueAttributes},
    RbmqError, RbmqResult,
};
use core::convert::TryFrom;
use rand::seq::IteratorRandom;
use redis::aio::ConnectionLike;
use std::convert::TryInto;
use std::time::Duration;

const JS_COMPAT_MAX_TIME_MILLIS: u64 = 9_999_999_000;

// 1 = microsecond scores, 0 = millisecond scores. Passed to Lua.
#[cfg(feature = "break-js-comp")]
const USE_MICROSECONDS: &str = "1";
#[cfg(not(feature = "break-js-comp"))]
const USE_MICROSECONDS: &str = "0";

// Sentinel for "use the queue's stored default value" passed in as ARGV strings.
const USE_DEFAULT: &str = "-1";

/// Translate Lua `error_reply("X")` results back into the corresponding `RbmqError`.
/// Redis adds an "ERR" prefix to error_reply strings that contain no space, so the
/// known marker ends up in `detail()` rather than `code()`.
fn map_script_error(err: redis::RedisError) -> RbmqError {
    let detail = err.detail();
    let code = err.code();
    let key = detail.or(code).unwrap_or("");
    match key {
        "QueueNotFound" => RbmqError::QueueNotFound,
        "QueueExists" => RbmqError::QueueExists,
        "MessageTooLong" => RbmqError::MessageTooLong,
        _ => RbmqError::from(err),
    }
}

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
    create_queue: String,
    delete_queue: String,
    delete_message: String,
    set_queue_attributes: String,
    get_queue_attributes: String,
    change_message_visibility: String,
    send_message: String,
    send_message_batch: String,
    receive_message: String,
    receive_message_batch: String,
    receive_message_or_dlq: String,
    move_message: String,
}

async fn load_script<T: ConnectionLike>(conn: &mut T, src: &str) -> RbmqResult<String> {
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(src)
        .query_async(conn)
        .await
        .map_err(map_script_error)?;
    Ok(sha)
}

impl CachedScript {
    async fn init<T: ConnectionLike>(conn: &mut T) -> RbmqResult<Self> {
        Ok(Self {
            create_queue: load_script(conn, include_str!("../scripts/createQueue.lua")).await?,
            delete_queue: load_script(conn, include_str!("../scripts/deleteQueue.lua")).await?,
            delete_message: load_script(conn, include_str!("../scripts/deleteMessage.lua")).await?,
            set_queue_attributes: load_script(
                conn,
                include_str!("../scripts/setQueueAttributes.lua"),
            )
            .await?,
            get_queue_attributes: load_script(
                conn,
                include_str!("../scripts/getQueueAttributes.lua"),
            )
            .await?,
            change_message_visibility: load_script(
                conn,
                include_str!("../scripts/changeMessageVisibility.lua"),
            )
            .await?,
            send_message: load_script(conn, include_str!("../scripts/sendMessage.lua")).await?,
            send_message_batch: load_script(
                conn,
                include_str!("../scripts/sendMessageBatch.lua"),
            )
            .await?,
            receive_message: load_script(conn, include_str!("../scripts/receiveMessage.lua"))
                .await?,
            receive_message_batch: load_script(
                conn,
                include_str!("../scripts/receiveMessageBatch.lua"),
            )
            .await?,
            receive_message_or_dlq: load_script(
                conn,
                include_str!("../scripts/receiveMessageOrDlq.lua"),
            )
            .await?,
            move_message: load_script(conn, include_str!("../scripts/moveMessage.lua")).await?,
        })
    }
}

impl<T: ConnectionLike> RbmqFunctions<T> {
    pub async fn load_scripts(&self, conn: &mut T) -> RbmqResult<CachedScript> {
        CachedScript::init(conn).await
    }

    fn cfg_key(&self, q: &str) -> String {
        format!("{}:{}:cfg", self.ns, q)
    }
    fn zset_key(&self, q: &str) -> String {
        format!("{}:{}", self.ns, q)
    }
    fn rt_channel(&self, q: &str) -> String {
        format!("{}:rt:{}", self.ns, q)
    }
    fn queues_key(&self) -> String {
        format!("{}:QUEUES", self.ns)
    }

    /// Change the hidden time of an already-sent message.
    pub async fn change_message_visibility(
        &self,
        conn: &mut T,
        qname: &str,
        message_id: &str,
        hidden: Duration,
        cached_script: &CachedScript,
    ) -> RbmqResult<()> {
        let hidden_ms = duration_ms(Some(hidden), &Duration::from_secs(30));
        number_in_range(hidden_ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let _: i64 = redis::cmd("EVALSHA")
            .arg(&cached_script.change_message_visibility)
            .arg(1)
            .arg(self.zset_key(qname))
            .arg(message_id)
            .arg(hidden_ms.to_string())
            .arg(USE_MICROSECONDS)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        Ok(())
    }

    /// Create a new queue.
    pub async fn create_queue(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
        cached_script: &CachedScript,
    ) -> RbmqResult<()> {
        valid_name_format(qname)?;
        let hidden_ms = duration_ms(hidden, &Duration::from_secs(30));
        let delay_ms = duration_ms(delay, &Duration::ZERO);
        let maxsize = maxsize.unwrap_or(65536);

        number_in_range(hidden_ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
        number_in_range(delay_ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
        if let Err(e) = number_in_range(maxsize, 1024, 65536) {
            if maxsize != -1 {
                return Err(e);
            }
        }

        let result: i64 = redis::cmd("EVALSHA")
            .arg(&cached_script.create_queue)
            .arg(2)
            .arg(self.cfg_key(qname))
            .arg(self.queues_key())
            .arg(qname)
            .arg(hidden_ms.to_string())
            .arg(delay_ms.to_string())
            .arg(maxsize.to_string())
            .query_async(conn)
            .await
            .map_err(map_script_error)?;

        if result == 0 {
            return Err(RbmqError::QueueExists);
        }
        Ok(())
    }

    /// Delete a message from the queue.
    pub async fn delete_message(
        &self,
        conn: &mut T,
        qname: &str,
        id: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<bool> {
        let result: i64 = redis::cmd("EVALSHA")
            .arg(&cached_script.delete_message)
            .arg(1)
            .arg(self.zset_key(qname))
            .arg(id)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        Ok(result == 1)
    }

    /// Delete the queue and all its messages.
    pub async fn delete_queue(
        &self,
        conn: &mut T,
        qname: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<()> {
        let result: i64 = redis::cmd("EVALSHA")
            .arg(&cached_script.delete_queue)
            .arg(2)
            .arg(self.zset_key(qname))
            .arg(self.queues_key())
            .arg(qname)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        if result == 0 {
            return Err(RbmqError::QueueNotFound);
        }
        Ok(())
    }

    /// Returns the queue attributes and statistics.
    pub async fn get_queue_attributes(
        &self,
        conn: &mut T,
        qname: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<RbmqQueueAttributes> {
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
        ) = redis::cmd("EVALSHA")
            .arg(&cached_script.get_queue_attributes)
            .arg(2)
            .arg(self.cfg_key(qname))
            .arg(self.zset_key(qname))
            .arg(USE_MICROSECONDS)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;

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
                .map(|d| Duration::from_millis(d.try_into().unwrap_or(0)))
                .unwrap_or(Duration::ZERO),
            delay: delay
                .map(|d| Duration::from_millis(d.try_into().unwrap_or(0)))
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

    /// Returns a list of queues in the namespace.
    pub async fn list_queues(&self, conn: &mut T) -> RbmqResult<Vec<String>> {
        let queues = redis::cmd("SMEMBERS")
            .arg(self.queues_key())
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        Ok(queues)
    }

    /// Pop a message: receive + delete in one atomic step.
    pub async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        cached_script: &CachedScript,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.receive_inner(conn, qname, None, true, cached_script)
            .await
    }

    /// Receive a message and reserve it for `hidden` time (or queue default).
    pub async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        cached_script: &CachedScript,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        if let Some(h) = hidden {
            number_in_range(
                u64::try_from(h.as_millis()).unwrap_or(u64::MAX),
                0,
                JS_COMPAT_MAX_TIME_MILLIS,
            )?;
        }
        self.receive_inner(conn, qname, hidden, false, cached_script)
            .await
    }

    async fn receive_inner<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        should_delete: bool,
        cached_script: &CachedScript,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        let hidden_arg = match hidden {
            Some(h) => u64::try_from(h.as_millis())
                .map_err(|_| RbmqError::InvalidValue("hidden".into(), "0".into(), "u64::MAX".into()))?
                .to_string(),
            None => USE_DEFAULT.to_string(),
        };

        let result: (bool, String, Vec<u8>, u64, u64, u64) = redis::cmd("EVALSHA")
            .arg(&cached_script.receive_message)
            .arg(1)
            .arg(self.zset_key(qname))
            .arg(hidden_arg)
            .arg(if should_delete { "true" } else { "false" })
            .arg(USE_MICROSECONDS)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;

        if !result.0 {
            return Ok(None);
        }

        let message =
            E::try_from(RedisBytes(result.2)).map_err(RbmqError::CannotDecodeMessage)?;
        Ok(Some(RbmqMessage {
            id: result.1,
            message,
            rc: result.3,
            fr: result.4,
            sent: result.5,
        }))
    }

    /// Send a single message.
    pub async fn send_message<E: Into<RedisBytes>>(
        &self,
        conn: &mut T,
        qname: &str,
        message: E,
        delay: Option<Duration>,
        cached_script: &CachedScript,
    ) -> RbmqResult<String> {
        let body: RedisBytes = message.into();
        let body_len: i64 = body
            .0
            .len()
            .try_into()
            .map_err(|_| RbmqError::MessageTooLong)?;
        let _ = body_len; // size is enforced inside the script against the queue's stored maxsize.

        let delay_arg = match delay {
            Some(d) => {
                let ms = u64::try_from(d.as_millis())
                    .map_err(|_| RbmqError::InvalidValue("delay".into(), "0".into(), "u64::MAX".into()))?;
                number_in_range(ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
                ms.to_string()
            }
            None => USE_DEFAULT.to_string(),
        };

        let id = Self::make_id(22)?;
        let realtime_flag = if self.realtime { "1" } else { "0" };

        let _returned: String = redis::cmd("EVALSHA")
            .arg(&cached_script.send_message)
            .arg(2)
            .arg(self.zset_key(qname))
            .arg(self.rt_channel(qname))
            .arg(&id)
            .arg(delay_arg)
            .arg(realtime_flag)
            .arg(USE_MICROSECONDS)
            .arg(body.0)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        Ok(id)
    }

    /// Atomically inserts a batch of messages into the queue.
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

        let delay_arg = match delay {
            Some(d) => {
                let ms = u64::try_from(d.as_millis())
                    .map_err(|_| RbmqError::InvalidValue("delay".into(), "0".into(), "u64::MAX".into()))?;
                number_in_range(ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
                ms.to_string()
            }
            None => USE_DEFAULT.to_string(),
        };

        let realtime_flag = if self.realtime { "1" } else { "0" };

        let mut ids: Vec<String> = Vec::with_capacity(messages.len());
        let mut bodies: Vec<Vec<u8>> = Vec::with_capacity(messages.len());
        for m in messages {
            let body: RedisBytes = m.into();
            ids.push(Self::make_id(22)?);
            bodies.push(body.0);
        }

        let mut cmd = redis::cmd("EVALSHA");
        cmd.arg(&cached_script.send_message_batch)
            .arg(2)
            .arg(self.zset_key(qname))
            .arg(self.rt_channel(qname))
            .arg(delay_arg)
            .arg(realtime_flag)
            .arg(USE_MICROSECONDS);
        for (id, body) in ids.iter().zip(bodies.into_iter()) {
            cmd.arg(id).arg(body);
        }

        let _: i64 = cmd
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        Ok(ids)
    }

    /// Atomically receives up to `max_count` visible messages.
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

        let hidden_arg = match hidden {
            Some(h) => {
                let ms = u64::try_from(h.as_millis()).unwrap_or(u64::MAX);
                number_in_range(ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
                ms.to_string()
            }
            None => USE_DEFAULT.to_string(),
        };

        let raw: Vec<(String, Vec<u8>, u64, u64, u64)> = redis::cmd("EVALSHA")
            .arg(&cached_script.receive_message_batch)
            .arg(1)
            .arg(self.zset_key(qname))
            .arg(hidden_arg)
            .arg("false")
            .arg(max_count)
            .arg(USE_MICROSECONDS)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;

        let mut out = Vec::with_capacity(raw.len());
        for (id, body, rc, fr, sent) in raw {
            let message = E::try_from(RedisBytes(body)).map_err(RbmqError::CannotDecodeMessage)?;
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

    /// Receive with consumer-side dead-letter routing.
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

        let hidden_arg = match hidden {
            Some(h) => {
                let ms = u64::try_from(h.as_millis()).unwrap_or(u64::MAX);
                number_in_range(ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
                ms.to_string()
            }
            None => USE_DEFAULT.to_string(),
        };

        let result: (bool, String, Vec<u8>, u64, u64, u64) = redis::cmd("EVALSHA")
            .arg(&cached_script.receive_message_or_dlq)
            .arg(2)
            .arg(self.zset_key(qname))
            .arg(self.zset_key(dlq))
            .arg(hidden_arg)
            .arg(max_receives.to_string())
            .arg(USE_MICROSECONDS)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;

        if !result.0 {
            return Ok(None);
        }
        let message =
            E::try_from(RedisBytes(result.2)).map_err(RbmqError::CannotDecodeMessage)?;
        Ok(Some(RbmqMessage {
            id: result.1,
            message,
            rc: result.3,
            fr: result.4,
            sent: result.5,
        }))
    }

    /// Atomically move a message from `src` to `dst`.
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

        let moved: i64 = redis::cmd("EVALSHA")
            .arg(&cached_script.move_message)
            .arg(2)
            .arg(self.zset_key(src))
            .arg(self.zset_key(dst))
            .arg(msg_id)
            .arg(USE_MICROSECONDS)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;
        Ok(moved == 1)
    }

    /// Update queue attributes. Returns the resulting attributes.
    pub async fn set_queue_attributes(
        &self,
        conn: &mut T,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
        cached_script: &CachedScript,
    ) -> RbmqResult<RbmqQueueAttributes> {
        let vt_arg = match hidden {
            Some(h) => {
                let ms = duration_ms(Some(h), &Duration::from_secs(30));
                number_in_range(ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
                ms.to_string()
            }
            None => String::new(),
        };
        let delay_arg = match delay {
            Some(d) => {
                let ms = duration_ms(Some(d), &Duration::ZERO);
                number_in_range(ms, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
                ms.to_string()
            }
            None => String::new(),
        };
        let maxsize_arg = match maxsize {
            Some(m) => {
                if let Err(e) = number_in_range(m, 1024, 65536) {
                    if m != -1 {
                        return Err(e);
                    }
                }
                m.to_string()
            }
            None => String::new(),
        };

        let exists: i64 = redis::cmd("EVALSHA")
            .arg(&cached_script.set_queue_attributes)
            .arg(1)
            .arg(self.cfg_key(qname))
            .arg(vt_arg)
            .arg(delay_arg)
            .arg(maxsize_arg)
            .query_async(conn)
            .await
            .map_err(map_script_error)?;

        if exists == 0 {
            return Err(RbmqError::QueueNotFound);
        }
        self.get_queue_attributes(conn, qname, cached_script).await
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

fn duration_ms(d: Option<Duration>, default: &Duration) -> u64 {
    d.as_ref()
        .map(Duration::as_millis)
        .map(u64::try_from)
        .and_then(Result::ok)
        .unwrap_or_else(|| u64::try_from(default.as_millis()).ok().unwrap_or(30_000))
}
