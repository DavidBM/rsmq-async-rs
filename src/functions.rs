use crate::types::RedisBytes;
use crate::{
    types::{QueueDescriptor, RsmqMessage, RsmqQueueAttributes},
    RsmqError, RsmqResult,
};
use core::convert::TryFrom;
use lazy_static::lazy_static;
use radix_fmt::radix_36;
use rand::seq::IteratorRandom;
use redis::{aio::ConnectionLike, pipe, Script};
use std::convert::TryInto;
use std::time::Duration;

lazy_static! {
    static ref CHANGE_MESSAGE_VISIVILITY: Script =
        Script::new(include_str!("./redis-scripts/changeMessageVisibility.lua"));
    static ref POP_MESSAGE: Script = Script::new(include_str!("./redis-scripts/popMessage.lua"));
    static ref RECEIVE_MESSAGE: Script =
        Script::new(include_str!("./redis-scripts/receiveMessage.lua"));
}

static JS_COMPAT_MAX_TIME_MILLIS: u64 = 9_999_999_000;

/// The main object of this library. Creates/Handles the redis connection and contains all the methods
#[derive(Clone)]
pub struct RsmqFunctions<T: ConnectionLike> {
    pub(crate) ns: String,
    pub(crate) realtime: bool,
    pub(crate) conn: std::marker::PhantomData<T>,
}

impl<T: ConnectionLike> std::fmt::Debug for RsmqFunctions<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RsmqFunctions")
    }
}

impl<T: ConnectionLike> RsmqFunctions<T> {
    /// Change the hidden time of a already sent message.
    pub async fn change_message_visibility(
        &self,
        conn: &mut T,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RsmqResult<()> {
        let hidden = get_redis_duration(Some(hidden), &Duration::from_secs(30));

        let queue = self.get_queue(conn, qname, false).await?;

        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        CHANGE_MESSAGE_VISIVILITY
            .key(format!("{}{}", self.ns, qname))
            .key(message_id)
            .key(queue.ts + hidden)
            .invoke_async::<_, bool>(conn)
            .await?;

        Ok(())
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
        maxsize: Option<i32>,
    ) -> RsmqResult<()> {
        valid_name_format(qname)?;

        let key = format!("{}{}:Q", self.ns, qname);
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

        let results: Vec<bool> = pipe()
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
            .query_async(conn)
            .await?;

        if !results[0] {
            return Err(RsmqError::QueueExists);
        }

        redis::cmd("SADD")
            .arg(format!("{}QUEUES", self.ns))
            .arg(qname)
            .query_async(conn)
            .await?;

        Ok(())
    }

    /// Deletes a message from the queue.
    ///
    /// Important to use when you are using receive_message.
    pub async fn delete_message(&self, conn: &mut T, qname: &str, id: &str) -> RsmqResult<bool> {
        let key = format!("{}{}", self.ns, qname);

        let results: (u16, u16) = pipe()
            .atomic()
            .cmd("ZREM")
            .arg(&key)
            .arg(id)
            .cmd("HDEL")
            .arg(format!("{}:Q", &key))
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
    pub async fn delete_queue(&self, conn: &mut T, qname: &str) -> RsmqResult<()> {
        let key = format!("{}{}", self.ns, qname);

        let results: (u16, u16) = pipe()
            .atomic()
            .cmd("DEL")
            .arg(format!("{}:Q", &key))
            .arg(key)
            .cmd("SREM")
            .arg(format!("{}QUEUES", self.ns))
            .arg(qname)
            .query_async(conn)
            .await?;

        if results.0 == 0 {
            return Err(RsmqError::QueueNotFound);
        }

        Ok(())
    }

    /// Returns the queue attributes and statistics
    pub async fn get_queue_attributes(
        &self,
        conn: &mut T,
        qname: &str,
    ) -> RsmqResult<RsmqQueueAttributes> {
        let key = format!("{}{}", self.ns, qname);

        let time: (u64, u64) = redis::cmd("TIME").query_async(conn).await?;

        let result: (Vec<Option<i64>>, u64, u64) = pipe()
            .atomic()
            .cmd("HMGET")
            .arg(format!("{}:Q", key))
            .arg("vt")
            .arg("delay")
            .arg("maxsize")
            .arg("totalrecv")
            .arg("totalsent")
            .arg("created")
            .arg("modified")
            .cmd("ZCARD")
            .arg(&key)
            .cmd("ZCOUNT")
            .arg(&key)
            .arg(time.0)
            .arg("+inf")
            .query_async(conn)
            .await?;

        let is_empty = result.0.contains(&None);

        if is_empty {
            return Err(RsmqError::QueueNotFound);
        }

        Ok(RsmqQueueAttributes {
            vt: result
                .0
                .first()
                .and_then(Option::as_ref)
                .map(|dur| Duration::from_millis((*dur).try_into().unwrap_or(0)))
                .unwrap_or(Duration::ZERO),
            delay: result
                .0
                .get(1)
                .and_then(Option::as_ref)
                .map(|dur| Duration::from_millis((*dur).try_into().unwrap_or(0)))
                .unwrap_or(Duration::ZERO),
            maxsize: result.0.get(2).unwrap_or(&Some(0)).unwrap_or(0),
            totalrecv: u64::try_from(result.0.get(3).unwrap_or(&Some(0)).unwrap_or(0)).unwrap_or(0),
            totalsent: u64::try_from(result.0.get(4).unwrap_or(&Some(0)).unwrap_or(0)).unwrap_or(0),
            created: u64::try_from(result.0.get(5).unwrap_or(&Some(0)).unwrap_or(0)).unwrap_or(0),
            modified: u64::try_from(result.0.get(6).unwrap_or(&Some(0)).unwrap_or(0)).unwrap_or(0),
            msgs: result.1,
            hiddenmsgs: result.2,
        })
    }

    /// Returns a list of queues in the namespace
    pub async fn list_queues(&self, conn: &mut T) -> RsmqResult<Vec<String>> {
        let queues = redis::cmd("SMEMBERS")
            .arg(format!("{}QUEUES", self.ns))
            .query_async(conn)
            .await?;

        Ok(queues)
    }

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    pub async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &self,
        conn: &mut T,
        qname: &str,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        let queue = self.get_queue(conn, qname, false).await?;

        let result: (bool, String, Vec<u8>, u64, u64) = POP_MESSAGE
            .key(format!("{}{}", self.ns, qname))
            .key(queue.ts)
            .invoke_async(conn)
            .await?;

        if !result.0 {
            return Ok(None);
        }

        let message = E::try_from(RedisBytes(result.2)).map_err(RsmqError::CannotDecodeMessage)?;

        Ok(Some(RsmqMessage {
            id: result.1.clone(),
            message,
            rc: result.3,
            fr: result.4,
            sent: u64::from_str_radix(&result.1[0..10], 36).unwrap_or(0),
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
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        let queue = self.get_queue(conn, qname, false).await?;

        let hidden = get_redis_duration(hidden, &queue.vt);
        number_in_range(hidden, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let result: (bool, String, Vec<u8>, u64, u64) = RECEIVE_MESSAGE
            .key(format!("{}{}", self.ns, qname))
            .key(queue.ts)
            .key(queue.ts + hidden)
            .invoke_async(conn)
            .await?;

        if !result.0 {
            return Ok(None);
        }

        let message = E::try_from(RedisBytes(result.2)).map_err(RsmqError::CannotDecodeMessage)?;

        Ok(Some(RsmqMessage {
            id: result.1.clone(),
            message,
            rc: result.3,
            fr: result.4,
            sent: u64::from_str_radix(&result.1[0..10], 36).unwrap_or(0),
        }))
    }

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or the queue settings) before being delivered to a client.
    pub async fn send_message<E: Into<RedisBytes>>(
        &self,
        conn: &mut T,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RsmqResult<String> {
        let queue = self.get_queue(conn, qname, true).await?;

        let delay = get_redis_duration(delay, &queue.delay);
        let key = format!("{}{}", self.ns, qname);

        number_in_range(delay, 0, JS_COMPAT_MAX_TIME_MILLIS)?;

        let message: RedisBytes = message.into();

        let msg_len: i64 = message
            .0
            .len()
            .try_into()
            .map_err(|_| RsmqError::MessageTooLong)?;

        if queue.maxsize != -1 && msg_len > queue.maxsize {
            return Err(RsmqError::MessageTooLong);
        }

        let queue_uid = match queue.uid {
            Some(uid) => uid,
            None => return Err(RsmqError::QueueNotFound),
        };

        let queue_key = format!("{}:Q", key);

        let mut piping = pipe();

        let mut commands = piping
            .atomic()
            .cmd("ZADD")
            .arg(&key)
            .arg(queue.ts + delay)
            .arg(&queue_uid)
            .cmd("HSET")
            .arg(&queue_key)
            .arg(&queue_uid)
            .arg(message.0)
            .cmd("HINCRBY")
            .arg(&queue_key)
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
                .query_async(conn)
                .await?;
        }

        Ok(queue_uid)
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
    ) -> RsmqResult<RsmqQueueAttributes> {
        self.get_queue(conn, qname, false).await?;

        let queue_name = format!("{}{}:Q", self.ns, qname);

        let time: (u64, u64) = redis::cmd("TIME").query_async(conn).await?;

        let mut commands = &mut pipe();

        commands = commands
            .atomic()
            .cmd("HSET")
            .arg(&queue_name)
            .arg("modified")
            .arg(time.0);

        if hidden.is_some() {
            let duration = get_redis_duration(hidden, &Duration::from_secs(30));
            number_in_range(duration, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
            commands = commands
                .cmd("HSET")
                .arg(&queue_name)
                .arg("vt")
                .arg(duration);
        }

        if delay.is_some() {
            let delay = get_redis_duration(delay, &Duration::ZERO);
            number_in_range(delay, 0, JS_COMPAT_MAX_TIME_MILLIS)?;
            commands = commands
                .cmd("HSET")
                .arg(&queue_name)
                .arg("delay")
                .arg(delay);
        }

        if let Some(maxsize) = maxsize {
            if let Err(error) = number_in_range(maxsize, 1024, 65536) {
                if maxsize != -1 {
                    // TODO: Create another error in order to explain that -1 is allowed
                    return Err(error);
                }
            }
            commands = commands
                .cmd("HSET")
                .arg(&queue_name)
                .arg("maxsize")
                .arg(maxsize);
        }

        commands.query_async(conn).await?;

        self.get_queue_attributes(conn, qname).await
    }

    async fn get_queue(&self, conn: &mut T, qname: &str, uid: bool) -> RsmqResult<QueueDescriptor> {
        let result: (Vec<Option<String>>, (u64, u64)) = pipe()
            .atomic()
            .cmd("HMGET")
            .arg(format!("{}{}:Q", self.ns, qname))
            .arg("vt")
            .arg("delay")
            .arg("maxsize")
            .cmd("TIME")
            .query_async(conn)
            .await?;

        let time_millis = (result.1).0 * 1000;

        let (hmget_first, hmget_second, hmget_third) =
            match (result.0.first(), result.0.get(1), result.0.get(2)) {
                (Some(Some(v0)), Some(Some(v1)), Some(Some(v2))) => (v0, v1, v2),
                _ => return Err(RsmqError::QueueNotFound),
            };

        let quid = if uid {
            Some(radix_36(time_millis).to_string() + &RsmqFunctions::<T>::make_id(22)?)
        } else {
            None
        };

        Ok(QueueDescriptor {
            vt: Duration::from_millis(hmget_first.parse().map_err(|_| RsmqError::CannotParseVT)?),
            delay: Duration::from_millis(
                hmget_second
                    .parse()
                    .map_err(|_| RsmqError::CannotParseDelay)?,
            ),
            maxsize: hmget_third
                .parse()
                .map_err(|_| RsmqError::CannotParseMaxsize)?,
            ts: time_millis,
            uid: quid,
        })
    }

    fn make_id(len: usize) -> RsmqResult<String> {
        let possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        let mut rng = rand::thread_rng();

        let mut id = String::with_capacity(len);

        for _ in 0..len {
            id.push(
                possible
                    .chars()
                    .choose(&mut rng)
                    .ok_or(RsmqError::BugCreatingRandonValue)?,
            );
        }

        Ok(id)
    }
}

fn number_in_range<T: std::cmp::PartialOrd + std::fmt::Display>(
    value: T,
    min: T,
    max: T,
) -> RsmqResult<()> {
    if value >= min && value <= max {
        Ok(())
    } else {
        Err(RsmqError::InvalidValue(
            format!("{}", value),
            format!("{}", min),
            format!("{}", max),
        ))
    }
}

fn valid_name_format(name: &str) -> RsmqResult<()> {
    if name.is_empty() && name.len() > 160 {
        return Err(RsmqError::InvalidFormat(name.to_string()));
    } else {
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
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
