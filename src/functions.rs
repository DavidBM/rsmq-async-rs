use crate::{
    types::{QueueDescriptor, RsmqMessage, RsmqQueueAttributes},
    RsmqError, RsmqResult,
};
use lazy_static::lazy_static;
use radix_fmt::radix_36;
use rand::seq::IteratorRandom;
use redis::{aio::ConnectionLike, pipe, Script};
use std::convert::TryInto;

lazy_static! {
    static ref CHANGE_MESSAGE_VISIVILITY: Script =
        Script::new(include_str!("./redis-scripts/changeMessageVisibility.lua"));
    static ref POP_MESSAGE: Script = Script::new(include_str!("./redis-scripts/popMessage.lua"));
    static ref RECEIVE_MESSAGE: Script =
        Script::new(include_str!("./redis-scripts/receiveMessage.lua"));
}

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
        seconds_hidden: u64,
    ) -> RsmqResult<()> {
        let queue = self.get_queue(conn, qname, false).await?;

        number_in_range(seconds_hidden, 0, 9_999_999)?;

        CHANGE_MESSAGE_VISIVILITY
            .key(format!("{}{}", self.ns, qname))
            .key(message_id)
            .key(queue.ts + seconds_hidden * 1000)
            .invoke_async::<_, bool>(conn)
            .await?;

        Ok(())
    }

    /// Creates a new queue. Attributes can be later modified with "set_queue_attributes" method
    ///
    /// seconds_hidden: Time the messages will be hidden when they are received with the "receive_message" method.
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited size)
    pub async fn create_queue(
        &self,
        conn: &mut T,
        qname: &str,
        seconds_hidden: Option<u32>,
        delay: Option<u32>,
        maxsize: Option<i32>,
    ) -> RsmqResult<()> {
        valid_name_format(qname)?;

        let key = format!("{}{}:Q", self.ns, qname);
        let seconds_hidden = seconds_hidden.unwrap_or(30);
        let delay = delay.unwrap_or(0);
        let maxsize = maxsize.unwrap_or(65536);

        number_in_range(seconds_hidden, 0, 9_999_999)?;
        number_in_range(delay, 0, 9_999_999)?;
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
            .arg(seconds_hidden)
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

        let result: (Vec<u64>, u64, u64) = pipe()
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
            .arg(time.0 * 1000)
            .arg("+inf")
            .query_async(conn)
            .await?;

        if result.0.is_empty() {
            return Err(RsmqError::QueueNotFound);
        }

        Ok(RsmqQueueAttributes {
            vt: *result.0.get(0).unwrap_or(&0),
            delay: *result.0.get(1).unwrap_or(&0),
            maxsize: *result.0.get(2).unwrap_or(&0),
            totalrecv: *result.0.get(3).unwrap_or(&0),
            totalsent: *result.0.get(4).unwrap_or(&0),
            created: *result.0.get(5).unwrap_or(&0),
            modified: *result.0.get(6).unwrap_or(&0),
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
    pub async fn pop_message(&self, conn: &mut T, qname: &str) -> RsmqResult<Option<RsmqMessage>> {
        let queue = self.get_queue(conn, qname, false).await?;

        let result: (bool, String, String, u64, u64) = POP_MESSAGE
            .key(format!("{}{}", self.ns, qname))
            .key(queue.ts)
            .invoke_async(conn)
            .await?;

        if !result.0 {
            return Ok(None);
        }

        Ok(Some(RsmqMessage {
            id: result.1.clone(),
            message: result.2,
            rc: result.3,
            fr: result.4,
            sent: u64::from_str_radix(&result.1[0..10], 36).unwrap_or(0),
        }))
    }

    /// Returns a message. The message stays hidden for some time (defined by "seconds_hidden" argument or the queue settings). After that time, the message will be redelivered. In order to avoid the redelivery, you need to use the "dekete_message" after this function.
    pub async fn receive_message(
        &self,
        conn: &mut T,
        qname: &str,
        seconds_hidden: Option<u64>,
    ) -> RsmqResult<Option<RsmqMessage>> {
        let queue = self.get_queue(conn, qname, false).await?;

        let seconds_hidden = seconds_hidden.unwrap_or(queue.vt) * 1000;

        number_in_range(seconds_hidden, 0, 9_999_999_000)?;

        let result: (bool, String, String, u64, u64) = RECEIVE_MESSAGE
            .key(format!("{}{}", self.ns, qname))
            .key(queue.ts)
            .key(queue.ts + seconds_hidden)
            .invoke_async(conn)
            .await?;

        if !result.0 {
            return Ok(None);
        }

        Ok(Some(RsmqMessage {
            id: result.1.clone(),
            message: result.2,
            rc: result.3,
            fr: result.4,
            sent: u64::from_str_radix(&result.1[0..10], 36).unwrap_or(0),
        }))
    }

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or the queue settings) before being delivered to a client.
    pub async fn send_message(
        &self,
        conn: &mut T,
        qname: &str,
        message: &str,
        delay: Option<u64>,
    ) -> RsmqResult<String> {
        let queue = self.get_queue(conn, qname, true).await?;

        let delay = delay.unwrap_or(queue.delay) * 1000;
        let key = format!("{}{}", self.ns, qname);

        number_in_range(delay, 0, 9_999_999)?;

        let msg_len: i64 = message
            .as_bytes()
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
            .arg(message)
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

    /// Modify the queue attributes. Keep in mind that "seconds_hidden" and "delay" can be overwritten when the message is sent. "seconds_hidden" can be changed by the method "change_message_visibility"
    ///
    /// seconds_hidden: Time the messages will be hidden when they are received with the "receive_message" method.
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited size)
    pub async fn set_queue_attributes(
        &self,
        conn: &mut T,
        qname: &str,
        seconds_hidden: Option<u64>,
        delay: Option<u64>,
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

        if let Some(duration) = seconds_hidden {
            number_in_range(duration, 0, 9_999_999)?;
            commands = commands
                .cmd("HSET")
                .arg(&queue_name)
                .arg("vt")
                .arg(duration);
        }

        if let Some(delay) = delay {
            number_in_range(delay, 0, 9_999_999)?;
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
        let result: (Vec<String>, (u64, u64)) = pipe()
            .atomic()
            .cmd("HMGET")
            .arg(format!("{}{}:Q", self.ns, qname))
            .arg("vt")
            .arg("delay")
            .arg("maxsize")
            .cmd("TIME")
            .query_async(conn)
            .await?;

        let time_seconds = (result.1).0;
        let time_microseconds = (result.1).1;

        let (hmget_first, hmget_second, hmget_third) =
            match (result.0.get(0), result.0.get(1), result.0.get(2)) {
                (Some(v0), Some(v1), Some(v2)) => (v0, v1, v2),
                _ => return Err(RsmqError::QueueNotFound),
            };

        let ts = time_seconds * 1000 + time_microseconds / 1000;

        let quid = if uid {
            Some(radix_36(ts).to_string() + &RsmqFunctions::<T>::make_id(22)?)
        } else {
            None
        };

        Ok(QueueDescriptor {
            vt: hmget_first.parse().map_err(|_| RsmqError::CannotParseVT)?,
            delay: hmget_second
                .parse()
                .map_err(|_| RsmqError::CannotParseDelay)?,
            maxsize: hmget_third
                .parse()
                .map_err(|_| RsmqError::CannotParseMaxsize)?,
            ts,
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
