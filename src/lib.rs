//! This is a port of the nodejs Redis Simple Message Queue package. 
//! It is a 1-to-1 conversion using async.
//! 
//! ```rust,no_run
//! use rsmq_async::{Rsmq, RsmqError};
//!
//! # async fn it_works() -> Result<(), RsmqError> {
//! let mut rsmq = Rsmq::new(Default::default()).await?;
//! 
//! let message = rsmq.receive_message("myqueue", None).await?;
//! 
//! if let Some(message) = message {
//!     rsmq.delete_message("myqueue", &message.id).await?;
//! }
//! 
//! # Ok(())
//! # }
//! ```
//! Main object documentation in: <a href="struct.Rsmq.html">Rsmq<a/>
//! 
//! ## Realtime
//! 
//! When [initializing](#initialize) RSMQ you can enable the realtime PUBLISH for 
//! new messages. On every new message that gets sent to RSQM via `sendMessage` a 
//! Redis PUBLISH will be issued to `{rsmq.ns}:rt:{qname}`.
//! 
//! Example for RSMQ with default settings:
//! 
//! * The queue `testQueue` already contains 5 messages.
//! * A new message is being sent to the queue `testQueue`.
//! * The following Redis command will be issued: `PUBLISH rsmq:rt:testQueue 6`
//! 
//! ### How to use the realtime option
//! 
//! Besides the PUBLISH when a new message is sent to RSMQ nothing else will happen. 
//! Your app could use the Redis SUBSCRIBE command to be notified of new messages 
//! and issue a `receiveMessage` then. However make sure not to listen with multiple 
//! workers for new messages with SUBSCRIBE to prevent multiple simultaneous 
//! `receiveMessage` calls.  
//! 
//! ## Guarantees
//! 
//! If you want to implement "at least one delivery" guarantee, you need to receive 
//! the messages using "receive_message" and then, once the message is successfully 
//! processed, delete it with "delete_message".
//! 
//! ```rust,no_run
//! use rsmq_async::Rsmq;
//! 
//! # #[tokio::main] //You can use Tokio or Async-std
//! # async fn main() {
//!     let mut rsmq = Rsmq::new(Default::default())
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
//!         .receive_message("myqueue", None)
//!         .await
//!         .expect("cannot receive message");
//! 
//!     if let Some(message) = message {
//!         rsmq.delete_message("myqueue", &message.id).await;
//!     }
//! # }
//! ```
//! ## Executor compatibility
//! 
//! Since version 0.16 redis dependency supports tokio and async_std executors. 
//! By default it will guess what you are using when creating the connection. 
//! You can check [redis](https://github.com/mitsuhiko/redis-rs/blob/master/Cargo.toml) 
//! `Cargo.tolm` for the flags `async-std-comp` and `tokio-comp`
//! 

#![forbid(unsafe_code)]

mod errors;

use errors::*;
use lazy_static::lazy_static;
use radix_fmt::radix_36;
use rand::seq::IteratorRandom;
use redis::{aio::Connection, pipe, Script};
pub use errors::RsmqError;

#[derive(Debug)]
struct QueueDescriptor {
    vt: u64,
    delay: u64,
    maxsize: u64,
    ts: u64,
    uid: Option<String>,
}

/// Options for creating a new RSMQ instance.
#[derive(Debug)]
pub struct RsmqOptions {
    /// Redis host
    pub host: String,
    /// Redis port
    pub port: String,
    /// If true, it will use redis pubsub to notify clients about new messages. More info in the general crate description 
    pub realtime: bool,
    /// Redis password
    pub password: Option<String>,
    /// RSMQ namespace (you can have several. "rsmq" by default)
    pub ns: String,
}

impl Default for RsmqOptions {
    fn default() -> Self {
        RsmqOptions {
            host: "localhost".to_string(),
            port: "6379".to_string(),
            realtime: false,
            password: None,
            ns: "rsmq".to_string(),
        }
    }
}

/// A new RSMQ message. You will get this when using pop_message or receive_message methods
#[derive(Debug)]
pub struct RsmqMessage {
    /// Message id. Used later for change_message_visibility and delete_message
    pub id: String,
    /// Message content. It is wrapped in an string. If you are sending other format (JSON, etc) you will need to decode the message in your code
    pub message: String,
    /// Number of times the message was received by a client
    pub rc: u64,
    /// Timestamp (epoch in seconds) of when was this message received
    pub fr: u64,
    /// Timestamp (epoch in seconds) of when was this message sent
    pub sent: u64,
}

/// Struct defining a queue. They are set on "create_queue" and "set_queue_attributes"
#[derive(Debug)]
pub struct RsmqQueueAttributes {
    /// How many seconds the message will be hidden when is received by a client
    pub vt: u64,
    /// How many second will take until the message is delivered to a client since it was sent
    pub delay: u64,
    /// Max size of the message in bytes in the queue
    pub maxsize: u64,
    /// Number of messages received by the queue
    pub totalrecv: u64,
    /// Number of messages sent by the queue
    pub totalsent: u64,
    /// When was this queue created. Timestamp (epoch in seconds)
    pub created: u64,
    /// When was this queue last modified. Timestamp (epoch in seconds)
    pub modified: u64,
    /// How many messages the queue contains
    pub msgs: u64,
    /// How many messages are hidden from the queue. This number depends of the "vt" attribute and messages with a different hidden time modified by "change_message_visibility" method
    pub hiddenmsgs: u64,
}

struct RedisConnection(Connection);

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RedisAsyncConnnection")
    }
}

lazy_static! {
    static ref CHANGE_MESSAGE_VISIVILITY: Script =
        Script::new(include_str!("./redis-scripts/changeMessageVisibility.lua"));
    static ref POP_MESSAGE: Script = Script::new(include_str!("./redis-scripts/popMessage.lua"));
    static ref RECEIVE_MESSAGE: Script =
        Script::new(include_str!("./redis-scripts/receiveMessage.lua"));
}

/// THe main object of this library. Creates/Handles the redis connection and contains all the methods
#[derive(Debug)]
pub struct Rsmq {
    connection: RedisConnection,
    options: RsmqOptions,
}

impl Rsmq {
    /// Creates a new RSMQ instance, including its connection
    pub async fn new(options: RsmqOptions) -> Result<Rsmq, RsmqError> {
        let password = if let Some(password) = options.password.clone() {
            format!("redis:{}@", password)
        } else {
            "".to_string()
        };

        let url = format!("redis://{}{}:{}", password, options.host, options.port);

        let client = redis::Client::open(url)?;

        let connection = client.get_async_connection().await?;

        Ok(Rsmq::new_with_connection(options, connection))
    }

    /// Special method for when you already have a redis-rs connection and you don't want redis_async to create a new one. 
    pub fn new_with_connection(options: RsmqOptions, connection: redis::aio::Connection) -> Rsmq {
        Rsmq {
            connection: RedisConnection(connection),
            options,
        }
    }

    /// Change the hidden time of a already sent message.
    pub async fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        seconds_hidden: u64,
    ) -> Result<(), RsmqError> {
        number_in_range(seconds_hidden, 0, 9_999_999)?;

        let queue = self.get_queue(qname, false).await?;

        CHANGE_MESSAGE_VISIVILITY
            .key(format!("{}{}", self.options.ns, qname))
            .key(message_id)
            .key(queue.ts + seconds_hidden * 1000)
            .invoke_async::<_, bool>(&mut self.connection.0)
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
        &mut self,
        qname: &str,
        seconds_hidden: Option<u32>,
        delay: Option<u32>,
        maxsize: Option<i32>,
    ) -> Result<(), RsmqError> {

        valid_name_format(qname)?;

        let key = format!("{}{}:Q", self.options.ns, qname);
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

        let time: (u64, u64) = redis::cmd("TIME")
            .query_async(&mut self.connection.0)
            .await?;

        let results: Vec<bool> = pipe()
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
            .arg(0)
            .cmd("HSETNX")
            .arg(&key)
            .arg("totalsent")
            .arg(0)
            .query_async(&mut self.connection.0)
            .await?;

        if !results[0] {
            return Err(QueueExists {}.into());
        }

        redis::cmd("SADD")
            .arg(format!("{}QUEUES", self.options.ns))
            .arg(qname)
            .query_async(&mut self.connection.0)
            .await?;

        Ok(())
    }

    /// Deletes a message from the queue.
    /// 
    /// Important to use when you are using receive_message. 
    pub async fn delete_message(&mut self, qname: &str, id: &str) -> Result<bool, RsmqError> {
        let key = format!("{}{}", self.options.ns, qname);

        let results: (u16, u16) = pipe()
            .cmd("ZREM")
            .arg(&key)
            .arg(id)
            .cmd("HDEL")
            .arg(format!("{}:Q", &key))
            .arg(id)
            .arg(format!("{}:rc", id))
            .arg(format!("{}:fr", id))
            .query_async(&mut self.connection.0)
            .await?;

        if results.0 == 1 && results.1 > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    /// Deletes the queue and all the messages on it
    pub async fn delete_queue(&mut self, qname: &str) -> Result<(), RsmqError> {
        let key = format!("{}{}", self.options.ns, qname);

        let results: (u16, u16) = pipe()
            .cmd("DEL")
            .arg(format!("{}:Q", &key))
            .arg(key)
            .cmd("SREM")
            .arg(format!("{}QUEUES", self.options.ns))
            .arg(qname)
            .query_async(&mut self.connection.0)
            .await?;

        if results.0 == 0 {
            return Err(QueueNotFound {}.into());
        }

        Ok(())
    }

    /// Returns the queue attributes and statistics
    pub async fn get_queue_attributes(
        &mut self,
        qname: &str,
    ) -> Result<RsmqQueueAttributes, RsmqError> {
        let key = format!("{}{}", self.options.ns, qname);

        let time: (u64, u64) = redis::cmd("TIME")
            .query_async(&mut self.connection.0)
            .await?;

        let result: (Vec<u64>, u64, u64) = pipe()
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
            .query_async(&mut self.connection.0)
            .await?;

        if result.0.is_empty() {
            return Err(QueueNotFound {}.into());
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
    pub async fn list_queues(&mut self) -> Result<Vec<String>, RsmqError> {
        let queues = redis::cmd("SMEMBERS")
            .arg(format!("{}QUEUES", self.options.ns))
            .query_async::<_, Vec<String>>(&mut self.connection.0)
            .await?;

        Ok(queues)
    }

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    pub async fn pop_message(&mut self, qname: &str) -> Result<Option<RsmqMessage>, RsmqError> {
        let queue = self.get_queue(qname, false).await?;

        let result: (bool, String, String, u64, u64) = POP_MESSAGE
            .key(format!("{}{}", self.options.ns, qname))
            .key(queue.ts)
            .invoke_async(&mut self.connection.0)
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
        &mut self,
        qname: &str,
        seconds_hidden: Option<u64>,
    ) -> Result<Option<RsmqMessage>, RsmqError> {
        let queue = self.get_queue(qname, false).await?;

        let seconds_hidden = seconds_hidden.unwrap_or(queue.vt) * 1000;

        number_in_range(seconds_hidden, 0, 9_999_999_000)?;

        let result: (bool, String, String, u64, u64) = RECEIVE_MESSAGE
            .key(format!("{}{}", self.options.ns, qname))
            .key(queue.ts)
            .key(queue.ts + seconds_hidden)
            .invoke_async(&mut self.connection.0)
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
        &mut self,
        qname: &str,
        message: &str,
        delay: Option<u64>,
    ) -> Result<String, RsmqError> {
        let queue = self.get_queue(qname, true).await?;
        let delay = delay.unwrap_or(queue.delay) * 1000;
        let key = format!("{}{}", self.options.ns, qname);

        number_in_range(delay, 0, 9_999_999)?;

        if message.len() as u64 > queue.maxsize {
            return Err(MessageTooLong {}.into());
        }

        let queue_uid = queue.uid.unwrap();
        let queue_key = format!("{}:Q", key);

        let mut piping = pipe();

        let mut commands = piping
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

        if self.options.realtime {
            commands = commands.cmd("ZCARD").arg(&key);
        }

        let result: Vec<i64> = commands.query_async(&mut self.connection.0).await?;

        if self.options.realtime {
            redis::cmd("PUBLISH")
                .arg(format!("{}rt:{}", self.options.ns, qname))
                .arg(result[3])
                .query_async::<_, Vec<String>>(&mut self.connection.0)
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
        &mut self,
        qname: &str,
        seconds_hidden: Option<u64>,
        delay: Option<u64>,
        maxsize: Option<i64>,
    ) -> Result<RsmqQueueAttributes, RsmqError> {
        self.get_queue(qname, false).await?;

        let queue_name = format!("{}{}:Q", self.options.ns, qname);


        let time: (u64, u64) = redis::cmd("TIME")
            .query_async(&mut self.connection.0)
            .await?;

        let mut commands = &mut pipe();

        commands = commands
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

        commands.query_async(&mut self.connection.0).await?;

        self.get_queue_attributes(qname).await
    }

    async fn get_queue(&mut self, qname: &str, uid: bool) -> Result<QueueDescriptor, RsmqError> {
        let result: (Vec<String>, (u64, u64)) = pipe()
            .cmd("HMGET")
            .arg(format!("{}{}:Q", self.options.ns, qname))
            .arg("vt")
            .arg("delay")
            .arg("maxsize")
            .cmd("TIME")
            .query_async(&mut self.connection.0)
            .await?;

        let time_seconds = (result.1).0;
        let time_microseconds = (result.1).1;

        let (hmget_first, hmget_second, hmget_third) =
            match (result.0.get(0), result.0.get(1), result.0.get(2)) {
                (Some(v0), Some(v1), Some(v2)) => (v0, v1, v2),
                _ => return Err(QueueNotFound {}.into()),
            };

        let ts = time_seconds * 1000 + time_microseconds / 1000;

        let quid = if uid {
            Some(radix_36(ts).to_string() + &Rsmq::make_id(22))
        } else {
            None
        };

        Ok(QueueDescriptor {
            vt: hmget_first.parse().expect("cannot parse queue vt"),
            delay: hmget_second.parse().expect("cannot parse queue delay"),
            maxsize: hmget_third.parse().expect("cannot parse queue maxsize"),
            ts,
            uid: quid,
        })
    }

    fn make_id(len: usize) -> String {
        let possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        let mut rng = rand::thread_rng();

        let mut id = String::with_capacity(len);

        for _ in 0..len {
            id.push(possible.chars().choose(&mut rng).unwrap());
        }

        id
    }

}

fn number_in_range<T: std::cmp::PartialOrd + std::fmt::Display>(value: T, min: T, max: T) -> Result<(), RsmqError> {
    if value >= min && value <= max {
        Ok(())
    } else {
        Err(InvalidValue(format!("{}", value), format!("{}", min), format!("{}", max)).into())
    }
}

fn valid_name_format(name: &str) -> Result<(), RsmqError> {
    if name.is_empty() && name.len() > 160 {
        return Err(InvalidFormat(name.to_string()).into());
    } else {
        name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    }

    Ok(())
}
