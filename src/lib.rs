//! This is a port of the nodejs Redis Simple Message Queue package. It is a 1-to-1 conversion using async.
//! 
//! ```rust,no_run
//! use rsmq_async::{Rsmq, RsmqError};
//!
//! # async fn it_works() -> Result<(), RsmqError> {
//! let mut rsmq = Rsmq::new(Default::default()).await?;
//! 
//! let message = rsmq.receive_message_async("myqueue", None).await?;
//! 
//! rsmq.delete_message_async("myqueue", &message.id).await?;
//! 
//! # Ok(())
//! # }
//! ```
//! 

mod errors;

use errors::*;
use lazy_static::lazy_static;
use radix_fmt::radix_36;
use rand::seq::IteratorRandom;
use redis::{aio::Connection, pipe, Client, Script};
pub use errors::RsmqError;

#[derive(Debug)]
struct QueueDescriptor {
    vt: u64,
    delay: u64,
    maxsize: u64,
    ts: u64,
    uid: Option<String>,
}

#[derive(Debug)]
pub struct RsmqOptions {
    pub host: String,
    pub port: String,
    pub realtime: bool,
    pub password: Option<String>,
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

#[derive(Debug)]
pub struct RsmqMessage {
    pub id: String,
    pub message: String,
    pub rc: u64,
    pub fr: u64,
    pub sent: u64,
}

#[derive(Debug)]
pub struct RsmqQueueAttributes {
    pub vt: u64,
    pub delay: u64,
    pub maxsize: u64,
    pub totalrecv: u64,
    pub totalsent: u64,
    pub created: u64,
    pub modified: u64,
    pub msgs: u64,
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

#[derive(Debug)]
pub struct Rsmq {
    client: Client,
    connection: RedisConnection,
    options: RsmqOptions,
}

impl Rsmq {
    pub async fn new(options: RsmqOptions) -> Result<Rsmq, RsmqError> {
        let password = if let Some(password) = options.password.clone() {
            format!("redis:{}@", password)
        } else {
            "".to_string()
        };

        let url = format!("redis://{}{}:{}", password, options.host, options.port);

        let client = redis::Client::open(url)?;

        let connection = client.get_async_connection().await?;

        Ok(Rsmq {
            client,
            connection: RedisConnection(connection),
            options,
        })
    }

    pub async fn change_message_visibility_async(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden_duration: u64,
    ) -> Result<(), RsmqError> {
        number_in_range(hidden_duration, 0, 9_999_999)?;

        let queue = self.get_queue(qname, false).await?;
        //options.id, q.ts + options.vt * 1000
        CHANGE_MESSAGE_VISIVILITY
            .arg(3)
            .arg(format!("{}{}", self.options.ns, qname))
            .arg(message_id)
            .arg(queue.ts + hidden_duration * 1000)
            .invoke_async::<_, bool>(&mut self.connection.0)
            .await?;

        Ok(())
    }

    pub async fn create_queue_async(
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

        let results: (bool, bool, bool, bool, bool) = pipe()
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
            .query_async(&mut self.connection.0)
            .await?;

        if !results.0 {
            return Err(QueueExists {}.into());
        }

        redis::cmd("SADD")
            .arg(format!("{}QUEUES", self.options.ns))
            .arg(qname)
            .query_async(&mut self.connection.0)
            .await?;

        Ok(())
    }

    pub async fn delete_message_async(&mut self, qname: &str, id: &str) -> Result<bool, RsmqError> {
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

    pub async fn delete_queue_async(&mut self, qname: &str) -> Result<(), RsmqError> {
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

    pub async fn get_queue_attributes_async(
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

    pub async fn list_queues_async(&mut self) -> Result<Vec<String>, RsmqError> {
        let queues = redis::cmd("SMEMBERS")
            .arg(format!("{}QUEUES", self.options.ns))
            .query_async::<_, Vec<String>>(&mut self.connection.0)
            .await?;

        Ok(queues)
    }

    pub async fn pop_message_async(&mut self, qname: &str) -> Result<RsmqMessage, RsmqError> {
        let queue = self.get_queue(qname, false).await?;

        let result: (String, String, u64, u64) = POP_MESSAGE
            .arg(2)
            .arg(format!("{}{}", self.options.ns, qname))
            .arg(queue.ts)
            .invoke_async(&mut self.connection.0)
            .await?;

        Ok(RsmqMessage {
            id: result.0.clone(),
            message: result.1,
            rc: result.2,
            fr: result.3,
            sent: u64::from_str_radix(&result.0[0..10], 36).unwrap_or(0),
        })
    }

    pub async fn receive_message_async(
        &mut self,
        qname: &str,
        hidden_duration: Option<u64>,
    ) -> Result<RsmqMessage, RsmqError> {
        let queue = self.get_queue(qname, false).await?;

        let hidden_duration = hidden_duration.unwrap_or(queue.vt) * 1000;

        number_in_range(hidden_duration, 0, 9_999_999)?;

        let result: (String, String, u64, u64) = RECEIVE_MESSAGE
            .arg(3)
            .arg(format!("{}{}", self.options.ns, qname))
            .arg(queue.ts)
            .arg(queue.ts + hidden_duration)
            .invoke_async(&mut self.connection.0)
            .await?;

        Ok(RsmqMessage {
            id: result.0.clone(),
            message: result.1,
            rc: result.2,
            fr: result.3,
            sent: u64::from_str_radix(&result.0[0..10], 36).unwrap_or(0),
        })
    }

    pub async fn send_message_async(
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

        let result: (u64, u64, i64, u64) = commands.query_async(&mut self.connection.0).await?;

        if self.options.realtime {
            redis::cmd("PUBLISH")
                .arg(format!("{}rt:{}", self.options.ns, qname))
                .arg(result.3)
                .query_async::<_, Vec<String>>(&mut self.connection.0)
                .await?;
        }

        Ok(queue_uid)
    }

    pub async fn set_queue_attributes_async(
        &mut self,
        qname: &str,
        hidden_duration: Option<u64>,
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

        if let Some(duration) = hidden_duration {
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

        self.get_queue_attributes_async(qname).await
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
