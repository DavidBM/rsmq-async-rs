mod errors;

use errors::*;
use lazy_static::lazy_static;
use rand::seq::IteratorRandom;
use redis::{aio::Connection, pipe, Client, Script};
use radix_fmt::radix_36;

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
    host: String,
    port: String,
    realtime: bool,
    password: Option<String>,
    ns: String,
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
pub struct Rsmq {
    client: Client,
    connection: RedisConnection,
    options: RsmqOptions,
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

    pub fn change_message_visibility_async() {
        unimplemented!();
    }

    pub fn create_queue_async() {
        unimplemented!();
    }

    pub fn delete_message_async() {
        unimplemented!();
    }

    pub fn delete_queue_async() {
        unimplemented!();
    }

    pub fn get_queue_attributes_async() {
        unimplemented!();
    }

    pub fn list_queues_async() {
        unimplemented!();
    }

    pub fn pop_message_async() {
        unimplemented!();
    }

    pub fn receive_message_async() {
        unimplemented!();
    }

    pub fn send_message_async() {
        unimplemented!();
    }

    pub fn set_queue_attributes_async() {
        unimplemented!();
    }

    async fn get_queue(&mut self, qname: &str, uid: bool) -> Result<QueueDescriptor, RsmqError> {
        let result: (Vec<String>, (String, String)) = pipe()
            .cmd("HMGET")
            .arg(format!("{}{}:Q", self.options.ns, qname))
            .arg("vt")
            .arg("delay")
            .arg("maxsize")
            .cmd("TIME")
            .query_async(&mut self.connection.0)
            .await?;

        let hmget_result = result.0.clone();

        if hmget_result.get(0).is_none() || hmget_result.get(1).is_none() || hmget_result.get(2).is_none() {
            return Err(QueueNotFound {}.into())
        }

        let ms = &match (result.1).1.len() {
            6 => (result.1).1,
            0..=5 => format!("{:0>6}", (result.1).1),
            _ => {
                let value = (result.1).1;
                let pos = value.len() - 6;
                value[pos..(pos+3)].to_string()
            }
        };

        let ts = (result.1).0.clone() + &ms;

        let quid = if uid {
            let number = ((result.1).0 + ms).parse::<i64>().expect("bad data in db, cannot parse number"); 
            Some(radix_36(number).to_string() + &Rsmq::make_id(22)) 
        } else { None };

        Ok(QueueDescriptor {
            vt: (result.0)[0].parse().expect("cannot parse queue vt"),
            delay: (result.0)[1].parse().expect("cannot parse queue delay"),
            maxsize: (result.0)[2].parse().expect("cannot parse queue maxsize"),
            ts: ts.parse().expect("cannot parse queue ts"),
            uid: quid,
        })
    }

    fn make_id(len: u16) -> String {
        let possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        let mut rng = rand::thread_rng();

        let mut id = String::new();

        for _ in 0..len {
            id.push(possible.chars().choose(&mut rng).unwrap());
        }

        id
    }
}
