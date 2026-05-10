use crate::functions::{CachedScript, RbmqFunctions};
use crate::r#trait::RbmqConnection;
use crate::types::{RedisBytes, RbmqMessage, RbmqOptions, RbmqQueueAttributes};
use crate::RbmqResult;
use core::convert::TryFrom;
use core::marker::PhantomData;
use std::time::Duration;

#[derive(Clone)]
struct RedisConnection(redis::aio::MultiplexedConnection);

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MultiplexedRedisAsyncConnnection")
    }
}

#[derive(Debug, Clone)]
pub struct Rbmq {
    connection: RedisConnection,
    functions: RbmqFunctions<redis::aio::MultiplexedConnection>,
    scripts: CachedScript,
}

impl Rbmq {
    /// Creates a new rbmq instance, including its connection
    pub async fn new(options: RbmqOptions) -> RbmqResult<Rbmq> {
        let mut redis_info = redis::RedisConnectionInfo::default()
            .set_db(options.db.into())
            .set_protocol(options.protocol);
        if let Some(username) = options.username {
            redis_info = redis_info.set_username(username);
        }
        if let Some(password) = options.password {
            redis_info = redis_info.set_password(password);
        }
        let conn_info = format!("redis://{}:{}", options.host, options.port)
            .parse::<redis::ConnectionInfo>()?
            .set_redis_settings(redis_info);

        let client = redis::Client::open(conn_info)?;

        let connection = client.get_multiplexed_async_connection().await?;

        Rbmq::new_with_connection(connection, options.realtime, Some(&options.ns)).await
    }

    /// Special method for when you already have a redis-rs connection and you don't want redis_async to create a new one.
    pub async fn new_with_connection(
        mut connection: redis::aio::MultiplexedConnection,
        realtime: bool,
        ns: Option<&str>,
    ) -> RbmqResult<Rbmq> {
        let functions = RbmqFunctions {
            ns: ns.unwrap_or("rbmq").to_string(),
            realtime,
            conn: PhantomData,
        };

        let scripts = functions.load_scripts(&mut connection).await?;

        Ok(Rbmq {
            connection: RedisConnection(connection),
            functions,
            scripts,
        })
    }
}

impl RbmqConnection for Rbmq {
    async fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RbmqResult<()> {
        self.functions
            .change_message_visibility(
                &mut self.connection.0,
                qname,
                message_id,
                hidden,
                &self.scripts,
            )
            .await
    }

    async fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<()> {
        self.functions
            .create_queue(
                &mut self.connection.0,
                qname,
                hidden,
                delay,
                maxsize,
                &self.scripts,
            )
            .await
    }

    async fn delete_message(&mut self, qname: &str, id: &str) -> RbmqResult<bool> {
        self.functions
            .delete_message(&mut self.connection.0, qname, id, &self.scripts)
            .await
    }
    async fn delete_queue(&mut self, qname: &str) -> RbmqResult<()> {
        self.functions
            .delete_queue(&mut self.connection.0, qname, &self.scripts)
            .await
    }
    async fn get_queue_attributes(&mut self, qname: &str) -> RbmqResult<RbmqQueueAttributes> {
        self.functions
            .get_queue_attributes(&mut self.connection.0, qname, &self.scripts)
            .await
    }

    async fn list_queues(&mut self) -> RbmqResult<Vec<String>> {
        self.functions.list_queues(&mut self.connection.0).await
    }

    async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.functions
            .pop_message::<E>(&mut self.connection.0, qname, &self.scripts)
            .await
    }

    async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.functions
            .receive_message::<E>(&mut self.connection.0, qname, hidden, &self.scripts)
            .await
    }

    async fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RbmqResult<String> {
        self.functions
            .send_message(&mut self.connection.0, qname, message, delay, &self.scripts)
            .await
    }

    async fn send_message_batch<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RbmqResult<Vec<String>> {
        self.functions
            .send_message_batch(
                &mut self.connection.0,
                qname,
                messages,
                delay,
                &self.scripts,
            )
            .await
    }

    async fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
    ) -> RbmqResult<Vec<RbmqMessage<E>>> {
        self.functions
            .receive_message_batch::<E>(
                &mut self.connection.0,
                qname,
                hidden,
                max_count,
                &self.scripts,
            )
            .await
    }

    async fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<RbmqQueueAttributes> {
        self.functions
            .set_queue_attributes(
                &mut self.connection.0,
                qname,
                hidden,
                delay,
                maxsize,
                &self.scripts,
            )
            .await
    }

    async fn move_message(&mut self, src: &str, msg_id: &str, dst: &str) -> RbmqResult<bool> {
        self.functions
            .move_message(&mut self.connection.0, src, msg_id, dst, &self.scripts)
            .await
    }

    async fn receive_message_or_dlq<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        dlq: &str,
        max_receives: u64,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.functions
            .receive_message_or_dlq::<E>(
                &mut self.connection.0,
                qname,
                hidden,
                dlq,
                max_receives,
                &self.scripts,
            )
            .await
    }
}
