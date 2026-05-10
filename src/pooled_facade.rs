use crate::functions::{CachedScript, RbmqFunctions};
use crate::r#trait::RbmqConnection;
use crate::types::RedisBytes;
use crate::types::{RbmqMessage, RbmqOptions, RbmqQueueAttributes};
use crate::RbmqResult;
use core::convert::TryFrom;
use redis::RedisError;
use std::marker::PhantomData;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: redis::Client,
}

impl RedisConnectionManager {
    pub fn from_client(client: redis::Client) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager { client })
    }
}

impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = redis::aio::MultiplexedConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_multiplexed_async_connection().await
    }

    async fn is_valid(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> Result<(), Self::Error> {
        redis::cmd("PING").query_async(conn).await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Debug, Clone, Default)]
pub struct PoolOptions {
    pub max_size: Option<u32>,
    pub min_idle: Option<u32>,
}

pub struct PooledRbmq {
    pool: bb8::Pool<RedisConnectionManager>,
    functions: RbmqFunctions<redis::aio::MultiplexedConnection>,
    scripts: CachedScript,
}

impl Clone for PooledRbmq {
    fn clone(&self) -> Self {
        PooledRbmq {
            pool: self.pool.clone(),
            functions: RbmqFunctions {
                ns: self.functions.ns.clone(),
                realtime: self.functions.realtime,
                conn: PhantomData,
            },
            scripts: self.scripts.clone(),
        }
    }
}

impl PooledRbmq {
    pub async fn new(options: RbmqOptions, pool_options: PoolOptions) -> RbmqResult<PooledRbmq> {
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

        let manager = RedisConnectionManager::from_client(client)?;
        let builder = bb8::Pool::builder();

        let mut builder = if let Some(value) = pool_options.max_size {
            builder.max_size(value)
        } else {
            builder
        };

        builder = builder.min_idle(pool_options.min_idle);

        let pool = builder.build(manager).await?;

        let mut conn = pool.get().await?;

        let functions = RbmqFunctions::<redis::aio::MultiplexedConnection> {
            ns: options.ns.clone(),
            realtime: options.realtime,
            conn: PhantomData,
        };

        let scripts = functions.load_scripts(&mut conn).await?;

        drop(conn);

        Ok(PooledRbmq {
            pool,
            functions,
            scripts,
        })
    }

    pub async fn new_with_pool(
        pool: bb8::Pool<RedisConnectionManager>,
        realtime: bool,
        ns: Option<&str>,
    ) -> RbmqResult<PooledRbmq> {
        let mut conn = pool.get().await?;

        let functions = RbmqFunctions::<redis::aio::MultiplexedConnection> {
            ns: ns.unwrap_or("rbmq").to_string(),
            realtime,
            conn: PhantomData,
        };

        let scripts = functions.load_scripts(&mut conn).await?;

        drop(conn);

        Ok(PooledRbmq {
            pool,
            functions,
            scripts,
        })
    }
}

impl RbmqConnection for PooledRbmq {
    async fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RbmqResult<()> {
        let mut conn = self.pool.get().await?;

        self.functions
            .change_message_visibility(&mut conn, qname, message_id, hidden, &self.scripts)
            .await
    }

    async fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<()> {
        let mut conn = self.pool.get().await?;

        self.functions
            .create_queue(&mut conn, qname, hidden, delay, maxsize)
            .await
    }

    async fn delete_message(&mut self, qname: &str, id: &str) -> RbmqResult<bool> {
        let mut conn = self.pool.get().await?;

        self.functions.delete_message(&mut conn, qname, id).await
    }
    async fn delete_queue(&mut self, qname: &str) -> RbmqResult<()> {
        let mut conn = self.pool.get().await?;

        self.functions.delete_queue(&mut conn, qname).await
    }
    async fn get_queue_attributes(&mut self, qname: &str) -> RbmqResult<RbmqQueueAttributes> {
        let mut conn = self.pool.get().await?;

        self.functions
            .get_queue_attributes(&mut conn, qname, &self.scripts)
            .await
    }

    async fn list_queues(&mut self) -> RbmqResult<Vec<String>> {
        let mut conn = self.pool.get().await?;

        self.functions.list_queues(&mut conn).await
    }

    async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        let mut conn = self.pool.get().await?;

        self.functions
            .pop_message::<E>(&mut conn, qname, &self.scripts)
            .await
    }

    async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        let mut conn = self.pool.get().await?;

        self.functions
            .receive_message::<E>(&mut conn, qname, hidden, &self.scripts)
            .await
    }

    async fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RbmqResult<String> {
        let mut conn = self.pool.get().await?;

        self.functions
            .send_message(&mut conn, qname, message, delay)
            .await
    }

    async fn send_message_batch<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RbmqResult<Vec<String>> {
        let mut conn = self.pool.get().await?;

        self.functions
            .send_message_batch(&mut conn, qname, messages, delay, &self.scripts)
            .await
    }

    async fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
    ) -> RbmqResult<Vec<RbmqMessage<E>>> {
        let mut conn = self.pool.get().await?;

        self.functions
            .receive_message_batch::<E>(&mut conn, qname, hidden, max_count, &self.scripts)
            .await
    }

    async fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<RbmqQueueAttributes> {
        let mut conn = self.pool.get().await?;

        self.functions
            .set_queue_attributes(&mut conn, qname, hidden, delay, maxsize, &self.scripts)
            .await
    }

    async fn move_message(&mut self, src: &str, msg_id: &str, dst: &str) -> RbmqResult<bool> {
        let mut conn = self.pool.get().await?;
        self.functions
            .move_message(&mut conn, src, msg_id, dst, &self.scripts)
            .await
    }

    async fn receive_message_or_dlq<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        dlq: &str,
        max_receives: u64,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        let mut conn = self.pool.get().await?;
        self.functions
            .receive_message_or_dlq::<E>(&mut conn, qname, hidden, dlq, max_receives, &self.scripts)
            .await
    }
}
