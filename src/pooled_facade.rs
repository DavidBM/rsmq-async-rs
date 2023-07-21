use crate::functions::RsmqFunctions;
use crate::r#trait::RsmqConnection;
use crate::types::RedisBytes;
use crate::types::{RsmqMessage, RsmqOptions, RsmqQueueAttributes};
use crate::RsmqResult;
use async_trait::async_trait;
use core::convert::TryFrom;
use redis::RedisError;
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: redis::Client,
}

impl RedisConnectionManager {
    pub fn from_client(client: redis::Client) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager { client })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = redis::aio::Connection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut redis::aio::Connection) -> Result<(), Self::Error> {
        redis::cmd("PING").query_async(conn).await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Default)]
pub struct PoolOptions {
    pub max_size: Option<u32>,
    pub min_idle: Option<u32>,
}

pub struct PooledRsmq {
    pool: bb8::Pool<RedisConnectionManager>,
    functions: RsmqFunctions<redis::aio::Connection>,
}

impl Clone for PooledRsmq {
    fn clone(&self) -> Self {
        PooledRsmq {
            pool: self.pool.clone(),
            functions: RsmqFunctions {
                ns: self.functions.ns.clone(),
                realtime: self.functions.realtime,
                conn: PhantomData,
            },
        }
    }
}

impl PooledRsmq {
    pub async fn new(options: RsmqOptions, pool_options: PoolOptions) -> RsmqResult<PooledRsmq> {
        let conn_info = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp(options.host, options.port),
            redis: redis::RedisConnectionInfo {
                db: options.db.into(),
                username: options.username,
                password: options.password,
            },
        };

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

        Ok(PooledRsmq {
            pool,
            functions: RsmqFunctions {
                ns: options.ns.clone(),
                realtime: options.realtime,
                conn: PhantomData,
            },
        })
    }
}

#[async_trait::async_trait]
impl RsmqConnection for PooledRsmq {
    async fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        seconds_hidden: u64,
    ) -> RsmqResult<()> {
        let mut conn = self.pool.get().await?;

        self.functions
            .change_message_visibility(&mut conn, qname, message_id, seconds_hidden)
            .await
    }

    async fn create_queue(
        &mut self,
        qname: &str,
        seconds_hidden: Option<u32>,
        delay: Option<u32>,
        maxsize: Option<i32>,
    ) -> RsmqResult<()> {
        let mut conn = self.pool.get().await?;

        self.functions
            .create_queue(&mut conn, qname, seconds_hidden, delay, maxsize)
            .await
    }

    async fn delete_message(&mut self, qname: &str, id: &str) -> RsmqResult<bool> {
        let mut conn = self.pool.get().await?;

        self.functions.delete_message(&mut conn, qname, id).await
    }
    async fn delete_queue(&mut self, qname: &str) -> RsmqResult<()> {
        let mut conn = self.pool.get().await?;

        self.functions.delete_queue(&mut conn, qname).await
    }
    async fn get_queue_attributes(&mut self, qname: &str) -> RsmqResult<RsmqQueueAttributes> {
        let mut conn = self.pool.get().await?;

        self.functions.get_queue_attributes(&mut conn, qname).await
    }

    async fn list_queues(&mut self) -> RsmqResult<Vec<String>> {
        let mut conn = self.pool.get().await?;

        self.functions.list_queues(&mut conn).await
    }

    async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        let mut conn = self.pool.get().await?;

        self.functions.pop_message::<E>(&mut conn, qname).await
    }

    async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        seconds_hidden: Option<u64>,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        let mut conn = self.pool.get().await?;

        self.functions
            .receive_message::<E>(&mut conn, qname, seconds_hidden)
            .await
    }

    async fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<u64>,
    ) -> RsmqResult<String> {
        let mut conn = self.pool.get().await?;

        self.functions
            .send_message(&mut conn, qname, message, delay)
            .await
    }

    async fn set_queue_attributes(
        &mut self,
        qname: &str,
        seconds_hidden: Option<u64>,
        delay: Option<u64>,
        maxsize: Option<i64>,
    ) -> RsmqResult<RsmqQueueAttributes> {
        let mut conn = self.pool.get().await?;

        self.functions
            .set_queue_attributes(&mut conn, qname, seconds_hidden, delay, maxsize)
            .await
    }
}
