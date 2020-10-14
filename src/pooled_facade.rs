use crate::functions::RsmqFunctions;
use crate::r#trait::RsmqConnection;
use crate::types::RedisBytes;
use crate::types::{RsmqMessage, RsmqOptions, RsmqQueueAttributes};
use crate::RsmqResult;
use core::convert::TryFrom;
use std::marker::PhantomData;

use async_trait::async_trait;
use redis::RedisError;

#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: redis::Client,
}

impl RedisConnectionManager {
    pub fn new<T: redis::IntoConnectionInfo>(
        info: T,
    ) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager {
            client: redis::Client::open(info.into_connection_info()?)?,
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = redis::aio::Connection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, mut conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        redis::cmd("PING").query_async::<_, _>(&mut conn).await?;
        Ok(conn)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

pub struct PoolOptions {
    max_size: Option<u32>,
    min_idle: Option<u32>,
}

impl Default for PoolOptions {
    fn default() -> Self {
        PoolOptions {
            max_size: None,
            min_idle: None,
        }
    }
}

pub struct PooledRsmq {
    pool: bb8::Pool<RedisConnectionManager>,
    options: RsmqOptions,
    functions: RsmqFunctions<redis::aio::Connection>,
}

impl Clone for PooledRsmq {
    fn clone(&self) -> Self {
        PooledRsmq {
            pool: self.pool.clone(),
            functions: RsmqFunctions {
                ns: self.options.ns.clone(),
                realtime: self.options.realtime,
                conn: PhantomData,
            },
            options: self.options.clone(),
        }
    }
}

impl PooledRsmq {
    pub async fn new(options: RsmqOptions, pool_options: PoolOptions) -> RsmqResult<PooledRsmq> {
        let password = if let Some(ref password) = options.password {
            format!("redis:{}@", password)
        } else {
            "".to_string()
        };

        let url = format!(
            "redis://{}{}:{}/{}",
            password, options.host, options.port, options.db
        );

        let manager = RedisConnectionManager::new(url).unwrap();
        let builder = bb8::Pool::builder();

        let mut builder = if let Some(value) = pool_options.max_size {
            builder.max_size(value)
        } else {
            builder
        };

        builder = builder.min_idle(pool_options.min_idle);

        let pool = builder.build(manager).await.unwrap();

        Ok(PooledRsmq {
            pool,
            functions: RsmqFunctions {
                ns: options.ns.clone(),
                realtime: options.realtime,
                conn: PhantomData,
            },
            options,
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
