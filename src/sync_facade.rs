use crate::functions::{CachedScript, RbmqFunctions};
use crate::r#trait::RbmqConnectionSync;
use crate::types::{RedisBytes, RbmqMessage, RbmqOptions, RbmqQueueAttributes};
use crate::{RbmqError, RbmqResult};
use core::convert::TryFrom;
use core::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Clone)]
struct RedisConnection(redis::aio::MultiplexedConnection);

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MultiplexedRedisAsyncConnnection")
    }
}

#[derive(Debug, Clone)]
pub struct RbmqSync {
    connection: RedisConnection,
    functions: RbmqFunctions<redis::aio::MultiplexedConnection>,
    runner: Arc<Runtime>,
    scripts: CachedScript,
}

impl RbmqSync {
    /// Creates a new rbmq instance, including its connection
    pub async fn new(options: RbmqOptions) -> RbmqResult<RbmqSync> {
        let runner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RbmqError::TokioStart(e.into()))?;

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

        let functions = RbmqFunctions {
            ns: options.ns,
            realtime: options.realtime,
            conn: PhantomData,
        };

        let (connection, scripts) = runner.block_on(async {
            let mut conn = client.get_multiplexed_async_connection().await?;
            let scripts = functions.load_scripts(&mut conn).await?;
            Result::<_, RbmqError>::Ok((conn, scripts))
        })?;

        Ok(RbmqSync {
            connection: RedisConnection(connection),
            functions,
            runner: Arc::new(runner),
            scripts,
        })
    }
}

impl RbmqConnectionSync for RbmqSync {
    fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RbmqResult<()> {
        self.runner.block_on(async {
            self.functions
                .change_message_visibility(
                    &mut self.connection.0,
                    qname,
                    message_id,
                    hidden,
                    &self.scripts,
                )
                .await
        })
    }

    fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<()> {
        self.runner.block_on(async {
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
        })
    }

    fn delete_message(&mut self, qname: &str, id: &str) -> RbmqResult<bool> {
        self.runner.block_on(async {
            self.functions
                .delete_message(&mut self.connection.0, qname, id, &self.scripts)
                .await
        })
    }
    fn delete_queue(&mut self, qname: &str) -> RbmqResult<()> {
        self.runner.block_on(async {
            self.functions
                .delete_queue(&mut self.connection.0, qname, &self.scripts)
                .await
        })
    }
    fn get_queue_attributes(&mut self, qname: &str) -> RbmqResult<RbmqQueueAttributes> {
        self.runner.block_on(async {
            self.functions
                .get_queue_attributes(&mut self.connection.0, qname, &self.scripts)
                .await
        })
    }

    fn list_queues(&mut self) -> RbmqResult<Vec<String>> {
        self.runner
            .block_on(async { self.functions.list_queues(&mut self.connection.0).await })
    }

    fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.runner.block_on(async {
            self.functions
                .pop_message::<E>(&mut self.connection.0, qname, &self.scripts)
                .await
        })
    }

    fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.runner.block_on(async {
            self.functions
                .receive_message::<E>(&mut self.connection.0, qname, hidden, &self.scripts)
                .await
        })
    }

    fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RbmqResult<String> {
        self.runner.block_on(async {
            self.functions
                .send_message(&mut self.connection.0, qname, message, delay, &self.scripts)
                .await
        })
    }

    fn send_message_batch<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RbmqResult<Vec<String>> {
        self.runner.block_on(async {
            self.functions
                .send_message_batch(
                    &mut self.connection.0,
                    qname,
                    messages,
                    delay,
                    &self.scripts,
                )
                .await
        })
    }

    fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
    ) -> RbmqResult<Vec<RbmqMessage<E>>> {
        self.runner.block_on(async {
            self.functions
                .receive_message_batch::<E>(
                    &mut self.connection.0,
                    qname,
                    hidden,
                    max_count,
                    &self.scripts,
                )
                .await
        })
    }

    fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<RbmqQueueAttributes> {
        self.runner.block_on(async {
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
        })
    }

    fn move_message(&mut self, src: &str, msg_id: &str, dst: &str) -> RbmqResult<bool> {
        self.runner.block_on(async {
            self.functions
                .move_message(&mut self.connection.0, src, msg_id, dst, &self.scripts)
                .await
        })
    }

    fn receive_message_or_dlq<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        dlq: &str,
        max_receives: u64,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.runner.block_on(async {
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
        })
    }
}
