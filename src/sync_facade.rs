use tokio::runtime::Runtime;

use crate::functions::RsmqFunctions;
use crate::r#trait::RsmqConnection;
use crate::types::{RedisBytes, RsmqMessage, RsmqOptions, RsmqQueueAttributes};
use crate::{RsmqError, RsmqResult};
use core::convert::TryFrom;
use core::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct RedisConnection(redis::aio::MultiplexedConnection);

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MultiplexedRedisAsyncConnnection")
    }
}

#[derive(Debug, Clone)]
pub struct RsmqSync {
    connection: RedisConnection,
    functions: RsmqFunctions<redis::aio::MultiplexedConnection>,
    runner: Arc<Runtime>,
}

impl RsmqSync {
    /// Creates a new RSMQ instance, including its connection
    pub async fn new(options: RsmqOptions) -> RsmqResult<RsmqSync> {
        let runner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RsmqError::TokioStart(e.into()))?;

        let conn_info = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp(options.host, options.port),
            redis: redis::RedisConnectionInfo {
                db: options.db.into(),
                username: options.username,
                password: options.password,
            },
        };

        let client = redis::Client::open(conn_info)?;

        let connection =
            runner.block_on(async move { client.get_multiplexed_async_connection().await })?;

        Ok(RsmqSync {
            connection: RedisConnection(connection),
            functions: RsmqFunctions {
                ns: options.ns,
                realtime: options.realtime,
                conn: PhantomData,
            },
            runner: Arc::new(runner),
        })
    }
}

#[async_trait::async_trait]
impl RsmqConnection for RsmqSync {
    async fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RsmqResult<()> {
        self.runner.block_on(async {
            self.functions
                .change_message_visibility(&mut self.connection.0, qname, message_id, hidden)
                .await
        })
    }

    async fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i32>,
    ) -> RsmqResult<()> {
        self.runner.block_on(async {
            self.functions
                .create_queue(&mut self.connection.0, qname, hidden, delay, maxsize)
                .await
        })
    }

    async fn delete_message(&mut self, qname: &str, id: &str) -> RsmqResult<bool> {
        self.runner.block_on(async {
            self.functions
                .delete_message(&mut self.connection.0, qname, id)
                .await
        })
    }
    async fn delete_queue(&mut self, qname: &str) -> RsmqResult<()> {
        self.runner.block_on(async {
            self.functions
                .delete_queue(&mut self.connection.0, qname)
                .await
        })
    }
    async fn get_queue_attributes(&mut self, qname: &str) -> RsmqResult<RsmqQueueAttributes> {
        self.runner.block_on(async {
            self.functions
                .get_queue_attributes(&mut self.connection.0, qname)
                .await
        })
    }

    async fn list_queues(&mut self) -> RsmqResult<Vec<String>> {
        self.runner
            .block_on(async { self.functions.list_queues(&mut self.connection.0).await })
    }

    async fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        self.runner.block_on(async {
            self.functions
                .pop_message::<E>(&mut self.connection.0, qname)
                .await
        })
    }

    async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        self.runner.block_on(async {
            self.functions
                .receive_message::<E>(&mut self.connection.0, qname, hidden)
                .await
        })
    }

    async fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RsmqResult<String> {
        self.runner.block_on(async {
            self.functions
                .send_message(&mut self.connection.0, qname, message, delay)
                .await
        })
    }

    async fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RsmqResult<RsmqQueueAttributes> {
        self.runner.block_on(async {
            self.functions
                .set_queue_attributes(&mut self.connection.0, qname, hidden, delay, maxsize)
                .await
        })
    }
}
