use crate::functions::RbmqFunctions;
use crate::r#trait::RbmqConnection;
use crate::types::{RbmqMessage, RbmqOptions, RbmqQueueAttributes, RedisBytes};
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
}

impl Rbmq {
    /// Creates a new rbmq instance, including its connection.
    pub async fn new(options: RbmqOptions) -> RbmqResult<Rbmq> {
        Self::from_client(&options.redis_client()?, options.realtime, &options.ns).await
    }

    /// Connect using a Redis URL in the default `rbmq` namespace.
    ///
    /// The URL may include authentication, a port, and a database number.
    pub async fn connect(url: &str) -> RbmqResult<Self> {
        Self::from_client(&redis::Client::open(url)?, false, "rbmq").await
    }

    pub(crate) async fn from_client(
        client: &redis::Client,
        realtime: bool,
        ns: &str,
    ) -> RbmqResult<Self> {
        let connection = client.get_multiplexed_async_connection().await?;
        Self::new_with_connection(connection, realtime, Some(ns)).await
    }

    /// For when you already have a redis-rs multiplexed connection and don't want a new one.
    pub async fn new_with_connection(
        connection: redis::aio::MultiplexedConnection,
        realtime: bool,
        ns: Option<&str>,
    ) -> RbmqResult<Rbmq> {
        let functions = RbmqFunctions {
            ns: ns.unwrap_or("rbmq").to_string(),
            realtime,
            conn: PhantomData,
        };
        Ok(Rbmq {
            connection: RedisConnection(connection),
            functions,
        })
    }

    #[cfg(feature = "worker")]
    pub(crate) async fn receive_for_worker(
        &mut self,
        qname: &str,
        lease: Duration,
    ) -> RbmqResult<Option<RbmqMessage<Vec<u8>>>> {
        self.functions
            .receive_for_worker(&mut self.connection.0, qname, lease)
            .await
    }

    #[cfg(feature = "worker")]
    pub(crate) async fn update_lease(
        &mut self,
        qname: &str,
        id: &str,
        receive_count: u64,
        hidden: Option<Duration>,
    ) -> RbmqResult<bool> {
        self.functions
            .update_visibility(
                &mut self.connection.0,
                qname,
                id,
                hidden,
                Some(receive_count),
            )
            .await
    }

    #[cfg(feature = "worker")]
    pub(crate) async fn acknowledge(
        &mut self,
        qname: &str,
        id: &str,
        receive_count: u64,
    ) -> RbmqResult<bool> {
        self.functions
            .delete_message(&mut self.connection.0, qname, id, Some(receive_count))
            .await
    }

    #[cfg(feature = "worker")]
    pub(crate) async fn dead_letter(
        &mut self,
        qname: &str,
        id: &str,
        receive_count: u64,
        target: &str,
    ) -> RbmqResult<bool> {
        self.functions
            .move_message(
                &mut self.connection.0,
                qname,
                id,
                target,
                Some(receive_count),
            )
            .await
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
            .change_message_visibility(&mut self.connection.0, qname, message_id, hidden)
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
            .create_queue(&mut self.connection.0, qname, hidden, delay, maxsize)
            .await
    }

    async fn delete_message(&mut self, qname: &str, id: &str) -> RbmqResult<bool> {
        self.functions
            .delete_message(&mut self.connection.0, qname, id, None)
            .await
    }

    async fn delete_queue(&mut self, qname: &str) -> RbmqResult<()> {
        self.functions
            .delete_queue(&mut self.connection.0, qname)
            .await
    }

    async fn get_queue_attributes(&mut self, qname: &str) -> RbmqResult<RbmqQueueAttributes> {
        self.functions
            .get_queue_attributes(&mut self.connection.0, qname)
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
            .pop_message::<E>(&mut self.connection.0, qname)
            .await
    }

    async fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RbmqResult<Option<RbmqMessage<E>>> {
        self.functions
            .receive_message::<E>(&mut self.connection.0, qname, hidden)
            .await
    }

    async fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RbmqResult<String> {
        self.functions
            .send_message(&mut self.connection.0, qname, message, delay)
            .await
    }

    async fn send_message_batch<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RbmqResult<Vec<String>> {
        self.functions
            .send_message_batch(&mut self.connection.0, qname, messages, delay)
            .await
    }

    async fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
    ) -> RbmqResult<Vec<RbmqMessage<E>>> {
        self.functions
            .receive_message_batch::<E>(&mut self.connection.0, qname, hidden, max_count)
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
            .set_queue_attributes(&mut self.connection.0, qname, hidden, delay, maxsize)
            .await
    }

    async fn move_message(&mut self, src: &str, msg_id: &str, dst: &str) -> RbmqResult<bool> {
        self.functions
            .move_message(&mut self.connection.0, src, msg_id, dst, None)
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
            .receive_message_or_dlq::<E>(&mut self.connection.0, qname, hidden, dlq, max_receives)
            .await
    }
}
