use crate::types::RedisBytes;
use crate::types::{RbmqMessage, RbmqQueueAttributes};
use crate::RbmqResult;
use core::convert::TryFrom;
use std::future::Future;
use std::time::Duration;

pub trait RbmqConnection {
    /// Change the hidden time of a already sent message.
    ///
    /// `hidden` has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart
    fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> impl Future<Output = RbmqResult<()>> + Send;

    /// Creates a new queue. Attributes can be later modified with "set_queue_attributes" method
    ///
    /// hidden: Time the messages will be hidden when they are received with the "receive_message" method. It
    /// has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited
    /// size)
    fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> impl Future<Output = RbmqResult<()>> + Send;

    /// Deletes a message from the queue.
    ///
    /// Important to use when you are using receive_message.
    fn delete_message(
        &mut self,
        qname: &str,
        id: &str,
    ) -> impl Future<Output = RbmqResult<bool>> + Send;

    /// Deletes the queue and all the messages on it
    fn delete_queue(&mut self, qname: &str) -> impl Future<Output = RbmqResult<()>> + Send;

    /// Returns the queue attributes and statistics
    fn get_queue_attributes(
        &mut self,
        qname: &str,
    ) -> impl Future<Output = RbmqResult<RbmqQueueAttributes>> + Send;

    /// Returns a list of queues in the namespace
    fn list_queues(&mut self) -> impl Future<Output = RbmqResult<Vec<String>>> + Send;

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> impl Future<Output = RbmqResult<Option<RbmqMessage<E>>>> + Send;

    /// Returns a message. The message stays hidden for some time (defined by "hidden" argument or the queue
    /// settings). After that time, the message will be redelivered. In order to avoid the redelivery, you need to use
    /// the "delete_message" after this function.
    ///
    /// `hidden` has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart.
    fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> impl Future<Output = RbmqResult<Option<RbmqMessage<E>>>> + Send;

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or
    /// the queue settings) before being delivered to a client.
    fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> impl Future<Output = RbmqResult<String>> + Send;

    /// Sends a batch of messages to the queue. Returns the assigned message IDs in input order.
    ///
    /// The default impl loops `send_message`, which means a mid-batch error leaves whatever
    /// succeeded before that point in the queue. Implementations like [`crate::Rbmq`] and
    /// [`crate::PooledRbmq`] override this with a single Lua script for full atomicity (either
    /// all messages land or none do) and at most one realtime PUBLISH per call.
    fn send_message_batch<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> impl Future<Output = RbmqResult<Vec<String>>> + Send
    where
        Self: Send,
    {
        async move {
            if messages.is_empty() {
                return Ok(Vec::new());
            }
            let mut ids = Vec::with_capacity(messages.len());
            for m in messages {
                ids.push(self.send_message(qname, m, delay).await?);
            }
            Ok(ids)
        }
    }

    /// Receives up to `max_count` messages, sharing the same hidden duration. Returns at most
    /// `max_count` messages but may return fewer (or zero) if not enough are visible.
    ///
    /// The default impl loops `receive_message`. Implementations like [`crate::Rbmq`] and
    /// [`crate::PooledRbmq`] override this with a single Lua script that picks all messages
    /// in one ZRANGE BYSCORE pass.
    fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>> + Send>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
    ) -> impl Future<Output = RbmqResult<Vec<RbmqMessage<E>>>> + Send
    where
        Self: Send,
    {
        async move {
            if max_count == 0 {
                return Ok(Vec::new());
            }
            let mut out = Vec::with_capacity(max_count as usize);
            for _ in 0..max_count {
                match self.receive_message(qname, hidden).await? {
                    Some(m) => out.push(m),
                    None => break,
                }
            }
            Ok(out)
        }
    }

    /// Modify the queue attributes. Keep in mind that "hidden" and "delay" can be overwritten when the message
    /// is sent. "hidden" can be changed by the method "change_message_visibility"
    ///
    /// hidden: Time the messages will be hidden when they are received with the "receive_message" method. It
    /// has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited
    /// size)
    fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> impl Future<Output = RbmqResult<RbmqQueueAttributes>> + Send;

    /// Atomically moves a message from `src` to `dst`, preserving its body and the
    /// `:rc` / `:fr` metadata. The message becomes visible in `dst` immediately
    /// (score = current time). Returns `true` if the message was found in `src`,
    /// `false` if it didn't exist there. Rejects `src == dst`.
    ///
    /// `dst` must be a queue that already exists (created via `create_queue`); this
    /// method does not initialize the destination queue's configuration, only inserts
    /// the message and bumps `totalsent`.
    fn move_message(
        &mut self,
        src: &str,
        msg_id: &str,
        dst: &str,
    ) -> impl Future<Output = RbmqResult<bool>> + Send;

    /// Receives the next visible message, but transparently routes any message whose
    /// post-increment `rc` exceeds `max_receives` to `dlq` (preserving `:rc` and `:fr`)
    /// and tries the next visible message. Returns the first message whose `rc` is
    /// still within budget, or `None` if no eligible message was found within the
    /// script's per-call iteration cap. Rejects `qname == dlq`.
    ///
    /// `max_receives = 0` ⇒ never deliver, always route to DLQ.
    /// `max_receives = N` ⇒ deliver at most N times before routing to DLQ.
    fn receive_message_or_dlq<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        dlq: &str,
        max_receives: u64,
    ) -> impl Future<Output = RbmqResult<Option<RbmqMessage<E>>>> + Send;
}

#[cfg(feature = "sync")]
pub trait RbmqConnectionSync {
    /// Change the hidden time of a already sent message.
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    /// * `message_id` - ID of the message to modify
    /// * `hidden` - New hidden duration. Has a max time of 9_999_999 for compatibility reasons with the JS version
    fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RbmqResult<()>;

    /// Creates a new queue. Attributes can be later modified with "set_queue_attributes" method
    ///
    /// # Arguments
    /// * `qname` - Name of the queue to create
    /// * `hidden` - Time the messages will be hidden when received. Max 9_999_999
    /// * `delay` - Time messages will be delayed before delivery
    /// * `maxsize` - Maximum message size in bytes (1024-65536 or -1 for unlimited)
    fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<()>;

    /// Deletes a message from the queue.
    ///
    /// Important to use when you are using receive_message.
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    /// * `id` - ID of the message to delete
    fn delete_message(&mut self, qname: &str, id: &str) -> RbmqResult<bool>;

    /// Deletes the queue and all messages in it
    ///
    /// # Arguments
    /// * `qname` - Name of the queue to delete
    fn delete_queue(&mut self, qname: &str) -> RbmqResult<()>;

    /// Returns the queue attributes and statistics
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    fn get_queue_attributes(&mut self, qname: &str) -> RbmqResult<RbmqQueueAttributes>;

    /// Returns a list of queues in the namespace
    fn list_queues(&mut self) -> RbmqResult<Vec<String>>;

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RbmqResult<Option<RbmqMessage<E>>>;

    /// Returns a message. The message stays hidden for some time (defined by "hidden" argument or the queue
    /// settings). After that time, the message will be redelivered. To avoid redelivery, use "delete_message"
    /// after this function.
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    /// * `hidden` - Optional custom hidden duration. Max 9_999_999
    fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RbmqResult<Option<RbmqMessage<E>>>;

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or
    /// the queue settings) before being delivered to a client.
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    /// * `message` - Message content to send
    /// * `delay` - Optional custom delay duration
    fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RbmqResult<String>;

    /// Sends a batch of messages. Returns assigned IDs in input order. The default impl loops
    /// `send_message`; specialized implementations atomically insert via a single Lua script.
    fn send_message_batch<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RbmqResult<Vec<String>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::with_capacity(messages.len());
        for m in messages {
            ids.push(self.send_message(qname, m, delay)?);
        }
        Ok(ids)
    }

    /// Receives up to `max_count` messages. Returns at most `max_count` but may return fewer.
    /// The default impl loops `receive_message`; specialized implementations use a single
    /// Lua script.
    fn receive_message_batch<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        max_count: u32,
    ) -> RbmqResult<Vec<RbmqMessage<E>>> {
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(max_count as usize);
        for _ in 0..max_count {
            match self.receive_message(qname, hidden)? {
                Some(m) => out.push(m),
                None => break,
            }
        }
        Ok(out)
    }

    /// Modify the queue attributes. Note that "hidden" and "delay" can be overwritten when sending messages.
    /// "hidden" can be changed by the method "change_message_visibility"
    ///
    /// # Arguments
    /// * `qname` - Name of the queue
    /// * `hidden` - Time messages will be hidden when received. Max 9_999_999
    /// * `delay` - Time messages will be delayed before delivery
    /// * `maxsize` - Maximum message size in bytes (1024-65536 or -1 for unlimited)
    fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RbmqResult<RbmqQueueAttributes>;

    /// Atomically moves a message from `src` to `dst`. See the async counterpart
    /// [`RbmqConnection::move_message`] for the full contract.
    fn move_message(&mut self, src: &str, msg_id: &str, dst: &str) -> RbmqResult<bool>;

    /// Atomically receives a message, routing over-budget messages to `dlq`.
    /// See the async counterpart [`RbmqConnection::receive_message_or_dlq`].
    fn receive_message_or_dlq<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        dlq: &str,
        max_receives: u64,
    ) -> RbmqResult<Option<RbmqMessage<E>>>;
}
