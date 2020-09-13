use crate::error::RsmqError;
use crate::types::{RsmqMessage, RsmqQueueAttributes};

#[async_trait::async_trait]
pub trait RsmqConnection {
    /// Change the hidden time of a already sent message.
    async fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        seconds_hidden: u64,
    ) -> Result<(), RsmqError>;

    /// Creates a new queue. Attributes can be later modified with "set_queue_attributes" method
    ///
    /// seconds_hidden: Time the messages will be hidden when they are received with the "receive_message" method.
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited size)
    async fn create_queue(
        &mut self,
        qname: &str,
        seconds_hidden: Option<u32>,
        delay: Option<u32>,
        maxsize: Option<i32>,
    ) -> Result<(), RsmqError>;

    /// Deletes a message from the queue.
    ///
    /// Important to use when you are using receive_message.
    async fn delete_message(&mut self, qname: &str, id: &str) -> Result<bool, RsmqError>;
    /// Deletes the queue and all the messages on it
    async fn delete_queue(&mut self, qname: &str) -> Result<(), RsmqError>;
    /// Returns the queue attributes and statistics
    async fn get_queue_attributes(&mut self, qname: &str)
        -> Result<RsmqQueueAttributes, RsmqError>;

    /// Returns a list of queues in the namespace
    async fn list_queues(&mut self) -> Result<Vec<String>, RsmqError>;

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    async fn pop_message(&mut self, qname: &str) -> Result<Option<RsmqMessage>, RsmqError>;

    /// Returns a message. The message stays hidden for some time (defined by "seconds_hidden" argument or the queue settings). After that time, the message will be redelivered. In order to avoid the redelivery, you need to use the "dekete_message" after this function.
    async fn receive_message(
        &mut self,
        qname: &str,
        seconds_hidden: Option<u64>,
    ) -> Result<Option<RsmqMessage>, RsmqError>;

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or the queue settings) before being delivered to a client.
    async fn send_message(
        &mut self,
        qname: &str,
        message: &str,
        delay: Option<u64>,
    ) -> Result<String, RsmqError>;

    /// Modify the queue attributes. Keep in mind that "seconds_hidden" and "delay" can be overwritten when the message is sent. "seconds_hidden" can be changed by the method "change_message_visibility"
    ///
    /// seconds_hidden: Time the messages will be hidden when they are received with the "receive_message" method.
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited size)
    async fn set_queue_attributes(
        &mut self,
        qname: &str,
        seconds_hidden: Option<u64>,
        delay: Option<u64>,
        maxsize: Option<i64>,
    ) -> Result<RsmqQueueAttributes, RsmqError>;
}
