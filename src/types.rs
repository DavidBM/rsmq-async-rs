use core::convert::TryFrom;

#[derive(Debug)]
pub(crate) struct QueueDescriptor {
    pub vt: u64,
    pub delay: u64,
    pub maxsize: i64,
    pub ts: u64,
    pub uid: Option<String>,
}

/// Options for creating a new RSMQ instance.
#[derive(Debug, Clone)]
pub struct RsmqOptions {
    /// Redis host
    pub host: String,
    /// Redis port
    pub port: String,
    /// Redis db
    pub db: u8,
    /// If true, it will use redis pubsub to notify clients about new messages.
    /// More info in the general crate description
    pub realtime: bool,
    /// Redis password
    pub password: Option<String>,
    /// RSMQ namespace (you can have several. "rsmq" by default)
    pub ns: String,
}

impl Default for RsmqOptions {
    fn default() -> Self {
        RsmqOptions {
            host: "localhost".to_string(),
            port: "6379".to_string(),
            db: 0,
            realtime: false,
            password: None,
            ns: "rsmq".to_string(),
        }
    }
}

/// A new RSMQ message. You will get this when using pop_message or receive_message methods
#[derive(Debug, Clone)]
pub struct RsmqMessage<T: TryFrom<RedisBytes> = String> {
    /// Message id. Used later for change_message_visibility and delete_message
    pub id: String,
    /// Message content. It is wrapped in an string. If you are sending other
    /// format (JSON, etc) you will need to decode the message in your code
    pub message: T,
    /// Number of times the message was received by a client
    pub rc: u64,
    /// Timestamp (epoch in seconds) of when was this message received
    pub fr: u64,
    /// Timestamp (epoch in seconds) of when was this message sent
    pub sent: u64,
}

/// Struct defining a queue. They are set on "create_queue" and "set_queue_attributes"
#[derive(Debug, Clone)]
pub struct RsmqQueueAttributes {
    /// How many seconds the message will be hidden when is received by a client
    pub vt: u64,
    /// How many second will take until the message is delivered to a client
    /// since it was sent
    pub delay: u64,
    /// Max size of the message in bytes in the queue
    pub maxsize: u64,
    /// Number of messages received by the queue
    pub totalrecv: u64,
    /// Number of messages sent by the queue
    pub totalsent: u64,
    /// When was this queue created. Timestamp (epoch in seconds)
    pub created: u64,
    /// When was this queue last modified. Timestamp (epoch in seconds)
    pub modified: u64,
    /// How many messages the queue contains
    pub msgs: u64,
    /// How many messages are hidden from the queue. This number depends of
    /// the "vt" attribute and messages with a different hidden time modified
    /// by "change_message_visibility" method
    pub hiddenmsgs: u64,
}

#[derive(Debug)]
pub struct RedisBytes(pub(crate) Vec<u8>);

impl TryFrom<RedisBytes> for String {
    type Error = Vec<u8>;

    fn try_from(bytes: RedisBytes) -> Result<Self, Self::Error> {
        // For the library user, they can just call into_bytes
        // for getting the original Vec<u8>
        String::from_utf8(bytes.0).map_err(|e| e.into_bytes())
    }
}

impl From<RedisBytes> for Vec<u8> {
    fn from(bytes: RedisBytes) -> Vec<u8> {
        bytes.0
    }
}

impl Into<RedisBytes> for String {
    fn into(self) -> RedisBytes {
        RedisBytes(self.into())
    }
}

impl Into<RedisBytes> for &str {
    fn into(self) -> RedisBytes {
        RedisBytes(self.into())
    }
}

impl Into<RedisBytes> for Vec<u8> {
    fn into(self) -> RedisBytes {
        RedisBytes(self.into())
    }
}
