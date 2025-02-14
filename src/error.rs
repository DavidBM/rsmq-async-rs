use bb8_redis::bb8::RunError;
use redis::RedisError;
use thiserror::Error as ThisError;

/// This is an alias of `Result<T, RsmqError>` for simplicity
pub type RsmqResult<T> = Result<T, RsmqError>;

/// This is the error type for any oprtation with this
/// library. It derives `ThisError`
#[derive(ThisError, Debug, PartialEq)]
pub enum RsmqError {
    #[error("Pool run error: `{0:?}`")]
    RunError(#[from] RunError<RedisError>),
    #[error("Redis error: `{0:?}`")]
    RedisError(#[from] RedisError),
    #[error("No connection acquired`")]
    NoConnectionAcquired,
    #[error("No attribute was supplied")]
    NoAttributeSupplied,
    #[error("No `{0:?}` supplied")]
    MissingParameter(String),
    #[error("Invalid `{0:?} format`")]
    InvalidFormat(String),
    #[error("{0:?} must be between {0:?} and {0:?}")]
    InvalidValue(String, String, String),
    #[error("Message not string")]
    MessageNotString,
    #[error("Message too long")]
    MessageTooLong,
    #[error("Queue not found")]
    QueueNotFound,
    #[error("Queue already exists")]
    QueueExists,
    #[error("Error when trying to create random value. This is a bug and realted with the rust random generator")]
    BugCreatingRandonValue,
    #[error("Cannot parse queue vt")]
    CannotParseVT,
    #[error("Cannot parse queue delay")]
    CannotParseDelay,
    #[error("Cannot parse queue maxsize")]
    CannotParseMaxsize,
    #[error("The message received from Redis cannot be decoded into the expected type. Try to use Vec<u8> instead.")]
    CannotDecodeMessage(Vec<u8>),
    #[error("Cannot start tokio runtime for sync facade")]
    TokioStart(Different<std::io::Error>),
}

#[derive(Debug)]
pub struct Different<T>(pub T);

impl<T> PartialEq for Different<T> {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl<T> From<T> for Different<T> {
    fn from(value: T) -> Self {
        Different(value)
    }
}
