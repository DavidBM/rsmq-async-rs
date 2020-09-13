use bb8_redis::{bb8::RunError, redis::RedisError};
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum RsmqError {
    #[error("Redis error: `{0:?}`")]
    RedisError(#[from] RedisError),
    #[error("Pool run error: `{0:?}`")]
    RunError(#[from] RunError<RedisError>),
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
}
