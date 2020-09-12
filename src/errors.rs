use redis::RedisError;
use std::{error::Error, fmt};

#[derive(Debug)]
pub struct NoAttributeSupplied;

impl Error for NoAttributeSupplied {}

impl fmt::Display for NoAttributeSupplied {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "No attribute was supplied")
    }
}

#[derive(Debug)]
pub struct MissingParameter(pub String);

impl Error for MissingParameter {}

impl fmt::Display for MissingParameter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "No {} supplied", self.0)
    }
}

#[derive(Debug)]
pub struct InvalidFormat(pub String);

impl Error for InvalidFormat {}

impl fmt::Display for InvalidFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid {} format", self.0)
    }
}

#[derive(Debug)]
pub struct InvalidValue(pub String, pub String, pub String);

impl Error for InvalidValue {}

impl fmt::Display for InvalidValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} must be between {} and {}", self.0, self.1, self.2)
    }
}

#[derive(Debug)]
pub struct MessageNotString();

impl Error for MessageNotString {}

impl fmt::Display for MessageNotString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "")
    }
}

#[derive(Debug)]
pub struct MessageTooLong();

impl Error for MessageTooLong {}

impl fmt::Display for MessageTooLong {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "")
    }
}

#[derive(Debug)]
pub struct QueueNotFound();

impl Error for QueueNotFound {}

impl fmt::Display for QueueNotFound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "")
    }
}

#[derive(Debug)]
pub struct QueueExists();

impl Error for QueueExists {}

impl fmt::Display for QueueExists {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "")
    }
}

#[derive(Debug)]
pub enum RsmqError {
    NoAttributeSupplied(NoAttributeSupplied),
    MissingParameter(MissingParameter),
    InvalidFormat(InvalidFormat),
    InvalidValue(InvalidValue),
    MessageNotString(MessageNotString),
    MessageTooLong(MessageTooLong),
    QueueNotFound(QueueNotFound),
    QueueExists(QueueExists),
    RedisError(RedisError),
}

impl fmt::Display for RsmqError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use RsmqError::*;
        match *self {
            NoAttributeSupplied(ref no_attribute_supplied) => no_attribute_supplied.fmt(f),
            MissingParameter(ref missing_parameter) => missing_parameter.fmt(f),
            InvalidFormat(ref invalid_format) => invalid_format.fmt(f),
            InvalidValue(ref invalid_value) => invalid_value.fmt(f),
            MessageNotString(ref message_not_string) => message_not_string.fmt(f),
            MessageTooLong(ref message_too_long) => message_too_long.fmt(f),
            QueueNotFound(ref queue_not_found) => queue_not_found.fmt(f),
            QueueExists(ref queue_exists) => queue_exists.fmt(f),
            RedisError(ref redis_error) => redis_error.fmt(f),
        }
    }
}

impl Error for RsmqError {}

impl From<RedisError> for RsmqError {
    fn from(error: RedisError) -> Self {
        RsmqError::RedisError(error)
    }
}

impl From<NoAttributeSupplied> for RsmqError {
    fn from(error: NoAttributeSupplied) -> Self {
        RsmqError::NoAttributeSupplied(error)
    }
}

impl From<MissingParameter> for RsmqError {
    fn from(error: MissingParameter) -> Self {
        RsmqError::MissingParameter(error)
    }
}

impl From<InvalidFormat> for RsmqError {
    fn from(error: InvalidFormat) -> Self {
        RsmqError::InvalidFormat(error)
    }
}

impl From<InvalidValue> for RsmqError {
    fn from(error: InvalidValue) -> Self {
        RsmqError::InvalidValue(error)
    }
}

impl From<MessageNotString> for RsmqError {
    fn from(error: MessageNotString) -> Self {
        RsmqError::MessageNotString(error)
    }
}

impl From<MessageTooLong> for RsmqError {
    fn from(error: MessageTooLong) -> Self {
        RsmqError::MessageTooLong(error)
    }
}

impl From<QueueNotFound> for RsmqError {
    fn from(error: QueueNotFound) -> Self {
        RsmqError::QueueNotFound(error)
    }
}

impl From<QueueExists> for RsmqError {
    fn from(error: QueueExists) -> Self {
        RsmqError::QueueExists(error)
    }
}
