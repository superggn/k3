use crate::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    // #[error("Not found for table: {0}, key: {1}")]
    // NotFound(String, String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Command is invalid: `{0}`")]
    InvalidCommand(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Frame is larger than max size")]
    FrameError,
    #[error("Certificate parse error: error to load {0} {0}")]
    CertifcateParseError(&'static str, &'static str),
    #[error("Cannot convert value {0} to {1}")]
    ConvertError(String, &'static str),

    // auto impl error conversion
    #[error("Failed to encode protobuf message")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),
    #[error("Failed to access sled db")]
    SledError(#[from] sled::Error),
    #[error("futures I/O error")]
    IoError(#[from] futures::io::Error),
}
