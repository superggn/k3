use crate::KvError;

use abi::command_request::RequestData;
use abi::*;
use bytes::Bytes;
use http::StatusCode;
use prost::Message;
use std::convert::TryFrom;

pub mod abi {
    include!(concat!(env!("OUT_DIR"), "/abi.rs"));
}

impl CommandRequest {
    pub fn new_hget(table_name: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table_name.into(),
                key: key.into(),
            })),
        }
    }
    pub fn new_hset(table_name: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table_name.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }

    pub fn new_hdel(table_name: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hdel(Hdel {
                table: table_name.into(),
                key: key.into(),
            })),
        }
    }
}

impl Kvpair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self {
            value: Some(value::Value::String(value)),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self {
            value: Some(value::Value::String(value.to_string())),
        }
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self {
            value: Some(value::Value::Integer(value.into())),
        }
    }
}
impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self {
            value: Some(value::Value::Integer(value.into())),
        }
    }
}

impl From<Bytes> for Value {
    fn from(buf: Bytes) -> Self {
        Self {
            value: Some(value::Value::Binary(buf.to_vec())),
        }
    }
}

// impl From<Bytes> for Value {
//     fn from(buf: Bytes) -> Self {
//         Self {
//             value: Some(value::Value::Binary(buf)),
//         }
//     }
// }

impl TryFrom<&[u8]> for Value {
    type Error = KvError;
    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        let msg = Value::decode(data)?;
        Ok(msg)
    }
}

impl From<Value> for CommandResponse {
    fn from(value: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![value],
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],
        };
        match e {
            KvError::NotFound(_, _) => result.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as _,
            _ => {}
        }
        result
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = KvError;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let mut buf = Vec::with_capacity(v.encoded_len());
        v.encode(&mut buf)?;
        Ok(buf)
    }
}
