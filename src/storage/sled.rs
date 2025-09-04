use crate::{KvError, Storage, Value};
use sled::Db;
use std::{convert::TryInto, path::Path, str};

pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }
    fn get_full_key(table_name: &str, key: &str) -> String {
        format!("{}:{}", table_name, key)
    }
}

impl Storage for SledDb {
    fn get(&self, table_name: &str, key: &str) -> anyhow::Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table_name, key);
        let res = self.0.get(name)?.map(|ivec| ivec.as_ref().try_into());
        let flipped = flip(res);
        flipped
    }
    fn set(
        &self,
        table_name: &str,
        key: String,
        value: Value,
    ) -> anyhow::Result<Option<Value>, KvError> {
        let full_key = SledDb::get_full_key(table_name, &key);
        let data: Vec<u8> = value.try_into()?;
        let res = self
            .0
            .insert(full_key, data)?
            .map(|ivec| ivec.as_ref().try_into());
        let flipped = flip(res);
        flipped
    }
    fn del(&self, table_name: &str, key: &str) -> anyhow::Result<Option<Value>, KvError> {
        let full_key = SledDb::get_full_key(table_name, &key);
        let res = self
            .0
            .remove(full_key)?
            .map(|ivec| ivec.as_ref().try_into());
        flip(res)
    }
}

fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}
