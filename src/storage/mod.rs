pub mod memory;
pub mod sled;

use crate::{KvError, Value};
use anyhow::Result;

pub trait Storage {
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Value>, KvError>;
    fn set(&self, table_name: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;
    fn del(&self, table_name: &str, key: &str) -> Result<Option<Value>, KvError>;
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{MemTable, SledDb};
    use tempfile::tempdir;

    #[test]
    fn memtable_should_work() {
        let store = MemTable::new();
        test_simple(store);
    }

    #[test]
    fn sleddb_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_simple(store);
    }

    fn test_simple(store: impl Storage) {
        // set
        let v = store.set("t1", "hello".to_string(), "world".into());
        assert!(v.unwrap().is_none());
        let v = store
            .set("t1", "hello".to_string(), "world1".into())
            .unwrap();
        assert_eq!(v, Some("world".into()));

        // get
        let v = store.get("t1", "hello").unwrap();
        assert_eq!(v, Some("world1".into()));

        assert_eq!(None, store.get("t1", "hello1").unwrap());
        // assert_eq!(Ok(None), store.get("t2", "hello1"));
        assert!(store.get("t2", "hello1").unwrap().is_none());

        // del
        let v = store.del("t1", "hello").unwrap();
        assert_eq!(v, Some("world1".into()));

        assert_eq!(None, store.del("t1", "hello1").unwrap());
        assert_eq!(None, store.del("t2", "hello").unwrap());
    }
}
