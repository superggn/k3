use crate::{CmdService, CommandResponse, KvError, MemTable, SledDb, Storage};

use crate::cmd::abi::*;

impl CmdService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(format!("table {} key {}", self.table, self.key)).into(),
            Err(e) => e.into(),
        }
    }
}

impl CmdService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => KvError::InvalidCommand(format!("{:?}", self)).into(),
        }
    }
}

impl CmdService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => Value::default().into(),
            Err(e) => e.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::service::{assert_res_error, assert_res_ok, exec_cmd};

    use super::*;

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        exec_cmd(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = exec_cmd(cmd, &store);
        assert_res_ok(&res, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = exec_cmd(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("t1", "hello");
        let res = exec_cmd(cmd, &store);
        assert_res_error(res, 404, "Not found");
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = exec_cmd(cmd.clone(), &store);
        assert_res_ok(&res, &[Value::default()], &[]);
        let res = exec_cmd(cmd, &store);
        assert_res_ok(&res, &["world".into()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "v1")], &store);
        let cmd = CommandRequest::new_hdel("t1", "u2");
        let res = exec_cmd(cmd, &store);
        assert_res_ok(&res, &[Value::default()], &[]);
        let cmd = CommandRequest::new_hdel("t1", "u1");
        let res = exec_cmd(cmd, &store);
        assert_res_ok(&res, &["v1".into()], &[]);
    }

    fn set_key_pairs<T: Into<Value>>(table: &str, pairs: Vec<(&str, T)>, store: &impl Storage) {
        pairs
            .into_iter()
            .map(|(k, v)| CommandRequest::new_hset(table, k, v.into()))
            .for_each(|cmd| {
                exec_cmd(cmd, store);
            });
    }
}
