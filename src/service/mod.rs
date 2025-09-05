mod cmd_impl;

use crate::{CommandRequest, CommandResponse, KvError, MemTable, RequestData, Storage};
use std::sync::Arc;

pub trait CmdService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub struct Service<Store = MemTable>
where
    Store: Storage,
{
    inner: Arc<ServiceInner<Store>>,
}

impl<Store> Clone for Service<Store>
where
    Store: Storage,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// considered as service builder
pub struct ServiceInner<Store>
where
    Store: Storage,
{
    store: Store,
    req_hooks: Vec<fn(&CommandRequest)>,
    resp_hooks: Vec<fn(&mut CommandResponse)>,
}

impl<Store> From<ServiceInner<Store>> for Service<Store>
where
    Store: Storage,
{
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<Store> ServiceInner<Store>
where
    Store: Storage,
{
    pub fn new(store: Store) -> Self {
        Self {
            store: store,
            req_hooks: vec![],
            resp_hooks: vec![],
        }
    }
    pub fn add_req_hook(mut self, f: fn(&CommandRequest)) -> Self {
        self.req_hooks.push(f);
        self
    }

    pub fn add_resp_hook(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.resp_hooks.push(f);
        self
    }
    pub fn build(self) -> Service<Store> {
        self.into()
    }
}

pub trait Hook<Arg> {
    fn exec_all(&self, arg: &Arg);
}
impl<Arg> Hook<Arg> for Vec<fn(&Arg)> {
    fn exec_all(&self, arg: &Arg) {
        for f in self {
            f(arg)
        }
    }
}

pub trait HookMut<Arg> {
    fn exec_all(&self, arg: &mut Arg);
}

impl<Arg> HookMut<Arg> for Vec<fn(&mut Arg)> {
    fn exec_all(&self, arg: &mut Arg) {
        for f in self {
            f(arg)
        }
    }
}

impl<Store> Service<Store>
where
    Store: Storage,
{
    // deprecated => replaced by builder flow
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner::new(store)),
        }
    }
    pub fn process_request(&self, cmd_req: CommandRequest) -> CommandResponse {
        self.inner.req_hooks.exec_all(&cmd_req);
        let mut resp = exec_cmd(cmd_req, &self.inner.store);
        self.inner.resp_hooks.exec_all(&mut resp);
        resp
    }
}

// operate on DB & gen response
pub fn exec_cmd(cmd_req: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd_req.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
    }
}

// 测试成功返回的结果
#[cfg(test)]
use crate::{Kvpair, Value};

#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
