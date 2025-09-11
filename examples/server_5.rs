use anyhow::Result;
use k3::{CommandRequest, CommandResponse, MemTable, ServerStream, service::ServiceInner};
use tokio::net::TcpListener;

// third party stream => customized stream
#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    let store = MemTable::new();
    let svc_builder = ServiceInner::new(store)
        .add_req_hook(|cmd_req: &CommandRequest| println!("hook 1 - request: {:?}", cmd_req))
        .add_resp_hook(|resp: &mut CommandResponse| println!("hook 2 - resp: {:?}", resp));
    let service = svc_builder.build();
    loop {
        let svc_cl = service.clone();
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let server_stream = ServerStream::new(stream, svc_cl);
            server_stream.process().await.unwrap();
        });
    }
}
