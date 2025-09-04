use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use k3::{CommandRequest, CommandResponse, MemTable, service::ServiceInner};
use tokio::net::TcpListener;

// service => chain operation + hooks
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
        // todo add service_builder
        let svc_cl = service.clone();
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                println!("cmd: {:?}", cmd);
                // impl service
                let resp = svc_cl.process_request(cmd);
                println!("resp: {:?}", resp);
                stream.send(resp).await.unwrap();
            }
        });
    }
}
