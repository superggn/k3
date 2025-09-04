use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use k3::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;

/// basic client => prost dummy client
#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;
    // modified
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();
    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());
    // 发送 HSET 命令
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        println!("Got response {:?}", data);
    }
    Ok(())
}
