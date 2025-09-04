use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use k3::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;

// prost dummy client => prost client
#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;
    // modified
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();
    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        println!("Got response {:?}", data);
    }
    // hset
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        println!("Got response {:?}", data);
    }
    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        println!("Got response {:?}", data);
    }
    // hdel
    let cmd = CommandRequest::new_hdel("table1", "hello");
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        println!("Got response {:?}", data);
    }
    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        println!("Got response {:?}", data);
    }
    Ok(())
}
