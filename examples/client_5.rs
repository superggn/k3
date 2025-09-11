use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use k3::{ClientStream, CommandRequest, CommandResponse};
use tokio::net::TcpStream;

// client stream => customized stream exec
#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;
    // pseudo code
    // client = ClientStream::new(stream);
    // cmd = ...;
    // resp = client.exec(cmd).await?;
    // modified
    let mut client = ClientStream::new(stream);
    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    // client.execute(cmd).await?;
    if let Ok(data) = client.execute(cmd).await {
        println!("Got response {:?}", data);
    }
    // hset
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());
    if let Ok(data) = client.execute(cmd).await {
        println!("Got response {:?}", data);
    }
    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    if let Ok(data) = client.execute(cmd).await {
        println!("Got response {:?}", data);
    }
    // hdel
    let cmd = CommandRequest::new_hdel("table1", "hello");
    if let Ok(data) = client.execute(cmd).await {
        println!("Got response {:?}", data);
    }
    // hget
    let cmd = CommandRequest::new_hget("table1", "hello");
    if let Ok(data) = client.execute(cmd).await {
        println!("Got response {:?}", data);
    }
    Ok(())
}
