use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;
    let framed = Framed::new(stream, LinesCodec::new());
    let (mut tx, mut rx) = framed.split::<String>();
    let _ = tx.send("hello".into()).await?;
    let resp = rx.next().await.unwrap();
    println!("resp: {:?}", resp);
    Ok(())
}
