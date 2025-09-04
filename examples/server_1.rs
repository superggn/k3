use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let framed = Framed::new(stream, LinesCodec::new());
            let (mut tx, mut rx) = framed.split::<String>();
            while let Some(Ok(msg)) = rx.next().await {
                println!("msg: {}", msg);
                let resp = format!("resp to: {}", msg);
                tx.send(resp).await.unwrap();
            }
        });
    }
}
