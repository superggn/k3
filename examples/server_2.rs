use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use k3::{CommandRequest, CommandResponse};
use tokio::net::TcpListener;

/// basic server => prost dummy server
#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            // modified
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                println!("msg: {:?}", cmd);
                let resp = CommandResponse {
                    status: 404,
                    message: "Not found".to_string(),
                    ..Default::default()
                };
                stream.send(resp).await.unwrap();
            }
        });
    }
}
