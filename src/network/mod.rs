pub mod frame;
pub mod stream;

pub use frame::{FrameCodec, read_frame};

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use tracing::info;

use crate::{CommandRequest, CommandResponse, KvError, ProstStream, Service};

pub struct ServerStream<S> {
    inner: ProstStream<S, CommandRequest, CommandResponse>,
    service: Service,
}

pub struct ClientStream<S> {
    inner: ProstStream<S, CommandResponse, CommandRequest>,
}

impl<S> ServerStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service) -> Self {
        Self {
            inner: ProstStream::new(stream),
            service: service,
        }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Some(Ok(cmd)) = self.inner.next().await {
            println!("got a new cmd: {:?}", cmd);
            let resp = self.service.process_request(cmd);
            self.inner.send(resp).await?;
        }
        Ok(())
    }
}

impl<S> ClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self {
            inner: ProstStream::new(stream),
        }
    }

    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        self.inner.send(cmd).await?;
        let raw_resp = self.inner.next().await;
        match raw_resp {
            Some(v) => v,
            None => Err(KvError::Internal("no response".to_string())),
        }
    }
}

pub mod utils {
    use bytes::{BufMut, BytesMut};
    use std::task::Poll;
    use tokio::io::{AsyncRead, AsyncWrite};

    pub struct DummyStream {
        pub buf: BytesMut,
    }

    // 要如何操作这个 dummy stream?
    // input data in stream buf
    // read data base on destination buf size
    // 暴露哪些 api?
    // dummy_stream

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data); // struct data => buf_in
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.get_mut().buf.put_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};

    use crate::{MemTable, ServiceInner, Value, assert_res_ok};

    use super::*;

    #[tokio::test]
    async fn client_server_basic_com_should_work() -> Result<()> {
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ClientStream::new(stream);
        // hset
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let resp = client.execute(cmd).await?;
        assert_res_ok(resp, &[Value::default()], &[]);
        // hset
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let resp2 = client.execute(cmd).await?;
        assert_res_ok(resp2, &[Value::from("v1".to_string())], &[]);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> Result<()> {
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ClientStream::new(stream);
        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t2", "k2", v.clone());
        let resp = client.execute(cmd).await?;
        assert_res_ok(resp, &[Value::default()], &[]);
        let cmd = CommandRequest::new_hget("t2", "k2");
        let resp = client.execute(cmd).await?;
        assert_res_ok(resp, &[v], &[]);
        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service = ServiceInner::new(MemTable::new()).into();
                let server = ServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });
        Ok(addr)
    }
}
