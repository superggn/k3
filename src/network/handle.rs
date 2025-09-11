use futures::future;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::TokioAsyncReadCompatExt;
use yamux::{Connection, ConnectionError, Mode};

pub struct YamuxHandle {
    incoming_rx: mpsc::UnboundedReceiver<Result<yamux::Stream, ConnectionError>>,
    cmd_tx: mpsc::UnboundedSender<DriverCmd>,
}

enum DriverCmd {
    OpenOutbound {
        resp: oneshot::Sender<Result<yamux::Stream, ConnectionError>>,
    },
    Close,
}

impl YamuxHandle {
    pub async fn next_incoming(&mut self) -> Option<yamux::Stream> {
        match self.incoming_rx.recv().await {
            Some(Ok(s)) => Some(s),
            Some(Err(e)) => {
                println!("error: {:?}", e);
                None
            }
            None => None,
        }
    }
    pub async fn open_outbound(&self) -> Result<yamux::Stream, ConnectionError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DriverCmd::OpenOutbound { resp: tx })
            .map_err(|_| ConnectionError::Closed)?;
        rx.await.map_err(|_| ConnectionError::Closed)?
    }
    pub fn close(&self) {
        let _ = self.cmd_tx.send(DriverCmd::Close);
    }
}

// client & server 通用
pub fn spawn_yamux_driver<S>(io: S, mode: Mode, config: yamux::Config) -> YamuxHandle
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (incoming_tx, incoming_rx) =
        mpsc::unbounded_channel::<Result<yamux::Stream, ConnectionError>>();
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<DriverCmd>();

    tokio::spawn(async move {
        let mut conn = Connection::new(io.compat(), config, mode);
        let mut pending_outbounds: VecDeque<
            oneshot::Sender<Result<yamux::Stream, ConnectionError>>,
        > = VecDeque::new();
        loop {
            // poll_fn => Ready<Option<is_outbound_stream, Result<stream, ConnectionError>>>
            //         => Ready(Some(outbound_flag, Ok(stream)))
            //         => Ready(Some(outbound_flag, Err(ConnectionError)))
            //         => Ready(None)
            let net_fut = future::poll_fn(|cx| {
                // if someone's waiting => try outbound
                if !pending_outbounds.is_empty() {
                    if let std::task::Poll::Ready(out_stream) = conn.poll_new_outbound(cx) {
                        // Some((true, 出站结果))
                        return std::task::Poll::Ready(Some((true, out_stream)));
                    }
                }
                // try inbound
                match conn.poll_next_inbound(cx) {
                    std::task::Poll::Ready(Some(inb)) => {
                        // Some((false, 入站结果))
                        std::task::Poll::Ready(Some((false, inb)))
                    }
                    std::task::Poll::Ready(None) => {
                        // 连接已关闭
                        std::task::Poll::Ready(None)
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            });
            tokio::select! {
                // —— 唯一借用 conn 的分支 —— //
                event = net_fut => {
                    match event {
                        // is_out == true => outbound_stream
                        Some((true, out_res)) => {
                            if let Some(tx) = pending_outbounds.pop_front() {
                                let _ = tx.send(out_res);
                            }
                        }
                        // is_out == false inbound_stream
                        Some((false, in_res)) => {
                            match in_res {
                                Ok(s) => { let _ = incoming_tx.send(Ok(s)); }
                                Err(e) => {
                                    println!("error, {:?}", e);
                                    let _ = incoming_tx.send(Err(e));
                                    // e 通常不可 Clone
                                    while let Some(tx) = pending_outbounds.pop_front() {
                                        let _ = tx.send(Err(ConnectionError::Closed));
                                    }
                                    break;
                                }
                            }
                        }
                        // None => 对端关闭
                        None => {
                            while let Some(tx) = pending_outbounds.pop_front() {
                                let _ = tx.send(Err(ConnectionError::Closed));
                            }
                            break;
                        }
                    }
                }
                // —— 不借用 conn 的分支 —— //
                // push oneshot_outbound_sender in pending_outbounds
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(DriverCmd::OpenOutbound { resp }) => {
                            pending_outbounds.push_back(resp);
                        }
                        Some(DriverCmd::Close) | None => {
                            // 优雅关闭：把 poll_close 变成可 .await 的 Future
                            let _ = future::poll_fn(|cx| conn.poll_close(cx)).await;
                            break;
                        }
                    }
                }
            }
        }
    });

    YamuxHandle {
        incoming_rx,
        cmd_tx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::{Framed, LinesCodec};
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    use yamux::{Config, Mode};

    /// 启动一个 echo server：收到的行加 "resp_" 前缀返回
    async fn start_echo_server() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let mut handle = spawn_yamux_driver(sock, Mode::Server, Config::default());

                tokio::spawn(async move {
                    while let Some(stream) = handle.next_incoming().await {
                        tokio::spawn(async move {
                            let mut framed = Framed::new(stream.compat(), LinesCodec::new());
                            while let Some(Ok(line)) = framed.next().await {
                                let _ = framed.send(format!("resp_{line}")).await;
                            }
                        });
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn basic_roundtrip_should_work() {
        let addr = start_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let sock = TcpStream::connect(addr).await.unwrap();
        let client = spawn_yamux_driver(sock, Mode::Client, Config::default());

        // 开一个出站子流
        let stream = client.open_outbound().await.unwrap();
        let mut framed = Framed::new(stream.compat(), LinesCodec::new());

        framed.send("hello").await.unwrap();
        let resp = framed.next().await.unwrap().unwrap();
        assert_eq!(resp, "resp_hello");
    }

    #[tokio::test]
    async fn multiple_outbounds_concurrent() {
        let addr = start_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let sock = TcpStream::connect(addr).await.unwrap();
        let client = spawn_yamux_driver(sock, Mode::Client, Config::default());

        let mut tasks = vec![];
        for i in 0..5 {
            let c = YamuxHandle {
                incoming_rx: {
                    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    rx
                },
                cmd_tx: client.cmd_tx.clone(),
            };
            tasks.push(tokio::spawn(async move {
                let s = c.open_outbound().await.unwrap();
                let mut framed = Framed::new(s.compat(), LinesCodec::new());
                let msg = format!("m{i}");
                framed.send(msg.as_str()).await.unwrap();
                let resp = framed.next().await.unwrap().unwrap();
                assert_eq!(resp, format!("resp_m{i}"));
            }));
        }

        for t in tasks {
            t.await.unwrap();
        }
    }

    #[tokio::test]
    async fn close_should_reject_future_open() {
        let addr = start_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let sock = TcpStream::connect(addr).await.unwrap();
        let client = spawn_yamux_driver(sock, Mode::Client, Config::default());

        // 正常先开一次
        let _ = client.open_outbound().await.unwrap();

        // 主动关闭
        client.close();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 再次 open 应该报错
        let res = client.open_outbound().await;
        assert!(res.is_err());
    }
}
