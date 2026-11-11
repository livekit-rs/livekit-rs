//! Native HTTPS/WSS transport. Permits cover handshake and the complete HTTP or
//! upgraded WebSocket lifetime; pending handshakes are cancelled on shutdown.
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio_rustls::{TlsAcceptor, server::TlsStream};

pub(crate) struct HttpsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    slots: Arc<Semaphore>,
    timeout: Duration,
    pending: JoinSet<Option<(HttpsIo, SocketAddr)>>,
    force_stop: watch::Receiver<bool>,
}

impl HttpsListener {
    pub(crate) fn new(
        listener: TcpListener,
        acceptor: TlsAcceptor,
        slots: Arc<Semaphore>,
        timeout: Duration,
        force_stop: watch::Receiver<bool>,
    ) -> Self {
        Self {
            listener,
            acceptor,
            slots,
            timeout,
            pending: JoinSet::new(),
            force_stop,
        }
    }
}

pub(crate) struct HttpsIo {
    stream: TlsStream<TcpStream>,
    _permit: OwnedSemaphorePermit,
    force_stop: Pin<Box<dyn Future<Output = ()> + Send>>,
    forced: bool,
}

impl axum::serve::Listener for HttpsListener {
    type Io = HttpsIo;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            tokio::select! {
                Some(joined) = self.pending.join_next(), if !self.pending.is_empty() => {
                    if let Ok(Some(ready)) = joined { return ready; }
                }
                accepted = self.listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(pair) => pair,
                        Err(error) => {
                            tracing::warn!(%error, "HTTPS TCP accept failed");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    let Ok(permit) = self.slots.clone().try_acquire_owned() else {
                        tracing::debug!(%peer, "HTTPS connection limit reached");
                        continue;
                    };
                    let acceptor = self.acceptor.clone();
                    let timeout = self.timeout;
                    let stop = crate::metrics::shutdown::Shutdown(self.force_stop.clone());
                    self.pending.spawn(async move {
                        match tokio::time::timeout(timeout, acceptor.accept(stream)).await {
                            Ok(Ok(stream)) => Some((HttpsIo { stream, _permit: permit, force_stop: Box::pin(stop.wait()), forced: false }, peer)),
                            Ok(Err(error)) => { tracing::debug!(%peer, %error, "HTTPS handshake rejected"); None }
                            Err(_) => { tracing::debug!(%peer, "HTTPS handshake timed out"); None }
                        }
                    });
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl HttpsIo {
    fn poll_forced(&mut self, cx: &mut Context<'_>) -> bool {
        if !self.forced && self.force_stop.as_mut().poll(cx).is_ready() {
            self.forced = true;
        }
        self.forced
    }
}

impl AsyncRead for HttpsIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.poll_forced(cx) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for HttpsIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.poll_forced(cx) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.poll_forced(cx) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.poll_forced(cx) {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::{ConnectInfo, WebSocketUpgrade};
    use axum::routing::get;
    use axum::serve::ListenerExt;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};

    type ClientIo = tokio_rustls::client::TlsStream<TcpStream>;

    async fn fixture(
        max: usize,
        timeout: Duration,
    ) -> (
        SocketAddr,
        watch::Sender<bool>,
        watch::Sender<bool>,
        Arc<Semaphore>,
        tokio::task::JoinHandle<()>,
    ) {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let config = rtc_tls::server_config_from_pem(
            include_bytes!("../../rtc-tls/tests/fixtures/server.pem"),
            include_bytes!("../../rtc-tls/tests/fixtures/server-key.pem"),
        )
        .unwrap();
        let (stop, rx) = watch::channel(false);
        let (force, force_rx) = watch::channel(false);
        let listener = HttpsListener::new(
            socket,
            TlsAcceptor::from(Arc::new(config)),
            Arc::new(Semaphore::new(max)),
            timeout,
            force_rx,
        );
        let slots = listener.slots.clone();
        let app = Router::new()
            .route(
                "/",
                get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move {
                    format!("verified {}", peer.ip())
                }),
            )
            .route(
                "/rtc",
                get(|upgrade: WebSocketUpgrade| async {
                    upgrade.on_upgrade(async |mut socket| {
                        while let Some(Ok(message)) = socket.recv().await {
                            if socket.send(message).await.is_err() {
                                break;
                            }
                        }
                    })
                }),
            );
        let task = tokio::spawn(async move {
            axum::serve(
                listener.tap_io(|_| {}),
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(crate::metrics::shutdown::Shutdown(rx).wait())
            .await
            .unwrap();
        });
        (addr, stop, force, slots, task)
    }

    async fn tls(addr: SocketAddr) -> io::Result<ClientIo> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../rtc-tls/tests/fixtures/root.pem");
        let cfg = rtc_tls::client_config_with_ca_file(&root).unwrap();
        TlsConnector::from(Arc::new(cfg))
            .connect(
                ServerName::try_from("localhost").unwrap(),
                TcpStream::connect(addr).await?,
            )
            .await
    }

    #[tokio::test]
    async fn verified_https_and_wss_keep_peer_address_and_avoid_handshake_head_of_line_blocking() {
        let (addr, stop, force, _, task) = fixture(3, Duration::from_millis(200)).await;
        let stalled = TcpStream::connect(addr).await.unwrap();
        let mut client = tokio::time::timeout(Duration::from_secs(2), tls(addr))
            .await
            .unwrap()
            .unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        let response = String::from_utf8(bytes).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("verified 127.0.0.1"));
        let (mut socket, _) =
            tokio_tungstenite::client_async("wss://localhost/rtc", tls(addr).await.unwrap())
                .await
                .unwrap();
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                "official signaling bytes remain unchanged".into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap().into_text().unwrap(),
            "official signaling bytes remain unchanged"
        );
        socket.close(None).await.unwrap();
        drop(stalled);
        stop.send(true).unwrap();
        force.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stalled_handshake_times_out_and_releases_connection_slot() {
        let (addr, stop, force, slots, task) = fixture(1, Duration::from_millis(80)).await;
        let stalled = TcpStream::connect(addr).await.unwrap();
        for _ in 0..100 {
            if slots.available_permits() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(slots.available_permits(), 0);
        assert!(
            tls(addr).await.is_err(),
            "unbounded extra connection admitted"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        let client = tls(addr).await.unwrap();
        drop(client);
        drop(stalled);
        stop.send(true).unwrap();
        force.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn websocket_holds_slot_until_forced_shutdown_closes_upgraded_transport() {
        let (addr, stop, force, slots, task) = fixture(1, Duration::from_secs(1)).await;
        let (mut socket, _) =
            tokio_tungstenite::client_async("wss://localhost/rtc", tls(addr).await.unwrap())
                .await
                .unwrap();
        assert_eq!(slots.available_permits(), 0);
        assert!(tls(addr).await.is_err());
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        force.send(true).unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap();
        assert!(closed.is_none() || closed.unwrap().is_err());
        for _ in 0..100 {
            if slots.available_permits() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(slots.available_permits(), 1);
    }
}
