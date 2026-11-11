//! Passive ICE-TCP with RFC 4571 framing (2-byte big-endian length prefix).

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::{NetProto, NetRouter, TcpCompletion, local_ufrag};
use tokio_util::sync::CancellationToken;

/// Frames buffered towards one TCP peer before dropping.
const TCP_SEND_QUEUE: usize = 512;
/// A connection must present a routable STUN request quickly.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn accept_loop(
    router: NetRouter,
    listener: TcpListener,
    stop: CancellationToken,
    completion: TcpCompletion,
) {
    loop {
        let res = tokio::select! {
            _ = stop.cancelled() => break,
            r = listener.accept() => r,
        };
        match res {
            Ok((stream, remote)) => {
                let _ = stream.set_nodelay(true);
                let Some((id, stop, completion)) = router.begin_tcp() else {
                    break;
                };
                tokio::spawn(connection(
                    router.clone(),
                    stream,
                    remote,
                    id,
                    stop,
                    completion,
                ));
            }
            Err(e) => {
                tracing::debug!(error = %e, "ice-tcp accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    // Drop the listening descriptor before signalling collection to waiters.
    drop(listener);
    drop(completion);
}

async fn read_frame(
    stream: &mut tokio::net::tcp::OwnedReadHalf,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut len = [0u8; 2];
    stream.read_exact(&mut len).await?;
    let len = u16::from_be_bytes(len) as usize;
    buf.resize(len, 0);
    stream.read_exact(buf).await?;
    Ok(())
}

async fn connection(
    router: NetRouter,
    stream: TcpStream,
    remote: SocketAddr,
    id: u64,
    stop: CancellationToken,
    _completion: TcpCompletion,
) {
    let Ok(bound) = stream.local_addr() else {
        return;
    };
    // The wildcard listener is dual-stack: IPv4 peers appear IPv4-mapped.
    let canonical = |a: SocketAddr| SocketAddr::new(a.ip().to_canonical(), a.port());
    let (bound, remote) = (canonical(bound), canonical(remote));
    let Some(local) = router.tcp_advertised(bound) else {
        return;
    };
    let (mut rd, mut wr) = stream.into_split();
    let mut buf = Vec::with_capacity(1500);

    // The first frame must be a STUN binding request naming one of our transports.
    let first = tokio::select! {
        _ = stop.cancelled() => return,
        first = tokio::time::timeout(FIRST_FRAME_TIMEOUT, read_frame(&mut rd, &mut buf)) => first,
    };
    if !matches!(first, Ok(Ok(()))) || local_ufrag(&buf).is_none() {
        return;
    }
    let Some(route) = router.route(NetProto::Tcp, bound, remote, &buf) else {
        return;
    };

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(TCP_SEND_QUEUE);
    if !router.register_tcp(id, &route, bound, remote, tx) {
        return;
    }
    router.deliver(
        &route,
        NetProto::Tcp,
        local,
        bound,
        remote,
        std::mem::take(&mut buf),
    );

    let writer_stop = stop.clone();
    let writer = tokio::spawn(async move {
        loop {
            let frame = tokio::select! {
                _=writer_stop.cancelled()=>break,
                frame=rx.recv()=>match frame {Some(frame)=>frame,None=>break},
            };
            if frame.len() > u16::MAX as usize {
                continue;
            }
            let len = (frame.len() as u16).to_be_bytes();
            let written = tokio::select! {
                _=writer_stop.cancelled()=>break,
                result=async {wr.write_all(&len).await?;wr.write_all(&frame).await}=>result,
            };
            if written.is_err() {
                break;
            }
        }
    });

    loop {
        let res = tokio::select! {
            _ = stop.cancelled() => break,
            r = read_frame(&mut rd, &mut buf) => r,
        };
        if res.is_err() || route.sink.is_closed() {
            break;
        }
        router.deliver(
            &route,
            NetProto::Tcp,
            local,
            bound,
            remote,
            std::mem::take(&mut buf),
        );
    }
    stop.cancel();
    // Completion is published only after the accepted writer task is collected.
    writer.abort();
    let _ = writer.await;
}
