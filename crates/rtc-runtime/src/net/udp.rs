use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

use super::{MAX_DATAGRAM, NetProto, NetRouter, Route, udp_listen_error};

/// Socket buffers sized for media bursts.
const SOCKET_BUFFER: usize = 4 * 1024 * 1024;

pub(super) fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    // Best effort: the OS may clamp these.
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER);
    let _ = socket.set_send_buffer_size(SOCKET_BUFFER);
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    UdpSocket::from_std(socket.into())
}

/// Bind a socket on `ip` with a port from `range` (inclusive); `(0, 0)` = ephemeral.
pub(super) fn bind_in_range(ip: IpAddr, range: (u16, u16)) -> io::Result<(UdpSocket, u16)> {
    let (start, end) = range;
    if start == 0 && end == 0 {
        let s = bind(SocketAddr::new(ip, 0))?;
        let port = s.local_addr()?.port();
        return Ok((s, port));
    }
    if start == 0 || end < start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid UDP port range",
        ));
    }
    let span = u32::from(end - start) + 1;
    // Like Pion, start randomly and visit each port exactly once.
    let first = (rtc_core::util::random_u64() % u64::from(span)) as u32;
    let mut last: Option<(u16, io::Error)> = None;
    for step in 0..span {
        let offset = ((first + step) % span) as u16;
        let port = start + offset;
        match bind(SocketAddr::new(ip, port)) {
            Ok(s) => return Ok((s, port)),
            Err(e) if e.kind() == io::ErrorKind::AddrNotAvailable => {
                return Err(udp_listen_error(SocketAddr::new(ip, port), &e));
            }
            Err(e) => last = Some((port, e)),
        }
    }
    // Preserve the concrete bind diagnostic when the range is exhausted.
    match last {
        Some((port, e)) => Err(udp_listen_error(SocketAddr::new(ip, port), &e)),
        None => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "UDP port range exhausted",
        )),
    }
}

/// Reader for a shared (muxed) socket.
pub(super) async fn mux_reader(
    router: NetRouter,
    socket: Arc<UdpSocket>,
    advertised: SocketAddr,
    bound: SocketAddr,
) {
    let stop = router.stop_token();
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let res = tokio::select! {
            _ = stop.cancelled() => return,
            r = socket.recv_from(&mut buf) => r,
        };
        match res {
            Ok((n, remote)) => {
                let data = &buf[..n];
                if let Some(route) = router.route(NetProto::Udp, bound, remote, data) {
                    router.deliver(
                        &route,
                        NetProto::Udp,
                        advertised,
                        bound,
                        remote,
                        data.to_vec(),
                    );
                }
            }
            Err(e) => {
                // ICMP-induced errors (e.g. connection reset on Windows/Linux) are not fatal.
                tracing::trace!(error = %e, %advertised, "udp recv error");
            }
        }
    }
}

/// Reader for a socket dedicated to a single transport.
pub(super) async fn dedicated_reader(
    router: NetRouter,
    socket: Arc<UdpSocket>,
    advertised: SocketAddr,
    bound: SocketAddr,
    route: Route,
    stop: CancellationToken,
) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let res = tokio::select! {
            _ = stop.cancelled() => return,
            r = socket.recv_from(&mut buf) => r,
        };
        match res {
            Ok((n, remote)) => {
                if route.sink.is_closed() {
                    return;
                }
                router.deliver(
                    &route,
                    NetProto::Udp,
                    advertised,
                    bound,
                    remote,
                    buf[..n].to_vec(),
                );
            }
            Err(e) => tracing::trace!(error = %e, %advertised, "udp recv error"),
        }
    }
}
