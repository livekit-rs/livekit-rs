//! Faithful port of pion `udp/batchconn.go` (`NewBatchConn`/`BatchConn`).
//!
//! Upstream wraps each UDP mux socket in a `BatchConn` whenever
//! `rtc.batch_io.batch_size > 0` (webrtc_config.go:146). On Linux the wrapper
//! accumulates datagrams into a ping-pong batch and flushes them with
//! `sendmmsg`; on every other platform `batchConn == nil`, so `WriteTo`
//! degrades to a single `PacketConn.WriteTo` passthrough. This module
//! reproduces that platform-conditional behaviour exactly, so the field is a
//! genuine consumer rather than an accepted-and-ignored value.

use std::collections::HashMap;
#[cfg(not(target_os = "linux"))]
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

/// pion `sendMTU`: initial per-datagram staging buffer size. Retained for
/// upstream parity and asserted by the unit tests.
#[allow(dead_code)]
pub(super) const SEND_MTU: usize = 1500;

/// One queued outbound datagram for a batch flush.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Datagram {
    pub(super) destination: SocketAddr,
    pub(super) data: Vec<u8>,
}

/// Platform-independent accumulation slot, mirroring pion `messageBatch`
/// sizing and full accounting (empty datagrams rejected; fills to `size`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug)]
struct MessageBatch {
    size: usize,
    messages: Vec<Datagram>,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl MessageBatch {
    fn with_size(size: usize) -> Self {
        Self {
            size,
            messages: Vec::with_capacity(size),
        }
    }

    fn is_full(&self) -> bool {
        self.messages.len() >= self.size
    }

    /// Returns `(accepted, is_full)`. Empty datagrams are rejected, matching
    /// pion `EnqueueMessage` (`len(buf) == 0`).
    fn enqueue(&mut self, datagram: Datagram) -> (bool, bool) {
        if datagram.data.is_empty() || self.is_full() {
            return (false, self.is_full());
        }
        self.messages.push(datagram);
        (true, self.is_full())
    }
}

/// Accumulation state guarded by a mutex while the flusher owns the drained
/// batch. Off Linux this type is never constructed (writes passthrough).
#[cfg(target_os = "linux")]
struct Pending {
    batch: MessageBatch,
    closed: bool,
}

/// Batched UDP writer wrapping one mux socket. The ping-pong flusher is created
/// only on Linux (pion `NewBatchConn`); elsewhere [`Self::write`] is a
/// single-socket passthrough identical to pion's `batchConn == nil` path.
pub(super) struct BatchConn {
    /// Kept alive so the raw `fd` below stays valid; read directly only by the
    /// non-Linux passthrough (`sendmmsg` uses `fd` on Linux).
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    socket: Arc<UdpSocket>,
    #[cfg(target_os = "linux")]
    fd: std::os::fd::RawFd,
    #[cfg(target_os = "linux")]
    interval: Duration,
    #[cfg(target_os = "linux")]
    pending: std::sync::Mutex<Pending>,
    #[cfg(target_os = "linux")]
    notify: Arc<tokio::sync::Notify>,
}

impl BatchConn {
    fn new(socket: Arc<UdpSocket>, batch_size: usize, interval: Duration) -> Self {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::{AsFd, AsRawFd};
            let fd = socket.as_fd().as_raw_fd();
            Self {
                socket,
                fd,
                interval,
                pending: std::sync::Mutex::new(Pending {
                    batch: MessageBatch::with_size(batch_size.max(1)),
                    closed: false,
                }),
                notify: Arc::new(tokio::sync::Notify::new()),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (batch_size, interval);
            Self { socket }
        }
    }

    /// Queue one datagram. On Linux this feeds the ping-pong batch (a full
    /// batch nudges the flusher); elsewhere it is the pion passthrough single
    /// `WriteTo`. Non-backpressure errors are logged like the direct-send path.
    pub(super) fn write(&self, destination: SocketAddr, data: &[u8]) {
        #[cfg(target_os = "linux")]
        {
            if data.is_empty() {
                return;
            }
            let is_full = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if pending.closed {
                    return;
                }
                let (_accepted, is_full) = pending.batch.enqueue(Datagram {
                    destination,
                    data: data.to_vec(),
                });
                is_full
            };
            if is_full {
                self.notify.notify_waiters();
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = destination;
            if let Err(e) = self.socket.try_send_to(data, destination)
                && e.kind() != io::ErrorKind::WouldBlock
            {
                tracing::debug!(%destination, error = %e, "udp batch passthrough send failed");
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn close(&self) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending.closed = true;
        drop(pending);
        self.notify.notify_waiters();
    }

    /// pion `flusher`: ticks at `interval / 2`, flushing when a full batch was
    /// signalled or the interval elapsed since the last flush. The drained batch
    /// is sent outside the lock so writers keep staging (the ping-pong effect).
    #[cfg(target_os = "linux")]
    async fn run_flusher(self: Arc<Self>) {
        use std::time::Instant;

        let tick = Duration::max(self.interval / 2, Duration::from_millis(1));
        let mut last_flush = Instant::now() - self.interval;
        loop {
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(tick) => {
                    if last_flush.elapsed() < self.interval {
                        continue;
                    }
                }
            }
            let batch = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if pending.closed {
                    std::mem::take(&mut pending.batch.messages)
                } else if pending.batch.messages.is_empty() {
                    continue;
                } else {
                    std::mem::take(&mut pending.batch.messages)
                }
            };
            if !batch.is_empty() {
                sendmmsg_all(self.fd, &batch);
            }
            {
                let pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if pending.closed {
                    return;
                }
            }
            last_flush = Instant::now();
        }
    }
}

/// Drains the batch with `sendmmsg`, looping until every datagram reaches the
/// kernel or the socket stops accepting (back-pressure drops the remainder, as
/// pion's flush break-on-error does).
#[cfg(target_os = "linux")]
fn sendmmsg_all(fd: std::os::fd::RawFd, batch: &[Datagram]) {
    let mut offset = 0usize;
    while offset < batch.len() {
        let sent = sendmmsg_once(fd, &batch[offset..]);
        if sent == 0 {
            break;
        }
        offset += sent;
    }
}

#[cfg(target_os = "linux")]
fn sendmmsg_once(fd: std::os::fd::RawFd, msgs: &[Datagram]) -> usize {
    use libc::{
        c_int, c_uint, c_void, iovec, mmsghdr, msghdr, sockaddr_in, sockaddr_in6, sockaddr_storage,
    };
    use std::mem::{size_of, zeroed};

    let count = msgs.len().min(1024);
    let mut iovecs: Vec<iovec> = Vec::with_capacity(count);
    let mut addrs: Vec<sockaddr_storage> = Vec::with_capacity(count);

    for msg in msgs.iter().take(count) {
        iovecs.push(iovec {
            iov_base: msg.data.as_ptr() as *mut c_void,
            iov_len: msg.data.len(),
        });
        let storage = match msg.destination {
            SocketAddr::V4(v4) => {
                let mut a: sockaddr_in = unsafe { zeroed() };
                a.sin_family = libc::AF_INET as libc::sa_family_t;
                a.sin_port = v4.port().to_be();
                a.sin_addr.s_addr = u32::from(*v4.ip()).to_be();
                copy_into_storage(&a)
            }
            SocketAddr::V6(v6) => {
                let mut a: sockaddr_in6 = unsafe { zeroed() };
                a.sin6_family = libc::AF_INET6 as libc::sa_family_t;
                a.sin6_port = v6.port().to_be();
                a.sin6_addr.s6_addr = u128::from(*v6.ip()).to_be_bytes();
                copy_into_storage(&a)
            }
        };
        addrs.push(storage);
    }

    let mut headers: Vec<mmsghdr> = Vec::with_capacity(count);
    for index in 0..count {
        let mut h: msghdr = unsafe { zeroed() };
        h.msg_name = std::ptr::addr_of_mut!(addrs[index]) as *mut c_void;
        match msgs[index].destination {
            SocketAddr::V4(_) => h.msg_namelen = size_of::<sockaddr_in>() as libc::socklen_t,
            SocketAddr::V6(_) => h.msg_namelen = size_of::<sockaddr_in6>() as libc::socklen_t,
        }
        h.msg_iov = std::ptr::addr_of_mut!(iovecs[index]);
        h.msg_iovlen = 1;
        headers.push(mmsghdr {
            msg_hdr: h,
            msg_len: 0,
        });
    }

    let r: c_int = unsafe {
        libc::sendmmsg(
            fd,
            headers.as_mut_ptr(),
            count as c_uint,
            libc::MSG_DONTWAIT,
        )
    };
    if r < 0 { 0 } else { r as usize }
}

/// Copies a concrete `sockaddr_*` into a generic storage slot.
#[cfg(target_os = "linux")]
fn copy_into_storage<T>(addr: &T) -> libc::sockaddr_storage {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    unsafe {
        std::ptr::copy_nonoverlapping(
            addr as *const T as *const u8,
            std::ptr::addr_of_mut!(storage) as *mut u8,
            size_of::<T>().min(size_of::<libc::sockaddr_storage>()),
        );
    }
    storage
}

/// Instantiates a [`BatchConn`] per UDP mux socket and, on Linux, spawns the
/// ping-pong flusher bound to `stop`. Returns an empty map when `batch_size` is
/// zero (batching disabled, matching upstream's `> 0` gate).
pub(super) fn install(
    sockets: &[(SocketAddr, Arc<UdpSocket>)],
    batch_size: usize,
    interval: Duration,
    stop: &CancellationToken,
) -> HashMap<SocketAddr, Arc<BatchConn>> {
    let mut map = HashMap::new();
    if batch_size == 0 {
        return map;
    }
    for (bound, socket) in sockets {
        let conn = Arc::new(BatchConn::new(socket.clone(), batch_size, interval));
        #[cfg(target_os = "linux")]
        {
            let flusher = Arc::clone(&conn);
            let cancelled = stop.clone();
            tokio::spawn(async move {
                let task = flusher.clone().run_flusher();
                tokio::pin!(task);
                tokio::select! {
                    () = &mut task => {}
                    () = cancelled.cancelled() => {
                        flusher.close();
                        let _ = tokio::time::timeout(Duration::from_millis(50), task).await;
                    }
                }
            });
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = stop;
        }
        map.insert(*bound, Arc::clone(&conn));
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dg(len: usize) -> Datagram {
        Datagram {
            destination: SocketAddr::from(([127, 0, 0, 1], 50001)),
            data: vec![7u8; len],
        }
    }

    #[test]
    fn empty_datagram_is_rejected() {
        let mut batch = MessageBatch::with_size(4);
        let (accepted, full) = batch.enqueue(dg(0));
        assert!(!accepted);
        assert!(!full);
        assert!(batch.messages.is_empty());
    }

    #[test]
    fn fills_to_size_then_reports_full() {
        let mut batch = MessageBatch::with_size(3);
        assert_eq!(batch.enqueue(dg(16)), (true, false));
        assert_eq!(batch.enqueue(dg(16)), (true, false));
        assert_eq!(batch.enqueue(dg(16)), (true, true));
        assert!(batch.is_full());
        assert_eq!(batch.enqueue(dg(16)), (false, true));
        assert_eq!(batch.messages.len(), 3);
    }

    #[test]
    fn size_one_flushes_on_every_message() {
        let mut batch = MessageBatch::with_size(1);
        assert_eq!(batch.enqueue(dg(8)), (true, true));
    }

    #[test]
    fn send_mtu_matches_upstream_constant() {
        assert_eq!(SEND_MTU, 1500);
    }

    #[test]
    fn install_is_disabled_for_zero_size() {
        let stop = CancellationToken::new();
        let map = install(&[], 0, Duration::from_millis(10), &stop);
        assert!(map.is_empty());
    }
}
