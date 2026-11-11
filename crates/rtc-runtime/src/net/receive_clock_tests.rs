//! Real network delivery producer timestamps, not post-decode event replacement.
use super::*;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
fn binding(user: &str) -> Vec<u8> {
    let padded = (user.len() + 3) & !3;
    let mut b = vec![0; 24 + padded];
    b[..2].copy_from_slice(&1u16.to_be_bytes());
    b[2..4].copy_from_slice(&((4 + padded) as u16).to_be_bytes());
    b[4..8].copy_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
    b[8..20].copy_from_slice(&[7; 12]);
    b[20..22].copy_from_slice(&6u16.to_be_bytes());
    b[22..24].copy_from_slice(&(user.len() as u16).to_be_bytes());
    b[24..24 + user.len()].copy_from_slice(user.as_bytes());
    b
}

// Tokio's paused current-thread clock may auto-advance to timeout(2s) before
// the OS readiness driver observes a real local socket. A running blocking
// task inhibits only automatic advancement (Tokio's documented mechanism).
// It is bounded independently: setup and actual delivery each get at most2s
// wall time, with delivery's original Tokio timeout still checked unchanged.
struct FrozenRealIo {
    begin: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), &'static str>>,
}
impl FrozenRealIo {
    async fn start() -> Self {
        let (begin, begin_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            begin_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(|_| "real socket setup did not finish within2s wall time")?;
            release_rx
                .recv_timeout(Duration::from_secs(2))
                .map_err(|_| "real packet delivery/assertions did not finish within2s wall time")?;
            Ok(())
        });
        ready.await.expect("real-I/O clock guard actually started");
        Self {
            begin,
            release,
            task,
        }
    }
    fn begin_delivery(&self) {
        self.begin
            .send(())
            .expect("real socket setup wall deadline");
    }
    async fn finish(self) {
        self.release
            .send(())
            .expect("real packet delivery wall deadline");
        self.task
            .await
            .expect("collect owned clock guard")
            .expect("real-I/O clock guard bounded completion");
    }
}

#[tokio::test(start_paused = true)]
async fn actual_udp_and_tcp_delivery_stamp_uses_the_runtime_monotonic_clock() {
    for tcp in [false, true] {
        let net = NetRouter::start(NetConfig {
            udp_enabled: !tcp,
            udp_port: Some(0),
            port_range: (0, 0),
            tcp_port: tcp.then_some(0),
            interfaces: vec![LocalInterface {
                bind: "127.0.0.1".parse().unwrap(),
                advertise: "127.0.0.1".parse().unwrap(),
            }],
        })
        .await
        .unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        let id = TransportId::next();
        let candidates = net.open_transport(id, "timestamp_owned", tx).unwrap();
        let target = candidates
            .iter()
            .find(|c| c.proto == if tcp { NetProto::Tcp } else { NetProto::Udp })
            .unwrap()
            .base;
        let raw = binding("timestamp_owned:peer");
        assert_eq!(
            local_ufrag(&raw),
            Some("timestamp_owned"),
            "actual STUN routing premise"
        );
        // Advance before TCP connects: an idle accepted TCP connection has a
        // real 10s first-frame timeout, independent of this producer-clock gate.
        // Keep the exact 120s timestamp premise without expiring a live socket.
        let before = tokio::time::Instant::now().into_std();
        tokio::time::advance(Duration::from_secs(120)).await;
        let io_clock = FrozenRealIo::start().await;
        let mut stream = if tcp {
            Some(tokio::net::TcpStream::connect(target).await.unwrap())
        } else {
            None
        };
        let udp = if tcp {
            None
        } else {
            Some(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap())
        };
        if let Some(s) = stream.as_mut() {
            s.write_all(&(raw.len() as u16).to_be_bytes())
                .await
                .unwrap();
            s.write_all(&raw).await.unwrap();
        }
        if let Some(s) = udp.as_ref() {
            s.send_to(&raw, target).await.unwrap();
        }
        io_clock.begin_delivery();
        eprintln!(
            "clock producer gate: tcp={tcp}, original_offset_s=120, original_timeout_s=2, bounded_real_io=true"
        );
        let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("actual packet delivery")
            .unwrap();
        assert_eq!(packet.transport, id);
        assert_eq!(packet.bound, target);
        assert_eq!(packet.data, raw);
        assert!(packet.received_at >= before + Duration::from_secs(120));
        assert_eq!(
            packet.received_at,
            tokio::time::Instant::now().into_std(),
            "source stamp must be actual producer runtime time, with no tolerance/post-decode rewrite"
        );
        io_clock.finish().await;
        net.close_transport(id);
        drop(stream);
        drop(udp);
        net.shutdown_and_wait().await;
    }
}
