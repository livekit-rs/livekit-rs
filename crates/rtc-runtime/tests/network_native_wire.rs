//! Actual OS sockets plus public native ICE/DTLS/SRTP APIs. No injected crypto,
//! received media, owner registration or connection-state events.
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rtc_core::ids::TransportId;
use rtc_runtime::net::{
    InboundPacket, LocalInterface, NetConfig, NetProto, NetRouter, OwnedNetSender,
};
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive, TcpType};
use str0m::{Candidate, Event, Input, Output, Rtc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

fn rtc(now: Instant, lite: bool) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_ice_lite(lite)
        .build(now)
}
fn config(tcp: bool) -> NetConfig {
    NetConfig {
        udp_enabled: !tcp,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: tcp.then_some(0),
        interfaces: vec![LocalInterface {
            bind: "127.0.0.1".parse().unwrap(),
            advertise: "127.0.0.1".parse().unwrap(),
        }],
    }
}
struct Peer {
    local: SocketAddr,
    remote: SocketAddr,
    udp: Option<tokio::net::UdpSocket>,
    tcp: Option<tokio::net::tcp::OwnedWriteHalf>,
    frames: mpsc::Receiver<Vec<u8>>,
    reader: Option<tokio::task::JoinHandle<()>>,
}
impl Peer {
    async fn new(tcp: bool, remote: SocketAddr) -> Self {
        let (tx, frames) = mpsc::channel(512);
        if tcp {
            let stream = tokio::net::TcpStream::connect(remote).await.unwrap();
            let local = stream.local_addr().unwrap();
            let (mut rd, wr) = stream.into_split();
            let reader = tokio::spawn(async move {
                loop {
                    let mut h = [0; 2];
                    if rd.read_exact(&mut h).await.is_err() {
                        break;
                    }
                    let mut body = vec![0; u16::from_be_bytes(h) as usize];
                    if rd.read_exact(&mut body).await.is_err() || tx.send(body).await.is_err() {
                        break;
                    }
                }
            });
            Self {
                local,
                remote,
                udp: None,
                tcp: Some(wr),
                frames,
                reader: Some(reader),
            }
        } else {
            let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let local = udp.local_addr().unwrap();
            Self {
                local,
                remote,
                udp: Some(udp),
                tcp: None,
                frames,
                reader: None,
            }
        }
    }
    async fn send(&mut self, data: &[u8]) {
        if let Some(udp) = &self.udp {
            udp.send_to(data, self.remote).await.unwrap();
        } else {
            let wr = self.tcp.as_mut().unwrap();
            wr.write_all(&(data.len() as u16).to_be_bytes())
                .await
                .unwrap();
            wr.write_all(data).await.unwrap();
        }
    }
    fn receive(&mut self) -> Option<Vec<u8>> {
        if let Some(udp) = &self.udp {
            let mut b = [0; 4096];
            match udp.try_recv_from(&mut b) {
                Ok((n, from)) => {
                    assert_eq!(
                        from, self.remote,
                        "actual OS sender must be the selected physical source"
                    );
                    Some(b[..n].to_vec())
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
                Err(e) => panic!("peer UDP receive: {e}"),
            }
        } else {
            self.frames.try_recv().ok()
        }
    }
    async fn close(mut self) {
        drop(self.tcp.take());
        if let Some(reader) = self.reader.take() {
            reader.abort();
            let _ = reader.await;
        }
    }
}
async fn outputs(
    rtc: &mut Rtc,
    now: Instant,
    server: bool,
    route: (NetProto, SocketAddr),
    peer: &mut Peer,
    sender: &OwnedNetSender,
    decoded: &mut usize,
) {
    let (proto, base) = route;
    let mut due = 0;
    for _ in 0..4096 {
        match rtc.poll_output().unwrap() {
            Output::Transmit(packet) => {
                if server {
                    assert_eq!(packet.source, base);
                    assert_eq!(packet.destination, peer.local);
                    sender.send(proto, packet.source, packet.destination, &packet.contents);
                } else {
                    assert_eq!(packet.source, peer.local);
                    assert_eq!(packet.destination, base);
                    peer.send(&packet.contents).await;
                }
            }
            Output::Event(Event::MediaData(data)) => {
                assert_eq!(data.data.as_ref(), [0xf8, 0xff, 0xfe]);
                *decoded += 1;
            }
            Output::Event(_) => {}
            Output::Timeout(t) if t <= now && due < 16 => {
                due += 1;
                rtc.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
        }
    }
    panic!("bounded native output drain did not settle");
}
async fn connected_pair(tcp: bool) {
    let net = NetRouter::start_with_udp_ports(config(tcp), if tcp { &[] } else { &[0, 0] })
        .await
        .unwrap();
    let now = Instant::now();
    let mut server = rtc(now, true);
    let mut client = rtc(now, false);
    let creds = server.direct_api().local_ice_credentials();
    let id = TransportId::next();
    let (tx, mut rx) = mpsc::channel::<InboundPacket>(512);
    let candidates = net.open_transport(id, &creds.ufrag, tx).unwrap();
    assert_eq!(
        candidates.len(),
        1,
        "per-physical-IP gather selects exactly one UDP port"
    );
    let c = candidates[0];
    let proto = if tcp { NetProto::Tcp } else { NetProto::Udp };
    assert_eq!(c.proto, proto);
    assert_eq!(c.addr, c.base);
    let server_candidate = if tcp {
        Candidate::builder()
            .tcp()
            .tcptype(TcpType::Passive)
            .mapped_host(c.addr, c.base)
            .build()
            .unwrap()
    } else {
        Candidate::builder()
            .udp()
            .mapped_host(c.addr, c.base)
            .build()
            .unwrap()
    };
    server.add_local_candidate(server_candidate).unwrap();
    let mut peer = Peer::new(tcp, c.base).await;
    let client_candidate = if tcp {
        Candidate::builder()
            .tcp()
            .tcptype(TcpType::Active)
            .host(peer.local)
            .build()
            .unwrap()
    } else {
        Candidate::host(peer.local, "udp").unwrap()
    };
    client.add_local_candidate(client_candidate).unwrap();
    let sender = net.sender().for_transport(id).unwrap();
    let mut change = client.sdp_api();
    let mid = change.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = server.sdp_api().accept_offer(offer).unwrap();
    client.sdp_api().accept_answer(pending, answer).unwrap();
    let (start, mut last, mut seq) = (Instant::now(), Instant::now(), 0u64);
    let (mut at_server, mut at_client) = (0, 0);
    while start.elapsed() < Duration::from_secs(8) {
        let now = Instant::now();
        client.handle_input(Input::Timeout(now)).unwrap();
        server.handle_input(Input::Timeout(now)).unwrap();
        if client.is_connected()
            && server.is_connected()
            && last.elapsed() >= Duration::from_millis(20)
        {
            last = now;
            seq += 1;
            for endpoint in [&mut client, &mut server] {
                let writer = endpoint.writer(mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(seq * 960, Frequency::FORTY_EIGHT_KHZ),
                        vec![0xf8, 0xff, 0xfe],
                    )
                    .unwrap();
            }
        }
        outputs(
            &mut client,
            now,
            false,
            (proto, c.base),
            &mut peer,
            &sender,
            &mut at_client,
        )
        .await;
        outputs(
            &mut server,
            now,
            true,
            (proto, c.base),
            &mut peer,
            &sender,
            &mut at_server,
        )
        .await;
        while let Ok(packet) = rx.try_recv() {
            assert_eq!(packet.transport, id);
            assert_eq!(packet.bound, c.base);
            assert_eq!(packet.source, peer.local);
            server
                .handle_input(Input::Receive(
                    now,
                    Receive::new(
                        if tcp { Protocol::Tcp } else { Protocol::Udp },
                        packet.source,
                        packet.bound,
                        &packet.data,
                    )
                    .unwrap(),
                ))
                .unwrap();
        }
        while let Some(packet) = peer.receive() {
            client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(
                        if tcp { Protocol::Tcp } else { Protocol::Udp },
                        c.addr,
                        peer.local,
                        &packet,
                    )
                    .unwrap(),
                ))
                .unwrap();
        }
        if at_server >= 10 && at_client >= 10 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        client.is_connected() && server.is_connected(),
        "actual ICE + DTLS pair must connect"
    );
    assert!(
        at_server >= 10 && at_client >= 10,
        "actual authenticated Opus must traverse both directions: server={at_server}, client={at_client}"
    );
    net.close_transport_and_wait(id).await;
    net.shutdown_and_wait().await;
    peer.close().await;
}
#[tokio::test]
async fn configured_multi_udp_native_ice_dtls_srtp_are_bidirectional_on_selected_port() {
    connected_pair(false).await;
}
#[tokio::test]
async fn passive_tcp_native_ice_dtls_srtp_are_bidirectional_on_actual_connection() {
    connected_pair(true).await;
}
