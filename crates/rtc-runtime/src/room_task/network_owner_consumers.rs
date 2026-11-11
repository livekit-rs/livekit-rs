//! Real RoomTask and RoomManager consumers; no injected native media events.
use super::*;
use crate::net::{LocalInterface, NetConfig};
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::events::NullSink;
use rtc_core::permission::{Grants, VideoGrant};
use rtc_core::session::{ClientCapabilities, ClientMeta, SdkKind};
use std::net::SocketAddr;
use str0m::channel::ChannelId;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct RecordData(mpsc::UnboundedSender<(Identity, Vec<u8>)>);
impl DataHandler for RecordData {
    fn inbound(
        &self,
        sender: &DataSender<'_>,
        kind: DataKind,
        label: &str,
        raw: &[u8],
    ) -> DataVerdict {
        assert_eq!(kind, DataKind::Reliable);
        assert_eq!(label, "_reliable");
        self.0
            .send((sender.identity.clone(), raw.to_vec()))
            .unwrap();
        DataVerdict::Consumed
    }
}
fn params(name: &str) -> JoinParams {
    JoinParams {
        room: RoomKey::new(ProjectId::local(), name),
        grants: Grants {
            identity: name.into(),
            video: VideoGrant {
                room_join: true,
                room: name.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        identity: name.into(),
        name: String::new(),
        metadata: String::new(),
        attributes: BTreeMap::new(),
        client: ClientMeta {
            sdk: SdkKind::Js,
            protocol: 17,
            ..Default::default()
        },
        capabilities: ClientCapabilities {
            subscriber_primary: true,
            ice_lite: true,
            unpublish: true,
            fast_start: true,
            request_response: true,
            ..Default::default()
        },
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc: true,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}
async fn manager(
    tcp: bool,
) -> (
    crate::RoomManager,
    NetRouter,
    mpsc::UnboundedReceiver<(Identity, Vec<u8>)>,
) {
    manager_with_policy(tcp, None).await
}
async fn manager_with_policy(
    tcp: bool,
    slow: Option<i64>,
) -> (
    crate::RoomManager,
    NetRouter,
    mpsc::UnboundedReceiver<(Identity, Vec<u8>)>,
) {
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
    let (tx, rx) = mpsc::unbounded_channel();
    let mut cfg = super::tests::test_config();
    cfg.rtc.use_ice_lite = true;
    if let Some(slow) = slow {
        let yaml = format!(
            "rtc:\n  node_ip: 127.0.0.1\n  tcp_port: 0\n  udp_port: 0\n  datachannel_slow_threshold: {slow}\n"
        );
        let info = lk_config::ProgramInfo {
            binary_name: "writer-test".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        let args = vec![
            "writer-test".into(),
            "--dev".into(),
            "--config-body".into(),
            yaml.into(),
        ];
        let lk_config::EntryAction::RunServer(loaded) =
            lk_config::parse(args, &lk_config::Env::default(), &info).unwrap()
        else {
            panic!("actual public YAML must produce RunServer")
        };
        cfg.rtc = loaded.server.rtc;
    }
    let manager = crate::RoomManager::new(crate::RuntimeDeps {
        config: Arc::new(cfg),
        net: net.clone(),
        events: Arc::new(NullSink),
        data: Arc::new(RecordData(tx)),
    });
    (manager, net, rx)
}
struct Client {
    rtc: Rtc,
    socket: tokio::net::UdpSocket,
    session: SessionHandle,
    channel: ChannelId,
    opened: bool,
    lossy_opened: bool,
    received: Vec<Vec<u8>>,
}
impl Client {
    async fn new(manager: &crate::RoomManager, name: &str) -> Self {
        Self::new_with_channels(manager, name, false).await
    }
    async fn new_with_channels(manager: &crate::RoomManager, name: &str, both: bool) -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut rtc = Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .build(Instant::now());
        rtc.add_local_candidate(Candidate::host(socket.local_addr().unwrap(), "udp").unwrap())
            .unwrap();
        let mut session = manager.join(params(name)).await.unwrap();
        let mut sdp = rtc.sdp_api();
        let channel = sdp.add_channel("_reliable".into());
        if both {
            sdp.add_channel("_lossy".into());
        }
        // Fixed ordinary primary is fully-established only after BOTH writers
        // opened. Retain all old real room-lifetime assertions with a valid source setup.
        sdp.add_channel_with_config(str0m::channel::ChannelConfig {
            label: "_lossy".into(),
            ordered: false,
            reliability: str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
            ..Default::default()
        });
        let (offer, pending) = sdp.apply().unwrap();
        assert!(
            session
                .apply(SessionRequest::Offer {
                    sdp: offer.to_sdp_string(),
                    id: 1
                })
                .await
        );
        let answer = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match session.output.recv().await.expect("room signal still live") {
                    SessionOutput::Signal(SignalEvent::Answer { sdp, .. }) => break sdp,
                    SessionOutput::Close => panic!("room closed before answer"),
                    _ => {}
                }
            }
        })
        .await
        .expect("real RoomTask produces answer");
        rtc.sdp_api()
            .accept_answer(
                pending,
                str0m::change::SdpAnswer::from_sdp_string(&answer).unwrap(),
            )
            .unwrap();
        Self {
            rtc,
            socket,
            session,
            channel,
            opened: false,
            lossy_opened: false,
            received: vec![],
        }
    }
    async fn drain(&mut self) {
        loop {
            match self.rtc.poll_output().unwrap() {
                Output::Transmit(p) => {
                    self.socket
                        .send_to(&p.contents, p.destination)
                        .await
                        .unwrap();
                }
                Output::Event(Event::ChannelOpen(id, _)) if id == self.channel => {
                    self.opened = true
                }
                Output::Event(Event::ChannelOpen(_, label)) if label == "_lossy" => {
                    self.lossy_opened = true
                }
                Output::Event(Event::ChannelData(d)) => self.received.push(d.data),
                Output::Timeout(_) => break,
                _ => {}
            }
        }
    }
    async fn step(&mut self) {
        let now = Instant::now();
        self.rtc.handle_input(Input::Timeout(now)).unwrap();
        self.drain().await;
        while let Ok(out) = self.session.output.try_recv() {
            if let SessionOutput::Signal(SignalEvent::Trickle {
                candidate,
                last: false,
                ..
            }) = out
            {
                self.rtc.add_remote_candidate(
                    Candidate::from_sdp_string(&candidate.candidate).unwrap(),
                );
                self.drain().await;
            }
        }
        loop {
            let mut b = [0; 4096];
            match self.socket.try_recv_from(&mut b) {
                Ok((n, source)) => {
                    self.rtc
                        .handle_input(Input::Receive(
                            now,
                            Receive::new(
                                Protocol::Udp,
                                source,
                                self.socket.local_addr().unwrap(),
                                &b[..n],
                            )
                            .unwrap(),
                        ))
                        .unwrap();
                    self.drain().await;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("real UDP receive: {e}"),
            }
        }
    }
    async fn write(&mut self, bytes: &[u8]) {
        assert!(
            self.rtc
                .channel(self.channel)
                .expect("actual SCTP channel open")
                .write(true, bytes)
                .unwrap()
        );
        self.drain().await;
    }
}
async fn pump(clients: &mut [&mut Client], ready: impl Fn(&[&mut Client]) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        for client in clients.iter_mut() {
            client.step().await;
        }
        if ready(clients) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "actual ICE/DTLS/SCTP progress deadline: {:?}",
            clients
                .iter()
                .map(|c| (
                    c.opened,
                    c.rtc.is_connected(),
                    c.received.len(),
                    c.received.iter().map(Vec::len).collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
async fn upstream(
    receipts: &mut mpsc::UnboundedReceiver<(Identity, Vec<u8>)>,
    name: &str,
    body: &[u8],
) {
    let (identity, raw) = tokio::time::timeout(Duration::from_secs(2), receipts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identity, Identity::from(name));
    assert_eq!(raw, body);
}
#[tokio::test]
async fn closing_actual_room_a_preserves_room_b_authenticated_sctp_data_both_directions() {
    let (manager, net, mut receipts) = manager(false).await;
    let mut a = Client::new(&manager, "wire-owner-a").await;
    let mut b = Client::new(&manager, "wire-owner-b").await;
    pump(&mut [&mut a, &mut b], |c| {
        c.iter().all(|c| c.opened && c.rtc.is_connected())
    })
    .await;
    for (client, name) in [(&mut a, "wire-owner-a"), (&mut b, "wire-owner-b")] {
        client.write(name.as_bytes()).await;
        pump(&mut [client], |_| !receipts.is_empty()).await;
        upstream(&mut receipts, name, name.as_bytes()).await;
        manager
            .send_data(
                &params(name).room,
                DataKind::Reliable,
                b"before-delete".to_vec(),
                vec![],
                vec![],
            )
            .await
            .unwrap();
        pump(&mut [client], |c| {
            c[0].received.iter().any(|p| p == b"before-delete")
        })
        .await;
    }
    let room_a = params("wire-owner-a").room;
    manager.delete_room(&room_a).await.unwrap();
    assert!(!manager.room_exists(&room_a));
    assert!(manager.room_exists(&params("wire-owner-b").room));
    b.write(b"b-after-a-close").await;
    pump(&mut [&mut b], |_| !receipts.is_empty()).await;
    upstream(&mut receipts, "wire-owner-b", b"b-after-a-close").await;
    manager
        .send_data(
            &params("wire-owner-b").room,
            DataKind::Reliable,
            b"still-b-downstream".to_vec(),
            vec![],
            vec![],
        )
        .await
        .unwrap();
    pump(&mut [&mut b], |c| {
        c[0].received.iter().any(|p| p == b"still-b-downstream")
    })
    .await;
    assert!(b.rtc.is_connected());
    manager.close_all().await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
async fn manager_close_all_collects_actual_accepted_idle_tcp_and_releases_listener() {
    let (manager, net, _receipts) = manager(true).await;
    let key = params("idle-manager-owner").room;
    manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    // Obtain the actual shared listener through its public candidate API. This
    // extra transport is real and unconnected; the idle peer has no invented owner.
    let (tx, _rx) = mpsc::channel(4);
    let id = TransportId::next();
    let address: SocketAddr = net.open_transport(id, "idle-probe", tx).unwrap()[0].base;
    let listener_tasks = net.tcp_owner_test_counts().0;
    assert_eq!(listener_tasks, 1, "one actual listener is registered");
    let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
    peer.write_all(&[0]).await.unwrap(); // incomplete RFC4571 header: no STUN/owner yet
    tokio::time::timeout(Duration::from_secs(2), async {
        while net.tcp_owner_test_counts().0 < listener_tasks + 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("must observe actual accepted task before shutdown");
    assert_eq!(net.tcp_owner_test_counts().1, 0);
    tokio::time::timeout(Duration::from_secs(3), manager.close_all())
        .await
        .expect("actual manager must await accepted reader collection");
    assert!(!manager.room_exists(&key));
    assert_eq!(
        net.tcp_owner_test_counts(),
        (0, 0),
        "return requires registry collection, not cancellation request only"
    );
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peer.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(peer);
    assert!(
        tokio::net::TcpStream::connect(address).await.is_err(),
        "listener must be stopped"
    );
    // Physical rebind proves no task retains the old listening socket.
    let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
    drop(rebound);
}

#[tokio::test]
async fn actual_manager_joining_send_data_waits_for_native_writers_and_replays_bounded_fifo() {
    let (manager, net, mut receipts) = manager(false).await;
    let name = "joining-actual-owner";
    let mut client = Client::new(&manager, name).await;
    // No client step/drain yet: real peer has not sent any ICE/DTLS packet.
    assert!(!client.opened);
    assert!(!client.rtc.is_connected());
    let key = params(name).room;
    for body in [vec![1; 50_000], vec![2; 50_000], vec![3]] {
        manager
            .send_data(&key, DataKind::Reliable, body, vec![], vec![])
            .await
            .unwrap();
    }
    manager
        .send_data(&key, DataKind::Lossy, vec![9], vec![], vec![])
        .await
        .unwrap();
    pump(&mut [&mut client], |c| {
        c[0].opened && c[0].rtc.is_connected() && c[0].received.len() >= 2
    })
    .await;
    assert_eq!(client.received, vec![vec![1; 50_000], vec![2; 50_000]]);
    assert!(
        receipts.is_empty(),
        "downstream API is not an invented inbound packet"
    );
    client.write(b"actual-upstream-after-replay").await;
    pump(&mut [&mut client], |_| !receipts.is_empty()).await;
    upstream(&mut receipts, name, b"actual-upstream-after-replay").await;
    manager
        .send_data(
            &key,
            DataKind::Reliable,
            b"direct-after-ready".to_vec(),
            vec![],
            vec![],
        )
        .await
        .unwrap();
    pump(&mut [&mut client], |c| c[0].received.len() >= 3).await;
    assert_eq!(client.received[2], b"direct-after-ready");
    assert_eq!(client.received.len(), 3);
    manager.close_all().await;
    net.shutdown_and_wait().await;
}
// Existing public RoomManager/SessionHandle + actual OS UDP/ICE/DTLS/SCTP.
// Baseline test never mentions new policy fields/commands or injects media events.
#[tokio::test]
async fn ordinary_default_nonblocking_api_writes_preserve_all_bodies_beyond_native_aggregate_budget()
 {
    let (manager, net, _receipts) = manager(false).await;
    let mut peer = Client::new_with_channels(&manager, "ordinary-default-buffer", true).await;
    pump(&mut [&mut peer], |clients| {
        clients[0].opened && clients[0].lossy_opened && clients[0].rtc.is_connected()
    })
    .await;
    let key = params("ordinary-default-buffer").room;
    manager
        .send_data(
            &key,
            DataKind::Reliable,
            b"actual-established-marker".to_vec(),
            vec![],
            vec![],
        )
        .await
        .unwrap();
    pump(&mut [&mut peer], |clients| {
        clients[0]
            .received
            .iter()
            .any(|p| p == b"actual-established-marker")
    })
    .await;
    peer.received.clear();
    // Keep the actual receiving peer idle during these calls. No fake ACK,
    // event, clock, capacity or successful write is supplied to the server.
    let bodies: Vec<Vec<u8>> = (0..32).map(|index| vec![index as u8; 60_000]).collect();
    for body in &bodies {
        tokio::time::timeout(
            Duration::from_secs(8),
            manager.send_data(&key, DataKind::Reliable, body.clone(), vec![], vec![]),
        )
        .await
        .expect("nonblocking source API completion")
        .unwrap();
    }
    pump(&mut [&mut peer], |clients| {
        clients[0].received.len() >= bodies.len()
    })
    .await;
    assert_eq!(
        peer.received, bodies,
        "every actual reliable body must arrive exactly once and in order"
    );
    manager.close_all().await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn configured_blocking_api_completion_waits_for_previous_actual_sctp_pending_data() {
    let (manager, net, _receipts) = manager_with_policy(false, Some(1)).await;
    let mut peer = Client::new_with_channels(&manager, "ordinary-blocking-writer", true).await;
    pump(&mut [&mut peer], |c| {
        c[0].opened && c[0].lossy_opened && c[0].rtc.is_connected()
    })
    .await;
    let key = params("ordinary-blocking-writer").room;
    manager
        .send_data(&key, DataKind::Reliable, b"ready".to_vec(), vec![], vec![])
        .await
        .unwrap();
    pump(&mut [&mut peer], |c| {
        c[0].received.iter().any(|p| p == b"ready")
    })
    .await;
    peer.received.clear();
    let first = vec![3; 60_000];
    let second = vec![4; 60_000];
    manager
        .send_data(&key, DataKind::Reliable, first.clone(), vec![], vec![])
        .await
        .unwrap();
    let second_manager = manager.clone();
    let second_key = key.clone();
    let send_body = second.clone();
    let pending = tokio::spawn(async move {
        second_manager
            .send_data(&second_key, DataKind::Reliable, send_body, vec![], vec![])
            .await
    });
    // Receiver is intentionally not driven. A successful first enqueue is not
    // proof the second native write has completed; actor must remain responsive.
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert!(
        !pending.is_finished(),
        "source unmeasured timeout retries rather than acknowledging an unperformed second write"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(8), manager.list_participants(&key))
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );
    pump(&mut [&mut peer], |c| c[0].received.len() >= 2).await;
    tokio::time::timeout(Duration::from_secs(8), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(peer.received, vec![first, second]);
    manager.close_all().await;
    net.shutdown_and_wait().await;
}

/// The process-retention assertions use real authenticated application messages,
/// not invalid UDP lengths masquerading as accepted media traffic. ICE, DTLS,
/// SCTP/DCEP setup alone must leave source media byte/packet counters unchanged.
pub(super) async fn process_traffic_lifetime_with_authenticated_messages() {
    let (manager, net, mut receipts) = manager(false).await;
    let name = "lifetime-counters";
    let key = params(name).room;
    let mut first = Client::new(&manager, name).await;
    pump(&mut [&mut first], |clients| {
        clients[0].opened && clients[0].lossy_opened && clients[0].rtc.is_connected()
    })
    .await;
    assert_eq!(
        manager.node_stats().bytes_in,
        0,
        "authenticated transport/control setup is not application/media traffic"
    );
    assert_eq!(manager.node_stats().packets_in, 0);
    let initial_room = manager
        .list_rooms(&key.project, std::slice::from_ref(&key.name))
        .pop()
        .unwrap();
    let first_payload = vec![0x42; 32];
    first.write(&first_payload).await;
    pump(&mut [&mut first], |_| !receipts.is_empty()).await;
    upstream(&mut receipts, name, &first_payload).await;
    pump(&mut [&mut first], |_| {
        manager.node_stats().bytes_in == 32 && manager.node_stats().packets_in == 1
    })
    .await;
    assert_eq!(manager.node_stats().bytes_in, 32);
    assert_eq!(manager.node_stats().packets_in, 1);
    // Repeated snapshots must not add this already-observed message again.
    for _ in 0..3 {
        assert_eq!(manager.node_stats().bytes_in, 32);
        assert_eq!(manager.node_stats().packets_in, 1);
    }
    manager.delete_room(&key).await.unwrap();
    drop(first);
    assert!(!manager.room_exists(&key));
    assert_eq!(
        manager.node_stats().bytes_in,
        32,
        "terminal sampling must not double count"
    );
    assert_eq!(
        manager.node_stats().packets_in,
        1,
        "closed room must not erase process traffic"
    );

    let mut replacement = Client::new(&manager, name).await;
    pump(&mut [&mut replacement], |clients| {
        clients[0].opened && clients[0].lossy_opened && clients[0].rtc.is_connected()
    })
    .await;
    let next_room = manager
        .list_rooms(&key.project, std::slice::from_ref(&key.name))
        .pop()
        .unwrap();
    assert_ne!(
        initial_room.sid, next_room.sid,
        "new generation must be a real new Room"
    );
    assert_eq!(
        manager.node_stats().bytes_in,
        32,
        "room replacement and its new secure handshake cannot reset or contaminate the process counter"
    );
    assert_eq!(manager.node_stats().packets_in, 1);
    let next_payload = vec![0x24; 48];
    replacement.write(&next_payload).await;
    pump(&mut [&mut replacement], |_| !receipts.is_empty()).await;
    upstream(&mut receipts, name, &next_payload).await;
    pump(&mut [&mut replacement], |_| {
        manager.node_stats().bytes_in == 80 && manager.node_stats().packets_in == 2
    })
    .await;
    assert_eq!(manager.node_stats().bytes_in, 80);
    assert_eq!(manager.node_stats().packets_in, 2);
    manager.delete_room(&key).await.unwrap();
    drop(replacement);
    assert_eq!(
        manager.node_stats().bytes_in,
        80,
        "second terminal sample retains both real generations exactly once"
    );
    assert_eq!(manager.node_stats().packets_in, 2);
    manager.close_all().await;
    assert_eq!(manager.node_stats().bytes_in, 80);
    assert_eq!(manager.node_stats().packets_in, 2);
    net.shutdown_and_wait().await;
}
