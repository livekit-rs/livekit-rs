//! Actual PSRPC server deadline + actual native ICE/DTLS/SCTP write blockage.
use super::*;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::{RedisTransport, TransportLimits},
    rpc_transport::RpcBus,
    wire,
};
use futures_util::{pin_mut, poll};
use prost::Message;
use rtc_core::{
    ids::*,
    media_api::DataKind,
    permission::{Grants, VideoGrant},
    session::{
        ClientCapabilities, ClientMeta, JoinParams, SdkKind, SessionOutput, SessionRequest,
        SignalEvent,
    },
};
use rtc_runtime::{
    SessionHandle,
    net::{LocalInterface, NetConfig, NetRouter},
};
use std::task::Poll;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use str0m::{
    Candidate, Event, Input, Output, Rtc,
    channel::ChannelId,
    net::{Protocol, Receive},
};
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
    async fn new(manager: &rtc_runtime::RoomManager, name: &str) -> Self {
        Self::new_with_channels(manager, name, false).await
    }
    async fn new_with_channels(manager: &rtc_runtime::RoomManager, name: &str, both: bool) -> Self {
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

fn limits() -> TransportLimits {
    TransportLimits {
        wire: wire::WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: wire::CompressionPolicy::default(),
        inbox_messages: 128,
        subscriptions: 128,
        commands: 32,
        io_timeout: Duration::from_secs(2),
    }
}
async fn actual_state() -> (AppState, NetRouter) {
    let mut cfg = crate::test_support::config();
    let args=vec!["native-expiry".into(),"--dev".into(),"--config-body".into(),"rtc:\n  node_ip: 127.0.0.1\n  tcp_port: 0\n  udp_port: 0\n  datachannel_slow_threshold: 1\n".into()];
    let info = lk_config::ProgramInfo {
        binary_name: "native-expiry".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let lk_config::EntryAction::RunServer(loaded) =
        lk_config::parse(args, &lk_config::Env::default(), &info).unwrap()
    else {
        panic!("actual public config")
    };
    cfg.rtc = loaded.server.rtc;
    cfg.rtc.use_ice_lite = true;
    let net = NetRouter::start(NetConfig {
        udp_enabled: true,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: "127.0.0.1".parse().unwrap(),
            advertise: "127.0.0.1".parse().unwrap(),
        }],
    })
    .await
    .unwrap();
    let state = crate::state::CompatServices::new(Arc::new(cfg))
        .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
    (state, net)
}
async fn until(mut check: impl FnMut() -> bool, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !check() {
        assert!(Instant::now() < deadline, "{label}");
        tokio::task::yield_now().await;
    }
}
fn request_options() -> CallOptions {
    let mut o = CallOptions::new(Duration::from_secs(1));
    o.selection.accept_first = true;
    o.selection.affinity_timeout = Some(Duration::from_secs(1));
    o
}
async fn campaign(bus: RpcBus, rpc: bool) {
    let (state, net) = actual_state().await;
    let name = rtc_core::ids::new_sid("native_expiry_");
    let key = state.room_key(&name);
    let created = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let mut peer = Client::new_with_channels(&state.manager, &name, true).await;
    pump(&mut [&mut peer], |c| {
        c[0].opened && c[0].lossy_opened && c[0].rtc.is_connected()
    })
    .await;
    let owner = state
        .manager
        .capture_room_owner(&key, &created.sid)
        .unwrap();
    owner
        .send_data_with_nonce(
            DataKind::Reliable,
            b"ready".to_vec(),
            vec![],
            vec![],
            vec![],
        )
        .await
        .unwrap();
    pump(&mut [&mut peer], |c| {
        c[0].received.iter().any(|b| b == b"ready")
    })
    .await;
    peer.received.clear();
    // Native SCTP is fully established, then peer pumping deliberately stops.
    // The actual configured1-byte slow threshold makes the following real write
    // wait for previous encrypted native data to drain, not a sleep fake.
    owner
        .send_data_with_nonce(DataKind::Reliable, vec![3; 60_000], vec![], vec![], vec![])
        .await
        .unwrap();
    assert_eq!(
        state.manager.list_participants(&key).await.unwrap().len(),
        1,
        "ordinary actor API remains responsive with a native write in flight"
    );
    // Freeze this real actor at an actual lifecycle membership subscription ACK,
    // using the same bounded LocalTransport gate as the existing readiness test.
    // No EventSink contract is violated and no media event is fabricated.
    let admission = state.local_dispatch.owned_admission_bus_for_test();
    let held = crate::service_bus::channels::Channel {
        legacy: "accepted-native-admission-held".into(),
        ..Default::default()
    };
    let sub = admission.subscribe_sized(&held, true, 1).await;
    admission.publish_encoded(&held, b"one").await.unwrap();
    let publishing = admission.publish_encoded(&held, b"two");
    pin_mut!(publishing);
    assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
    let registering = admission.subscribe_sized(&held, true, 1);
    pin_mut!(registering);
    assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
    let mut joining_params = params(&name);
    joining_params.identity = "admission-control".into();
    joining_params.grants.identity = "admission-control".into();
    let joining = state.manager.join(joining_params);
    pin_mut!(joining);
    assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
    // Production ROOM_INBOX is bounded at1024. Queue read-only actor operations
    // after the Join; the first registration gate prevents the actor consuming
    // these reads. Dropping their response waiters cannot withdraw queued inputs.
    for _ in 0..1025 {
        let read = tokio::task::unconstrained(owner.list_participants());
        pin_mut!(read);
        assert!(matches!(poll!(read.as_mut()), Poll::Pending));
    }
    assert_eq!(
        owner.control_capacity(),
        0,
        "actual bounded actor inbox must be full; cooperative Pending is not proof"
    );
    let accepted = Arc::new(super::accepted::Accepted::default());
    let sl = ServerLimits {
        concurrent: 16,
        max_request_duration: Duration::from_secs(10),
        allow_skip_claim: false,
    };
    let server = if rpc {
        super::accepted::register_perform_rpc(
            bus.clone(),
            &owner,
            &name,
            state.clone(),
            sl,
            accepted.clone(),
        )
        .await
        .unwrap()
    } else {
        super::accepted::register_send_data(bus.clone(), &owner, sl, accepted.clone())
            .await
            .unwrap()
    };
    let client = Arc::new(
        RpcClient::new_for_service(bus.clone(), 32, if rpc { "Participant" } else { "Room" })
            .unwrap(),
    );
    client
        .ready_for_service(if rpc { "Participant" } else { "Room" })
        .await
        .unwrap();
    let caller = {
        let client = client.clone();
        let name = name.clone();
        tokio::spawn(async move {
            if rpc {
                client
                    .unary::<_, pb::PerformRpcResponse>(
                        &super::participants::participant_spec(&name, &name, "PerformRpc"),
                        &pb::PerformRpcRequest {
                            room: name.clone(),
                            destination_identity: name.clone(),
                            method: "accepted.native".into(),
                            payload: "real-native".into(),
                            response_timeout_ms: 5000,
                            ..Default::default()
                        },
                        request_options(),
                    )
                    .await
                    .map(|_| ())
            } else {
                client
                    .unary::<_, pb::SendDataResponse>(
                        &super::room_spec(&name, "SendData"),
                        &pb::SendDataRequest {
                            room: name,
                            data: vec![4; 60_000],
                            nonce: vec![5; 16],
                            ..Default::default()
                        },
                        request_options(),
                    )
                    .await
                    .map(|_| ())
            }
        })
    };
    until(
        || accepted.pending() == 1,
        "real original RPC handler must enter before original expiry",
    )
    .await;
    if rpc {
        assert_eq!(
            state.rpc.pending_count(),
            0,
            "lookup is genuinely backpressured before reaching the actor"
        );
    }
    let error = caller.await.unwrap().unwrap_err();
    assert_eq!(error.code, "deadline_exceeded");
    assert_eq!(
        accepted.pending(),
        1,
        "PSRPC expiry must not cancel actual blocked application work"
    );
    if rpc {
        assert_eq!(
            state.rpc.pending_count(),
            0,
            "expired caller cannot bypass the still-full real actor inbox"
        );
    }
    accepted.retire_server(owner.sid(), server).await.unwrap();
    let finishing = {
        let accepted = accepted.clone();
        tokio::spawn(async move { accepted.finish().await })
    };
    tokio::task::yield_now().await;
    assert!(
        !finishing.is_finished(),
        "shutdown must wait for the actual blocked operation"
    );
    let (closed, sent, extra_sub) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
    closed.unwrap();
    sent.unwrap();
    extra_sub.unsubscribe(&held).await.unwrap();
    let extra = joining.await.unwrap();
    if rpc {
        until(
            || state.rpc.pending_count() == 1,
            "accepted old wire-expiry work must reach actual actor after admission unblocks",
        )
        .await;
        pump(&mut [&mut peer], |c| {
            c[0].received.iter().any(|raw| {
                matches!(
                    pb::DataPacket::decode(raw.as_slice())
                        .ok()
                        .and_then(|d| d.value),
                    Some(pb::data_packet::Value::RpcRequest(_))
                )
            })
        })
        .await;
        let request = peer
            .received
            .iter()
            .find_map(
                |raw| match pb::DataPacket::decode(raw.as_slice()).ok()?.value? {
                    pb::data_packet::Value::RpcRequest(r) => Some(r),
                    _ => None,
                },
            )
            .unwrap();
        assert_eq!(request.payload, "real-native");
        for value in [
            pb::data_packet::Value::RpcAck(pb::RpcAck {
                request_id: request.id.clone(),
            }),
            pb::data_packet::Value::RpcResponse(pb::RpcResponse {
                request_id: request.id,
                value: Some(pb::rpc_response::Value::Payload(
                    "native-completed-after-wire-expiry".into(),
                )),
            }),
        ] {
            peer.write(
                &pb::DataPacket {
                    value: Some(value),
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .await;
        }
        pump(&mut [&mut peer], |_| state.rpc.pending_count() == 0).await;
    } else {
        pump(&mut[&mut peer],|c|c[0].received.iter().any(|raw|matches!(pb::DataPacket::decode(raw.as_slice()).ok().and_then(|d|d.value),Some(pb::data_packet::Value::User(u))if u.payload==vec![4;60_000]&&u.nonce==vec![5;16]))).await;
    }
    tokio::time::timeout(Duration::from_secs(5), finishing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(accepted.pending(), 0);
    assert_eq!(
        accepted.outcomes(),
        vec![None],
        "actual business operation must complete successfully, not time out after releasing the peer"
    );
    state.manager.delete_room(&key).await.unwrap();
    let replacement = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    assert_ne!(replacement.sid, created.sid);
    let old_sid = owner.sid().clone();
    let replacement_operations = super::accepted::Accepted::default();
    assert!(
        replacement_operations
            .run(&old_sid, async move {
                owner
                    .send_data_with_nonce(DataKind::Reliable, vec![9], vec![], vec![], vec![])
                    .await
                    .map_err(super::api)
            })
            .await
            .is_err(),
        "captured old actor must not route into a replacement room"
    );
    replacement_operations.finish().await;
    drop(extra);
    client.close_and_wait().await;
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
async fn actual_local_psrpc_expiry_preserves_blocked_native_send_data() {
    campaign(LocalTransport::new(limits()).unwrap().into(), false).await;
}
#[tokio::test]
async fn actual_local_psrpc_expiry_preserves_native_rpc_ack_response_lifetime() {
    campaign(LocalTransport::new(limits()).unwrap().into(), true).await;
}
#[tokio::test]
#[ignore = "requires owned ACCEPTED_REDIS_URL; actual PSRPC expiry + authenticated native SCTP, no fake IO"]
async fn actual_redis_psrpc_expiry_preserves_blocked_native_send_data() {
    let url = std::env::var("ACCEPTED_REDIS_URL").unwrap();
    campaign(
        RedisTransport::connect(&url, limits())
            .await
            .unwrap()
            .into(),
        false,
    )
    .await;
}
#[tokio::test]
#[ignore = "requires owned ACCEPTED_REDIS_URL; actual PSRPC expiry + authenticated native SCTP, no fake IO"]
async fn actual_redis_psrpc_expiry_preserves_native_rpc_ack_response_lifetime() {
    let url = std::env::var("ACCEPTED_REDIS_URL").unwrap();
    campaign(
        RedisTransport::connect(&url, limits())
            .await
            .unwrap()
            .into(),
        true,
    )
    .await;
}
