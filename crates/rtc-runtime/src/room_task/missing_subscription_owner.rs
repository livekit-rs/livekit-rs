//! Actual RoomManager/RoomTask timer -> SessionHandle output over live native ICE/DTLS.
//! Paused clock drives BOTH server and native client; no injected transport/failure event.
use super::*;
use crate::net::{LocalInterface, NetConfig};
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::events::NullSink;
use rtc_core::permission::{Grants, VideoGrant};
use rtc_core::session::*;
use str0m::change::SdpAnswer;
use str0m::media::{Direction, MediaKind};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Input, Output, Rtc};
use tokio::net::UdpSocket;
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}
struct DropData;
impl DataHandler for DropData {
    fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
        DataVerdict::Drop
    }
}
fn params(identity: &str, protocol: i32, single_pc: bool) -> JoinParams {
    JoinParams {
        room: RoomKey::new(ProjectId::local(), "r1"),
        grants: Grants {
            identity: identity.into(),
            video: VideoGrant {
                room_join: true,
                room: "r1".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        identity: identity.into(),
        name: String::new(),
        metadata: String::new(),
        attributes: BTreeMap::new(),
        client: ClientMeta {
            sdk: SdkKind::Js,
            protocol,
            ..Default::default()
        },
        capabilities: ClientCapabilities {
            video_publication_requires_rtp: false,
            codec_switch_requires_renegotiation: false,
            ice_tcp: true,
            one_shot: false,
            subscriber_primary: true,
            speaker_changed: true,
            connection_quality: true,
            ice_lite: true,
            unpublish: true,
            fast_start: true,
            disconnected_update: true,
            sync_stream_id: true,
            disable_playout_delay_for_sync: false,
            connection_quality_lost: true,
            identity_based_reconnection: true,
            regions_in_leave: true,
            non_error_signal_response: true,
            request_response: true,
            audio_red: true,
        },
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}

async fn setup(enabled: bool) -> (Arc<crate::RoomManager>, NetRouter) {
    let mut config = super::tests::test_config();
    config.rtc.node_ip = Default::default();
    config.rtc.reconnect_on_subscription_error = enabled;
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
    let manager = Arc::new(crate::RoomManager::new(crate::RuntimeDeps {
        config: Arc::new(config),
        net: net.clone(),
        events: Arc::new(NullSink),
        data: Arc::new(DropData),
    }));
    (manager, net)
}
struct Peer {
    handle: SessionHandle,
    rtc: Rtc,
    socket: UdpSocket,
    signals: Vec<SignalEvent>,
    closed: bool,
}
impl Peer {
    async fn join(manager: &crate::RoomManager, identity: &str) -> Self {
        let mut input = params(identity, 17, true);
        input.auto_subscribe = false;
        let mut handle = manager.join(input).await.unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.writable().await.unwrap();
        let mut rtc = Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .set_ice_lite(false)
            .build(now());
        rtc.add_local_candidate(Candidate::host(socket.local_addr().unwrap(), "udp").unwrap())
            .unwrap();
        let mut change = rtc.sdp_api();
        change.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
        let (offer, pending) = change.apply().unwrap();
        assert!(
            handle
                .apply(SessionRequest::Offer {
                    sdp: offer.to_sdp_string(),
                    id: 1
                })
                .await
        );
        let mut signals = vec![];
        let answer = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match handle.output.recv().await.expect("actual owner answer") {
                    SessionOutput::Signal(SignalEvent::Answer { sdp, .. }) => break sdp,
                    SessionOutput::Signal(event) => signals.push(event),
                    SessionOutput::Close => panic!("owner closed before native handshake"),
                }
            }
        })
        .await
        .expect("actual owner SDP answer within bounded setup");
        rtc.sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
            .unwrap();
        let mut peer = Self {
            handle,
            rtc,
            socket,
            signals,
            closed: false,
        };
        for _ in 0..1000 {
            peer.step().await;
            if peer.rtc.is_connected() {
                let info = manager
                    .get_participant(
                        &RoomKey::new(ProjectId::local(), "r1"),
                        &Identity::from(identity),
                    )
                    .await
                    .unwrap();
                if info.state == ParticipantState::Active {
                    return peer;
                }
            }
        }
        panic!("real ICE/DTLS and owner ACTIVE not established before10s");
    }
    fn drain_session(&mut self) {
        for _ in 0..1024 {
            match self.handle.output.try_recv() {
                Ok(SessionOutput::Signal(event)) => self.signals.push(event),
                Ok(SessionOutput::Close) => self.closed = true,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    self.closed = true;
                    return;
                }
            }
        }
        panic!("bounded owner signal drain exceeded")
    }
    async fn step(&mut self) {
        self.rtc.handle_input(Input::Timeout(now())).unwrap();
        for _ in 0..1024 {
            match self.rtc.poll_output().unwrap() {
                Output::Transmit(p) => {
                    assert_eq!(p.source, self.socket.local_addr().unwrap());
                    self.socket
                        .send_to(&p.contents, p.destination)
                        .await
                        .unwrap();
                }
                Output::Event(_) => {}
                Output::Timeout(_) => break,
            }
        }
        let mut buf = [0; 65536];
        for _ in 0..256 {
            match self.socket.try_recv_from(&mut buf) {
                Ok((len, from)) => self
                    .rtc
                    .handle_input(Input::Receive(
                        now(),
                        Receive::new(
                            Protocol::Udp,
                            from,
                            self.socket.local_addr().unwrap(),
                            &buf[..len],
                        )
                        .unwrap(),
                    ))
                    .unwrap(),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("owned native receive {e}"),
            }
        }
        self.drain_session();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }
    async fn elapsed(&mut self, duration: Duration) {
        let until = now() + duration;
        while now() < until {
            self.step().await;
        }
        self.drain_session();
    }
    fn missing(&self, track: &TrackSid) -> usize {
        self.signals.iter().filter(|e|matches!(e,SignalEvent::SubscriptionResponse{track_sid,error:SubscriptionError::TrackNotFound}if track_sid==track)).count()
    }
    fn no_leave(&self) {
        assert!(!self.closed);
        assert!(
            !self
                .signals
                .iter()
                .any(|e| matches!(e, SignalEvent::Leave { .. }))
        );
        assert!(
            self.rtc.is_connected(),
            "native peer must stay connected throughout missing-track wait"
        );
    }
}
#[tokio::test(start_paused = true)]
async fn real_owner_timer_and_session_output_missing_nonfatal_false_and_true() {
    for enabled in [false, true] {
        let (manager, net) = setup(enabled).await;
        let mut peer = Peer::join(&manager, "subscriber").await;
        let track = TrackSid::from("TR_owner_never_published");
        assert!(
            peer.handle
                .apply(SessionRequest::Subscription {
                    track_sids: vec![track.clone()],
                    subscribe: true
                })
                .await
        );
        peer.elapsed(Duration::from_secs(60)).await;
        assert_eq!(peer.missing(&track), 0);
        peer.no_leave();
        peer.elapsed(Duration::from_secs(4)).await;
        assert_eq!(peer.missing(&track), 1);
        peer.no_leave();
        peer.elapsed(Duration::from_secs(6)).await;
        assert_eq!(peer.missing(&track), 1);
        peer.no_leave();
        assert!(
            manager
                .get_participant(
                    &RoomKey::new(ProjectId::local(), "r1"),
                    &peer.handle.identity
                )
                .await
                .is_ok()
        );
        manager.close_all().await;
        drop(peer);
        net.shutdown_and_wait().await;
    }
}
#[tokio::test(start_paused = true)]
async fn actual_owner_close_and_same_identity_replacement_do_not_leak_old_missing_clock() {
    let (manager, net) = setup(true).await;
    let mut old = Peer::join(&manager, "subscriber").await;
    let track = TrackSid::from("TR_old_generation");
    assert!(
        old.handle
            .apply(SessionRequest::Subscription {
                track_sids: vec![track.clone()],
                subscribe: true
            })
            .await
    );
    old.elapsed(Duration::from_secs(30)).await;
    let old_sid = old.handle.participant_sid.clone();
    let mut current = Peer::join(&manager, "subscriber").await;
    assert_ne!(
        old_sid, current.handle.participant_sid,
        "duplicate identity gets a new owner SID"
    );
    old.drain_session();
    assert!(old.closed);
    assert_eq!(old.missing(&track), 0);
    drop(old);
    current.elapsed(Duration::from_secs(40)).await;
    current.no_leave();
    assert_eq!(current.missing(&track), 0);
    let fresh = TrackSid::from("TR_replacement_own_clock");
    assert!(
        current
            .handle
            .apply(SessionRequest::Subscription {
                track_sids: vec![fresh.clone()],
                subscribe: true
            })
            .await
    );
    current.elapsed(Duration::from_secs(20)).await;
    manager
        .delete_room(&RoomKey::new(ProjectId::local(), "r1"))
        .await
        .unwrap();
    current.drain_session();
    assert!(current.closed);
    assert_eq!(current.missing(&fresh), 0);
    tokio::time::advance(Duration::from_secs(70)).await;
    tokio::task::yield_now().await;
    current.drain_session();
    assert_eq!(current.missing(&fresh), 0);
    manager.close_all().await;
    drop(current);
    net.shutdown_and_wait().await;
}
#[tokio::test(start_paused = true)]
async fn physical_peer_loss_retires_unfulfilled_intent_without_delayed_missing_reply() {
    let (manager, net) = setup(true).await;
    let peer = Peer::join(&manager, "subscriber").await;
    let track = TrackSid::from("TR_disconnected");
    assert!(
        peer.handle
            .apply(SessionRequest::Subscription {
                track_sids: vec![track.clone()],
                subscribe: true
            })
            .await
    );
    // Drop the only actual UDP peer, not a fake fatal/core event. Keep its real
    // SessionHandle to observe the owner's actual native failure/cleanup output.
    let Peer {
        handle,
        rtc,
        socket,
        signals,
        ..
    } = peer;
    drop(socket);
    drop(rtc);
    let mut handle = handle;
    let mut signals = signals;
    for _ in 0..9000 {
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        while let Ok(output) = handle.output.try_recv() {
            if let SessionOutput::Signal(event) = output {
                signals.push(event)
            }
        }
    }
    assert!(
        manager
            .get_participant(&RoomKey::new(ProjectId::local(), "r1"), &handle.identity)
            .await
            .is_err(),
        "actual native failure must retire disconnected owner"
    );
    assert!(!signals.iter().any(|e|matches!(e,SignalEvent::SubscriptionResponse{track_sid,error:SubscriptionError::TrackNotFound}if track_sid==&track)),"removed/disconnected owner must not emit stale missing error");
    manager.close_all().await;
    drop(handle);
    net.shutdown_and_wait().await;
}
