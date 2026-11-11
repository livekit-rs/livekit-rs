//! Actual dual-PC ICE/DTLS/SRTP owner; no injected Prepared/Bound/timer epoch.
use super::*;
use crate::net::{LocalInterface, NetConfig};
use rtc_core::data::{DataHandler, DataSender, DataVerdict};
use rtc_core::events::NullSink;
use rtc_core::permission::{Grants, VideoGrant};
use rtc_core::session::*;
use str0m::change::{SdpAnswer, SdpOffer, SdpPendingOffer};
use str0m::media::{Direction, MediaKind};
use str0m::media::{Frequency, MediaTime, Mid};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Input, Output, Rtc};
use tokio::net::UdpSocket;
fn now() -> Instant {
    Instant::now()
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

struct Endpoint {
    rtc: Rtc,
    socket: UdpSocket,
    media: usize,
}
impl Endpoint {
    async fn new() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.writable().await.unwrap();
        let mut rtc = Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .set_ice_lite(false)
            .build(now());
        rtc.add_local_candidate(Candidate::host(socket.local_addr().unwrap(), "udp").unwrap())
            .unwrap();
        Self {
            rtc,
            socket,
            media: 0,
        }
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
                Output::Event(str0m::Event::MediaData(_)) => self.media += 1,
                Output::Event(_) => {}
                Output::Timeout(_) => break,
            }
        }
        let mut buf = [0; 65536];
        for _ in 0..256 {
            match self.socket.try_recv_from(&mut buf) {
                Ok((n, src)) => self
                    .rtc
                    .handle_input(Input::Receive(
                        now(),
                        Receive::new(
                            Protocol::Udp,
                            src,
                            self.socket.local_addr().unwrap(),
                            &buf[..n],
                        )
                        .unwrap(),
                    ))
                    .unwrap(),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("owned UDP: {e}"),
            }
        }
    }
}
struct Peer {
    handle: SessionHandle,
    publisher: Endpoint,
    subscriber: Endpoint,
    pending: Option<SdpPendingOffer>,
    audio: Option<Mid>,
    sequence: u64,
    last_audio: Instant,
    signals: Vec<SignalEvent>,
    hold: bool,
    held: Vec<(String, u32, BTreeMap<String, String>)>,
    closed: bool,
    answered: usize,
}
impl Peer {
    async fn join(manager: &crate::RoomManager, id: &str, publish: bool) -> Self {
        let mut input = params(id, 17, false);
        input.auto_subscribe = false;
        let handle = manager.join(input).await.unwrap();
        let mut p = Self {
            handle,
            publisher: Endpoint::new().await,
            subscriber: Endpoint::new().await,
            pending: None,
            audio: None,
            sequence: 0,
            last_audio: now(),
            signals: vec![],
            hold: false,
            held: vec![],
            closed: false,
            answered: 0,
        };
        if publish {
            assert!(
                p.handle
                    .apply(SessionRequest::AddTrack(AddTrack {
                        token: 1,
                        cid: "source_audio".into(),
                        name: "epoch_audio".into(),
                        kind: TrackKind::Audio,
                        width: 0,
                        height: 0,
                        muted: false,
                        disable_dtx: false,
                        source: TrackSource::Microphone,
                        layers: vec![],
                        simulcast_codecs: vec![],
                        sid: None,
                        stereo: false,
                        disable_red: false,
                        encryption: EncryptionType::None,
                        stream: String::new(),
                        backup_codec_policy: BackupCodecPolicy::PreferRegression,
                        audio_features: vec![],
                        packet_trailer_features: vec![],
                    }))
                    .await
            );
            let mut change = p.publisher.rtc.sdp_api();
            p.audio = Some(change.add_media(
                MediaKind::Audio,
                Direction::SendOnly,
                Some("stream".into()),
                Some("source_audio".into()),
                None,
            ));
            let (offer, pending) = change.apply().unwrap();
            p.pending = Some(pending);
            assert!(
                p.handle
                    .apply(SessionRequest::Offer {
                        sdp: offer.to_sdp_string(),
                        id: 1
                    })
                    .await
            );
        }
        p
    }
    async fn answer(&mut self, sdp: String, id: u32) {
        let answer = self
            .subscriber
            .rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(&sdp).unwrap())
            .unwrap();
        assert!(
            self.handle
                .apply(SessionRequest::Answer {
                    sdp: answer.to_sdp_string(),
                    id
                })
                .await
        );
        self.answered += 1;
    }
    async fn step(&mut self) {
        let mut events = vec![];
        for _ in 0..1024 {
            match self.handle.output.try_recv() {
                Ok(SessionOutput::Signal(e)) => events.push(e),
                Ok(SessionOutput::Close) => self.closed = true,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    self.closed = true;
                    break;
                }
            }
        }
        for event in events {
            match &event {
                SignalEvent::Offer {
                    sdp,
                    id,
                    mid_to_track_id,
                } => {
                    if self.hold {
                        self.held.push((sdp.clone(), *id, mid_to_track_id.clone()));
                    } else {
                        self.answer(sdp.clone(), *id).await;
                    }
                }
                SignalEvent::Answer { sdp, .. } => {
                    self.publisher
                        .rtc
                        .sdp_api()
                        .accept_answer(
                            self.pending.take().expect("actual pending publisher offer"),
                            SdpAnswer::from_sdp_string(sdp).unwrap(),
                        )
                        .unwrap();
                }
                SignalEvent::Trickle {
                    candidate, target, ..
                } if !candidate.candidate.is_empty() => {
                    let c = Candidate::from_sdp_string(&candidate.candidate).unwrap();
                    let endpoint = match target {
                        SignalTarget::Publisher => &mut self.publisher,
                        SignalTarget::Subscriber => &mut self.subscriber,
                    };
                    endpoint.rtc.add_remote_candidate(c);
                }
                _ => {}
            }
            self.signals.push(event);
        }
        if self.publisher.rtc.is_connected()
            && now().duration_since(self.last_audio) >= Duration::from_millis(20)
            && let Some(mid) = self.audio
        {
            let writer = self.publisher.rtc.writer(mid).unwrap();
            let pt = writer
                .payload_params()
                .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                .unwrap()
                .pt();
            writer
                .write(
                    pt,
                    now(),
                    MediaTime::new(self.sequence * 960, Frequency::FORTY_EIGHT_KHZ),
                    vec![0xf8, 0xff, 0xfe],
                )
                .unwrap();
            self.sequence += 1;
            self.last_audio = now();
        }
        self.publisher.step().await;
        self.subscriber.step().await;
    }
    fn assert_alive(&self) {
        assert!(!self.closed);
        assert!(self.subscriber.rtc.is_connected());
        assert!(
            !self
                .signals
                .iter()
                .any(|e| matches!(e, SignalEvent::Leave { .. }))
        );
    }
}
async fn drive(a: &mut Peer, b: &mut Peer, d: Duration) {
    let until = now() + d;
    while now() < until {
        a.step().await;
        b.step().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn established() -> (Arc<crate::RoomManager>, NetRouter, Peer, Peer, TrackSid) {
    let (manager, net) = setup(false).await;
    let mut a = Peer::join(&manager, "publisher", true).await;
    let mut b = Peer::join(&manager, "subscriber", false).await;
    let deadline = now() + Duration::from_secs(10);
    let track = loop {
        drive(&mut a, &mut b, Duration::from_millis(20)).await;
        if let Some(t) = a.signals.iter().find_map(|e| match e {
            SignalEvent::TrackPublished { track, .. } => Some(track.sid.clone()),
            _ => None,
        }) {
            break t;
        }
        assert!(now() < deadline, "actual publication");
    };
    (manager, net, a, b, track)
}
async fn set_disabled(b: &Peer, track: &TrackSid) {
    assert!(
        b.handle
            .apply(SessionRequest::TrackSettings {
                track_sids: vec![track.clone()],
                settings: TrackSettings {
                    disabled: true,
                    ..Default::default()
                }
            })
            .await
    );
}
async fn desire(m: &crate::RoomManager, track: &TrackSid, on: bool) {
    m.update_subscriptions(
        &RoomKey::new(ProjectId::local(), "r1"),
        &Identity::from("subscriber"),
        vec![track.clone()],
        on,
    )
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "real RoomManager native ICE/DTLS/SRTP and normal wall-clock source 60s keep; coordinator healthy gate"]
async fn owner_settings_only_expire_before_later_actual_media_admission() {
    let (m, net, mut a, mut b, t) = established().await;
    set_disabled(&b, &t).await;
    drive(&mut a, &mut b, Duration::from_secs(63)).await;
    b.assert_alive();
    let before = b.subscriber.media;
    desire(&m, &t, true).await;
    let deadline = now() + Duration::from_secs(10);
    while b.subscriber.media < before + 5 {
        drive(&mut a, &mut b, Duration::from_millis(20)).await;
        assert!(
            now() < deadline,
            "expired disable settings must not suppress actual newly subscribed SRTP"
        );
    }
    b.assert_alive();
    m.close_all().await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
#[ignore = "real RoomManager removal and fresh offer/answer, native wire media; coordinator healthy gate"]
async fn owner_real_unsubscribe_close_drops_active_settings_without_sixty_second_wait() {
    let (m, net, mut a, mut b, t) = established().await;
    set_disabled(&b, &t).await;
    desire(&m, &t, true).await;
    let deadline = now() + Duration::from_secs(10);
    while !b
        .handle
        .named_subscriptions_prepared(vec![NamedSubscription {
            publisher: "publisher".into(),
            track_name: "epoch_audio".into(),
        }])
        .await
        || !b.subscriber.rtc.is_connected()
    {
        drive(&mut a, &mut b, Duration::from_millis(20)).await;
        assert!(
            now() < deadline,
            "actual disabled downtrack prepared and transport connected"
        );
    }
    drive(&mut a, &mut b, Duration::from_secs(2)).await;
    let before = b.subscriber.media;
    drive(&mut a, &mut b, Duration::from_secs(1)).await;
    assert_eq!(
        b.subscriber.media, before,
        "active disabled setting must suppress this source's real media"
    );
    let previous = b.answered;
    desire(&m, &t, false).await;
    let deadline = now() + Duration::from_secs(5);
    while b.answered == previous {
        drive(&mut a, &mut b, Duration::from_millis(20)).await;
        assert!(now() < deadline, "real removal offer/answer");
    }
    desire(&m, &t, true).await;
    let deadline = now() + Duration::from_secs(10);
    while b.subscriber.media < before + 5 {
        drive(&mut a, &mut b, Duration::from_millis(20)).await;
        assert!(
            now() < deadline,
            "post-close new subscription uses default enabled settings immediately"
        );
    }
    b.assert_alive();
    m.close_all().await;
    net.shutdown_and_wait().await;
}
