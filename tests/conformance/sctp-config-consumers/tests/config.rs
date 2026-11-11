//! Same-old-public-API controls: loader -> real engine -> encrypted SCTP output.
//! No new core/native setting getter appears in either baseline assertion.
use rtc_core::ids::{ParticipantSid, TransportId};
use rtc_core::media_api::{MediaCommand, MediaEvent, PcRole, TransportSpec};
use rtc_core::session::SdpType;
use rtc_media::{
    Datagram, IceCredentials, LocalCandidate, MediaEngine, MediaEngineConfig, PacketOut, Proto,
};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use str0m::change::SdpAnswer;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
#[derive(Clone, Default)]
struct Packets(Arc<Mutex<Vec<Vec<u8>>>>);
impl PacketOut for Packets {
    fn send(&self, _: Proto, _: SocketAddr, _: SocketAddr, bytes: &[u8]) {
        self.0.lock().unwrap().push(bytes.to_vec());
    }
}
struct NoProbe;
impl lk_config::netdetect::NetProbe for NoProbe {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().unwrap()]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("no external discovery is part of this fixture")
    }
    fn sleep(&self, _: Duration) {
        panic!("no source discovery retries")
    }
}
fn config(min: i64) -> rtc_core::config::ServerConfig {
    let body =
        format!("rtc:\n  sctp_min_cwnd: {min}\n  datachannel_data_track_target_latency: 0s\n");
    let args: Vec<std::ffi::OsString> = vec![
        "livecloud".into(),
        "--dev".into(),
        "--node-ip".into(),
        "127.0.0.1".into(),
        "--config-body".into(),
        body.into(),
    ];
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    match lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &NoProbe).unwrap() {
        lk_config::EntryAction::RunServer(v) => v.server,
        other => panic!("expected actual normalized run configuration: {other:?}"),
    }
}
struct Pair {
    engine: MediaEngine,
    client: Rtc,
    packets: Packets,
    tid: TransportId,
    start: Instant,
    server: SocketAddr,
    peer: SocketAddr,
    open: bool,
    answer: String,
}
impl Pair {
    fn new(min: i64) -> Self {
        let cfg = config(min);
        let start = Instant::now();
        let packets = Packets::default();
        let mut engine = MediaEngine::new(
            MediaEngineConfig {
                rtc: cfg.rtc,
                audio: cfg.audio,
                video: cfg.video,
                enabled_codecs: cfg.room.enabled_codecs,
                playout_delay: None,
                sif_trailer: vec![],
            },
            Box::new(packets.clone()),
            start,
        )
        .unwrap();
        let tid = TransportId::next();
        let server = "127.0.0.1:9501".parse().unwrap();
        let peer = "127.0.0.1:9502".parse().unwrap();
        engine.create_transport(
            TransportSpec {
                one_shot: false,
                id: tid,
                participant: ParticipantSid::from("PA_consumer"),
                role: PcRole::Single,
                ice_lite: true,
                server_data_channels: false,
                enable_data_tracks: true,
                primary: true,
                client_supports_red: false,
                allow_pause: false,
            },
            IceCredentials::random(),
            vec![LocalCandidate {
                proto: Proto::Udp,
                addr: server,
                base: server,
            }],
            start,
        );
        let mut client = Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .build(start);
        client
            .add_local_candidate(Candidate::host(peer, "udp").unwrap())
            .unwrap();
        let mut edit = client.sdp_api();
        edit.add_channel_with_config(str0m::channel::ChannelConfig {
            label: "_data_track".into(),
            ordered: false,
            reliability: str0m::channel::Reliability::MaxRetransmits { retransmits: 0 },
            ..Default::default()
        });
        let (offer, pending) = edit.apply().unwrap();
        engine.handle_command(
            MediaCommand::RemoteDescription {
                transport: tid,
                kind: SdpType::Offer,
                sdp: offer.to_sdp_string(),
                id: 1,
            },
            start,
        );
        let mut events = vec![];
        engine.drain_events(&mut events);
        let answer = events
            .iter()
            .find_map(|ev| {
                if let MediaEvent::LocalDescription {
                    kind: SdpType::Answer,
                    sdp,
                    ..
                } = ev
                {
                    Some(sdp.clone())
                } else {
                    None
                }
            })
            .expect("real native answer");
        client
            .sdp_api()
            .accept_answer(pending, SdpAnswer::from_sdp_string(&answer).unwrap())
            .unwrap();
        Self {
            engine,
            client,
            packets,
            tid,
            start,
            server,
            peer,
            open: false,
            answer,
        }
    }
    fn drain_client(&mut self, now: Instant) {
        loop {
            match self.client.poll_output().unwrap() {
                Output::Transmit(p) => self.engine.handle_datagram(Datagram {
                    transport: self.tid,
                    proto: Proto::Udp,
                    source: self.peer,
                    destination: self.server,
                    data: &p.contents,
                    received_at: now,
                }),
                Output::Event(Event::ChannelOpen(_, label)) if label == "_data_track" => {
                    self.open = true
                }
                Output::Timeout(_) => break,
                _ => {}
            }
        }
    }
    fn step(&mut self, tick: u64) {
        let now = self.start + Duration::from_millis(tick);
        self.client.handle_input(Input::Timeout(now)).unwrap();
        self.drain_client(now);
        self.engine.handle_timeout(now);
        let mut events = vec![];
        self.engine.drain_events(&mut events);
        let bytes: Vec<_> = self.packets.0.lock().unwrap().drain(..).collect();
        for bytes in bytes {
            self.client
                .handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, self.server, self.peer, &bytes).unwrap(),
                ))
                .unwrap();
            self.drain_client(now);
        }
    }
    fn connect(&mut self) -> u64 {
        for tick in 0..3000 {
            self.step(tick);
            if self.open && self.client.is_connected() {
                for t in tick + 1..tick + 101 {
                    self.step(t);
                }
                return tick + 100;
            }
        }
        panic!("actual ICE/DTLS/SCTP must connect");
    }
    fn initial_flight(mut self) -> usize {
        let tick = self.connect();
        assert!(self.packets.0.lock().unwrap().is_empty());
        self.engine.handle_command(
            MediaCommand::SendDataTrack {
                to: "PA_consumer".into(),
                payload: vec![0x66; 100_000],
            },
            self.start + Duration::from_millis(tick + 1),
        );
        self.packets.0.lock().unwrap().iter().map(|p| p.len()).sum()
    }
}
#[test]
fn source_local_receive_capability_is_actually_advertised_65535() {
    let pair = Pair::new(0);
    assert!(
        pair.answer.contains("a=max-message-size:65535"),
        "actual local SDP answer, not a config projection"
    );
}
#[test]
fn signed_positive_min_cwnd_changes_actual_encrypted_initial_flight_after_public_loader() {
    let baseline = Pair::new(0).initial_flight();
    let configured = Pair::new(50_000).initial_flight();
    assert!(baseline > 0, "real encrypted SCTP payload output required");
    assert!(
        configured > baseline * 5,
        "configured source minimum must change actual pre-feedback flight: default={baseline},configured={configured}"
    );
}
