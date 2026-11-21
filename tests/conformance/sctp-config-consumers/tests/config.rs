//! Same-old-public-API controls: loader -> real engine -> encrypted SCTP output.
//! No new core/native setting getter appears in either baseline assertion.
//!
//! Each `rtc.sctp_*` leaf is discriminated by a different actual SCTP consequence
//! measured through encrypted wire bytes:
//! * `sctp_min_cwnd` raises the pre-feedback initial flight floor.
//! * `sctp_fast_rtx_wnd` raises the per-trigger retransmission burst budget
//!   (`mtu.max(fast_rtx_wnd)`), so one fast-retransmit event can retransmit
//!   several already-marked chunks instead of exactly one.
//! * `sctp_cwnd_ca_step` raises the congestion-avoidance increment
//!   (`mtu.max(cwnd_ca_step)` per cwnd bytes acknowledged).
//! Legs run on whole virtual milliseconds, so every comparison states the margin
//! it needs and a leg that never completes fails instead of timing out silently.
use rtc_core::ids::{ParticipantSid, TransportId};
use rtc_core::media_api::{DataKind, MediaCommand, MediaEvent, PcRole, TransportSpec};
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

/// One ordered reliable message. With a 1000-byte payload the padded DATA chunk
/// stays just below one MTU, so the native burst budget (`mtu`) retransmits
/// exactly one chunk per fast-retransmit trigger while a positive source window
/// retransmits several.
const MSG_LEN: usize = 1_000;
/// Messages kept unsent-blocked in the real writer, far above any congestion
/// window used here so the association, never the fixture, limits the pipe.
const PUMP_WINDOW_MSGS: usize = 1_024;
const PUMP_PER_TICK: usize = 256;
/// 30 virtual seconds: covers retransmission rounds and a full RTO episode.
const MEASURE_CAP_TICKS: u64 = 30_000;

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
fn config(min: i64, fast: i64, step: i64) -> rtc_core::config::ServerConfig {
    let body = format!(
        "rtc:\n  sctp_min_cwnd: {min}\n  sctp_fast_rtx_wnd: {fast}\n  sctp_cwnd_ca_step: {step}\n  datachannel_data_track_target_latency: 0s\n"
    );
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
/// Outcome of one lossy reliable-delivery leg.
#[derive(Debug)]
struct Delivery {
    /// Virtual ticks from the armed start until the last byte was delivered.
    ticks: u64,
    /// Engine->client datagrams emitted during the leg.
    emitted: u64,
    /// Datagrams the fixture actually discarded.
    dropped: u64,
    /// Bytes the real client reassembled and surfaced on `_reliable`.
    delivered: usize,
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
    reliable_open: bool,
    answer: String,
    /// Arm the engine->client loss schedule once negotiation settled.
    armed: bool,
    drop_first: bool,
    drop_every: u64,
    emitted: u64,
    dropped: u64,
    delivered: usize,
    delivered_msgs: usize,
}
impl Pair {
    fn new(min: i64, fast: i64, step: i64) -> Self {
        let cfg = config(min, fast, step);
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
        edit.add_channel_with_config(str0m::channel::ChannelConfig {
            label: "_reliable".into(),
            ordered: true,
            reliability: str0m::channel::Reliability::Reliable,
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
            reliable_open: false,
            answer,
            armed: false,
            drop_first: false,
            drop_every: 0,
            emitted: 0,
            dropped: 0,
            delivered: 0,
            delivered_msgs: 0,
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
                Output::Event(Event::ChannelOpen(_, label)) => match label.as_str() {
                    "_data_track" => self.open = true,
                    "_reliable" => self.reliable_open = true,
                    _ => {}
                },
                Output::Event(Event::ChannelData(data)) => {
                    self.delivered += data.data.len();
                    self.delivered_msgs += 1;
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
            self.emitted += 1;
            let lose = (self.drop_first && self.emitted == 1)
                || (self.drop_every != 0 && self.emitted % self.drop_every == 0);
            if self.armed && lose {
                self.dropped += 1;
                continue;
            }
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
            if self.open && self.reliable_open && self.client.is_connected() {
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
    /// Pump `msgs` ordered reliable messages through the real writer while
    /// discarding scheduled engine->client datagrams, and report the virtual
    /// ticks until the real client reassembles every byte.
    fn deliver_with_loss(mut self, drop_first: bool, drop_every: u64, msgs: usize) -> Delivery {
        let base = self.connect();
        assert!(
            self.reliable_open,
            "the negotiated _reliable m-line must open an actual ordered channel"
        );
        let target = msgs * MSG_LEN;
        let mut sent_msgs = 0usize;
        self.armed = true;
        self.drop_first = drop_first;
        self.drop_every = drop_every;
        self.emitted = 0;
        for tick in base + 1..=base + MEASURE_CAP_TICKS {
            if self.delivered >= target {
                return Delivery {
                    ticks: tick - base,
                    emitted: self.emitted,
                    dropped: self.dropped,
                    delivered: self.delivered,
                };
            }
            let now = self.start + Duration::from_millis(tick);
            let outstanding = sent_msgs.saturating_sub(self.delivered_msgs);
            let quota = PUMP_PER_TICK
                .min(PUMP_WINDOW_MSGS.saturating_sub(outstanding))
                .min(msgs - sent_msgs);
            for _ in 0..quota {
                self.engine.handle_command(
                    MediaCommand::SendData {
                        to: ParticipantSid::from("PA_consumer"),
                        kind: DataKind::Reliable,
                        payload: vec![0x5a; MSG_LEN],
                    },
                    now,
                );
                sent_msgs += 1;
            }
            self.step(tick);
        }
        panic!(
            "reliable delivery must finish: sent_msgs={sent_msgs} delivered_bytes={} target={target} drops={} emitted={}",
            self.delivered, self.dropped, self.emitted
        );
    }
}
#[test]
fn source_local_receive_capability_is_actually_advertised_65535() {
    let pair = Pair::new(0, 0, 0);
    assert!(
        pair.answer.contains("a=max-message-size:65535"),
        "actual local SDP answer, not a config projection"
    );
}
#[test]
fn signed_positive_min_cwnd_changes_actual_encrypted_initial_flight_after_public_loader() {
    let baseline = Pair::new(0, 0, 0).initial_flight();
    let configured = Pair::new(50_000, 0, 0).initial_flight();
    assert!(baseline > 0, "real encrypted SCTP payload output required");
    assert!(
        configured > baseline * 5,
        "configured source minimum must change actual pre-feedback flight: default={baseline},configured={configured}"
    );
}
/// Loss injection control: with the same schedule and every knob at the native
/// default, the leg still completes, so a positive-window leg that finishes
/// faster cannot be attributed to an unfinished or vacuous run.
#[test]
fn scheduled_datagram_loss_is_actually_discarded_and_recovered_by_the_default_leg() {
    let leg = Pair::new(60_000, 0, 0).deliver_with_loss(false, 12, 1_536);
    assert!(
        leg.dropped >= 32,
        "the fixture must really discard scheduled datagrams: {leg:?}"
    );
    assert!(
        leg.emitted >= 1_536,
        "one actual encrypted SCTP datagram per message is required: {leg:?}"
    );
    assert_eq!(leg.delivered, 1_536 * MSG_LEN, "all bytes must be reassembled");
}
/// `rtc.sctp_fast_rtx_wnd`: Pion's fast window is a burst budget, so a positive
/// value retransmits several marked chunks per trigger instead of one per MTU.
/// Only this field differs between the legs, so a mutant that drops the handoff
/// makes both legs identical and the required margin fails.
#[test]
fn positive_fast_rtx_wnd_bundles_actual_sctp_retransmission_bursts_under_real_loss() {
    let baseline = Pair::new(60_000, 0, 0).deliver_with_loss(false, 12, 1_536);
    let configured = Pair::new(60_000, 20_000, 0).deliver_with_loss(false, 12, 1_536);
    assert!(
        configured.dropped >= 32 && baseline.dropped >= 32,
        "both legs need the same actual loss pressure: baseline={baseline:?},configured={configured:?}"
    );
    assert!(
        configured.ticks + 4 <= baseline.ticks,
        "a positive source burst budget must shorten real loss recovery: default={baseline:?},configured={configured:?}"
    );
}
/// `rtc.sctp_cwnd_ca_step`: one early loss puts the association into fast
/// recovery and then congestion avoidance, where the increment is
/// `mtu.max(cwnd_ca_step)` per cwnd bytes acknowledged. Only this field differs.
#[test]
fn positive_cwnd_ca_step_actually_grows_the_congestion_window_in_congestion_avoidance() {
    let baseline = Pair::new(0, 0, 0).deliver_with_loss(true, 0, 4_096);
    let configured = Pair::new(0, 0, 50_000).deliver_with_loss(true, 0, 4_096);
    assert_eq!(baseline.dropped, 1, "one loss enters fast recovery: {baseline:?}");
    assert_eq!(configured.dropped, 1, "one loss enters fast recovery: {configured:?}");
    assert!(
        configured.ticks * 2 < baseline.ticks,
        "the configured CA increment must move actual bytes faster: default={baseline:?},configured={configured:?}"
    );
}
