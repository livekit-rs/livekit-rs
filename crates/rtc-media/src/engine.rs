//! The media engine: owns every str0m `Rtc` of one room, dispatches commands, drives
//! the `Rtc`s and runs the engine timers. Signaling lives in `signaling.rs`, media
//! forwarding and the periodic algorithms in `forward.rs`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rtc_core::config::{AudioConfig, RtcConfig as CoreRtcConfig, VideoConfig};
use rtc_core::ids::{ParticipantSid, TrackSid, TransportId};
use rtc_core::media_api::{MediaCommand, MediaEvent, PcRole, PublishedTrackSpec, TransportSpec};
use rtc_core::model::CodecSpec;
use str0m::bwe::Bitrate;
use str0m::config::{CryptoProvider, DtlsCert};
use str0m::media::Mid;
use str0m::net::{Receive, TcpType};
use str0m::{Candidate, Event, IceConnectionState, IceCreds, Input, Rtc};

use crate::audio_level::AudioLevelParams;
use crate::codecs;
use crate::downtrack::DownTrack;
use crate::error::MediaError;
use crate::net::{Datagram, IceCredentials, LocalCandidate, PacketOut, Proto};
use crate::slab::Slab;
use crate::transport::{DISCONNECT_TIMEOUT, IoCounters, NegState, Transport, recoverable};
use crate::uptrack::UpTrack;

/// Allocation, bitrate windows and target refresh cadence.
pub(crate) const TICK_INTERVAL: Duration = Duration::from_secs(1);
/// Initial str0m BWE estimate on sending transports.
const INITIAL_BWE: Bitrate = Bitrate::bps(crate::allocator::INITIAL_CAPACITY_BPS);
/// Guard against runaway event/drive cycles within one engine call.
const MAX_EVENTS_PER_RUN: usize = 1_000_000;

/// Fixed Go forwards each audio packet on arrival (a late one out of order),
/// so an upstream gap never delays the audio that follows it. str0m's frame
/// buffer would hold audio after a gap for 15 packets or 1 s waiting for a
/// retransmission. Every Opus/G.711 packet is independently decodable; waiting
/// only matters for packets that are merely reordered. 40 ms keeps upstream
/// reordering (measured with 30 ms jitter: no loss, as in Go). A complete Opus
/// packet following a proven long source outage bypasses the repair wait.
/// Other gaps cost at most 40 ms instead of the 300 ms+ concealment seen before
/// (livekit-rs-docs/evidence/2026-11-07/spec0077-m06-media-packets).
pub(crate) const AUDIO_REORDER_WAIT: Duration = Duration::from_millis(40);

pub struct MediaEngineConfig {
    pub rtc: CoreRtcConfig,
    pub audio: AudioConfig,
    pub video: VideoConfig,
    pub enabled_codecs: Vec<CodecSpec>,
    /// `(min_ms, max_ms)` for the playout-delay extension on subscriber video.
    pub playout_delay: Option<(u32, u32)>,
    /// Appended to synthesized frames for encrypted down tracks (§11).
    pub sif_trailer: Vec<u8>,
}

/// Engine counters (cumulative).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub transports: usize,
    pub uptracks: usize,
    pub downtracks: usize,
    /// Actual SubscriptionBound gauge; queued/unnegotiated downtracks do not
    /// count, while muted/paused bound subscriptions retain their lifecycle.
    pub subscribed_tracks: usize,
    /// Source media telemetry: logical authenticated RTP, not raw UDP.
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub packets_in: u64,
    pub packets_out: u64,
    pub retransmit_bytes_out: u64,
    pub retransmit_packets_out: u64,
    pub data_bytes_out: u64,
    pub data_packets_out: u64,
    /// Socket diagnostics include ICE/DTLS/SRTCP/malformed datagrams and must
    /// never be projected into source NodeStats media counters.
    pub raw_bytes_in: u64,
    pub raw_bytes_out: u64,
    pub raw_packets_in: u64,
    pub raw_packets_out: u64,
    /// NACKs received from subscribers (egress stats).
    pub nacks: u64,
    /// PLIs the engine requested from publishers.
    pub plis: u64,
    pub estimated_capacity_bps: u64,
    pub allocated_bps: u64,
    pub allocation_overshoot_bps: u64,
    pub paused_video_tracks: usize,
    pub probe_attempts: u64,
    pub probe_successes: u64,
    pub probe_failures: u64,
    pub congestion_events: u64,
}

pub struct MediaEngine {
    pub(crate) cfg: MediaEngineConfig,
    pub(crate) audio_params: AudioLevelParams,
    crypto: Arc<CryptoProvider>,
    cert: DtlsCert,
    pub(crate) out: Box<dyn PacketOut>,

    pub(crate) transports: HashMap<TransportId, Transport>,
    pub(crate) participants: HashMap<ParticipantSid, Vec<TransportId>>,

    pub(crate) ups: Slab<UpTrack>,
    pub(crate) up_by_sid: HashMap<TrackSid, usize>,
    pub(crate) codec_receivers: HashMap<TrackSid, crate::codec_receivers::CodecReceivers>,
    pub(crate) room_participant_count: usize,
    pub(crate) receiver_traffic: crate::receiver_traffic::ReceiverTraffic,
    pub(crate) up_by_mid: HashMap<(TransportId, Mid), usize>,
    /// Latest known spec per track (ExpectTrack / UpdatePublishedTrack).
    pub(crate) published: HashMap<TrackSid, PublishedTrackSpec>,

    pub(crate) downs: Slab<DownTrack>,
    pub(crate) down_by_key: HashMap<(ParticipantSid, TrackSid), usize>,
    pub(crate) down_by_mid: HashMap<(TransportId, Mid), usize>,

    pub(crate) events: Vec<MediaEvent>,
    pub(crate) ufrag_changes: Vec<(TransportId, String)>,
    pub(crate) queue: VecDeque<(TransportId, Event)>,
    pub(crate) dirty: Vec<TransportId>,
    pub(crate) io: IoCounters,
    pub(crate) retired_media_traffic: str0m::MediaTraffic,
    pub(crate) accepted_data_out: (u64, u64),

    pub(crate) data_fanout: Vec<crate::data_fanout::Batch>,
    pub(crate) data_replays: std::collections::HashSet<(ParticipantSid, TransportId)>,
    pub(crate) data_clock: Instant,
    pub(crate) data_admissions: std::collections::HashMap<u64, bool>,
    pub(crate) next_data_admission: u64,
    pub(crate) next_data_write: u64,
    pub(crate) next_tick_at: Instant,
    pub(crate) next_level_at: Instant,
    pub(crate) next_quality_at: Instant,
}

impl MediaEngine {
    pub fn new(
        config: MediaEngineConfig,
        out: Box<dyn PacketOut>,
        now: Instant,
    ) -> Result<MediaEngine, MediaError> {
        let cert = rtc_crypto::dtls_cert().map_err(|e| MediaError::Certificate(e.to_string()))?;
        Ok(Self::with_cert(config, out, cert, now))
    }

    /// Construct with an externally provided DTLS certificate (shared by all
    /// transports of this engine).
    pub fn with_cert(
        config: MediaEngineConfig,
        out: Box<dyn PacketOut>,
        cert: DtlsCert,
        now: Instant,
    ) -> MediaEngine {
        let audio_params = AudioLevelParams::from(&config.audio);
        let level_interval = audio_params.update_interval;
        MediaEngine {
            cfg: config,
            audio_params,
            crypto: Arc::new(rtc_crypto::provider()),
            cert,
            out,
            transports: HashMap::new(),
            participants: HashMap::new(),
            ups: Slab::new(),
            up_by_sid: HashMap::new(),
            codec_receivers: HashMap::new(),
            room_participant_count: 0,
            receiver_traffic: Default::default(),
            up_by_mid: HashMap::new(),
            published: HashMap::new(),
            downs: Slab::new(),
            down_by_key: HashMap::new(),
            down_by_mid: HashMap::new(),
            events: Vec::new(),
            ufrag_changes: Vec::new(),
            queue: VecDeque::new(),
            dirty: Vec::new(),
            io: IoCounters::default(),
            retired_media_traffic: Default::default(),
            accepted_data_out: (0, 0),
            data_fanout: Vec::new(),
            data_replays: std::collections::HashSet::new(),
            data_clock: now,
            data_admissions: std::collections::HashMap::new(),
            next_data_admission: 0,
            next_data_write: 0,
            next_tick_at: now + TICK_INTERVAL,
            next_level_at: now + level_interval,
            next_quality_at: now + crate::quality::QUALITY_WINDOW,
        }
    }

    /// Resolved room baseline before each transport applies its client policy.
    /// This observes configuration, not packet transmission or receiver decoding.
    pub fn playout_delay(&self) -> Option<(u32, u32)> {
        self.cfg.playout_delay
    }

    /// CreateTransport: the runtime obtained credentials and candidates from its socket
    /// layer first.
    pub fn create_transport(
        &mut self,
        spec: TransportSpec,
        creds: IceCredentials,
        candidates: Vec<LocalCandidate>,
        now: Instant,
    ) {
        self.create_transport_with_srflx(spec, creds, candidates, false, now);
    }

    /// Create a transport whose socket layer will report one server-reflexive
    /// gathering completion for every ICE generation, even on probe failure.
    pub fn create_transport_with_srflx(
        &mut self,
        spec: TransportSpec,
        creds: IceCredentials,
        candidates: Vec<LocalCandidate>,
        gather_srflx: bool,
        now: Instant,
    ) {
        if self.transports.contains_key(&spec.id) {
            tracing::warn!(transport = %spec.id, "transport already exists");
            return;
        }
        let role = spec.role;
        let has_bwe = self.cfg.rtc.congestion_control.enabled
            && matches!(role, PcRole::Subscriber | PcRole::Single);
        let rtc = self.build_rtc(&spec, &creds, now);
        let tid = spec.id;
        let participant = spec.participant.clone();
        let mut t = Transport::new(spec, rtc, creds.ufrag.clone(), has_bwe, now);
        t.gather_srflx = gather_srflx && !t.spec.ice_lite;
        t.srflx_pending = t.gather_srflx;
        t.owned_out = self.out.for_transport(tid);
        if matches!(role, PcRole::Subscriber | PcRole::Single)
            && self.cfg.rtc.congestion_control.effective_feedback_mode()
                != rtc_core::config::FeedbackMode::NativeSendSide
        {
            t.remote_feedback = Some(crate::feedback::Feedback::new(
                &self.cfg.rtc.congestion_control,
                now,
            ));
        }
        for c in candidates {
            let built = match c.proto {
                Proto::Udp => Candidate::builder()
                    .udp()
                    .mapped_host(c.addr, c.base)
                    .build(),
                Proto::Tcp => Candidate::builder()
                    .tcp()
                    .mapped_host(c.addr, c.base)
                    .tcptype(TcpType::Passive)
                    .build(),
            };
            match built {
                Ok(cand) => {
                    if let Some(accepted) = t.rtc.add_local_candidate(cand) {
                        t.candidates.push(accepted.clone());
                    }
                    // Every mutation is followed by a drain (the drive below).
                }
                Err(e) => {
                    tracing::warn!(transport = %tid, addr = %c.addr, error = %e, "invalid local candidate")
                }
            }
        }
        if let Some(previous) = self.transports.insert(tid, t) {
            self.retired_media_traffic += previous.rtc.media_traffic_stats();
        }
        self.participants
            .entry(participant.clone())
            .or_default()
            .push(tid);
        self.mark_dirty(tid);

        // Down tracks subscribed before this transport existed.
        if matches!(role, PcRole::Subscriber | PcRole::Single) {
            let waiting: Vec<usize> = self
                .downs
                .iter()
                .filter(|(_, d)| d.transport.is_none() && d.spec.subscriber == participant)
                .map(|(i, _)| i)
                .collect();
            for di in waiting {
                self.attach_down(di, now);
            }
        }
        // Subscriber-primary dual PC: server data channels go into an immediate first
        // offer (signaling.md §7).
        if role == PcRole::Subscriber
            && self
                .transports
                .get(&tid)
                .is_some_and(|t| t.spec.server_data_channels)
        {
            self.negotiate(tid, true, now);
        }
        self.run(now);
    }

    fn build_rtc(&self, spec: &TransportSpec, creds: &IceCredentials, now: Instant) -> Rtc {
        let rtc_cfg = &self.cfg.rtc;
        // SPEC-0074 C20: WARP's SNAP and authenticated DTLS-in-STUN consumers. Upstream gates `enableWarp` on
        // `!useOneShotSignallingMode` (roommanager.go:487) and then calls
        // `se.EnableSctpSnap(true)` (transport.go:389-393), which is what makes
        // pion put `a=sctp-init` in offers and answers. It logs `Debugw("enable
        // warp")` at the same point; the message is reproduced here.
        let warp = rtc_cfg.enable_warp && !spec.one_shot;
        if warp {
            tracing::debug!(transport = %spec.id, "enable warp");
        }
        let mut cfg = Rtc::builder()
            .set_crypto_provider(self.crypto.clone())
            .set_dtls_cert(self.cert.clone())
            .set_local_ice_credentials(IceCreds {
                ufrag: creds.ufrag.clone(),
                pass: creds.pwd.clone(),
            })
            .set_ice_lite(spec.ice_lite)
            .set_ice_acceptance_min_waits(rtc_cfg.ice_acceptance_min_waits)
            .set_snap_enabled(warp)
            .set_sped_enabled(warp)
            .set_stats_interval(Some(Duration::from_secs(1)))
            .set_sctp_advertised_max_message_size(65535)
            .set_sctp_transport_options(str0m::SctpTransportOptions {
                min_cwnd: positive_sctp_u32(rtc_cfg.sctp.min_cwnd),
                fast_rtx_wnd: positive_sctp_u32(rtc_cfg.sctp.fast_rtx_wnd),
                source_partial_reliability: true,
                cwnd_ca_step: positive_sctp_u32(rtc_cfg.sctp.cwnd_ca_step),
            })
            .set_reordering_timeout_audio(Some(AUDIO_REORDER_WAIT))
            .set_resume_opus_after_outage(true)
            // Pion answers in the offerer's codec order (mediaengine.go negotiated codecs).
            .set_answer_in_offer_order(true)
            .set_send_buffer_audio(rtc_cfg.packet_buffer_size_audio.max(1))
            .set_send_buffer_video(rtc_cfg.packet_buffer_size_video.max(1));
        cfg = codecs::configure_codecs(
            cfg,
            &self.cfg.enabled_codecs,
            spec.role,
            spec.client_supports_red,
        );
        cfg = codecs::configure_extensions(
            cfg,
            spec.role,
            spec.effective_playout_delay(self.cfg.playout_delay)
                .is_some(),
        );
        let sending = matches!(spec.role, PcRole::Subscriber | PcRole::Single);
        let mode = rtc_cfg.congestion_control.effective_feedback_mode();
        if spec.role == PcRole::Subscriber && mode != rtc_core::config::FeedbackMode::NativeSendSide
        {
            for payload in cfg.codec_config().iter_mut() {
                payload.set_fb_transport_cc(
                    mode == rtc_core::config::FeedbackMode::InterceptorEstimate
                        && payload.spec().codec.is_video(),
                );
                payload.set_fb_remb(
                    mode == rtc_core::config::FeedbackMode::ReceiverEstimate
                        && payload.spec().codec.is_video(),
                );
            }
            let kept: Vec<_> = cfg
                .extension_map()
                .iter()
                .filter(|(_, ext)| {
                    if mode == rtc_core::config::FeedbackMode::ReceiverEstimate {
                        **ext != str0m::rtp::Extension::TransportSequenceNumber
                    } else {
                        **ext != str0m::rtp::Extension::AbsoluteSendTime
                    }
                })
                .map(|(id, ext)| (id, ext.clone()))
                .collect();
            let mut extensions = str0m::rtp::ExtensionMap::empty();
            for (id, ext) in kept {
                extensions.set(id, ext);
            }
            cfg = cfg.set_extension_map(extensions);
        }
        if mode != rtc_core::config::FeedbackMode::NativeSendSide {
            // Fixed config.go audio has neither TWCC nor ABS (all directions,
            // including the Single union). Do not erase the video extensions.
            cfg.extension_map().set_media_applicability(
                str0m::rtp::Extension::TransportSequenceNumber,
                false,
                true,
            );
            cfg.extension_map().set_media_applicability(
                str0m::rtp::Extension::AbsoluteSendTime,
                false,
                true,
            );
            for payload in cfg
                .codec_config()
                .iter_mut()
                .filter(|p| p.spec().codec.is_audio())
            {
                payload.set_fb_transport_cc(false);
                payload.set_fb_remb(false);
            }
        }
        if sending && mode == rtc_core::config::FeedbackMode::NativeSendSide {
            cfg = cfg.enable_bwe(Some(INITIAL_BWE));
        }
        // Keep actual authenticated primary-RTP publication registration for
        // all publisher directions, independently of feedback estimate source.
        cfg = cfg.enable_raw_packets(
            spec.one_shot || matches!(spec.role, PcRole::Publisher | PcRole::Single),
        );
        let mut rtc = cfg.build(now);
        // Pion advertises the same local preference for all UDP host and
        // server-reflexive candidates, including those gathered on restart.
        rtc.direct_api()
            .set_ice_local_preference(|candidate: &Candidate, same_kind| {
                if matches!(
                    candidate.kind(),
                    str0m::CandidateKind::Host | str0m::CandidateKind::ServerReflexive
                ) && candidate.proto() == str0m::net::Protocol::Udp
                {
                    u32::from(u16::MAX)
                } else {
                    str0m::ice::default_local_preference(candidate, same_kind)
                }
            });
        if matches!(spec.role, PcRole::Publisher | PcRole::Single)
            && mode != rtc_core::config::FeedbackMode::NativeSendSide
        {
            rtc.direct_api().enable_livekit_publisher_twcc();
        }
        rtc.set_ice_selected_receive_timeout(Some(Duration::from_secs(15)));
        rtc.set_video_packet_events(true);
        rtc.set_all_video_packet_events(self.cfg.video.stream_trackers.uses_frame_tracking());
        rtc.set_rtp_stream_restart_detection(rtc_cfg.enable_rtp_stream_restart_detection);
        rtc.set_forwarding_timing_enabled(rtc_cfg.forward_stats.is_some());
        // Pion RTCP readers unmarshal the entire authenticated compound before
        // delivering any REMB/NACK/TWCC, in every feedback mode and direction.
        rtc.direct_api()
            .set_rtcp_admission(Some(crate::pion_gcc::admit_compound));
        // Diagnostic collection is registration-local and default-off. Select
        // it once from the application logger, never from native callsite state.
        rtc.bwe()
            .set_incoming_twcc_observations(tracing::enabled!(tracing::Level::DEBUG));
        // The server's ProbeController owns actual-demand sustained discovery.
        // Keep GCC/TWCC/media pacing, but do not run a second short-train policy.
        rtc.bwe().set_automatic_probing(false);
        if sending && mode != rtc_core::config::FeedbackMode::NativeSendSide {
            rtc.direct_api().set_remote_feedback_meter(true);
            rtc.direct_api()
                .set_send_transport_cc(mode == rtc_core::config::FeedbackMode::InterceptorEstimate);
            if mode == rtc_core::config::FeedbackMode::InterceptorEstimate {
                rtc.direct_api().set_send_side_feedback(true);
            }
        }
        rtc
    }

    /// All MediaCommand variants except CreateTransport (logged and ignored).
    pub fn handle_command(&mut self, cmd: MediaCommand, now: Instant) {
        self.data_clock = now;
        match cmd {
            MediaCommand::PrepareOneShotOffer { transport, sdp, id } => {
                self.prepare_one_shot_offer(transport, sdp, id, now)
            }
            MediaCommand::FinalizeOneShotAnswer { transport, id } => {
                self.finalize_one_shot_answer(transport, id, now)
            }
            MediaCommand::CreateTransport(spec) => {
                tracing::warn!(transport = %spec.id, "CreateTransport must go through create_transport(); ignored");
            }
            MediaCommand::CloseTransport(tid) => self.close_transport(tid, now),
            MediaCommand::SetSignalingRtt {
                participant,
                rtt_ms,
            } => {
                for transport in self.participants.get(&participant).into_iter().flatten() {
                    if let Some(t) = self.transports.get_mut(transport) {
                        t.signaling_rtt_ms = rtt_ms;
                    }
                }
            }
            MediaCommand::SetIcePreference {
                transport,
                preference,
            } => self.set_ice_preference(transport, preference),
            MediaCommand::RemoteDescription {
                transport,
                kind,
                sdp,
                id,
            } => self.on_remote_description(transport, kind, sdp, id, now),
            MediaCommand::RemoteCandidate {
                transport,
                candidate,
            } => self.on_remote_candidate(transport, candidate),
            MediaCommand::BeginTransportRecovery {
                participant,
                recovery_epoch,
            } => {
                for tid in self.transports_of(&participant) {
                    if let Some(t) = self.transports.get_mut(&tid) {
                        t.recovery_epoch = recovery_epoch;
                        // Signal reconnection alone does not require a publisher
                        // offer. Fence queued events for every PC, but wait for new
                        // credentials only on PCs with real failure/loss evidence.
                        // An unused publisher never had a remote offer; a healthy
                        // publisher retains its authenticated selected generation.
                        if t.failed
                            || t.disconnected_since.is_some()
                            || t.recovery_pending.is_some()
                        {
                            t.recovery_pending = Some(crate::transport::RecoveryPending {
                                started: now,
                                deadline: now + crate::transport::ANSWER_TIMEOUT,
                                sdp_applied: false,
                            });
                            t.selected_ice_last_receive = None;
                            t.disconnected_since = None;
                            t.tcp_ice_deadline = None;
                        }
                    }
                }
            }
            MediaCommand::ArmShortConnectionReset { participant } => {
                for tid in self.transports_of(&participant) {
                    if let Some(t) = self.transports.get_mut(&tid) {
                        t.reset_established_on_ice_restart = true;
                    }
                }
            }
            MediaCommand::IceRestart { transport } => {
                // Fixed Go ParticipantImpl.ICERestart restarts every published
                // track: MediaTrackReceiver.Restart expects the HIGH layer again.
                if let Some(participant) = self
                    .transports
                    .get(&transport)
                    .map(|t| t.spec.participant.clone())
                {
                    for (_, up) in self.ups.iter_mut() {
                        if up.publisher == participant {
                            let high = crate::layers::spatial_for_quality(
                                &up.layers,
                                rtc_core::model::VideoQuality::High,
                            );
                            up.set_max_expected_spatial(high);
                        }
                    }
                }
                if let Some(t) = self.transports.get_mut(&transport) {
                    if t.role() == PcRole::Subscriber {
                        t.ice_restart_pending = true;
                        t.ice_restart_requested = true;
                        t.selected_ice_last_receive = None;
                        t.disconnected_since = None;
                        t.tcp_ice_deadline = None;
                        self.negotiate(transport, true, now);
                    } else {
                        tracing::debug!(transport = %transport, "ice restart ignored: client offers on this transport");
                    }
                }
            }
            MediaCommand::ExpectTrack { transport, track } => {
                self.published.insert(track.sid.clone(), track.clone());
                if let Some(t) = self.transports.get_mut(&transport) {
                    t.expected.retain(|e| e.sid != track.sid);
                    t.expected.push(track);
                }
            }
            MediaCommand::BindTrack {
                transport,
                mid,
                track_sid,
            } => self.bind_track(transport, &mid, track_sid, now),
            MediaCommand::UpdatePublishedTrack { track } => self.update_published(track, now),
            MediaCommand::UnpublishTrack { track_sid } => self.unpublish(&track_sid, now),
            MediaCommand::SetPublisherMuted { track_sid, muted } => {
                self.set_publisher_muted(&track_sid, muted, now)
            }
            MediaCommand::Subscribe(spec) => self.subscribe(spec, now),
            MediaCommand::Unsubscribe {
                subscriber,
                track_sid,
            } => {
                if let Some(di) = self
                    .down_by_key
                    .get(&(subscriber.clone(), track_sid.clone()))
                    .copied()
                {
                    self.remove_down(di, now);
                } else {
                    self.events.push(MediaEvent::SubscriptionRemoved {
                        subscriber,
                        track_sid,
                    });
                }
            }
            MediaCommand::UpdateTrackSettings {
                subscriber,
                track_sid,
                settings,
            } => {
                if let Some(di) = self.down_by_key.get(&(subscriber, track_sid)).copied() {
                    let applied = self
                        .downs
                        .get_mut(di)
                        .is_some_and(|d| d.update_settings(settings, now));
                    if applied {
                        self.on_settings_applied(di, now);
                    }
                }
            }
            MediaCommand::SendDataFanout { id, writes } => {
                self.enqueue_data_fanout(Some(id), writes)
            }
            MediaCommand::DataWriteAdmitted { request, write } => {
                self.data_admissions.insert(request, write);
            }
            MediaCommand::ReplayJoiningReliable {
                participant,
                transport,
                writes,
            } => self.enqueue_reliable_replay(participant, transport, writes),
            MediaCommand::ForwardData { source, writes } => self.forward_data(source, writes),
            MediaCommand::SendData { to, kind, payload } => self.send_data(&to, kind, &payload),
            MediaCommand::SendDataTrack { to, payload } => {
                self.send_data_track_at(&to, &payload, now)
            }
            MediaCommand::SetBandwidthOverride { participant, bps } => {
                let tids = self
                    .participants
                    .get(&participant)
                    .cloned()
                    .unwrap_or_default();
                for tid in tids {
                    if let Some(t) = self.transports.get_mut(&tid)
                        && t.sends_media()
                    {
                        t.bw_override_bps = bps;
                    }
                }
                // Re-allocate right away.
                self.next_tick_at = now;
            }
            MediaCommand::RemoveParticipant(p) => self.remove_participant(&p, now),
        }
        self.run(now);
    }

    pub fn handle_datagram(&mut self, d: Datagram<'_>) {
        tracing::trace!(transport = %d.transport, proto = ?d.proto, source = %d.source,
            destination = %d.destination, len = d.data.len(), first_octet = d.data.first(),
            "media datagram received");
        let now = d.received_at;
        self.io.bytes_in += d.data.len() as u64;
        self.io.packets_in += 1;
        let Some(t) = self.transports.get_mut(&d.transport) else {
            return;
        };
        if !t.rtc.is_alive() {
            return;
        }
        if t.spec.one_shot {
            t.ingress_metrics.wire(d.data, d.received_at);
        }
        let recv = match Receive::new(d.proto.to_str0m(), d.source, d.destination, d.data) {
            Ok(r) => r,
            Err(e) => {
                tracing::trace!(transport = %d.transport, error = %e, "unparseable datagram dropped");
                return;
            }
        };
        t.rtc_clock = t.rtc_clock.max(now);
        if let Err(e) = t.rtc.handle_input(Input::Receive(now, recv)) {
            if recoverable(&e) {
                tracing::debug!(transport = %d.transport, error = %e, "rtc input: frame dropped");
            } else {
                tracing::warn!(transport = %d.transport, error = %e, "rtc input failed, disconnecting");
                t.rtc.disconnect();
            }
        }
        self.mark_dirty(d.transport);
        self.run(now);
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        self.bitrate_report_timers(now);
        self.poll_stream_trackers(now);
        // str0m timers
        let due: Vec<TransportId> = self
            .transports
            .values()
            .filter(|t| t.next_timeout <= now && t.rtc.is_alive())
            .map(|t| t.id())
            .collect();
        for tid in due {
            let Some(t) = self.transports.get_mut(&tid) else {
                continue;
            };
            t.rtc_clock = t.rtc_clock.max(now);
            if let Err(e) = t.rtc.handle_input(Input::Timeout(now)) {
                if recoverable(&e) {
                    tracing::debug!(transport = %tid, error = %e, "rtc timeout: frame dropped");
                } else {
                    tracing::warn!(transport = %tid, error = %e, "rtc timeout failed, disconnecting");
                    t.rtc.disconnect();
                }
            }
            self.mark_dirty(tid);
        }
        self.run(now);

        self.transport_timers(now);
        self.down_timers(now);
        self.dynacast_timers(now);
        if self.next_tick_at <= now {
            self.next_tick_at = now + TICK_INTERVAL;
            self.periodic_tick(now);
        }
        if !self.audio_params.update_interval.is_zero() && self.next_level_at <= now {
            self.next_level_at = now + self.audio_params.update_interval;
            self.emit_audio_levels(now);
        }
        if self.next_quality_at <= now {
            self.next_quality_at = now + crate::quality::QUALITY_WINDOW;
            self.emit_quality(now);
        }
        self.run(now);
    }

    pub fn poll_timeout(&self) -> Option<Instant> {
        let mut min: Option<Instant> = None;
        let mut take = |t: Option<Instant>| {
            if let Some(t) = t {
                min = Some(min.map_or(t, |m| m.min(t)));
            }
        };
        take(self.data_write_deadline());
        take(self.receiver_traffic_deadline());
        for t in self.transports.values() {
            if t.rtc.is_alive() {
                take(Some(t.next_timeout));
            }
            take(t.debounce_at);
            take(t.recovery_pending.map(|pending| pending.deadline));
            if !t.negotiation_failed {
                take(t.answer_deadline);
            }
            if !t.failed {
                take(
                    t.remote_closed_at
                        .map(|at| at + crate::ice_fallback::SOURCE_ICE_FAILED_TOTAL),
                );
                take(Self::next_initial_ice_check(t));
                take(t.tcp_ice_deadline);
                take(
                    Self::selected_ice_failure_deadline(t)
                        .or_else(|| t.disconnected_since.map(|s| s + DISCONNECT_TIMEOUT)),
                );
            }
        }
        for (_, d) in self.downs.iter() {
            take(d.next_deadline());
        }
        for (_, u) in self.ups.iter() {
            take(u.dynacast.as_ref().and_then(|d| d.next_deadline()));
            take(u.next_bitrate_report());
        }
        if !self.transports.is_empty() {
            take(Some(self.next_tick_at));
            take(Some(self.next_quality_at));
            if !self.audio_params.update_interval.is_zero()
                && self.ups.iter().any(|(_, u)| u.audio_level.is_some())
            {
                take(Some(self.next_level_at));
            }
        }
        min
    }

    pub fn drain_events(&mut self, out: &mut Vec<MediaEvent>) {
        for mut event in std::mem::take(&mut self.events) {
            if let MediaEvent::SubscribedQuality {
                track_sid, codecs, ..
            } = &mut event
            {
                if let Some(combined) = self.codec_quality_snapshot(track_sid) {
                    *codecs = combined;
                }
            }
            out.push(event);
        }
    }

    /// Current owner room count, read before every room/media command. Codec
    /// regression evaluates it only when a new real receiver starts.
    pub fn set_room_participant_count(&mut self, count: usize) {
        self.room_participant_count = count;
    }

    pub fn drain_ufrag_changes(&mut self, out: &mut Vec<(TransportId, String)>) {
        out.append(&mut self.ufrag_changes);
    }

    pub fn transports_of(&self, participant: &ParticipantSid) -> Vec<TransportId> {
        self.participants
            .get(participant)
            .cloned()
            .unwrap_or_default()
    }

    pub fn has_transport(&self, id: TransportId) -> bool {
        self.transports.contains_key(&id)
    }

    pub fn stats(&self) -> EngineStats {
        let traffic = self
            .transports
            .values()
            .fold(self.retired_media_traffic, |mut total, t| {
                total += t.rtc.media_traffic_stats();
                total
            });
        EngineStats {
            transports: self.transports.len(),
            uptracks: self.ups.len(),
            downtracks: self.downs.len(),
            subscribed_tracks: self.downs.iter().filter(|(_, d)| d.bound_emitted).count(),
            bytes_in: self.receiver_traffic.accepted.bytes_in,
            bytes_out: traffic.bytes_out.saturating_add(self.accepted_data_out.0),
            packets_in: self.receiver_traffic.accepted.packets_in,
            packets_out: traffic.packets_out.saturating_add(self.accepted_data_out.1),
            retransmit_bytes_out: traffic.retransmit_bytes_out,
            retransmit_packets_out: traffic.retransmit_packets_out,
            data_bytes_out: self.accepted_data_out.0,
            data_packets_out: self.accepted_data_out.1,
            raw_bytes_in: self.io.bytes_in,
            raw_bytes_out: self.io.bytes_out,
            raw_packets_in: self.io.packets_in,
            raw_packets_out: self.io.packets_out,
            nacks: self
                .receiver_traffic
                .accepted
                .nacks_sent
                .saturating_add(traffic.nacks_received),
            plis: self.io.plis,
            estimated_capacity_bps: self
                .transports
                .values()
                .filter(|t| t.has_bwe)
                .filter_map(|t| t.capacity())
                .sum(),
            allocated_bps: self.transports.values().map(|t| t.allocated_bps).sum(),
            allocation_overshoot_bps: self
                .transports
                .values()
                .map(|t| t.allocation_overshoot_bps)
                .sum(),
            paused_video_tracks: self
                .transports
                .values()
                .map(|t| t.paused_video_tracks)
                .sum(),
            probe_attempts: self
                .transports
                .values()
                .map(|t| t.probes.counters.attempts)
                .sum(),
            probe_successes: self
                .transports
                .values()
                .map(|t| t.probes.counters.successes)
                .sum(),
            probe_failures: self
                .transports
                .values()
                .map(|t| t.probes.counters.failures)
                .sum(),
            congestion_events: self
                .transports
                .values()
                .map(|t| t.probes.counters.congestion_events)
                .sum(),
        }
    }

    // ------------------------------------------------------------------
    // run loop
    // ------------------------------------------------------------------

    pub(crate) fn mark_dirty(&mut self, tid: TransportId) {
        if !self.dirty.contains(&tid) {
            self.dirty.push(tid);
        }
    }

    fn drive(&mut self, tid: TransportId, now: Instant) {
        let Self {
            transports,
            out,
            io,
            queue,
            ..
        } = self;
        let changed = if let Some(t) = transports.get_mut(&tid) {
            t.drive(now, &**out, io, queue);
            std::mem::take(&mut t.remote_allocation_dirty)
        } else {
            false
        };
        if changed {
            self.allocate_transport(tid, now);
        }
    }

    /// Drain every touched `Rtc` to rest, then process one queued str0m event (which
    /// may mutate other `Rtc`s and mark them dirty), repeat. Every mutation of an
    /// `Rtc` is therefore followed by a full drain before the next event is handled.
    pub(crate) fn run(&mut self, now: Instant) {
        self.data_clock = now;
        let mut handled = 0usize;
        loop {
            while let Some(tid) = self.dirty.pop() {
                self.drive(tid, now);
            }
            self.update_tcp_ice_timers(now);
            self.update_selected_ice_liveness(now);
            self.poll_receiver_traffic(now);
            self.poll_subscriber_rtt();
            let Some((tid, ev)) = self.queue.pop_front() else {
                if self.progress_data_fanout(now) {
                    continue;
                }
                break;
            };
            self.handle_event(tid, ev, now);
            handled += 1;
            if handled > MAX_EVENTS_PER_RUN {
                tracing::warn!("media engine event loop did not settle");
                break;
            }
        }
        self.check_dead_transports();
        self.flush_requirements();
    }

    // Actual event consumer, separated only to test observer/action propagation.
    // A supplied snapshot is native in production; regression fixtures label
    // synthetic matched observations explicitly and do not claim wire proof.
    pub(crate) fn on_peer_stats(
        &mut self,
        tid: TransportId,
        p: str0m::stats::PeerStats,
        snapshot: Option<str0m::bwe::BweSnapshot>,
        now: Instant,
    ) {
        let mut reallocate = false;
        if let Some(t) = self.transports.get_mut(&tid) {
            let pair_rtt = p
                .selected_candidate_pair
                .as_ref()
                .and_then(|c| c.current_round_trip_time);
            if let Some(rtt) = p.rtt.or(pair_rtt) {
                t.rtt = Some(rtt);
            }
            if t.has_bwe {
                if let Some(remote) = &mut t.remote_feedback {
                    // Official Remote policy consumes ICE selected-pair RTT, not TWCC report age.
                    remote.rtt(pair_rtt, now);
                    return;
                }
                let capacity_before = t.capacity();
                let congestion_before = t.probes.counters.congestion_events;
                if let Some(snapshot) = snapshot {
                    t.probes.observe_native(snapshot, now);
                }
                // Both observers always run: a safety no-op does not erase
                // a native budget update or already pending RTC cancellation.
                let safety_reallocated =
                    t.probes
                        .on_feedback(p.egress_loss_fraction, p.rtt, p.bytes_tx, now);
                let capacity_after = t.capacity();
                let native_control_pending = t.probes.has_pending_control();
                reallocate = capacity_before != capacity_after
                    || safety_reallocated
                    || native_control_pending;
                tracing::debug!(transport = %tid, stats_rtt = ?p.rtt,
                    rolling_egress_loss = ?p.egress_loss_fraction,
                    native_feedback = ?snapshot.and_then(|s| s.latest_feedback),
                    native_cause = ?snapshot.map(|s| s.cause),
                    capacity_before, capacity_after,
                    congestion_before, congestion_after = t.probes.counters.congestion_events,
                    safety_reallocated, native_control_pending, reallocate,
                    "transport safety sample inputs and result");
            }
        }
        if reallocate {
            self.allocate_transport(tid, now);
        }
    }

    pub(crate) fn handle_event(&mut self, tid: TransportId, ev: Event, now: Instant) {
        let connected =
            matches!(&ev,Event::IceConnectionStateChange(state) if state.is_connected());
        self.observe_ice_path(tid, connected, now);
        match ev {
            Event::RawPacket(packet) => {
                if let str0m::rtp::RawPacket::RtpRx(header, payload) = *packet {
                    self.observe_publication_rtp(tid, &header, &payload);
                    self.record_one_shot_rtp(tid, header, &payload);
                }
            }
            Event::ConnectionStarted => {
                if let Some(t) = self.transports.get_mut(&tid)
                    && !t.established_reported
                {
                    t.established_reported = true;
                    self.events.push(MediaEvent::TransportEstablished {
                        participant: t.spec.participant.clone(),
                        transport: tid,
                    });
                }
            }
            Event::Connected => {
                let mut initial_connection = false;
                if let Some(t) = self.transports.get_mut(&tid) {
                    t.connected = true;
                    initial_connection = t.record_data_connection_proof();
                    t.disconnected_since = None;
                    let participant = t.spec.participant.clone();
                    self.events.push(MediaEvent::TransportConnected {
                        participant,
                        transport: tid,
                    });
                }
                if initial_connection {
                    self.maybe_report_fully_established(tid);
                }
            }
            Event::IceConnectionStateChange(state) => {
                if let Some(t) = self.transports.get_mut(&tid) {
                    match state {
                        IceConnectionState::Disconnected if t.ice_first_connected_at.is_some() => {
                            // Only a formerly connected PC has a recovery window.
                            t.disconnected_since.get_or_insert(now);
                        }
                        IceConnectionState::Connected | IceConnectionState::Completed => {
                            t.disconnected_since = None;
                            // str0m Connected is the initial DTLS event. After a real
                            // ICE restart, an authenticated nominated ICE connection
                            // plus the still-connected DTLS/session is the new PC proof.
                            if t.rtc.is_connected() {
                                t.record_data_connection_proof();
                            }
                        }
                        _ => {}
                    }
                }
                // Source only initial PC Connected notifies fully-established.
                // A post-reset connection restores the proof; it does not replay
                // until a subsequent real channel-open notification.
            }
            Event::MediaAdded(m) => self.on_media_added(tid, m.mid),
            Event::MediaChanged(c) => {
                if !c.direction.is_receiving() {
                    self.report_remote_stopped(tid, c.mid);
                }
            }
            Event::MediaPacket(data) => {
                let frame = self.up_by_mid.get(&(tid, data.mid)).and_then(|&ui| {
                    self.ups
                        .get(ui)?
                        .frame_tracker_spatial(&data)
                        .map(|spatial| {
                            (
                                ui,
                                spatial,
                                data.packet.header.marker,
                                data.packet.header.timestamp,
                                data.packet.timestamp,
                            )
                        })
                });
                if matches!(
                    data.params.spec().codec,
                    str0m::format::Codec::Av1 | str0m::format::Codec::Vp9
                ) {
                    self.observe_temporal_packet(tid, &data);
                    self.on_av1_packet(tid, data, now);
                }
                // Descriptor-driven SVC can claim the tracker on this very
                // packet. Its availability takes precedence over frame mode.
                if let Some((ui, spatial, marker, timestamp, received)) = frame
                    && let Some(up) = self.ups.get_mut(ui)
                {
                    up.observe_layer_frame(spatial, marker, timestamp, received);
                }
            }
            Event::MediaData(data) => self.on_media_data(tid, data, now),
            Event::KeyframeRequest(req) => self.on_keyframe_request(tid, req.mid, now),
            Event::ChannelOpen(id, label) => self.on_channel_open_at(tid, id, label, now),
            Event::ChannelData(d) => self.on_channel_data(tid, d),
            Event::ChannelClose(id) => {
                if let Some(t) = self.transports.get_mut(&tid) {
                    // Record only a real formerly-open data-packet writer. A
                    // close itself is not an error callback; the next actual
                    // attempted write observes the closed-writer result.
                    let kind = t.channels.iter().find(|c| c.id == id).and_then(|c| c.kind);
                    t.channels.retain(|c| c.id != id);
                    t.ordinary_data_writers.remove(&id);
                    if t.data_track_writer
                        .as_ref()
                        .is_some_and(|(writer_id, _)| *writer_id == id)
                    {
                        t.data_track_writer = None;
                    }
                    if let Some(kind) = kind
                        && t.channel_for(kind).is_none()
                    {
                        t.closed_data_kinds[crate::transport::Transport::data_kind_index(kind)] =
                            true;
                    }
                }
            }
            Event::MediaIngressStats(s) => self.on_ingress_stats(tid, s, now),
            Event::MediaEgressStats(s) => self.on_egress_stats(tid, s, now),
            Event::PeerStats(p) => {
                let snapshot = self
                    .transports
                    .get_mut(&tid)
                    .filter(|t| t.has_bwe)
                    .and_then(|t| t.rtc.bwe().snapshot());
                self.on_peer_stats(tid, p, snapshot, now);
            }
            Event::EgressBitrateEstimate(kind) => self.on_bwe(tid, kind, now),
            Event::SendSideFeedback(event) => self.on_interceptor_input(tid, event),
            Event::StreamPaused(p) => {
                if p.paused
                    && let Some(&ui) = self.up_by_mid.get(&(tid, p.mid))
                    && let Some(u) = self.ups.get_mut(ui)
                {
                    u.mark_paused(p.rid);
                }
            }
            Event::SenderFeedback(f) => {
                if let Some(&ui) = self.up_by_mid.get(&(tid, f.mid))
                    && let Some(u) = self.ups.get_mut(ui)
                {
                    u.scorer.mark_rtcp(now);
                    if let Some(layer) = u.one_dd_layer(&u.mime, f.rid) {
                        u.one_dd.report(layer, f.sender_info);
                    }
                }
            }
            Event::Closed => self.remote_closed(tid, now),
            _ => {}
        }
    }

    // ------------------------------------------------------------------
    // transport lifecycle
    // ------------------------------------------------------------------

    /// str0m reports a received DTLS close_notify or a lost SCTP association as
    /// `Event::Closed`. Fixed pion moves only the DTLS transport to closed: the
    /// PeerConnection stays connected, also while an ICE restart is pending, and
    /// `handleConnectionFailed` runs when ICE fails (selected pair silent for
    /// 10s + 5s, transport.go:81-82). A PC that never connected still fails now.
    fn remote_closed(&mut self, tid: TransportId, now: Instant) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if t.failed || t.remote_closed_at.is_some() {
            return;
        }
        if t.ice_first_connected_at.is_some() {
            t.remote_closed_at = Some(now);
            tracing::debug!(transport = ?tid, "remote closed DTLS/SCTP; waiting for ICE failure");
        } else {
            self.fail_transport(tid, "closed");
        }
    }

    pub(crate) fn fail_transport(&mut self, tid: TransportId, reason: &'static str) {
        self.observe_ice_path(tid, false, self.data_clock);
        if let Some(t) = self.transports.get_mut(&tid)
            && !t.failed
            && t.recovery_pending.is_none()
        {
            t.failed = true;
            t.tcp_ice_deadline = None;
            // A recoverable ICE failure does not close the source interceptor.
            // t.failed fences late input until a real accepted ICE restart.
            let participant = t.spec.participant.clone();
            tracing::info!(%participant, transport = ?tid, reason, "peer connection failed");
            self.events.push(MediaEvent::TransportFailed {
                participant,
                transport: tid,
                recovery_epoch: t.recovery_epoch,
            });
        }
    }

    fn check_dead_transports(&mut self) {
        let dead: Vec<TransportId> = self
            .transports
            .values()
            .filter(|t| {
                !t.failed
                    && t.recovery_pending.is_none()
                    && t.remote_closed_at.is_none()
                    && !t.rtc.is_alive()
            })
            .map(|t| t.id())
            .collect();
        for tid in dead {
            self.fail_transport(tid, "dead");
        }
    }

    fn transport_timers(&mut self, now: Instant) {
        let mut offers = Vec::new();
        let mut failed = Vec::new();
        let mut neg_failed = Vec::new();
        let mut tcp_failed = Vec::new();
        for t in self.transports.values_mut() {
            if let Some(pending) = t.recovery_pending {
                if now >= pending.deadline {
                    t.recovery_pending = None;
                    t.failed = false; // emit a current-epoch failure even if old ICE had already failed
                    failed.push(t.id());
                    continue;
                }
            }
            if t.recovery_pending.is_none()
                && t.tcp_ice_deadline.is_some_and(|deadline| deadline <= now)
            {
                t.tcp_ice_deadline = None;
                if !t.failed && t.rtc.ice_connection_state() == str0m::IceConnectionState::Checking
                {
                    t.failed = true;
                    tcp_failed.push((t.spec.participant.clone(), t.id()));
                }
            }
            if t.debounce_at.is_some_and(|d| d <= now) {
                t.debounce_at = None;
                offers.push(t.id());
            }
            if t.neg != NegState::Idle
                && !t.negotiation_failed
                && t.answer_deadline.is_some_and(|d| d <= now)
            {
                t.answer_deadline = None;
                // Only a connected PC escalates (transport.go: negotiation timeout
                // while connected → full reconnect).
                if t.connected && t.disconnected_since.is_none() {
                    t.negotiation_failed = true;
                    neg_failed.push((t.spec.participant.clone(), t.recovery_epoch));
                }
            }
            if Self::next_initial_ice_check(t).is_some_and(|check| now >= check) {
                if t.ice_checking_since
                    .is_some_and(|at| now > at + crate::ice_fallback::SOURCE_ICE_FAILED_TOTAL)
                {
                    failed.push(t.id());
                    continue;
                }
                // The pinned ICE loop restarts its timer after it contacts
                // candidates. A delayed wake shifts only this PC's cadence.
                t.ice_next_check_at = Some(now + crate::ice_fallback::SOURCE_ICE_CHECK_INTERVAL);
            }
            // No more ICE consent arrives after a remote close: the source ICE
            // failure follows 15s later even without a selected-pair deadline.
            if !t.failed
                && t.recovery_pending.is_none()
                && t.remote_closed_at
                    .is_some_and(|at| now >= at + crate::ice_fallback::SOURCE_ICE_FAILED_TOTAL)
            {
                failed.push(t.id());
                continue;
            }
            let selected_deadline = Self::selected_ice_failure_deadline(t);
            if !t.failed
                && !Self::selected_ice_restart_pending(t)
                && selected_deadline.map_or_else(
                    || {
                        t.disconnected_since
                            .is_some_and(|s| now.saturating_duration_since(s) >= DISCONNECT_TIMEOUT)
                    },
                    |deadline| now >= deadline,
                )
            {
                failed.push(t.id());
            }
        }
        for (participant, transport) in tcp_failed {
            self.events.push(MediaEvent::IceTcpConnectTimedOut {
                participant,
                transport,
                recovery_epoch: self
                    .transports
                    .get(&transport)
                    .map_or(0, |t| t.recovery_epoch),
            });
        }
        for (p, recovery_epoch) in neg_failed {
            self.events.push(MediaEvent::NegotiationFailed {
                participant: p,
                recovery_epoch,
            });
        }
        for tid in failed {
            self.fail_transport(tid, "ice-timeout");
        }
        for tid in offers {
            self.create_offer(tid, now);
        }
        self.run(now);
    }

    fn close_transport(&mut self, tid: TransportId, now: Instant) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if let Some(remote) = &mut t.remote_feedback {
            remote.close_interceptor();
        }
        if t.rtc.is_alive() {
            // Starts the local shutdown (RTCP BYE, SCTP shutdown, DTLS close_notify).
            if let Err(e) = t.rtc.close() {
                tracing::debug!(transport = %tid, error = %e, "rtc close failed");
                t.rtc.disconnect();
            }
        }
        self.drive(tid, now);
        // Events from the closing drain belong to a transport that is going away.
        self.queue.retain(|(t, _)| *t != tid);
        self.dirty.retain(|t| *t != tid);
        self.retire_transport_traffic(tid, now);
        let Some(t) = self.transports.remove(&tid) else {
            return;
        };
        self.retired_media_traffic += t.rtc.media_traffic_stats();
        if let Some(list) = self.participants.get_mut(&t.spec.participant) {
            list.retain(|x| *x != tid);
            if list.is_empty() {
                self.participants.remove(&t.spec.participant);
            }
        }
        // Up tracks stay (the room unpublishes them) but stop receiving media.
        self.up_by_mid.retain(|(t, _), _| *t != tid);
        // Down tracks on this transport become pending for a future transport.
        let detached: Vec<usize> = self
            .downs
            .iter()
            .filter(|(_, d)| d.transport == Some(tid))
            .map(|(i, _)| i)
            .collect();
        self.down_by_mid.retain(|(t, _), _| *t != tid);
        for di in detached {
            if let Some(d) = self.downs.get_mut(di) {
                d.transport = None;
                d.mid = None;
                d.negotiated = false;
                d.bound_emitted = false;
                d.rr_source_id = None;
                d.rr_report_sequence = 0;
                d.last_pt = None;
                d.stop_forwarding();
            }
        }
    }

    fn remove_participant(&mut self, p: &ParticipantSid, now: Instant) {
        for tid in self.transports_of(p) {
            self.close_transport(tid, now);
        }
        let downs: Vec<usize> = self
            .downs
            .iter()
            .filter(|(_, d)| &d.spec.subscriber == p)
            .map(|(i, _)| i)
            .collect();
        for di in downs {
            self.remove_down(di, now);
        }
        let ups: Vec<TrackSid> = self
            .ups
            .iter()
            .filter(|(_, u)| &u.publisher == p)
            .map(|(_, u)| u.sid.clone())
            .collect();
        for sid in ups {
            self.unpublish(&sid, now);
        }
    }
}

impl MediaEngine {
    /// Native RawPacket::RtpRx is emitted only after SRTP authentication and
    /// padding validation, before depacketizer/full-frame assembly.
    fn observe_publication_rtp(
        &mut self,
        tid: TransportId,
        header: &str0m::rtp::RtpHeader,
        _payload: &[u8],
    ) {
        let Some(t) = self.transports.get_mut(&tid).filter(|t| t.receives_media()) else {
            return;
        };
        let mime = t
            .rtc
            .codec_config()
            .params()
            .iter()
            .find(|p| p.pt() == header.payload_type)
            .map(|p| codecs::mime_of(p.spec().codec))
            .unwrap_or("");
        if mime.is_empty() {
            return;
        } // RTX/FEC/unknown PT is not a primary codec.
        let mid = {
            let mut direct = t.rtc.direct_api();
            direct.stream_rx(&header.ssrc).map(|stream| stream.mid())
        };
        let Some(mid) = mid else {
            return;
        }; // RTX SSRC does not resolve as primary.
        let Some(info) = t.remote_info.get(&mid) else {
            return;
        };
        if info
            .primary_ssrcs
            .as_ref()
            .is_some_and(|sources| !sources.contains(&*header.ssrc))
        {
            // A native stream's MID can survive a publication replacement.
            // Only the current declared source can complete that publication.
            return;
        }
        let cid = info.msid_track_id.clone();
        if t.publication_rtp
            .get(&mid)
            .is_some_and(|(old, _, pt)| old == &cid && (*pt != 0 || *header.payload_type == 0))
        {
            return;
        }
        t.publication_rtp
            .insert(mid, (cid, mime.to_owned(), *header.payload_type));
        if let Some(up) = self
            .up_by_mid
            .get(&(tid, mid))
            .and_then(|ui| self.ups.get(*ui))
        {
            self.events.push(MediaEvent::PublicationRtpRegistered {
                participant: t.spec.participant.clone(),
                transport: tid,
                track_sid: up.sid.clone(),
                mime_type: mime.into(),
                payload_type: *header.payload_type,
            });
        }
    }

    fn record_one_shot_rtp(
        &mut self,
        tid: TransportId,
        header: str0m::rtp::RtpHeader,
        payload: &[u8],
    ) {
        let Some(t) = self.transports.get_mut(&tid).filter(|t| t.spec.one_shot) else {
            return;
        };
        let Some(auth_tag_len) = t.rtc.receive_srtp_auth_tag_len() else {
            return;
        };
        let mut direct = t.rtc.direct_api();
        let mapping = direct
            .stream_rx(&header.ssrc)
            .map(|stream| (stream.mid(), stream.ssrc(), false));
        let mapping = mapping.or_else(|| {
            self.ups
                .iter()
                .filter(|(_, u)| u.transport == tid)
                .find_map(|(_, u)| {
                    let rids: Vec<_> = if u.sdp_rids.is_empty() {
                        vec![None]
                    } else {
                        u.sdp_rids
                            .iter()
                            .map(|s| Some(str0m::media::Rid::from(s.as_str())))
                            .collect()
                    };
                    rids.into_iter().find_map(|rid| {
                        direct.stream_rx_by_mid(u.mid, rid).and_then(|stream| {
                            if stream.rtx() == Some(header.ssrc) {
                                Some((stream.mid(), stream.ssrc(), true))
                            } else {
                                None
                            }
                        })
                    })
                })
        });
        let Some((_mid, main, repair)) = mapping else {
            return;
        };
        let sequence = if repair {
            let Some(prefix) = payload.get(..2) else {
                return;
            };
            u16::from_be_bytes(prefix.try_into().unwrap())
        } else {
            header.sequence_number
        };
        t.ingress_metrics
            .received(crate::ingress_metrics::AuthenticatedRtp {
                mid: _mid.to_string(),
                outer_ssrc: *header.ssrc,
                outer_seq: header.sequence_number,
                main_ssrc: *main,
                sequence,
                header_len: header.header_len,
                payload_len: payload.len(),
                auth_tag_len,
                repair,
            });
    }
    pub fn published_average_bitrate(&mut self, track: &TrackSid, now: Instant) -> u32 {
        let Some(up) = self.up_by_sid.get(track).and_then(|ui| self.ups.get(*ui)) else {
            return 0;
        };
        self.transports
            .get(&up.transport)
            .map_or(0, |t| t.ingress_metrics.bitrate(&up.mid.to_string(), now))
    }
}

// Fixed RTCConfig tests signed >0 before Go's uint32 conversion. In particular,
// 2^32 is positive yet converts to zero (default), not a clamped maximum.
fn positive_sctp_u32(value: i64) -> u32 {
    if value > 0 { value as u32 } else { 0 }
}
