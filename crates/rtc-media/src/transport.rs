//! One client peer connection: a str0m `Rtc` plus its signaling, candidate,
//! data-channel and single-PC section bookkeeping. Cross-object logic (down tracks,
//! up tracks) lives in the engine; this module owns the str0m drive loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use rtc_core::ids::TransportId;
use rtc_core::media_api::{DataKind, PcRole, PublishedTrackSpec, TransportSpec};
use str0m::change::SdpPendingOffer;
use str0m::channel::ChannelId;
use str0m::media::{MediaKind, Mid};
use str0m::{Candidate, Event, Input, Output, Rtc, RtcError};

use crate::net::{PacketOut, Proto};

/// Data channel labels (signaling.md §7).
pub(crate) const LABEL_RELIABLE: &str = "_reliable";
pub(crate) const LABEL_LOSSY: &str = "_lossy";
pub(crate) const LABEL_DATA_TRACK: &str = "_data_track";

/// Subscriber answer timeout (§3.4).
pub(crate) const ANSWER_TIMEOUT: Duration = Duration::from_secs(15);
/// ICE disconnected this long → transport failed.
pub(crate) const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Negotiation debounce (transport.go:2009-2052).
pub(crate) const NEGOTIATE_SHORT: Duration = Duration::from_millis(10);
pub(crate) const NEGOTIATE_LONG: Duration = Duration::from_millis(150);

/// Bound on drain rounds per drive (each round may feed `Input::Timeout(now)` to let
/// str0m packetize queued frames, one per media per round).
const MAX_DRIVE_ROUNDS: usize = 16;
/// Far future used when an `Rtc` is closed.
const FAR: Duration = Duration::from_secs(3600);
/// Recoverable errors tolerated per drive before the `Rtc` is given up.
const MAX_SOFT_ERRORS: usize = 64;

/// Per-frame errors (bad payload from a publisher, a frame for a stream that just
/// went away): str0m has already dropped the offending item, polling may go on.
pub(crate) fn recoverable(e: &RtcError) -> bool {
    matches!(
        e,
        RtcError::Packet(..)
            | RtcError::NoSenderSource
            | RtcError::UnknownPt(_)
            | RtcError::UnknownRid(_)
            | RtcError::WriteWithoutPoll
    )
}

/// Server-offer negotiation state (`negotiationState`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NegState {
    Idle,
    /// Offer sent, waiting for the answer.
    Remote,
    /// Changes arrived while waiting; offer again right after the answer.
    Retry,
}

pub(crate) struct ChannelEntry {
    pub id: ChannelId,
    pub label: String,
    /// `None` for labels other than `_reliable` / `_lossy`.
    pub kind: Option<DataKind>,
}

/// Remote media info captured from the client's offer.
#[derive(Clone, Debug, Default)]
pub(crate) struct RemoteInfo {
    pub msid_track_id: String,
    pub rids: Vec<String>,
    pub prefers_audio_red: bool,
    /// Current non-RID receiver's declared primary sources. Native receive
    /// streams may outlive an inactive m-line and its previous publication.
    /// RID and undeclared-source discovery have no fixed SSRC list.
    pub primary_ssrcs: Option<Vec<u32>>,
}

#[derive(Default)]
pub(crate) struct IoCounters {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub packets_in: u64,
    pub packets_out: u64,
    pub nacks: u64,
    pub plis: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct RecoveryPending {
    pub started: Instant,
    pub deadline: Instant,
    pub sdp_applied: bool,
}

pub(crate) struct Transport {
    pub spec: TransportSpec,
    pub recovery_epoch: u64,
    pub recovery_pending: Option<RecoveryPending>,
    pub owned_out: Option<Box<dyn PacketOut>>,
    pub ingress_metrics: crate::ingress_metrics::IngressMetrics,
    /// Real accepted remote offer retained only for explicit one-shot finalization.
    pub one_shot_offer: Option<(u32, String)>,
    /// Ready to answer once the current ICE generation finishes gathering.
    pub one_shot_answer_waiting: Option<(u32, String)>,
    pub refresh_one_shot_host: bool,
    pub defer_one_shot_answer: bool,
    pub rtc: Rtc,
    pub ufrag: String,
    /// Local candidates as accepted by str0m (ufrag adopted), in signalling order.
    pub candidates: Vec<Candidate>,
    pub next_timeout: Instant,
    /// Latest time fed to `rtc` (str0m `last_now`). str0m stamps abs-send-time
    /// and paces with it, so `drive` advances it before sending.
    pub rtc_clock: Instant,
    pub has_bwe: bool,
    pub remote_feedback: Option<crate::feedback::Feedback>,
    pub remote_allocation_dirty: bool,

    // ---- signaling ----
    pub local_description_sent: bool,
    /// All local candidates are supplied before create_transport returns. This
    /// marks the first committed local offer, after synchronous host gathering.
    pub local_host_gathering_complete: bool,
    pub last_local_offer_sdp: Option<String>,
    pub candidate_cursor: usize,
    pub gather_srflx: bool,
    pub srflx_pending: bool,
    pub last_local_first_mid: Option<String>,
    pub remote_description_applied: bool,
    pub queued_remote_candidates: Vec<Candidate>,

    // ---- connection ----
    pub connected: bool,
    /// One notification for native aggregate PC!=New, earlier than DTLS.
    pub established_reported: bool,
    pub ice_first_connected_at: Option<Instant>,
    pub selected_ice_last_receive: Option<Instant>,
    pub ice_started_at: Option<Instant>,
    pub tcp_ice_deadline: Option<Instant>,
    /// The remote closed DTLS/SCTP on a connected PC. Fixed pion keeps the PC
    /// connected until ICE consent expires, so failure waits for the source
    /// selected-pair deadline instead of firing on the close itself.
    pub remote_closed_at: Option<Instant>,
    /// When ICE checking began on a PC that has never connected (first applied
    /// remote description). Pion fails a checking agent after 10s + 5s
    /// (livekit/ice agent.go connectivityChecks).
    pub ice_checking_since: Option<Instant>,
    /// Next periodic contact of the initial ICE connectivity check loop.
    pub ice_next_check_at: Option<Instant>,
    pub signaling_rtt_ms: u32,
    pub last_ice_path: Option<(rtc_core::transport_fallback::IceConnection, Option<Instant>)>,
    pub first_remote_offer_has_data: Option<bool>,
    pub opened_data_kinds: [bool; 2],
    pub fully_established_reported: bool,
    /// Source resettable peerConnectionLastconnectedAt, separate from transport liveness.
    pub fully_established_connected: bool,
    pub data_connection_seen: bool,
    pub reset_established_on_ice_restart: bool,
    pub remote_offer_ice: Option<(String, String)>,
    pub disconnected_since: Option<Instant>,
    pub failed: bool,

    // ---- server-offer negotiation (dual-PC subscriber) ----
    pub neg: NegState,
    pub pending_offer: Option<SdpPendingOffer>,
    pub offer_seq: u32,
    pub answer_deadline: Option<Instant>,
    pub negotiation_failed: bool,
    pub debounce_at: Option<Instant>,
    pub last_negotiation: Option<Instant>,
    pub ice_restart_pending: bool,
    /// One explicit restart invocation; deferred credentials remain separate.
    pub ice_restart_requested: bool,
    pub pending_restart_ufrag: Option<String>,
    /// Down tracks waiting for an m-line in the next offer.
    pub pending_adds: Vec<usize>,
    /// Mids to stop in the next offer.
    pub pending_stops: Vec<Mid>,
    /// Mids added by the outstanding offer.
    pub offered: Vec<Mid>,
    pub channels_created: bool,

    // ---- data channels ----
    pub channels: Vec<ChannelEntry>,
    pub data_track_writer: Option<(ChannelId, crate::data_track_writer::DataTrackWriter)>,
    /// Two bounded terminal writer slots, only after an actual open/close pair.
    pub closed_data_kinds: [bool; 2],
    pub ordinary_data_writers: HashMap<ChannelId, crate::ordinary_data_writer::Writer>,

    // ---- publisher side ----
    /// Remote-offered receive mids already reported (RemoteMediaAdded).
    pub remote_mids: HashSet<Mid>,
    pub remote_info: HashMap<Mid, RemoteInfo>,
    /// Last negotiated encoder hints for each publisher transceiver, retained
    /// on its own MID if the next answer makes that transceiver inactive.
    pub publisher_bitrate_hints: HashMap<Mid, String>,
    /// First authenticated primary packet codec, keyed by current MID/CID.
    pub publication_rtp: HashMap<Mid, (String, String, u8)>,
    /// Announced pending publications (ExpectTrack), not yet bound.
    pub expected: Vec<PublishedTrackSpec>,

    // ---- single PC ----
    /// Client-offered sections we may send on (offer order).
    pub send_sections: Vec<(Mid, MediaKind)>,
    /// Active down tracks per assigned mid.
    pub assigned: Vec<(Mid, usize)>,
    /// Mids that ever carried a down track (never reused for another track).
    pub used_mids: HashSet<Mid>,
    /// Mids whose down track ended; answered `inactive` forever.
    pub retired: HashSet<Mid>,
    /// Down tracks waiting for a client section (FIFO).
    pub pending_single: VecDeque<usize>,
    /// Sections already requested via MediaSectionsRequirement (audio, video).
    pub requested: (u32, u32),
    pub requirement_dirty: bool,
    pub requirement_force: bool,

    // ---- bandwidth ----
    pub estimate_bps: Option<u64>,
    pub probes: crate::probe_controller::ProbeController,
    pub layer_bitrates: crate::allocator::LayerBitrateCache,
    pub allocated_bps: u64,
    pub allocation_overshoot_bps: u64,
    pub paused_video_tracks: usize,
    /// A TWCC (send-side) estimate was seen; REMB estimates are then ignored.
    pub twcc_estimates: bool,
    pub bw_override_bps: u64,
    pub rtt: Option<Duration>,
}

impl Transport {
    pub(crate) fn new(
        spec: TransportSpec,
        mut rtc: Rtc,
        ufrag: String,
        has_bwe: bool,
        now: Instant,
    ) -> Self {
        rtc.set_receiver_report_rtt_enabled(true);
        rtc.set_require_fir_media_ssrc(true);
        Self {
            spec,
            recovery_epoch: 0,
            recovery_pending: None,
            owned_out: None,
            ingress_metrics: Default::default(),
            one_shot_offer: None,
            one_shot_answer_waiting: None,
            refresh_one_shot_host: false,
            defer_one_shot_answer: false,
            rtc,
            ufrag,
            candidates: Vec::new(),
            next_timeout: now,
            rtc_clock: now,
            has_bwe,
            remote_feedback: None,
            remote_allocation_dirty: false,
            local_description_sent: false,
            local_host_gathering_complete: false,
            last_local_offer_sdp: None,
            candidate_cursor: 0,
            gather_srflx: false,
            srflx_pending: false,
            last_local_first_mid: None,
            remote_description_applied: false,
            queued_remote_candidates: Vec::new(),
            connected: false,
            established_reported: false,
            ice_first_connected_at: None,
            selected_ice_last_receive: None,
            ice_started_at: None,
            tcp_ice_deadline: None,
            remote_closed_at: None,
            ice_checking_since: None,
            ice_next_check_at: None,
            signaling_rtt_ms: 0,
            last_ice_path: None,
            first_remote_offer_has_data: None,
            opened_data_kinds: [false; 2],
            fully_established_reported: false,
            fully_established_connected: false,
            data_connection_seen: false,
            reset_established_on_ice_restart: false,
            remote_offer_ice: None,
            disconnected_since: None,
            failed: false,
            neg: NegState::Idle,
            pending_offer: None,
            offer_seq: 0,
            answer_deadline: None,
            negotiation_failed: false,
            debounce_at: None,
            last_negotiation: None,
            ice_restart_pending: false,
            ice_restart_requested: false,
            pending_restart_ufrag: None,
            pending_adds: Vec::new(),
            pending_stops: Vec::new(),
            offered: Vec::new(),
            channels_created: false,
            channels: Vec::new(),
            data_track_writer: None,
            closed_data_kinds: [false; 2],
            ordinary_data_writers: HashMap::new(),
            remote_mids: HashSet::new(),
            remote_info: HashMap::new(),
            publisher_bitrate_hints: HashMap::new(),
            publication_rtp: HashMap::new(),
            expected: Vec::new(),
            send_sections: Vec::new(),
            assigned: Vec::new(),
            used_mids: HashSet::new(),
            retired: HashSet::new(),
            pending_single: VecDeque::new(),
            requested: (0, 0),
            requirement_dirty: false,
            requirement_force: false,
            estimate_bps: has_bwe.then_some(crate::allocator::INITIAL_CAPACITY_BPS),
            probes: crate::probe_controller::ProbeController::new(now),
            layer_bitrates: Default::default(),
            allocated_bps: 0,
            allocation_overshoot_bps: 0,
            paused_video_tracks: 0,
            twcc_estimates: false,
            bw_override_bps: 0,
            rtt: None,
        }
    }

    pub(crate) fn id(&self) -> TransportId {
        self.spec.id
    }

    pub(crate) fn role(&self) -> PcRole {
        self.spec.role
    }

    /// Carries down tracks (dual subscriber or single PC).
    pub(crate) fn sends_media(&self) -> bool {
        matches!(self.spec.role, PcRole::Subscriber | PcRole::Single)
    }

    /// Receives published media (dual publisher or single PC).
    pub(crate) fn receives_media(&self) -> bool {
        matches!(self.spec.role, PcRole::Publisher | PcRole::Single)
    }

    pub(crate) fn usable(&self) -> bool {
        !self.failed && self.rtc.is_alive()
    }

    /// Current ICE path state for discovery; `connected` alone is latched after
    /// the first handshake and remains true through a recoverable disconnect.
    pub(crate) fn probe_path_connected(&self) -> bool {
        self.connected && self.disconnected_since.is_none()
    }

    /// The m-line exists and currently sends (writing to a non-sending media would
    /// make str0m fail the next packetize step).
    pub(crate) fn can_send(&self, mid: Mid) -> bool {
        self.rtc
            .media(mid)
            .is_some_and(|m| m.direction().is_sending() && !m.disabled())
    }

    /// Effective egress allocation capacity, including an explicit override.
    pub(crate) fn capacity(&self) -> Option<u64> {
        if self.remote_feedback.is_some() && !self.has_bwe {
            return (self.bw_override_bps > 0).then_some(self.bw_override_bps);
        }
        if self.has_bwe && self.bw_override_bps > 0 {
            return Some(self.bw_override_bps);
        }
        match (
            if self.has_bwe {
                if let Some(remote) = &self.remote_feedback {
                    Some(remote.capacity())
                } else {
                    self.probes.capacity(self.estimate_bps)
                }
            } else {
                self.estimate_bps
            },
            self.bw_override_bps,
        ) {
            (Some(e), 0) => Some(e),
            (Some(e), o) => Some(e.min(o)),
            (None, 0) => None,
            (None, o) => Some(o),
        }
    }

    pub(crate) fn data_kind_index(kind: DataKind) -> usize {
        match kind {
            DataKind::Reliable => 0,
            DataKind::Lossy | DataKind::Unrecognized(_) => 1,
        }
    }

    /// Source First is never cleared by resetShortConn; only Last is reset.
    pub(crate) fn record_data_connection_proof(&mut self) -> bool {
        self.fully_established_connected = true;
        !std::mem::replace(&mut self.data_connection_seen, true)
    }
    pub(crate) fn consume_short_connection_reset(&mut self) -> bool {
        if !std::mem::take(&mut self.reset_established_on_ice_restart) {
            return false;
        }
        self.fully_established_connected = false;
        self.ice_first_connected_at = None;
        self.ice_started_at = None;
        self.tcp_ice_deadline = None;
        true
    }

    pub(crate) fn has_closed_data_writer(&self, kind: DataKind) -> bool {
        self.closed_data_kinds[Self::data_kind_index(kind)]
    }

    /// Use the negotiated channel with the requested reliability semantics.
    /// A missing reliable channel must never downgrade delivery to lossy.
    pub(crate) fn channel_for(&self, kind: DataKind) -> Option<(ChannelId, DataKind)> {
        let kind = kind.delivery_class();
        self.channels
            .iter()
            .find(|c| c.kind == Some(kind))
            .map(|c| (c.id, kind))
    }

    /// Drain str0m output until it reports a future timeout: transmits go straight to
    /// the socket layer, events are queued for the engine (processed after the drain,
    /// so this `Rtc` is at rest before any cross-transport mutation). When str0m's
    /// next timeout is already due (queued frames to packetize, pacer), feed
    /// `Input::Timeout(now)` and drain again.
    pub(crate) fn drive(
        &mut self,
        now: Instant,
        out: &dyn PacketOut,
        io: &mut IoCounters,
        sink: &mut VecDeque<(TransportId, Event)>,
    ) {
        let id = self.spec.id;
        // Media written by another transport's event (forwarding) is sent from
        // this poll. Without advancing the clock, str0m would stamp it with the
        // time of this Rtc's last own input; fixed Go stamps abs-send-time at the
        // actual write (pacer pass-through), and Chrome's delay-based estimate
        // reads the stale spread as overuse.
        if now > self.rtc_clock && self.rtc.is_alive() {
            self.rtc_clock = now;
            if let Err(e) = self.rtc.handle_input(Input::Timeout(now)) {
                if recoverable(&e) {
                    tracing::debug!(transport = %id, error = %e, "rtc clock: frame dropped");
                } else {
                    tracing::warn!(transport = %id, error = %e, "rtc clock advance failed, disconnecting");
                    self.rtc.disconnect();
                }
            }
        }
        let connected = self.probe_path_connected();
        if let Some(remote) = &mut self.remote_feedback {
            self.remote_allocation_dirty |= remote.service(&mut self.rtc, connected, now);
        }
        let mut soft_errors = 0usize;
        for _ in 0..MAX_DRIVE_ROUNDS {
            let timeout = loop {
                match self.rtc.poll_output() {
                    Ok(Output::Transmit(t)) => {
                        io.bytes_out += t.contents.len() as u64;
                        io.packets_out += 1;
                        self.owned_out.as_deref().unwrap_or(out).send(
                            Proto::from_str0m(t.proto),
                            t.source,
                            t.destination,
                            &t.contents,
                        );
                    }
                    Ok(Output::Event(e)) => sink.push_back((id, e)),
                    Ok(Output::Timeout(t)) => break Some(t),
                    Err(e)
                        if recoverable(&e)
                            && soft_errors < MAX_SOFT_ERRORS
                            && self.rtc.is_alive() =>
                    {
                        soft_errors += 1;
                        tracing::debug!(transport = %id, error = %e, "rtc poll: frame dropped");
                    }
                    Err(e) => {
                        tracing::warn!(transport = %id, error = %e, "rtc poll failed, disconnecting");
                        self.rtc.disconnect();
                        break None;
                    }
                }
            };
            let Some(t) = timeout else {
                self.next_timeout = now + FAR;
                return;
            };
            if t <= now && self.rtc.is_alive() {
                self.rtc_clock = self.rtc_clock.max(now);
                if let Err(e) = self.rtc.handle_input(Input::Timeout(now)) {
                    if recoverable(&e) && soft_errors < MAX_SOFT_ERRORS {
                        soft_errors += 1;
                        tracing::debug!(transport = %id, error = %e, "rtc timeout: frame dropped");
                    } else {
                        tracing::warn!(transport = %id, error = %e, "rtc timeout handling failed, disconnecting");
                        self.rtc.disconnect();
                        self.next_timeout = now + FAR;
                        return;
                    }
                }
                continue;
            }
            self.next_timeout = if self.rtc.is_alive() {
                self.remote_feedback
                    .as_ref()
                    .and_then(|f| f.deadline())
                    .map_or(t, |at| at.min(t))
            } else {
                now + FAR
            };
            return;
        }
        // Still due: the runtime will call handle_timeout right away.
        self.next_timeout = now;
    }
}
