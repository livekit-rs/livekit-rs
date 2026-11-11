//! Room state machine. Pure logic: every method mutates state and appends
//! [`Effect`]s; the runtime executes them (signals to sessions, media commands,
//! lifecycle events). Upstream behavior references: impl-notes/signaling.md,
//! impl-notes/sfu-media.md, impl-notes/api-auth-webhook.md.

mod api;
mod broadcast;
mod data;
mod data_blob;
mod data_track;
mod debug;
mod error_supervision;
mod join;
mod media_events;
mod participant;
mod participant_updates;
mod publish;
mod requests;
mod subscribe;
#[cfg(test)]
mod tests;
mod transport_fallback;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::RoomPolicy;
use crate::data::DataHandler;
use crate::events::{EngineEvent, RoomEndReason};
use crate::ids::*;
use crate::media_api::MediaCommand;
use crate::model::*;
use crate::permission::RoomConfig;
use crate::session::{LeaveAction, SignalEvent};

mod lifecycle;
mod media_rtt;
pub use lifecycle::LifecycleCounters;

pub use api::OpError;
pub use debug::{DebugParticipantSnapshot, DebugPendingTracks, DebugRoomSnapshot};
pub use join::Joined;
pub(crate) use participant::Participant;
pub use participant_updates::ParticipantUpdateSizer;

/// Upstream `PingIntervalSeconds` / `PingTimeoutSeconds`.
pub const PING_INTERVAL_S: i32 = 5;
pub const PING_TIMEOUT_S: i32 = 15;
/// Non-publisher participant updates are batched and flushed at this interval.
const UPDATE_BATCH_INTERVAL: Duration = Duration::from_secs(3);
/// RoomUpdate for participant count changes is debounced.
const ROOM_UPDATE_DEBOUNCE: Duration = Duration::from_secs(5);
/// A participant must reach ACTIVE within this time.
const JOIN_VERIFY_TIMEOUT: Duration = Duration::from_secs(60);
/// After a transport failure the participant is closed unless it resumes.
const DISCONNECT_CLEANUP: Duration = Duration::from_secs(5);
/// Duplicate `UserPacket.nonce` suppression window.
const NONCE_WINDOW: Duration = Duration::from_secs(2);
const NONCE_CACHE_MAX: usize = 1024;
/// Recently disconnected participants reported after a resume (v>8).
const RECENT_DISCONNECT_MEMORY: usize = 64;
const MAX_PENDING_TRACKS: usize = 20;
const MAX_QUEUED_PER_CID: usize = 3;

/// Something the runtime must do on behalf of the room.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "Effects are moved through a room-local queue; keep the stable adapter contract without an allocation per event."
)]
pub enum Effect {
    Signal {
        to: ParticipantSid,
        event: SignalEvent,
    },
    /// Flush and close the participant's current signal connection.
    CloseSignal {
        to: ParticipantSid,
    },
    Media(MediaCommand),
    Event(EngineEvent),
}

pub struct Room {
    lifecycle: LifecycleCounters,
    key: RoomKey,
    info: RoomInfo,
    policy: Arc<RoomPolicy>,
    data: Arc<dyn DataHandler>,
    /// Application messages accepted by participant permission/state, before
    /// decoding or reliable-sequence suppression (fixed participant.go2491).
    data_bytes_in: u64,
    data_packets_in: u64,
    sif_trailer: Vec<u8>,
    participants: BTreeMap<ParticipantSid, Participant>,
    by_identity: HashMap<Identity, ParticipantSid>,
    /// Immutable copies updated by real participant changes, not private receiver metadata.
    stored_participants: BTreeMap<Identity, ParticipantInfo>,
    track_owner: HashMap<TrackSid, ParticipantSid>,
    /// Unix seconds; first join of a non-dependent participant.
    joined_at_s: i64,
    /// Unix seconds; last removal of any participant.
    left_at_s: i64,
    closed: bool,
    effects: Vec<Effect>,
    batch_pending: Vec<ParticipantSid>,
    participant_update_sizer: Option<Arc<dyn ParticipantUpdateSizer>>,
    next_batch_flush: Instant,
    room_update_due: Option<Instant>,
    /// Last quantized level sent per active speaker.
    speakers: BTreeMap<ParticipantSid, f32>,
    /// `simulate speaker_update`: inactive speaker updates due at the given time.
    simulated_speaker_clears: Vec<(Instant, ParticipantSid)>,
    publication_monitors:
        BTreeMap<(ParticipantSid, TrackSid), error_supervision::PublicationMonitor>,
    subscription_monitors:
        BTreeMap<(ParticipantSid, TrackSid), error_supervision::SubscriptionMonitor>,
    publisher_connected: std::collections::BTreeSet<ParticipantSid>,
    next_error_supervision: Instant,
    nonces: VecDeque<(Instant, Vec<u8>)>,
    recently_disconnected: VecDeque<(Instant, ParticipantInfo)>,
    sync_streams: bool,
    playout_delay: Option<(u32, u32)>,
}

impl Room {
    /// Create a room. `overrides` are CreateRoom / token room config values (> 0
    /// or non-empty overrides the configured default).
    pub fn new(
        key: RoomKey,
        policy: Arc<RoomPolicy>,
        data: Arc<dyn DataHandler>,
        overrides: &RoomConfig,
        now: Instant,
    ) -> Room {
        Self::construct(key, policy, data, overrides, now, None)
    }

    /// Instantiate the sole local owner using a catalog-allocated identity.
    /// Live participant and recording counts are always computed by this owner.
    pub fn from_bootstrap(
        key: RoomKey,
        policy: Arc<RoomPolicy>,
        data: Arc<dyn DataHandler>,
        overrides: &RoomConfig,
        now: Instant,
        mut info: RoomInfo,
    ) -> Result<Room, &'static str> {
        if info.sid.0.is_empty() || info.name != key.name {
            return Err("invalid shared room identity");
        }
        info.num_participants = 0;
        info.num_publishers = 0;
        info.active_recording = false;
        Ok(Self::construct(
            key,
            policy,
            data,
            overrides,
            now,
            Some(info),
        ))
    }

    fn construct(
        key: RoomKey,
        policy: Arc<RoomPolicy>,
        data: Arc<dyn DataHandler>,
        overrides: &RoomConfig,
        now: Instant,
        bootstrap: Option<RoomInfo>,
    ) -> Room {
        let d = &policy;
        let pick = |v: Option<u32>, default: u32| v.filter(|v| *v > 0).unwrap_or(default);
        let info = bootstrap.unwrap_or_else(|| RoomInfo {
            sid: RoomSid::generate(),
            name: key.name.clone(),
            empty_timeout_s: pick(overrides.empty_timeout_s, d.empty_timeout.as_secs() as u32),
            departure_timeout_s: pick(
                overrides.departure_timeout_s,
                d.departure_timeout.as_secs() as u32,
            ),
            max_participants: pick(overrides.max_participants, d.max_participants),
            creation_time_ms: crate::util::unix_ms(),
            metadata: overrides.metadata.clone().unwrap_or_default(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: d.enabled_codecs.clone(),
            version: TimedVersion::now(),
            turn_password: crate::util::random_secret(),
        });
        let sync_streams = overrides.sync_streams.unwrap_or(d.sync_streams);
        // Upstream replaces the entire pair if either request endpoint is set.
        let min = overrides.min_playout_delay.unwrap_or(0);
        let max = overrides.max_playout_delay.unwrap_or(0);
        let playout_delay = if min > 0 || max > 0 {
            Some((min, max))
        } else {
            d.playout_delay
        };
        let mut room = Room {
            lifecycle: Default::default(),
            key,
            info,
            policy,
            data,
            sif_trailer: random_trailer(),
            participants: BTreeMap::new(),
            by_identity: HashMap::new(),
            stored_participants: BTreeMap::new(),
            track_owner: HashMap::new(),
            joined_at_s: 0,
            left_at_s: 0,
            closed: false,
            effects: Vec::new(),
            batch_pending: Vec::new(),
            data_bytes_in: 0,
            data_packets_in: 0,
            participant_update_sizer: None,
            next_batch_flush: now + UPDATE_BATCH_INTERVAL,
            room_update_due: None,
            speakers: BTreeMap::new(),
            simulated_speaker_clears: Vec::new(),
            publication_monitors: BTreeMap::new(),
            subscription_monitors: BTreeMap::new(),
            publisher_connected: Default::default(),
            next_error_supervision: now + Duration::from_secs(1),
            nonces: VecDeque::new(),
            recently_disconnected: VecDeque::new(),
            sync_streams,
            playout_delay,
        };
        let snapshot = room.info();
        room.effects
            .push(Effect::Event(EngineEvent::RoomStarted { room: snapshot }));
        room
    }

    /// Resolved per-generation playout pair passed to the media owner.
    pub fn playout_delay(&self) -> Option<(u32, u32)> {
        self.playout_delay
    }

    pub fn key(&self) -> &RoomKey {
        &self.key
    }

    pub fn sid(&self) -> &RoomSid {
        &self.info.sid
    }

    pub fn sif_trailer(&self) -> &[u8] {
        &self.sif_trailer
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Room snapshot with live participant/publisher counts.
    pub fn info(&self) -> RoomInfo {
        let mut info = self.info.clone();
        let live = self.participants.values().filter(|p| !p.is_disconnected());
        let mut n = 0u32;
        let mut publishers = 0u32;
        for p in live {
            n += 1;
            if p.info.is_publisher {
                publishers += 1;
            }
        }
        info.num_participants = n;
        info.num_publishers = publishers;
        info
    }

    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    /// Room-lifetime data-channel bytes and message count, including malformed
    /// accepted messages. Transport/SCTP framing and denied senders are excluded.
    pub fn data_traffic_in(&self) -> (u64, u64) {
        (self.data_bytes_in, self.data_packets_in)
    }

    pub fn lifecycle_counters(&self) -> LifecycleCounters {
        self.lifecycle
    }

    pub fn published_track_count(&self) -> usize {
        self.track_owner.len()
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    pub fn participant_generation(&self, sid: &ParticipantSid) -> Option<SessionGeneration> {
        self.participants.get(sid).map(|p| p.generation)
    }

    /// Periodic housekeeping. Returns the next time `tick` must run.
    pub fn tick(&mut self, now: Instant) -> Instant {
        if now >= self.next_batch_flush {
            self.flush_batched_updates();
            self.next_batch_flush = now + UPDATE_BATCH_INTERVAL;
        }
        if self.room_update_due.is_some_and(|t| now >= t) {
            self.room_update_due = None;
            self.broadcast_room_update();
        }
        let due: Vec<ParticipantSid> = {
            let (due, pending): (Vec<_>, Vec<_>) =
                std::mem::take(&mut self.simulated_speaker_clears)
                    .into_iter()
                    .partition(|(at, _)| now >= *at);
            self.simulated_speaker_clears = pending;
            due.into_iter().map(|(_, sid)| sid).collect()
        };
        for sid in due {
            self.send_speaker_changes(vec![SpeakerInfo {
                sid,
                level: 0.0,
                active: false,
            }]);
        }
        self.expire_nonces(now);
        self.reconcile_missing_subscriptions(now);
        self.supervise_errors(now);

        // Join verification and post-failure cleanup.
        let mut to_close = Vec::new();
        for p in self.participants.values() {
            if !p.ever_active && now >= p.join_deadline {
                to_close.push((p.sid().clone(), DisconnectReason::JoinFailure, true));
            } else if p
                .transport_failed_at
                .is_some_and(|t| now >= t + DISCONNECT_CLEANUP)
            {
                to_close.push((p.sid().clone(), DisconnectReason::ConnectionTimeout, false));
            }
        }
        for (sid, reason, send_leave) in to_close {
            self.remove_participant(&sid, reason, send_leave, now);
        }

        let mut next = self.next_batch_flush;
        if !self.publication_monitors.is_empty() || !self.subscription_monitors.is_empty() {
            next = next.min(self.next_error_supervision);
        }
        if let Some(t) = self.room_update_due {
            next = next.min(t);
        }
        for (at, _) in &self.simulated_speaker_clears {
            next = next.min(*at);
        }
        for p in self.participants.values() {
            if p.subscriptions
                .values()
                .any(|s| !s.requested && s.desired_since.is_some())
            {
                next = next.min(p.next_subscription_reconcile);
            }
            if !p.ever_active {
                next = next.min(p.join_deadline);
            }
            if let Some(t) = p.transport_failed_at {
                next = next.min(t + DISCONNECT_CLEANUP);
            }
        }
        next
    }

    /// Idle rules (api-auth-webhook.md §3), evaluated by the runtime every second.
    pub fn should_close(&self) -> bool {
        if self.closed {
            return false;
        }
        if self.participants.values().any(|p| !p.is_dependent()) {
            return false;
        }
        let now_s = crate::util::unix_s();
        if self.joined_at_s > 0 && self.left_at_s > 0 {
            now_s - self.left_at_s >= i64::from(self.info.departure_timeout_s)
        } else {
            now_s - self.info.creation_time_ms / 1000 >= i64::from(self.info.empty_timeout_s)
        }
    }

    /// Close the room, disconnecting everyone.
    pub fn close(&mut self, reason: RoomEndReason, now: Instant) {
        if self.closed {
            return;
        }
        let participant_reason = match reason {
            RoomEndReason::ApiDelete => DisconnectReason::RoomDeleted,
            RoomEndReason::ServerShutdown => DisconnectReason::ServerShutdown,
            _ => DisconnectReason::RoomClosed,
        };
        let sids: Vec<ParticipantSid> = self.participants.keys().cloned().collect();
        for sid in sids {
            self.remove_participant(&sid, participant_reason, true, now);
        }
        self.closed = true;
        let room = self.info();
        self.effects
            .push(Effect::Event(EngineEvent::RoomFinished { room, reason }));
    }

    // ----- helpers shared by the submodules -----

    fn signal(&mut self, to: &ParticipantSid, event: SignalEvent) {
        self.effects.push(Effect::Signal {
            to: to.clone(),
            event,
        });
    }

    fn media(&mut self, cmd: MediaCommand) {
        self.effects.push(Effect::Media(cmd));
    }

    fn event(&mut self, ev: EngineEvent) {
        self.effects.push(Effect::Event(ev));
    }

    /// Send a leave honoring the client's protocol version (signaling.md §8).
    fn send_leave(&mut self, sid: &ParticipantSid, reason: DisconnectReason, action: LeaveAction) {
        let Some(p) = self.participants.get(sid) else {
            return;
        };
        // Old clients get nothing for the transport-failure (resume) case.
        if action == LeaveAction::Resume && !p.capabilities.regions_in_leave {
            return;
        }
        self.signal(sid, SignalEvent::Leave { reason, action });
    }

    fn schedule_room_update(&mut self, now: Instant) {
        if self.room_update_due.is_none() {
            self.room_update_due = Some(now + ROOM_UPDATE_DEBOUNCE);
        }
    }

    fn expire_nonces(&mut self, now: Instant) {
        while self
            .nonces
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) >= NONCE_WINDOW)
        {
            self.nonces.pop_front();
        }
    }

    fn sid_by_identity(&self, identity: &Identity) -> Option<ParticipantSid> {
        self.by_identity.get(identity).cloned()
    }

    fn enabled_publish_codecs(&self) -> Vec<CodecSpec> {
        self.info
            .enabled_codecs
            .iter()
            .filter(|c| !c.mime.eq_ignore_ascii_case("video/rtx"))
            .cloned()
            .collect()
    }

    /// Stream id for a down track (sfu-media.md §10).
    fn stream_id_for(
        &self,
        subscriber: &Participant,
        publisher: &ParticipantSid,
        track: &TrackInfo,
    ) -> String {
        if self.sync_streams && subscriber.capabilities.sync_stream_id && !track.stream.is_empty() {
            format!("{}|{}", publisher, track.stream)
        } else {
            format!("{}|{}", publisher, track.sid)
        }
    }
}

/// Per-room SIF trailer: ASCII base62 of 32 random bytes (signaling.md §2.1).
fn random_trailer() -> Vec<u8> {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut bytes = [0u8; 32];
    crate::util::fill_random(&mut bytes);
    // Repeated division of the 256-bit big-endian number by 62.
    let mut num = bytes.to_vec();
    let mut out = Vec::with_capacity(44);
    while num.iter().any(|b| *b != 0) {
        let mut rem: u32 = 0;
        for b in num.iter_mut() {
            let acc = (rem << 8) | u32::from(*b);
            *b = (acc / 62) as u8;
            rem = acc % 62;
        }
        out.push(ALPHABET[rem as usize]);
    }
    out.reverse();
    out
}
