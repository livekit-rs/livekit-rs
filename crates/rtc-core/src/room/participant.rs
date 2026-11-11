//! Per-participant room state.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use crate::ids::*;
use crate::media_api::PcRole;
use crate::model::*;
use crate::permission::Grants;
use crate::session::{AddTrack, ClientCapabilities, ClientMeta};

/// Remembered cids of released pending tracks (late media for them is dropped).
pub(crate) const RELEASED_CID_MEMORY: usize = 32;

/// A publication whose SDP section is bound but which is not yet visible to the room.
///
/// Fixed Go v1.13.10 publishes a track from pion `OnTrack` (`onMediaTrack` → `handleTrackPublished`).
/// The room manager sets `FireOnTrackBySdp`, so for an SDP receiver with a declared SSRC and no RID
/// `OnTrack` fires from the SDP — but pion queues `startRTP` behind `startTransports`, which blocks until
/// ICE and DTLS are established on the first negotiation. Simulcast (RID) tracks and sections without a
/// declared SSRC are not covered by that setting and wait for their first RTP. Until then other
/// participants must not see the track (otherwise a publisher whose media never flows shows a track that
/// never delivers media, which fixed Go never exposes).
#[derive(Clone, Debug)]
pub(crate) struct DeferredPublication {
    /// The original AddTrack request and info, for release on cancellation.
    pub pending: PendingTrack,
    /// The bound info (mid, mime, rids) to publish.
    pub info: TrackInfo,
    pub sdp_registered: bool,
    /// True when only the first RTP releases it (RID or undeclared SSRC).
    pub needs_rtp: bool,
}

/// Track announced by `AddTrack` whose media has not been matched yet.
#[derive(Clone, Debug)]
pub(crate) struct PendingTrack {
    pub request: AddTrack,
    pub info: TrackInfo,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Subscription {
    /// Explicit source desired state, independent of downtrack admission.
    pub desired: bool,
    pub attempt_reported: bool,
    pub event_reported: bool,
    /// Settings-only undesired entries survive until this source keepUntil.
    pub keep_until: Option<Instant>,
    /// Real engine removal acknowledgement outstanding; never admit a replacement early.
    pub closing: bool,
    /// Unresolved explicit intent clock, preserved across resolver retries.
    pub desired_since: Option<Instant>,
    pub prepared: bool,
    /// Media layer created the down track.
    pub requested: bool,
    pub bound: bool,
    /// Last allowed value reported to the subscriber; `None` = never reported.
    pub reported_allowed: Option<bool>,
}

#[derive(Debug)]
pub(crate) struct DataSubscription {
    pub handle: Option<u16>,
    pub target_fps: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct Participant {
    pub info: ParticipantInfo,
    /// Info changed since the last snapshot; the next snapshot bumps `version`.
    pub dirty: bool,
    pub grants: Grants,
    pub client: ClientMeta,
    pub capabilities: ClientCapabilities,
    pub auto_subscribe: bool,
    pub auto_subscribe_data_track: bool,
    pub data_subscriptions: BTreeMap<TrackSid, DataSubscription>,
    pub next_data_handle: u16,
    pub data_blobs: Vec<DataBlob>,
    pub adaptive_stream: bool,
    pub transports: Vec<(TransportId, PcRole)>,
    pub primary: TransportId,
    /// Current physical transports whose native PC state has left New. Retained
    /// across ICE restart/resume; a new participant/transport gets a fresh set.
    pub established_transports: HashSet<TransportId>,

    pub pending: Vec<PendingTrack>,
    /// AddTracks waiting for an earlier publication with the same cid.
    pub queued: HashMap<String, VecDeque<AddTrack>>,
    pub released_cids: VecDeque<Vec<String>>,
    /// Remote media not yet matched to an AddTrack.
    pub parked_media: Vec<crate::media_api::RemoteMedia>,
    /// Bound publications waiting for fixed Go's OnTrack moment (DTLS or first RTP).
    pub deferred_publications: Vec<DeferredPublication>,

    pub subscription_permission: Option<SubscriptionPermission>,
    /// Subscriptions this participant wants (track → state).
    pub subscriptions: BTreeMap<TrackSid, Subscription>,
    /// Original per-participant subscription-manager three-second ticker.
    pub next_subscription_reconcile: Instant,
    pub track_settings: HashMap<TrackSid, TrackSettings>,
    /// Highest ParticipantInfo version this recipient received per participant.
    pub sent_versions: HashMap<ParticipantSid, u32>,
    /// Join/Reconnect response delivered; other signals may flow.
    pub ready: bool,
    /// ParticipantUpdates generated before `ready`.
    pub queued_updates: BTreeMap<ParticipantSid, ParticipantInfo>,
    pub generation: SessionGeneration,
    pub signal_connected: bool,
    pub signal_closed_at: Option<Instant>,
    /// Publisher-side: tracks that already announced their first subscriber.
    pub track_subscribed_sent: HashSet<TrackSid>,
    pub last_reliable_seq: u32,
    /// Source per-participant joining queue: encoded unsequenced reliable bytes.
    pub joining_reliable: VecDeque<Vec<u8>>,
    pub joining_reliable_bytes: usize,
    /// Latched after source fully-established, not ACTIVE or a signal resume.
    pub can_write_reliable: bool,
    pub joining_replay_in_progress: bool,
    /// Primary transport has connected at least once (participant reached ACTIVE).
    pub ever_active: bool,
    pub active_at: Option<Instant>,
    /// Any transport connected at least once (ICE established).
    pub ice_connected_once: bool,
    pub session_started_at: Instant,
    pub join_deadline: Instant,
    pub transport_failed_at: Option<Instant>,
    pub media_recovery_epoch: u64,
    pub signal_rtt_ms: u32,
    pub fallback: crate::transport_fallback::TransportFallback,
    pub media_rtt: super::media_rtt::MediaRtt,
    /// `simulate disconnect_signal_on_resume`: drop the next resumed signal.
    pub disconnect_on_resume: bool,
    pub last_quality: Option<ConnectionQuality>,
}

impl Participant {
    pub fn sid(&self) -> &ParticipantSid {
        &self.info.sid
    }

    pub fn is_hidden(&self) -> bool {
        self.info.permission.hidden
    }

    /// Agents, egress and recorders neither count towards `max_participants` nor
    /// keep a room alive.
    pub fn is_dependent(&self) -> bool {
        matches!(
            self.info.kind,
            ParticipantKind::Agent | ParticipantKind::Egress
        ) || self.info.permission.agent
            || self.info.permission.recorder
            || self.capabilities.one_shot
    }

    pub fn is_agent(&self) -> bool {
        self.info.kind == ParticipantKind::Agent || self.info.permission.agent
    }

    pub fn is_active(&self) -> bool {
        self.info.state == ParticipantState::Active
    }

    pub fn is_disconnected(&self) -> bool {
        self.info.state == ParticipantState::Disconnected
    }

    pub fn transport(&self, role: PcRole) -> Option<TransportId> {
        self.transports
            .iter()
            .find(|(_, r)| *r == role)
            .map(|(t, _)| *t)
    }

    /// Transport that receives the client's offers and published media.
    pub fn publish_transport(&self) -> TransportId {
        self.transport(PcRole::Single)
            .or_else(|| self.transport(PcRole::Publisher))
            .unwrap_or(self.primary)
    }

    pub fn has_transport(&self, id: TransportId) -> bool {
        self.transports.iter().any(|(t, _)| *t == id)
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Snapshot for signaling: bumps the version once per batch of changes.
    pub fn snapshot(&mut self) -> ParticipantInfo {
        if self.dirty {
            self.info.version = self.info.version.wrapping_add(1);
            self.dirty = false;
        }
        self.info.clone()
    }

    /// Monotonic state transition; lower or equal states are ignored.
    pub fn set_state(&mut self, state: ParticipantState) -> bool {
        let rank = |s: ParticipantState| match s {
            ParticipantState::Joining => 0,
            ParticipantState::Joined => 1,
            ParticipantState::Active => 2,
            ParticipantState::Disconnected => 3,
        };
        if rank(state) <= rank(self.info.state) {
            return false;
        }
        self.info.state = state;
        self.mark_dirty();
        true
    }

    pub fn published(&self, sid: &TrackSid) -> Option<&TrackInfo> {
        self.info.tracks.iter().find(|t| &t.sid == sid)
    }

    pub fn published_mut(&mut self, sid: &TrackSid) -> Option<&mut TrackInfo> {
        self.info.tracks.iter_mut().find(|t| &t.sid == sid)
    }

    pub fn remember_released_cids(&mut self, mut cids: Vec<String>) {
        cids.retain(|cid| !cid.is_empty());
        cids.sort();
        cids.dedup();
        if cids.is_empty() {
            return;
        }
        if self.released_cids.len() >= RELEASED_CID_MEMORY {
            self.released_cids.pop_front();
        }
        self.released_cids.push_back(cids);
    }

    pub fn is_released_cid(&self, cid: &str) -> bool {
        self.released_cids
            .iter()
            .any(|group| group.iter().any(|released| released == cid))
    }

    /// Whether `subscriber` may receive `track` under this publisher's
    /// subscription permission. Never set ⇒ everyone.
    pub fn allows_subscriber(
        &self,
        track: &TrackSid,
        subscriber_sid: &ParticipantSid,
        subscriber_identity: &Identity,
    ) -> bool {
        let Some(perm) = &self.subscription_permission else {
            return true;
        };
        if perm.all_participants {
            return true;
        }
        perm.track_permissions.iter().any(|tp| {
            let who = tp.participant_identity.as_ref() == Some(subscriber_identity)
                || tp.participant_sid.as_ref() == Some(subscriber_sid);
            who && (tp.all_tracks || tp.track_sids.contains(track))
        })
    }
}
