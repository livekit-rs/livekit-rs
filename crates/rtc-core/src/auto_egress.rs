//! Protocol-neutral automatic recording policy and generation-owned publication
//! authorization. This module never interprets protobuf bytes, calls workers or
//! treats a launch request as recording success.
use crate::ids::{Identity, ParticipantSid, RoomSid, TrackSid};
use crate::model::ParticipantKind;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Exact floating point configuration payload without making RoomConfig's Eq
/// depend on IEEE floating-point comparison (including signed zero).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FloatBits(pub u64);
impl FloatBits {
    pub fn from_value(value: f64) -> Self {
        Self(value.to_bits())
    }
    pub fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct UploadProxy {
    pub url: String,
    pub username: String,
    pub password: String,
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct S3Destination {
    pub access_key: String,
    pub secret: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub force_path_style: bool,
    pub metadata: BTreeMap<String, String>,
    pub tagging: String,
    pub content_disposition: String,
    pub proxy: Option<UploadProxy>,
    pub session_token: String,
    pub assume_role_arn: String,
    pub assume_role_external_id: String,
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct GcpDestination {
    pub credentials: String,
    pub bucket: String,
    pub proxy: Option<UploadProxy>,
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AzureDestination {
    pub account_name: String,
    pub account_key: String,
    pub container_name: String,
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AliOssDestination {
    pub access_key: String,
    pub secret: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
}
#[derive(Clone, PartialEq, Eq)]
pub enum UploadDestination {
    S3(Box<S3Destination>),
    Gcp(GcpDestination),
    Azure(AzureDestination),
    AliOss(AliOssDestination),
}
impl std::fmt::Debug for UploadDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::S3(_) => "S3(<redacted>)",
            Self::Gcp(_) => "Gcp(<redacted>)",
            Self::Azure(_) => "Azure(<redacted>)",
            Self::AliOss(_) => "AliOss(<redacted>)",
        })
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvancedEncoding {
    pub width: i32,
    pub height: i32,
    pub depth: i32,
    pub framerate: i32,
    pub audio_codec: i32,
    pub audio_bitrate: i32,
    pub audio_frequency: i32,
    pub video_codec: i32,
    pub video_bitrate: i32,
    pub key_frame_interval: FloatBits,
    pub audio_quality: i32,
    pub video_quality: i32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordingEncoding {
    Preset(i32),
    Advanced(AdvancedEncoding),
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EncodedFilePolicy {
    pub file_type: i32,
    pub filepath: String,
    pub disable_manifest: bool,
    pub destination: Option<UploadDestination>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SegmentedFilePolicy {
    pub protocol: i32,
    pub filename_prefix: String,
    pub playlist_name: String,
    pub segment_duration: u32,
    pub disable_manifest: bool,
    pub filename_suffix: i32,
    pub live_playlist_name: String,
    pub destination: Option<UploadDestination>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParticipantRecordingPolicy {
    pub encoding: Option<RecordingEncoding>,
    pub files: Vec<EncodedFilePolicy>,
    pub segments: Vec<SegmentedFilePolicy>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrackRecordingPolicy {
    pub filepath: String,
    /// Retain configuration for token/preset roundtrip. The fixed source's
    /// auto-track request builder does NOT copy this to DirectFileOutput.
    pub disable_manifest: bool,
    pub destination: Option<UploadDestination>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamRecordingPolicy {
    pub protocol: i32,
    pub urls: Vec<String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImageRecordingPolicy {
    pub capture_interval: u32,
    pub width: i32,
    pub height: i32,
    pub filename_prefix: String,
    pub filename_suffix: i32,
    pub image_codec: i32,
    pub disable_manifest: bool,
    pub destination: Option<UploadDestination>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordingEventFilter {
    pub include_events: Vec<String>,
    pub exclude_events: Vec<String>,
}
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RecordingWebhook {
    pub url: String,
    pub signing_key: String,
    pub filter: Option<RecordingEventFilter>,
}
impl std::fmt::Debug for RecordingWebhook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingWebhook")
            .field("url", &self.url)
            .field("signing_key", &"[REDACTED]")
            .field("filter", &self.filter)
            .finish()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompositeLegacyOutput {
    File(EncodedFilePolicy),
    Stream(StreamRecordingPolicy),
    Segments(SegmentedFilePolicy),
}
/// Create-only full composite policy; the runtime holds the actual generation
/// while awaiting its adapter launcher. No protobuf type or bytes enter core.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomCompositeRecordingPolicy {
    pub room_name: String,
    pub layout: String,
    pub audio_only: bool,
    pub audio_mixing: i32,
    pub video_only: bool,
    pub custom_base_url: String,
    pub legacy_output: Option<CompositeLegacyOutput>,
    pub encoding: Option<RecordingEncoding>,
    pub files: Vec<EncodedFilePolicy>,
    pub streams: Vec<StreamRecordingPolicy>,
    pub segments: Vec<SegmentedFilePolicy>,
    pub images: Vec<ImageRecordingPolicy>,
    pub webhooks: Vec<RecordingWebhook>,
}
/// This is RoomInternal's recording subset. RoomComposite is a create-only
/// request and must be resolved separately before obtaining the actual owner.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AutomaticRecording {
    pub room: Option<RoomCompositeRecordingPolicy>,
    pub participant: Option<ParticipantRecordingPolicy>,
    pub tracks: Option<TrackRecordingPolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordingLaunch {
    Participant {
        room_sid: RoomSid,
        participant_sid: ParticipantSid,
        identity: Identity,
        policy: ParticipantRecordingPolicy,
    },
    Track {
        room_sid: RoomSid,
        participant_sid: ParticipantSid,
        identity: Identity,
        track_sid: TrackSid,
        policy: TrackRecordingPolicy,
    },
}
struct Publisher {
    sid: ParticipantSid,
    has_published: bool,
    tracks: HashSet<TrackSid>,
}
/// Exactly one room generation owns this object; never a name-only global map.
/// Attempts are marked synchronously before the returned launch is scheduled.
/// A failure does not undo dedupe; leave/replacement/room close clear ownership.
/// No retry/result queue is hidden here: the runtime must consume both returned
/// launches reliably and route actual failure events through the adapter.
pub struct RecordingLifecycle {
    room_sid: RoomSid,
    policy: AutomaticRecording,
    publishers: HashMap<Identity, Publisher>,
    closed: bool,
}
impl RecordingLifecycle {
    pub fn new(room_sid: RoomSid, policy: AutomaticRecording) -> Self {
        Self {
            room_sid,
            policy,
            publishers: HashMap::new(),
            closed: false,
        }
    }
    /// Invoke on actual owner participant registration, not a stale publication
    /// or a telemetry sink. Resume of the same SID retains prior attempt state.
    pub fn registered(&mut self, room_sid: &RoomSid, identity: Identity, sid: ParticipantSid) {
        if self.closed || room_sid != &self.room_sid {
            return;
        }
        if self.publishers.get(&identity).is_some_and(|p| p.sid == sid) {
            return;
        }
        self.publishers.insert(
            identity,
            Publisher {
                sid,
                has_published: false,
                tracks: HashSet::new(),
            },
        );
    }
    /// Call only after actual media publication, not AddTrack/TrackPublished
    /// acknowledgement and not data-track publication. At most two actions.
    pub fn published(
        &mut self,
        room_sid: &RoomSid,
        identity: &Identity,
        sid: &ParticipantSid,
        kind: ParticipantKind,
        track_sid: TrackSid,
    ) -> Vec<RecordingLaunch> {
        if self.closed || room_sid != &self.room_sid {
            return Vec::new();
        }
        let Some(p) = self.publishers.get_mut(identity).filter(|p| &p.sid == sid) else {
            return Vec::new();
        };
        let mut actions = Vec::with_capacity(2);
        if !p.has_published {
            p.has_published = true;
            if let Some(policy) = &self.policy.participant {
                actions.push(RecordingLaunch::Participant {
                    room_sid: self.room_sid.clone(),
                    participant_sid: sid.clone(),
                    identity: identity.clone(),
                    policy: policy.clone(),
                });
            }
        }
        // Upstream excludes EGRESS only for auto-track. Do not accidentally
        // apply that predicate to the independent auto-participant branch.
        if kind != ParticipantKind::Egress
            && let Some(policy) = &self.policy.tracks
            && p.tracks.insert(track_sid.clone())
        {
            actions.push(RecordingLaunch::Track {
                room_sid: self.room_sid.clone(),
                participant_sid: sid.clone(),
                identity: identity.clone(),
                track_sid,
                policy: policy.clone(),
            });
        }
        actions
    }
    pub fn left(&mut self, room_sid: &RoomSid, identity: &Identity, sid: &ParticipantSid) {
        if room_sid != &self.room_sid {
            return;
        }
        if self.publishers.get(identity).is_some_and(|p| &p.sid == sid) {
            self.publishers.remove(identity);
        }
    }
    pub fn closed(&mut self, room_sid: &RoomSid) {
        if room_sid == &self.room_sid {
            self.closed = true;
            self.publishers.clear();
        }
    }
}
/// Exact fixed Go rtc/egress.go getFilePath: first byte '.' in the whole path,
/// not filepath.Ext or the final basename. Known awkward directory-dot behavior
/// remains observable and must not be silently corrected by this compatibility layer.
pub fn auto_track_filepath(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') || path.contains("{track_id}") {
        return path.into();
    }
    match path.find('.') {
        Some(i) => format!("{}-{{track_id}}{}", &path[..i], &path[i..]),
        None => format!("{path}-{{track_id}}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ids() -> (RoomSid, Identity, ParticipantSid) {
        ("RM_old".into(), "publisher".into(), "PA_old".into())
    }
    fn policy() -> AutomaticRecording {
        AutomaticRecording {
            participant: Some(Default::default()),
            tracks: Some(Default::default()),
            ..Default::default()
        }
    }
    #[test]
    fn attempts_only_after_registered_actual_publication_and_deduped_even_after_failure() {
        let (room, id, sid) = ids();
        let mut life = RecordingLifecycle::new(room.clone(), policy());

        assert!(
            life.published(&room, &id, &sid, ParticipantKind::Standard, "TR_a".into())
                .is_empty()
        );
        life.registered(&room, id.clone(), sid.clone());
        let first = life.published(&room, &id, &sid, ParticipantKind::Standard, "TR_a".into());
        assert_eq!(first.len(), 2);
        assert!(
            life.published(&room, &id, &sid, ParticipantKind::Standard, "TR_a".into())
                .is_empty()
        );
        // Returning an actual RPC error must not reset source attempt state.
        let next = life.published(&room, &id, &sid, ParticipantKind::Standard, "TR_b".into());
        assert!(
            matches!(next.as_slice(),[RecordingLaunch::Track{track_sid,..}]if track_sid.0=="TR_b")
        );
        life.registered(&room, id.clone(), sid.clone()); // resume preserves attempts
        assert!(
            life.published(&room, &id, &sid, ParticipantKind::Standard, "TR_b".into())
                .is_empty()
        );
    }
    #[test]
    fn replacement_and_stale_leave_are_sid_fenced_not_name_only() {
        let (room, id, old) = ids();
        let new: ParticipantSid = "PA_new".into();
        let mut life = RecordingLifecycle::new(room.clone(), policy());
        life.registered(&room, id.clone(), old.clone());
        assert_eq!(
            life.published(&room, &id, &old, ParticipantKind::Standard, "TR_a".into())
                .len(),
            2
        );
        life.registered(&room, id.clone(), new.clone());
        life.left(&room, &id, &old);
        assert!(
            life.published(&room, &id, &old, ParticipantKind::Standard, "TR_a".into())
                .is_empty()
        );
        assert_eq!(
            life.published(&room, &id, &new, ParticipantKind::Standard, "TR_a".into())
                .len(),
            2
        );
        life.left(&room, &id, &new);
        assert!(
            life.published(&room, &id, &new, ParticipantKind::Standard, "TR_b".into())
                .is_empty()
        );
    }
    #[test]
    fn egress_exclusion_is_only_track_and_other_room_cannot_clear() {
        let (room, id, sid) = ids();
        let mut life = RecordingLifecycle::new(room.clone(), policy());
        life.registered(&room, id.clone(), sid.clone());
        let actions = life.published(&room, &id, &sid, ParticipantKind::Egress, "TR_a".into());
        assert!(matches!(
            actions.as_slice(),
            [RecordingLaunch::Participant { .. }]
        ));
        life.closed(&RoomSid("RM_new".into()));
        assert!(
            life.published(&room, &id, &sid, ParticipantKind::Egress, "TR_b".into())
                .is_empty()
        );
        life.closed(&room);
        life.registered(&room, id.clone(), sid.clone());
        assert!(
            life.published(&room, &id, &sid, ParticipantKind::Standard, "TR_b".into())
                .is_empty()
        );
    }
    #[test]
    fn filepath_uses_fixed_source_first_dot_and_preserves_utf8() {
        for (input, output) in [
            ("", ""),
            ("dir/", "dir/"),
            ("{track_id}.ogg", "{track_id}.ogg"),
            ("x", "x-{track_id}"),
            ("x.ogg", "x-{track_id}.ogg"),
            ("dir.with.dot/x", "dir-{track_id}.with.dot/x"),
            ("é.ogg", "é-{track_id}.ogg"),
        ] {
            assert_eq!(auto_track_filepath(input), output);
        }
    }
    #[test]
    fn config_keeps_optional_zero_and_sensitive_debug_redacts() {
        assert_ne!(
            AutomaticRecording::default(),
            AutomaticRecording {
                tracks: Some(Default::default()),
                ..Default::default()
            }
        );
        assert_ne!(None, Some(RecordingEncoding::Preset(0)));
        let value = UploadDestination::Gcp(GcpDestination {
            credentials: "sensitive-test".into(),
            ..Default::default()
        });
        assert!(!format!("{value:?}").contains("sensitive-test"));
        let negative_zero = FloatBits::from_value(-0.0);
        assert_eq!(negative_zero.value().to_bits(), (-0.0f64).to_bits());
    }
}
