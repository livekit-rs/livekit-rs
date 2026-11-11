//! Permissions and token grants in engine terms. The adapter parses JWTs and fills
//! [`Grants`]; the engine only enforces them.

use std::collections::BTreeMap;

use crate::model::{ParticipantKind, TrackSource};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantPermission {
    pub can_subscribe: bool,
    pub can_publish: bool,
    pub can_publish_data: bool,
    /// Empty means every source is allowed.
    pub can_publish_sources: Vec<TrackSource>,
    pub hidden: bool,
    pub recorder: bool,
    pub can_update_metadata: bool,
    pub agent: bool,
    pub can_subscribe_metrics: bool,
    pub can_manage_agent_session: bool,
}

impl Default for ParticipantPermission {
    fn default() -> Self {
        Self {
            can_subscribe: true,
            can_publish: true,
            can_publish_data: true,
            can_publish_sources: Vec::new(),
            hidden: false,
            recorder: false,
            can_update_metadata: false,
            agent: false,
            can_subscribe_metrics: false,
            can_manage_agent_session: false,
        }
    }
}

impl ParticipantPermission {
    pub fn can_publish_source(&self, source: TrackSource) -> bool {
        if !self.can_publish {
            return false;
        }
        self.can_publish_sources.is_empty() || self.can_publish_sources.contains(&source)
    }
}

/// Video grant of an access token. `Option<bool>` distinguishes "unset" from an
/// explicit `false`; upstream defaults unset publish/subscribe/data to `true`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VideoGrant {
    pub room_create: bool,
    pub room_list: bool,
    pub room_record: bool,
    pub room_admin: bool,
    pub room_join: bool,
    pub room: String,
    pub destination_room: String,
    pub can_publish: Option<bool>,
    pub can_subscribe: Option<bool>,
    pub can_publish_data: Option<bool>,
    /// Original JWT source strings. Public enum permission and exact publication
    /// admission deliberately have different projections of these bytes.
    pub can_publish_sources: Vec<String>,
    pub can_update_own_metadata: Option<bool>,
    pub ingress_admin: bool,
    pub hidden: bool,
    pub recorder: bool,
    pub agent: bool,
    pub can_subscribe_metrics: Option<bool>,
    pub can_manage_agent_session: Option<bool>,
}

impl VideoGrant {
    /// Go VideoGrant.GetCanPublishSource: exact raw string vs canonical name.
    /// This is not the case-folded public ParticipantPermission projection.
    pub fn can_publish_source(&self, source: TrackSource) -> bool {
        self.can_publish.unwrap_or(true)
            && (self.can_publish_sources.is_empty()
                || self
                    .can_publish_sources
                    .iter()
                    .any(|s| s.as_str() == canonical_source(source).as_ref()))
    }

    /// Permission a joining participant receives from this grant.
    pub fn to_permission(&self) -> ParticipantPermission {
        let can_publish = self.can_publish.unwrap_or(true);
        ParticipantPermission {
            can_subscribe: self.can_subscribe.unwrap_or(true),
            can_publish,
            // Upstream: unset canPublishData follows GetCanPublish().
            can_publish_data: self.can_publish_data.unwrap_or(can_publish),
            can_publish_sources: self
                .can_publish_sources
                .iter()
                .map(|s| source_permission(s))
                .collect(),
            hidden: self.hidden,
            recorder: self.recorder,
            can_update_metadata: self.can_update_own_metadata.unwrap_or(false),
            agent: self.agent,
            can_subscribe_metrics: self.can_subscribe_metrics.unwrap_or(false),
            can_manage_agent_session: self.can_manage_agent_session.unwrap_or(false),
        }
    }
}

/// Verified token contents.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Grants {
    pub api_key: String,
    pub identity: String,
    pub name: String,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub video: VideoGrant,
    pub kind: ParticipantKind,
    pub sha256: String,
    pub room_preset: String,
    /// Opaque room configuration carried by the token (adapter-specific encoding).
    pub room_config: Option<RoomConfig>,
    /// Token expiry, unix seconds.
    pub expires_at: i64,
}

/// Protocol-independent specification for dispatching an agent into a new room.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomAgentDispatchOptions {
    pub agent_name: String,
    pub metadata: String,
    pub restart_policy: i32,
    pub deployment: String,
    pub attributes: BTreeMap<String, String>,
}

/// Room options a token may carry (`roomConfig`) or CreateRoom may set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomConfig {
    /// Protocol search tags preserved across configuration and JWT projection.
    /// Fixed standalone named-preset allocation does not inherit these tags,
    /// and the standalone owner has no tag-search/index consumer.
    pub tags: BTreeMap<String, String>,
    /// Owner-local automatic recording, independent of the external protocol.
    pub automatic_recording: Option<crate::auto_egress::AutomaticRecording>,
    pub empty_timeout_s: Option<u32>,
    pub departure_timeout_s: Option<u32>,
    pub max_participants: Option<u32>,
    pub metadata: Option<String>,
    pub min_playout_delay: Option<u32>,
    pub max_playout_delay: Option<u32>,
    pub sync_streams: Option<bool>,
    /// None leaves preset defaults available; an empty list still creates the
    /// upstream unnamed default dispatch when a room owner starts.
    pub agent_dispatches: Option<Vec<RoomAgentDispatchOptions>>,
}

impl RoomConfig {
    /// Fixed allocator defaults only unset values. Whole Egress pointer presence
    /// suppresses every preset child, including an explicit empty configuration.
    pub fn inherit(&mut self, p: &Self) {
        self.empty_timeout_s = self.empty_timeout_s.or(p.empty_timeout_s);
        self.departure_timeout_s = self.departure_timeout_s.or(p.departure_timeout_s);
        self.max_participants = self.max_participants.or(p.max_participants);
        self.metadata = self.metadata.take().or_else(|| p.metadata.clone());
        self.min_playout_delay = self.min_playout_delay.or(p.min_playout_delay);
        self.max_playout_delay = self.max_playout_delay.or(p.max_playout_delay);
        self.sync_streams = self.sync_streams.or(p.sync_streams);
        self.agent_dispatches = self
            .agent_dispatches
            .take()
            .or_else(|| p.agent_dispatches.clone());
        self.automatic_recording = self
            .automatic_recording
            .take()
            .or_else(|| p.automatic_recording.clone());
    }
}

#[cfg(test)]
mod automatic_recording_inheritance_tests {
    use super::*;
    use crate::auto_egress::*;
    #[test]
    fn whole_egress_pointer_is_inherited_only_when_absent() {
        let preset = RoomConfig {
            automatic_recording: Some(AutomaticRecording {
                room: Some(Default::default()),
                participant: Some(Default::default()),
                tracks: Some(Default::default()),
            }),
            metadata: Some("preset".into()),
            ..Default::default()
        };
        let mut absent = RoomConfig::default();
        absent.inherit(&preset);
        assert_eq!(absent.automatic_recording, preset.automatic_recording);
        let mut empty = RoomConfig {
            automatic_recording: Some(Default::default()),
            ..Default::default()
        };
        empty.inherit(&preset);
        assert_eq!(
            empty.automatic_recording,
            Some(AutomaticRecording::default())
        );
        assert_eq!(empty.metadata, Some("preset".into()));
        let mut partial = RoomConfig {
            automatic_recording: Some(AutomaticRecording {
                tracks: Some(Default::default()),
                ..Default::default()
            }),
            ..Default::default()
        };
        partial.inherit(&preset);
        let value = partial.automatic_recording.unwrap();
        assert!(value.tracks.is_some());
        assert!(value.room.is_none());
        assert!(value.participant.is_none());
    }
}

#[cfg(test)]
mod room_tag_source_boundaries {
    use super::*;
    #[test]
    fn named_preset_inheritance_does_not_invent_upstream_tag_defaults() {
        let preset = RoomConfig {
            tags: BTreeMap::from([("preset".into(), "value".into())]),
            ..Default::default()
        };
        let mut request = RoomConfig::default();
        request.inherit(&preset);
        assert!(request.tags.is_empty());
        request.tags.insert("request".into(), "kept".into());
        request.inherit(&preset);
        assert_eq!(
            request.tags,
            BTreeMap::from([("request".into(), "kept".into())])
        );
    }
}

/// Fixed sourceToString for the modeled neutral TrackSource enum.
pub fn canonical_source(source: TrackSource) -> std::borrow::Cow<'static, str> {
    use std::borrow::Cow;
    match source.as_i32() {
        0 => Cow::Borrowed("unknown"),
        1 => Cow::Borrowed("camera"),
        2 => Cow::Borrowed("microphone"),
        3 => Cow::Borrowed("screen_share"),
        4 => Cow::Borrowed("screen_share_audio"),
        value => Cow::Owned(value.to_string()),
    }
}

/// Fixed sourceToProto uses strings.ToUpper before the enum-name lookup.
/// The original Go CaseRanges ASCII preimage has exactly two non-ASCII
/// scalars: dotless i and long s. The source-bound generator verifies that
/// finite preimage; this helper is not a general Unicode uppercaser.
fn source_permission(raw: &str) -> TrackSource {
    let upper: String = raw
        .chars()
        .map(|c| match c {
            '\u{0131}' => 'I',
            '\u{017f}' => 'S',
            c => c.to_ascii_uppercase(),
        })
        .collect();
    match upper.as_str() {
        "CAMERA" => TrackSource::Camera,
        "MICROPHONE" => TrackSource::Microphone,
        "SCREEN_SHARE" => TrackSource::ScreenShare,
        "SCREEN_SHARE_AUDIO" => TrackSource::ScreenShareAudio,
        _ => TrackSource::Unknown,
    }
}
