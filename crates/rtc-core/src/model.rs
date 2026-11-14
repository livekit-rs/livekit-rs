//! Protocol-agnostic domain model. Field sets mirror what the LiveKit adapter needs,
//! but nothing here depends on generated wire types (ARCH-01).

use std::collections::BTreeMap;

use crate::ids::*;
use crate::permission::ParticipantPermission;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrackKind {
    Audio,
    Video,
    Data,
    /// Open protocol type, retained until the owner checks AUDIO/VIDEO.
    Unrecognized(UnrecognizedTrackKind),
}

/// Private inner value prevents a second representation of known0..2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnrecognizedTrackKind(i32);

impl TrackKind {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Audio,
            1 => Self::Video,
            2 => Self::Data,
            value => Self::Unrecognized(UnrecognizedTrackKind(value)),
        }
    }

    pub fn as_i32(self) -> i32 {
        match self {
            Self::Audio => 0,
            Self::Video => 1,
            Self::Data => 2,
            Self::Unrecognized(value) => value.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum TrackSource {
    #[default]
    Unknown,
    Camera,
    Microphone,
    ScreenShare,
    ScreenShareAudio,
    /// Open protocol values outside the five currently modeled source names.
    /// Use from_i32: the opaque inner value cannot alias a known variant.
    Unrecognized(UnrecognizedTrackSource),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnrecognizedTrackSource(i32);

impl TrackSource {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Unknown,
            1 => Self::Camera,
            2 => Self::Microphone,
            3 => Self::ScreenShare,
            4 => Self::ScreenShareAudio,
            other => Self::Unrecognized(UnrecognizedTrackSource(other)),
        }
    }
    pub fn as_i32(self) -> i32 {
        match self {
            Self::Unknown => 0,
            Self::Camera => 1,
            Self::Microphone => 2,
            Self::ScreenShare => 3,
            Self::ScreenShareAudio => 4,
            Self::Unrecognized(value) => value.0,
        }
    }
}
impl Ord for TrackSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_i32().cmp(&other.as_i32())
    }
}
impl PartialOrd for TrackSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Open protobuf quality. Comparisons use the original signed int32 value,
/// including values unknown to this server, as in fixed Go's enum consumers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VideoQuality {
    Low,
    Medium,
    High,
    Off,
    Unrecognized(UnrecognizedVideoQuality),
}

/// Canonical unknown value: known numbers cannot alias this variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnrecognizedVideoQuality(i32);

impl Ord for VideoQuality {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_i32().cmp(&other.as_i32())
    }
}
impl PartialOrd for VideoQuality {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl VideoQuality {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Low,
            1 => Self::Medium,
            2 => Self::High,
            3 => Self::Off,
            value => Self::Unrecognized(UnrecognizedVideoQuality(value)),
        }
    }
    pub fn as_i32(self) -> i32 {
        match self {
            Self::Low => 0,
            Self::Medium => 1,
            Self::High => 2,
            Self::Off => 3,
            Self::Unrecognized(value) => value.0,
        }
    }
    pub fn spatial_index(self) -> Option<usize> {
        match self {
            VideoQuality::Low => Some(0),
            VideoQuality::Medium => Some(1),
            VideoQuality::High => Some(2),
            VideoQuality::Off | VideoQuality::Unrecognized(_) => None,
        }
    }

    pub fn from_spatial_index(i: usize) -> VideoQuality {
        match i {
            0 => VideoQuality::Low,
            1 => VideoQuality::Medium,
            _ => VideoQuality::High,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VideoLayerMode {
    #[default]
    Unused,
    OneSpatialLayerPerStream,
    MultipleSpatialLayersPerStream,
    OneSpatialLayerPerStreamIncompleteRtcpSr,
    Unrecognized(UnrecognizedVideoLayerMode),
}

/// A signed protobuf mode that does not name a known layer layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnrecognizedVideoLayerMode(i32);

impl VideoLayerMode {
    pub const fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Unused,
            1 => Self::OneSpatialLayerPerStream,
            2 => Self::MultipleSpatialLayersPerStream,
            3 => Self::OneSpatialLayerPerStreamIncompleteRtcpSr,
            other => Self::Unrecognized(UnrecognizedVideoLayerMode(other)),
        }
    }

    pub const fn as_i32(self) -> i32 {
        match self {
            Self::Unused => 0,
            Self::OneSpatialLayerPerStream => 1,
            Self::MultipleSpatialLayersPerStream => 2,
            Self::OneSpatialLayerPerStreamIncompleteRtcpSr => 3,
            Self::Unrecognized(value) => value.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VideoLayer {
    pub quality: VideoQuality,
    pub width: u32,
    pub height: u32,
    pub bitrate: u32,
    pub ssrc: u32,
    pub spatial_layer: i32,
    pub rid: String,
    pub repair_ssrc: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EncryptionType {
    #[default]
    None,
    Gcm,
    Custom,
    Unrecognized(UnrecognizedEncryptionType),
}

/// Canonical opaque protocol value; known enum numbers cannot alias this case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnrecognizedEncryptionType(i32);

impl EncryptionType {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Gcm,
            2 => Self::Custom,
            value => Self::Unrecognized(UnrecognizedEncryptionType(value)),
        }
    }

    pub fn as_i32(self) -> i32 {
        match self {
            Self::None => 0,
            Self::Gcm => 1,
            Self::Custom => 2,
            Self::Unrecognized(value) => value.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AudioFeature {
    Stereo,
    NoDtx,
    AutoGainControl,
    EchoCancellation,
    NoiseSuppression,
    EnhancedNoiseCancellation,
    PreconnectBuffer,
    Unrecognized(UnrecognizedAudioFeature),
}

/// Canonical opaque protocol value; known enum numbers cannot alias this case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnrecognizedAudioFeature(i32);

impl AudioFeature {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Stereo,
            1 => Self::NoDtx,
            2 => Self::AutoGainControl,
            3 => Self::EchoCancellation,
            4 => Self::NoiseSuppression,
            5 => Self::EnhancedNoiseCancellation,
            6 => Self::PreconnectBuffer,
            value => Self::Unrecognized(UnrecognizedAudioFeature(value)),
        }
    }

    pub fn as_i32(self) -> i32 {
        match self {
            Self::Stereo => 0,
            Self::NoDtx => 1,
            Self::AutoGainControl => 2,
            Self::EchoCancellation => 3,
            Self::NoiseSuppression => 4,
            Self::EnhancedNoiseCancellation => 5,
            Self::PreconnectBuffer => 6,
            Self::Unrecognized(value) => value.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BackupCodecPolicy {
    #[default]
    PreferRegression,
    Simulcast,
    Regression,
    Unrecognized(UnrecognizedBackupCodecPolicy),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnrecognizedBackupCodecPolicy(i32);

impl BackupCodecPolicy {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::PreferRegression,
            1 => Self::Simulcast,
            2 => Self::Regression,
            value => Self::Unrecognized(UnrecognizedBackupCodecPolicy(value)),
        }
    }

    pub fn as_i32(self) -> i32 {
        match self {
            Self::PreferRegression => 0,
            Self::Simulcast => 1,
            Self::Regression => 2,
            Self::Unrecognized(value) => value.0,
        }
    }
}

/// One codec of a published track (primary or backup simulcast codec).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TrackCodec {
    pub mime_type: String,
    pub mid: String,
    pub cid: String,
    pub sdp_cid: String,
    pub layers: Vec<VideoLayer>,
    pub layer_mode: VideoLayerMode,
}

/// Monotonic version as used by upstream `TimedVersion` (unix ns + ticks).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TimedVersion {
    pub unix_micro: i64,
    pub ticks: i32,
}

impl TimedVersion {
    pub fn now() -> Self {
        Self {
            unix_micro: crate::util::unix_ns() / 1_000,
            ticks: 0,
        }
    }

    /// Advance so the result is strictly greater than `self`.
    pub fn advance(&mut self) {
        let now = crate::util::unix_ns() / 1_000;
        if now > self.unix_micro {
            self.unix_micro = now;
            self.ticks = 0;
        } else {
            self.ticks = self.ticks.saturating_add(1);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TrackInfo {
    pub sid: TrackSid,
    pub kind: TrackKind,
    pub name: String,
    pub muted: bool,
    pub width: u32,
    pub height: u32,
    pub simulcast: bool,
    pub source: TrackSource,
    pub layers: Vec<VideoLayer>,
    pub mime_type: String,
    pub mid: String,
    pub codecs: Vec<TrackCodec>,
    pub disable_red: bool,
    pub disable_dtx: bool,
    pub stereo: bool,
    pub encryption: EncryptionType,
    pub stream: String,
    pub version: TimedVersion,
    pub audio_features: Vec<AudioFeature>,
    /// Raw signed feature numbers; native packet-trailer consumption is separate.
    pub packet_trailer_features: Vec<i32>,
    pub backup_codec_policy: BackupCodecPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ParticipantState {
    #[default]
    Joining,
    Joined,
    Active,
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ParticipantKind {
    #[default]
    Standard,
    Ingress,
    Egress,
    Sip,
    Agent,
    Connector,
    Bridge,
}

/// Why a participant left. Mirrors upstream `DisconnectReason` semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DisconnectReason {
    #[default]
    Unknown,
    ClientInitiated,
    DuplicateIdentity,
    ServerShutdown,
    ParticipantRemoved,
    RoomDeleted,
    StateMismatch,
    JoinFailure,
    Migration,
    SignalClose,
    RoomClosed,
    UserUnavailable,
    UserRejected,
    SipTrunkFailure,
    ConnectionTimeout,
    MediaFailure,
    AgentError,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantInfo {
    pub sid: ParticipantSid,
    pub identity: Identity,
    pub name: String,
    pub state: ParticipantState,
    pub tracks: Vec<TrackInfo>,
    /// Published raw data tracks, independent of audio/video RTP tracks.
    pub data_tracks: Vec<DataTrackInfo>,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub joined_at_ms: i64,
    pub version: u32,
    pub permission: ParticipantPermission,
    pub region: String,
    pub is_publisher: bool,
    pub kind: ParticipantKind,
    pub disconnect_reason: DisconnectReason,
    /// Protocol version the client announced; adapters gate behavior on it.
    pub client_protocol: i32,
    /// Raw `ClientInfo.Capability` enum values the client declared at connect,
    /// echoed back like upstream `ParticipantImpl.ToProtoWithVersion`.
    pub capabilities: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecSpec {
    pub mime: String,
    pub fmtp_line: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoomInfo {
    pub sid: RoomSid,
    pub name: RoomName,
    pub empty_timeout_s: u32,
    pub departure_timeout_s: u32,
    pub max_participants: u32,
    pub creation_time_ms: i64,
    pub metadata: String,
    pub num_participants: u32,
    pub num_publishers: u32,
    pub active_recording: bool,
    pub enabled_codecs: Vec<CodecSpec>,
    pub version: TimedVersion,
    /// `Room.turn_password`: `utils.RandomSecret()` fixed at room creation.
    pub turn_password: String,
}

/// Per-participant subscribe/publish speaker info for SpeakersChanged.
#[derive(Clone, Debug, PartialEq)]
pub struct SpeakerInfo {
    pub sid: ParticipantSid,
    pub level: f32,
    pub active: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionQuality {
    Poor,
    Good,
    Excellent,
    Lost,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionQualityInfo {
    pub participant_sid: ParticipantSid,
    pub quality: ConnectionQuality,
    pub score: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    Active,
    Paused,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamStateInfo {
    pub participant_sid: ParticipantSid,
    pub track_sid: TrackSid,
    pub state: StreamState,
}

/// Subscriber demand for one quality of one codec, sent to the publisher (dynacast).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscribedQuality {
    pub quality: VideoQuality,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscribedCodec {
    pub codec: String,
    pub qualities: Vec<SubscribedQuality>,
}

/// Subscriber's requested settings for a subscribed track (adaptive stream / manual).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackSettings {
    pub disabled: bool,
    pub quality: VideoQuality,
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub priority: u32,
}

impl Default for TrackSettings {
    fn default() -> Self {
        Self {
            disabled: false,
            quality: VideoQuality::High,
            width: 0,
            height: 0,
            fps: 0,
            priority: 0,
        }
    }
}

/// Publisher-side permission for who may subscribe to its tracks.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SubscriptionPermission {
    pub all_participants: bool,
    pub track_permissions: Vec<TrackPermission>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TrackPermission {
    pub participant_sid: Option<ParticipantSid>,
    pub participant_identity: Option<Identity>,
    pub all_tracks: bool,
    pub track_sids: Vec<TrackSid>,
}

/// Encoding oneof retained without restricting unknown well-known enum values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataTrackEncoding {
    Unset,
    WellKnown(i32),
    Custom(String),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataTrackSchema {
    pub name: String,
    pub encoding: Option<DataTrackEncoding>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DataTrackInfo {
    pub sid: TrackSid,
    pub pub_handle: u32,
    pub name: String,
    pub encryption: EncryptionType,
    pub frame_encoding: Option<DataTrackEncoding>,
    pub schema: Option<DataTrackSchema>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataTrackHandleInfo {
    pub publisher_identity: Identity,
    pub publisher_sid: ParticipantSid,
    pub track_sid: TrackSid,
}

impl TrackInfo {
    /// Apply actual receiver video dimensions (MediaTrackReceiver.UpdateVideoSize).
    /// This pure projection does not authorize a source or advance a track version;
    /// the room's generation/one-shot owner validates the incoming observation.
    pub fn apply_observed_video_sizes(&mut self, mime: &str, sizes: &[(u32, u32)]) -> bool {
        if self.kind != TrackKind::Video || sizes.len() > 3 {
            return false;
        }
        let (mut width, mut height) = (0, 0);
        for &(w, h) in sizes {
            if w > width {
                width = w;
                height = h;
            }
        }
        let mut changed = self.width != width || self.height != height;
        self.width = width;
        self.height = height;
        for codec in self
            .codecs
            .iter_mut()
            .filter(|c| c.mime_type.eq_ignore_ascii_case(mime))
        {
            for (layer, &(w, h)) in codec.layers.iter_mut().zip(sizes) {
                if (w != 0 || h != 0) && (layer.width != w || layer.height != h) {
                    layer.width = w;
                    layer.height = h;
                    changed = true;
                }
            }
        }
        changed
    }
}

/// Identifying known fields, kept separate from request-owned protobuf provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataBlobKeyValue {
    Unset,
    Generic(String),
    Schema(DataTrackSchema),
}
/// Text-equivalent identity is independent of exact encoded representation.
/// The adapter normalizes unknown values according to pinned Go prototext;
/// normal/core callers have no extension identity or wire provenance.
#[derive(Clone, Debug)]
pub struct DataBlobKey {
    pub value: DataBlobKeyValue,
    pub identity_extensions: Vec<Vec<u8>>,
    pub wire: Option<Vec<u8>>,
}
impl PartialEq for DataBlobKey {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.identity_extensions == other.identity_extensions
    }
}
impl Eq for DataBlobKey {}
impl DataBlobKey {
    pub fn known(value: DataBlobKeyValue) -> Self {
        Self {
            value,
            identity_extensions: Vec::new(),
            wire: None,
        }
    }
    pub fn unset() -> Self {
        Self::known(DataBlobKeyValue::Unset)
    }
    pub fn generic(value: String) -> Self {
        Self::known(DataBlobKeyValue::Generic(value))
    }
    pub fn schema(value: DataTrackSchema) -> Self {
        Self::known(DataBlobKeyValue::Schema(value))
    }
    /// Identifying UTF-8 content, not serialized protobuf/text length.
    pub fn identifying_len(&self) -> usize {
        match &self.value {
            DataBlobKeyValue::Unset => 0,
            DataBlobKeyValue::Generic(value) => value.len(),
            DataBlobKeyValue::Schema(schema) => {
                schema.name.len()
                    + match &schema.encoding {
                        Some(DataTrackEncoding::Custom(value)) => value.len(),
                        _ => 0,
                    }
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct DataBlob {
    pub key: Option<DataBlobKey>,
    pub contents: Vec<u8>,
    /// Entire request-owned source blob, including unknown fields at every level.
    pub wire: Option<Vec<u8>>,
}
impl PartialEq for DataBlob {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.contents == other.contents
    }
}
impl Eq for DataBlob {}

#[cfg(test)]
mod open_video_quality_tests {
    use super::VideoQuality;
    #[test]
    fn signed_protocol_identity_order_and_known_indices_are_independent() {
        for raw in [i32::MIN, -1, 0, 1, 2, 3, 4, 77, i32::MAX] {
            let q = VideoQuality::from_i32(raw);
            assert_eq!(q.as_i32(), raw);
            assert_eq!(
                q.spatial_index(),
                (0..=2).contains(&raw).then_some(raw as usize)
            );
        }
        let mut values = [77, 3, 0, -1, 2].map(VideoQuality::from_i32);
        values.sort();
        assert_eq!(values.map(VideoQuality::as_i32), [-1, 0, 2, 3, 77]);
        assert_eq!(VideoQuality::from_i32(0), VideoQuality::Low);
        assert_eq!(VideoQuality::from_i32(3), VideoQuality::Off);
    }
}
