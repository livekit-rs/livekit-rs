//! Session ⇄ room contract in engine terms. The LiveKit adapter translates
//! `SignalRequest`/`SignalResponse` to and from these types; nothing here is a wire
//! type. Variants mirror upstream semantics documented in
//! `livekit-rs-docs/impl-notes/signaling.md`.

use std::collections::BTreeMap;

use crate::ids::*;
use crate::model::*;
use crate::permission::Grants;

/// Which peer connection a description/candidate belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SignalTarget {
    Publisher,
    Subscriber,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SdpType {
    Offer,
    Answer,
}

/// `RTCIceCandidateInit` contents.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct CandidateInit {
    /// `candidate:...` attribute value; empty string = end of candidates.
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u32>,
    pub username_fragment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum SdkKind {
    #[default]
    Unknown,
    Js,
    Swift,
    Android,
    Flutter,
    Go,
    Unity,
    ReactNative,
    Rust,
    Python,
    Cpp,
    UnityWeb,
    Node,
    Unreal,
    Esp32,
    DotNet,
}

/// Client metadata from query params or `JoinRequest.client_info`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientMeta {
    pub sdk: SdkKind,
    pub version: String,
    /// Raw signal protocol metadata; compatibility policy lives in the adapter.
    pub protocol: i32,
    pub client_protocol: i32,
    pub os: String,
    pub os_version: String,
    pub device_model: String,
    pub browser: String,
    pub browser_version: String,
    pub address: String,
    pub network: String,
    pub packet_trailer: bool,
    /// Raw `ClientInfo.Capability` enum values the client declared at connect,
    /// in declaration order. Echoed into `ParticipantInfo.capabilities` like
    /// upstream `ClientInfo.GetCapabilities()`; `packet_trailer` is the derived
    /// `CAP_PACKET_TRAILER` bit kept for the media path.
    pub capabilities: Vec<i32>,
}

/// Neutral behavior projection supplied by the signaling adapter. Metadata is
/// retained separately; room logic never infers protocol/browser versions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientCapabilities {
    /// Go SDK video receiver callback waits for a negotiated first RTP codec.
    pub video_publication_requires_rtp: bool,
    /// Downstream codec changes require replacing the negotiated sender.
    pub codec_switch_requires_renegotiation: bool,
    /// No persistent interactive signaling peer; source WHIP one-shot mode.
    pub one_shot: bool,
    /// Fixed ClientInfo.SupportsICETCP, supplied by protocol adapter.
    pub ice_tcp: bool,
    pub subscriber_primary: bool,
    pub speaker_changed: bool,
    pub connection_quality: bool,
    pub ice_lite: bool,
    pub unpublish: bool,
    pub fast_start: bool,
    pub disconnected_update: bool,
    pub sync_stream_id: bool,
    /// Fixed Firefox-family transport cannot combine stream sync with playout delay.
    pub disable_playout_delay_for_sync: bool,
    pub connection_quality_lost: bool,
    pub identity_based_reconnection: bool,
    pub regions_in_leave: bool,
    pub non_error_signal_response: bool,
    pub request_response: bool,
    pub audio_red: bool,
}

/// Actual named subscription object to await before a one-shot final answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedSubscription {
    pub publisher: Identity,
    pub track_name: String,
}

/// Track publication request (`AddTrackRequest`).
#[derive(Clone, Debug, PartialEq)]
pub struct AddTrack {
    /// Adapter-chosen token echoed in `RequestResponse` (the adapter keeps the
    /// original request to copy into the response).
    pub token: u32,
    pub cid: String,
    pub name: String,
    pub kind: TrackKind,
    pub width: u32,
    pub height: u32,
    pub muted: bool,
    pub disable_dtx: bool,
    pub source: TrackSource,
    pub layers: Vec<VideoLayer>,
    pub simulcast_codecs: Vec<SimulcastCodecRequest>,
    /// Existing track to add a codec to.
    pub sid: Option<TrackSid>,
    pub stereo: bool,
    pub disable_red: bool,
    pub encryption: EncryptionType,
    pub stream: String,
    pub backup_codec_policy: BackupCodecPolicy,
    pub audio_features: Vec<AudioFeature>,
    pub packet_trailer_features: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SimulcastCodecRequest {
    pub codec: String,
    pub cid: String,
    pub layers: Vec<VideoLayer>,
    pub layer_mode: VideoLayerMode,
}

/// Join request in engine terms (query params or `JoinRequest`).
#[derive(Clone, Debug)]
pub struct JoinParams {
    pub room: RoomKey,
    pub grants: Grants,
    pub identity: Identity,
    pub name: String,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub client: ClientMeta,
    pub capabilities: ClientCapabilities,
    pub auto_subscribe: bool,
    pub auto_subscribe_data_track: bool,
    pub adaptive_stream: bool,
    /// `/rtc/v1` or any `join_request`: one peer connection, client always offers.
    pub single_pc: bool,
    pub disable_ice_lite: bool,
    pub subscriber_allow_pause: Option<bool>,
    pub reconnect: bool,
    pub reconnect_reason: i32,
    /// Only meaningful with `reconnect`; upstream never verifies it.
    pub participant_sid: Option<ParticipantSid>,
    /// `/rtc/v1` only: processed after join, in order.
    pub add_tracks: Vec<AddTrack>,
    pub publisher_offer: Option<(String, u32)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncState {
    /// Published tracks the client believes it has.
    pub publish_tracks: Vec<TrackSid>,
    /// Missing wire DataTrackInfo is represented by an empty SID and fails resume.
    pub publish_data_tracks: Vec<TrackSid>,
    pub track_sids_disabled: Vec<TrackSid>,
    pub subscribe: bool,
    pub subscription_track_sids: Vec<TrackSid>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Simulate {
    SpeakerUpdate { seconds: i32 },
    NodeFailure,
    Migration,
    ServerLeave,
    SwitchCandidateProtocol(i32),
    SubscriberBandwidth(i64),
    DisconnectSignalOnResume,
    DisconnectSignalOnResumeNoMessages,
    LeaveRequestFullReconnect,
}

/// What a client may ask of its room, in engine terms.
#[derive(Clone, Debug)]
pub enum SessionRequest {
    PrepareOneShotOffer {
        sdp: String,
        id: u32,
    },
    FinalizeOneShotAnswer {
        id: u32,
    },
    Offer {
        sdp: String,
        id: u32,
    },
    Answer {
        sdp: String,
        id: u32,
    },
    Trickle {
        token: u32,
        candidate: CandidateInit,
        target: SignalTarget,
        last: bool,
    },
    AddTrack(AddTrack),
    PublishDataTrack(PublishDataTrack),
    StoreDataBlob {
        request_id: u32,
        blob: Option<DataBlob>,
    },
    GetDataBlob {
        request_id: u32,
        participant_identity: Identity,
        key: Option<DataBlobKey>,
    },
    UnpublishDataTrack {
        token: u32,
        pub_handle: u32,
    },
    DataSubscription(Vec<DataTrackSubscriptionUpdate>),
    Mute {
        token: u32,
        sid: TrackSid,
        muted: bool,
    },
    Subscription {
        track_sids: Vec<TrackSid>,
        subscribe: bool,
    },
    TrackSettings {
        track_sids: Vec<TrackSid>,
        settings: TrackSettings,
    },
    Leave {
        reason: DisconnectReason,
    },
    SubscriptionPermission(SubscriptionPermission),
    SyncState(SyncState),
    Simulate(Simulate),
    UpdateMetadata {
        token: u32,
        request_id: u32,
        name: String,
        metadata: String,
        attributes: BTreeMap<String, String>,
    },
    UpdateAudioTrack {
        token: u32,
        sid: TrackSid,
        features: Vec<AudioFeature>,
    },
    UpdateVideoTrack {
        token: u32,
        sid: TrackSid,
        width: u32,
        height: u32,
    },
    /// Successfully decoded public signal without another actor operation.
    /// Refreshes owner activity only; it is never an RTT measurement.
    SignalActivity,
    /// Signaling RTT reported by `ping_req.rtt` (ms).
    SignalRtt(u32),
    /// Any request the engine does not implement; answered with
    /// [`SignalEvent::RequestResponse`] so the client never waits forever (C15).
    Unsupported {
        token: u32,
        request_id: u32,
        what: &'static str,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestOutcome {
    Ok,
    NotFound,
    NotAllowed,
    LimitExceeded,
    Queued,
    UnsupportedType,
    UnclassifiedError,
    InvalidHandle,
    InvalidName,
    DuplicateHandle,
    DuplicateName,
    InvalidRequest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    Unknown,
    CodecUnsupported,
    TrackNotFound,
}

/// `LeaveRequest.action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaveAction {
    Disconnect,
    Resume,
    Reconnect,
}

/// Data for `JoinResponse`; the adapter adds ICE servers, client configuration and
/// server info.
#[derive(Clone, Debug)]
pub struct JoinInfo {
    pub ice_preference: crate::transport_fallback::IcePreference,
    pub room: RoomInfo,
    pub participant: ParticipantInfo,
    pub other_participants: Vec<ParticipantInfo>,
    pub subscriber_primary: bool,
    pub sif_trailer: Vec<u8>,
    pub enabled_publish_codecs: Vec<CodecSpec>,
    pub fast_publish: bool,
    pub ping_interval_s: i32,
    pub ping_timeout_s: i32,
}

#[derive(Clone, Debug)]
pub struct ReconnectInfo {
    pub ice_servers_preference: crate::transport_fallback::IcePreference,
    pub ice_preference: crate::transport_fallback::IcePreference,
    pub last_message_seq: u32,
}

/// Owner-authoritative grant shape captured with the corresponding participant.
/// Effective permissions cannot reconstruct omitted values after a real update
/// and resume with an older JWT. No protocol/serialized fields live here.
#[derive(Clone, Debug)]
pub struct SignalClaimsSnapshot {
    pub participant: ParticipantInfo,
    pub video: crate::permission::VideoGrant,
}

impl SignalClaimsSnapshot {
    pub fn new(participant: ParticipantInfo, video: crate::permission::VideoGrant) -> Self {
        Self { participant, video }
    }
}

impl std::ops::Deref for SignalClaimsSnapshot {
    type Target = ParticipantInfo;
    fn deref(&self) -> &Self::Target {
        &self.participant
    }
}

/// Room → session messages. One variant per server signal the engine produces.
#[derive(Clone, Debug)]
pub enum SignalEvent {
    Join(Box<JoinInfo>),
    Reconnect(ReconnectInfo),
    Offer {
        sdp: String,
        id: u32,
        mid_to_track_id: BTreeMap<String, String>,
    },
    Answer {
        sdp: String,
        id: u32,
        mid_to_track_id: BTreeMap<String, String>,
    },
    Trickle {
        candidate: CandidateInit,
        target: SignalTarget,
        last: bool,
    },
    ParticipantUpdate(Vec<ParticipantInfo>),
    TrackPublished {
        cid: String,
        track: TrackInfo,
    },
    DataTrackPublished(DataTrackInfo),
    DataBlobStored {
        request_id: u32,
        key: DataBlobKey,
    },
    DataBlobRetrieved {
        request_id: u32,
        blob: DataBlob,
    },
    DataTrackUnpublished(DataTrackInfo),
    DataTrackSubscriberHandles(BTreeMap<u32, DataTrackHandleInfo>),
    TrackUnpublished {
        track_sid: TrackSid,
    },
    Leave {
        reason: DisconnectReason,
        action: LeaveAction,
    },
    Mute {
        track_sid: TrackSid,
        muted: bool,
    },
    SpeakersChanged(Vec<SpeakerInfo>),
    RoomUpdate(RoomInfo),
    ConnectionQuality(Vec<ConnectionQualityInfo>),
    StreamState(Vec<StreamStateInfo>),
    SubscribedQuality {
        track_sid: TrackSid,
        codecs: Vec<SubscribedCodec>,
    },
    SubscriptionPermissionUpdate {
        participant_sid: ParticipantSid,
        track_sid: TrackSid,
        allowed: bool,
    },
    /// Claims changed; the adapter mints and sends a refreshed token.
    ClaimsChanged(Box<SignalClaimsSnapshot>),
    SubscriptionResponse {
        track_sid: TrackSid,
        error: SubscriptionError,
    },
    RequestResponse {
        token: u32,
        request_id: u32,
        outcome: RequestOutcome,
        message: String,
    },
    /// Original admin update carried by owner rejection without a WS cache token.
    OwnedMetadataRequestResponse {
        request: Box<MetadataRequestEcho>,
        outcome: RequestOutcome,
        message: String,
    },
    TrackSubscribed {
        track_sid: TrackSid,
    },
    MediaSectionsRequirement {
        audios: u32,
        videos: u32,
    },
}

/// Owner-origin metadata update echoed on rejection. No adapter cache token
/// exists for an administrative RPC; preserve the actual attempted values.
#[derive(Clone, Debug)]
pub struct MetadataRequestEcho {
    pub request_id: u32,
    pub name: String,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
}

/// What the room delivers to a session.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "Session messages are moved through bounded channels; avoid an allocation for every signal message."
)]
pub enum SessionOutput {
    Signal(SignalEvent),
    /// Close the signal connection after flushing. `None` = no leave was sent.
    Close,
}

/// Why `join` failed before a session existed; maps to an HTTP status.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum JoinError {
    #[error("room has exceeded its max participants")]
    MaxParticipants,
    #[error("room is closed")]
    RoomClosed,
    #[error("participant already joined")]
    AlreadyJoined,
    #[error("requested room does not exist")]
    RoomNotFound,
    /// Resume of a participant that no longer exists; adapter sends
    /// `Leave{RECONNECT, STATE_MISMATCH}` as the only message.
    #[error("participant not found for reconnect")]
    ReconnectStateMismatch,
    #[error("{0}")]
    ProfileLimit(&'static str),
    #[error("{0}")]
    Internal(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishDataTrack {
    pub token: u32,
    pub pub_handle: u32,
    pub name: String,
    pub encryption: EncryptionType,
    pub frame_encoding: Option<DataTrackEncoding>,
    pub schema: Option<DataTrackSchema>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataTrackSubscriptionUpdate {
    pub track_sid: TrackSid,
    pub subscribe: bool,
    /// Preserved as subscription metadata: pinned upstream does not rate-limit yet.
    pub target_fps: Option<u32>,
}
