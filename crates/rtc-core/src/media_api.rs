//! Room logic ⇄ media engine contract. Both run inside the same room task: the room
//! issues [`MediaCommand`]s synchronously and drains [`MediaEvent`]s after every
//! media step, so forwarding-graph changes take effect immediately (M-02).

use std::collections::BTreeMap;
use std::time::Instant;

use crate::ids::*;
use crate::model::*;
use crate::session::{CandidateInit, SdpType, SignalTarget, SubscriptionError};

/// Role of one transport (client peer connection).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PcRole {
    /// Dual-PC publisher: client offers, server receives.
    Publisher,
    /// Dual-PC subscriber: server offers, server sends.
    Subscriber,
    /// Single PC: client always offers; server receives and sends.
    Single,
}

impl PcRole {
    pub fn target(self) -> SignalTarget {
        match self {
            PcRole::Subscriber => SignalTarget::Subscriber,
            PcRole::Publisher | PcRole::Single => SignalTarget::Publisher,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataKind {
    Reliable,
    Lossy,
    Unrecognized(UnrecognizedDataKind),
}

/// An unrecognized open protobuf enum value. Known values have one canonical representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnrecognizedDataKind(i32);

impl DataKind {
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Reliable,
            1 => Self::Lossy,
            value => Self::Unrecognized(UnrecognizedDataKind(value)),
        }
    }

    pub fn as_i32(self) -> i32 {
        match self {
            Self::Reliable => 0,
            Self::Lossy => 1,
            Self::Unrecognized(value) => value.0,
        }
    }

    /// Fixed transport.go: only RELIABLE(0) selects the reliable data channel.
    /// Keep this delivery class separate from the original serialized packet kind.
    pub fn delivery_class(self) -> Self {
        if self == Self::Reliable {
            Self::Reliable
        } else {
            Self::Lossy
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataWrite {
    pub to: ParticipantSid,
    pub kind: DataKind,
    pub payload: Vec<u8>,
    pub expected_transport: Option<TransportId>,
}

#[derive(Clone, Debug)]
pub struct TransportSpec {
    pub ice_preference: crate::transport_fallback::IcePreference,
    pub one_shot: bool,
    pub id: TransportId,
    pub participant: ParticipantSid,
    pub role: PcRole,
    pub ice_lite: bool,
    /// Subscriber-primary dual PC: server creates `_reliable`/`_lossy` channels.
    pub server_data_channels: bool,
    /// Independent raw data-track SCTP channel authorization.
    pub enable_data_tracks: bool,
    /// This transport carries downstream data for the participant.
    pub primary: bool,
    /// Client is Firefox / Safari / supports RED (codec negotiation tweaks).
    pub client_supports_red: bool,
    /// Downstream codec changes require replacing the negotiated sender.
    pub codec_switch_requires_renegotiation: bool,
    /// Source browser/room policy, distinct from codec and stream-ID capabilities.
    pub disable_playout_delay: bool,
    pub allow_pause: bool,
}

impl TransportSpec {
    /// The same per-PC value drives negotiation and actual outgoing RTP.
    pub fn effective_playout_delay(&self, configured: Option<(u32, u32)>) -> Option<(u32, u32)> {
        configured.filter(|_| !self.disable_playout_delay)
    }
}

/// What the media layer needs to know about a track being published.
#[derive(Clone, Debug)]
pub struct PublishedTrackSpec {
    pub backup_codec_policy: BackupCodecPolicy,
    pub sid: TrackSid,
    pub kind: TrackKind,
    pub source: TrackSource,
    pub cid: String,
    pub layers: Vec<VideoLayer>,
    pub codecs: Vec<TrackCodec>,
    pub stereo: bool,
    pub disable_dtx: bool,
    pub disable_red: bool,
    pub encrypted: bool,
    pub muted: bool,
}

/// Subscriber-side identity of a down track (§10 of sfu-media notes).
#[derive(Clone, Debug)]
pub struct DownTrackSpec {
    pub subscriber: ParticipantSid,
    pub publisher: ParticipantSid,
    pub track_sid: TrackSid,
    /// `a=msid` stream id, e.g. `PA_x|TR_y`.
    pub stream_id: String,
    pub settings: TrackSettings,
    pub adaptive_stream: bool,
}

#[derive(Clone, Debug)]
pub enum MediaCommand {
    PrepareOneShotOffer {
        transport: TransportId,
        sdp: String,
        id: u32,
    },
    FinalizeOneShotAnswer {
        transport: TransportId,
        id: u32,
    },
    CreateTransport(TransportSpec),
    CloseTransport(TransportId),
    SetSignalingRtt {
        participant: ParticipantSid,
        rtt_ms: u32,
    },
    SetIcePreference {
        transport: TransportId,
        preference: crate::transport_fallback::IcePreference,
    },
    RemoteDescription {
        transport: TransportId,
        kind: SdpType,
        sdp: String,
        id: u32,
    },
    RemoteCandidate {
        transport: TransportId,
        candidate: CandidateInit,
    },
    /// Server-initiated ICE restart (dual-PC subscriber after resume).
    /// Failed reconnect arms source short-connection reset on both existing PCs.
    /// It is consumed only by an actual ICE credential restart, not a resume alone.
    /// Fence both existing PCs before starting a real participant resume.
    BeginTransportRecovery {
        participant: ParticipantSid,
        recovery_epoch: u64,
    },
    ArmShortConnectionReset {
        participant: ParticipantSid,
    },
    IceRestart {
        transport: TransportId,
    },
    /// Announce a pending publication so the next client offer can be matched and
    /// the answer munged (opus fmtp, simulcast rids).
    ExpectTrack {
        transport: TransportId,
        track: PublishedTrackSpec,
    },
    /// Room matched remote media `mid` to `track`.
    BindTrack {
        transport: TransportId,
        mid: String,
        track_sid: TrackSid,
    },
    UpdatePublishedTrack {
        track: PublishedTrackSpec,
    },
    UnpublishTrack {
        track_sid: TrackSid,
    },
    SetPublisherMuted {
        track_sid: TrackSid,
        muted: bool,
    },
    Subscribe(DownTrackSpec),
    Unsubscribe {
        subscriber: ParticipantSid,
        track_sid: TrackSid,
    },
    UpdateTrackSettings {
        subscriber: ParticipantSid,
        track_sid: TrackSid,
        settings: TrackSettings,
    },
    /// Raw `_data_track` channel packet; never a protobuf DataPacket envelope.
    SendDataTrack {
        to: ParticipantSid,
        payload: Vec<u8>,
    },
    /// API fanout completion is generated only after every actual writer outcome.
    SendDataFanout {
        id: u64,
        writes: Vec<DataWrite>,
    },
    DataWriteAdmitted {
        request: u64,
        write: bool,
    },
    ReplayJoiningReliable {
        participant: ParticipantSid,
        transport: TransportId,
        writes: Vec<DataWrite>,
    },
    /// One upstream-forwarded packet (BroadcastDataPacketForRoom) with all its
    /// recipients. Batches from one source complete in FIFO order, preserving
    /// the publisher read loop's synchronous per-sender ordering.
    ForwardData {
        source: ParticipantSid,
        writes: Vec<DataWrite>,
    },
    SendData {
        to: ParticipantSid,
        kind: DataKind,
        payload: Vec<u8>,
    },
    /// Test hook (`simulate subscriber_bandwidth`): override the subscriber's allocation capacity (bps, 0 = clear).
    SetBandwidthOverride {
        participant: ParticipantSid,
        bps: u64,
    },
    /// Close every transport of a participant.
    RemoveParticipant(ParticipantSid),
}

/// Pinned receiver callback eligibility, independent of complete-frame decoding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PublicationRegistration {
    #[default]
    Pending,
    /// Remote SDP has a nonzero primary SSRC and no RID receive encoding.
    SdpReceiver,
}

#[derive(Clone, Debug)]
pub struct RemoteMedia {
    /// Explicit source registration proof. Plain m-line announcements are Pending.
    pub registration: PublicationRegistration,

    pub participant: ParticipantSid,
    pub transport: TransportId,
    pub mid: String,
    pub kind: TrackKind,
    /// Second token of `a=msid`: the client's MediaStreamTrack id (≈ cid).
    pub msid_track_id: String,
    /// Simulcast rids from `a=simulcast` in offer order (`~` stripped).
    pub rids: Vec<String>,
    pub mime_type: String,
}

#[derive(Clone, Debug)]
pub struct AudioLevel {
    pub participant: ParticipantSid,
    /// Linear smoothed audio level; short update windows may exceed one.
    pub level: f32,
    pub active: bool,
}

#[derive(Clone, Debug)]
pub struct QualitySample {
    pub participant: ParticipantSid,
    pub quality: ConnectionQuality,
    /// MOS 1.0..=4.5.
    pub score: f32,
}

#[derive(Clone, Debug)]
pub enum MediaEvent {
    IceTcpConnectTimedOut {
        recovery_epoch: u64,
        participant: ParticipantSid,
        transport: TransportId,
    },
    /// Actual nominated ICE path; candidate type is not inferred from an API preference.
    IcePath {
        transport: TransportId,
        connection: crate::transport_fallback::IceConnection,
        first_connected: Option<Instant>,
    },
    /// Authenticated RR primary sender fraction loss, not an averaged stats sample.
    ReceiverLoss {
        recovery_epoch: u64,
        transport: TransportId,
        track: TrackSid,
        fraction_lost: u8,
        received_at: Instant,
    },
    /// A new genuine downtrack Receiver Report observation, never TWCC/ICE RTT.
    SubscriberRtt {
        transport: TransportId,
        track: TrackSid,
        rtt_ms: u32,
        received_at: std::time::Instant,
    },
    DataFanoutCompleted {
        id: u64,
    },
    DataWriteAdmission {
        request: u64,
        to: ParticipantSid,
        kind: DataKind,
        payload: Vec<u8>,
        expected_transport: Option<TransportId>,
    },
    JoiningReliableReplayed {
        participant: ParticipantSid,
        transport: TransportId,
    },
    /// Source fully-established boundary, emitted by real transport state/data-channel events.
    /// Ordinary SDK: connected plus both labeled writers opened; one-shot/no-data
    /// first offers follow their explicit source bypass.
    PrimaryTransportEstablished {
        participant: ParticipantSid,
        transport: TransportId,
    },
    /// Real authenticated primary RTP with a negotiated non-repair payload type.
    PublicationRtpRegistered {
        participant: ParticipantSid,
        transport: TransportId,
        track_sid: TrackSid,
        mime_type: String,
        payload_type: u8,
    },

    /// Actual typed SCTP closed-stream write failure; not backpressure/unavailable.
    /// Local transport identity prevents stale-session delivery.
    DataSendError {
        participant: ParticipantSid,
        transport: TransportId,
    },
    /// Actual bounded one-shot codec/DD dimensions.
    TrackVideoSizeChanged {
        track_sid: TrackSid,
        mime_type: String,
        sizes: Vec<(u32, u32)>,
    },
    /// Only emitted by the explicit one-shot path, not ordinary SDK signaling.
    RemoteDescriptionRejected {
        participant: ParticipantSid,
        id: u32,
        message: String,
    },
    /// The actual media DownTrack object exists, before SDP answer binding.
    SubscriptionPrepared {
        subscriber: ParticipantSid,
        track_sid: TrackSid,
    },
    LocalDescription {
        participant: ParticipantSid,
        transport: TransportId,
        target: SignalTarget,
        kind: SdpType,
        sdp: String,
        /// SDK signaling publishes this generation's candidates through LocalCandidate.
        trickle_only: bool,
        id: u32,
        mid_to_track_id: BTreeMap<String, String>,
    },
    LocalCandidate {
        participant: ParticipantSid,
        target: SignalTarget,
        candidate: CandidateInit,
        last: bool,
    },
    /// Single PC: ask the client to re-offer with this many extra recv sections.
    MediaSectionsRequirement {
        participant: ParticipantSid,
        audios: u32,
        videos: u32,
    },
    RemoteMediaAdded(RemoteMedia),
    /// A bound published track received its first RTP; layer rids now known.
    TrackMediaStarted {
        track_sid: TrackSid,
        mime_type: String,
        layers: Vec<VideoLayer>,
    },
    /// Remote side stopped a published m-line (client unpublish via SDP).
    RemoteMediaStopped {
        participant: ParticipantSid,
        track_sid: TrackSid,
    },
    /// Native aggregate PC state has left New after valid SDP/direct DTLS
    /// setup, not ICE bookkeeping before SDP, RTP, RTT or DTLS Connected.
    TransportEstablished {
        participant: ParticipantSid,
        transport: TransportId,
    },
    TransportConnected {
        participant: ParticipantSid,
        transport: TransportId,
    },
    TransportFailed {
        recovery_epoch: u64,
        participant: ParticipantSid,
        transport: TransportId,
    },
    DataReceived {
        participant: ParticipantSid,
        kind: DataKind,
        label: String,
        payload: Vec<u8>,
    },
    /// Actual media removal completed, or engine confirmed no downtrack exists.
    SubscriptionRemoved {
        subscriber: ParticipantSid,
        track_sid: TrackSid,
    },
    /// Down track negotiated and bound; `mid` on the subscriber transport.
    SubscriptionBound {
        subscriber: ParticipantSid,
        track_sid: TrackSid,
        mid: String,
    },
    SubscriptionFailed {
        subscriber: ParticipantSid,
        track_sid: TrackSid,
        error: SubscriptionError,
    },
    StreamState {
        subscriber: ParticipantSid,
        publisher: ParticipantSid,
        track_sid: TrackSid,
        state: StreamState,
    },
    /// Dynacast: committed demand changed for a publisher's track.
    SubscribedQuality {
        publisher: ParticipantSid,
        track_sid: TrackSid,
        codecs: Vec<SubscribedCodec>,
    },
    /// Emitted every `audio.update_interval`, one entry per participant with a level.
    AudioLevels(Vec<AudioLevel>),
    /// Emitted every 5 s for participants with media.
    Quality(Vec<QualitySample>),
    /// Subscriber negotiation got no answer in time: full reconnect required.
    NegotiationFailed {
        recovery_epoch: u64,
        participant: ParticipantSid,
    },
}

#[derive(Clone, Debug)]
pub struct PublishedMediaState {
    pub track: TrackInfo,
    pub average_bitrate: u32,
}
