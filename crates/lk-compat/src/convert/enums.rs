//! Engine enum ⇄ protobuf enum mappings. Protobuf enum fields are `i32` in prost
//! (open enums); TrackSource retains unknown int32 values; other legacy enum mappings retain their documented fallback.

use lk_protocol::livekit as pb;
use rtc_core::events::RoomEndReason;
use rtc_core::media_api::DataKind;
use rtc_core::model::*;
use rtc_core::session::{SdkKind, SubscriptionError};

/// Generates `$to(core) -> i32` and `$from(i32) -> core`.
macro_rules! enum_map {
    ($to:ident, $from:ident, $core:ty, $pb:ty, $default:expr, { $($c:path => $p:path),* $(,)? }) => {
        pub fn $to(v: $core) -> i32 {
            match v {
                $($c => $p as i32,)*
            }
        }

        pub fn $from(v: i32) -> $core {
            match <$pb>::try_from(v) {
                $(Ok($p) => $c,)*
                #[allow(unreachable_patterns)]
                _ => $default,
            }
        }
    };
}

/// TrackSource is an open int32 enum in the fixed binary WS/protobuf API.
pub fn track_source_to_proto(source: TrackSource) -> i32 {
    source.as_i32()
}
pub fn track_source_from_proto(value: i32) -> TrackSource {
    TrackSource::from_i32(value)
}

/// Preserve the original open enum until AddTrack's source-shaped type check.
pub fn track_kind_to_proto(kind: TrackKind) -> i32 {
    kind.as_i32()
}
pub fn track_kind_from_proto(value: i32) -> TrackKind {
    TrackKind::from_i32(value)
}

/// Fixed Go protobuf quality fields retain every signed int32; media consumers
/// decide whether the value names a declared layer or an unsupported fallback.
pub fn video_quality_to_proto(value: VideoQuality) -> i32 {
    value.as_i32()
}
pub fn video_quality_from_proto(value: i32) -> VideoQuality {
    VideoQuality::from_i32(value)
}

pub fn layer_mode_to_proto(mode: VideoLayerMode) -> i32 {
    mode.as_i32()
}
pub fn layer_mode_from_proto(value: i32) -> VideoLayerMode {
    VideoLayerMode::from_i32(value)
}

pub fn encryption_to_proto(value: EncryptionType) -> i32 {
    value.as_i32()
}
pub fn encryption_from_proto(value: i32) -> EncryptionType {
    EncryptionType::from_i32(value)
}

pub fn audio_feature_to_proto(value: AudioFeature) -> i32 {
    value.as_i32()
}

/// Raw protobuf enums stay open in production TrackInfo and update consumers.
pub fn audio_feature_from_proto(value: i32) -> AudioFeature {
    AudioFeature::from_i32(value)
}

/// Existing known-only helper retained for callers explicitly requesting a filter.
pub fn audio_feature_from_proto_opt(value: i32) -> Option<AudioFeature> {
    pb::AudioTrackFeature::try_from(value).ok()?;
    Some(audio_feature_from_proto(value))
}

pub fn backup_policy_to_proto(value: BackupCodecPolicy) -> i32 {
    value.as_i32()
}

pub fn backup_policy_from_proto(value: i32) -> BackupCodecPolicy {
    BackupCodecPolicy::from_i32(value)
}

enum_map!(participant_state_to_proto, participant_state_from_proto, ParticipantState, pb::participant_info::State, ParticipantState::Joining, {
    ParticipantState::Joining => pb::participant_info::State::Joining,
    ParticipantState::Joined => pb::participant_info::State::Joined,
    ParticipantState::Active => pb::participant_info::State::Active,
    ParticipantState::Disconnected => pb::participant_info::State::Disconnected,
});

enum_map!(participant_kind_to_proto, participant_kind_from_proto, ParticipantKind, pb::participant_info::Kind, ParticipantKind::Standard, {
    ParticipantKind::Standard => pb::participant_info::Kind::Standard,
    ParticipantKind::Ingress => pb::participant_info::Kind::Ingress,
    ParticipantKind::Egress => pb::participant_info::Kind::Egress,
    ParticipantKind::Sip => pb::participant_info::Kind::Sip,
    ParticipantKind::Agent => pb::participant_info::Kind::Agent,
    ParticipantKind::Connector => pb::participant_info::Kind::Connector,
    ParticipantKind::Bridge => pb::participant_info::Kind::Bridge,
});

enum_map!(disconnect_reason_to_proto, disconnect_reason_from_proto, DisconnectReason, pb::DisconnectReason, DisconnectReason::Unknown, {
    DisconnectReason::Unknown => pb::DisconnectReason::UnknownReason,
    DisconnectReason::ClientInitiated => pb::DisconnectReason::ClientInitiated,
    DisconnectReason::DuplicateIdentity => pb::DisconnectReason::DuplicateIdentity,
    DisconnectReason::ServerShutdown => pb::DisconnectReason::ServerShutdown,
    DisconnectReason::ParticipantRemoved => pb::DisconnectReason::ParticipantRemoved,
    DisconnectReason::RoomDeleted => pb::DisconnectReason::RoomDeleted,
    DisconnectReason::StateMismatch => pb::DisconnectReason::StateMismatch,
    DisconnectReason::JoinFailure => pb::DisconnectReason::JoinFailure,
    DisconnectReason::Migration => pb::DisconnectReason::Migration,
    DisconnectReason::SignalClose => pb::DisconnectReason::SignalClose,
    DisconnectReason::RoomClosed => pb::DisconnectReason::RoomClosed,
    DisconnectReason::UserUnavailable => pb::DisconnectReason::UserUnavailable,
    DisconnectReason::UserRejected => pb::DisconnectReason::UserRejected,
    DisconnectReason::SipTrunkFailure => pb::DisconnectReason::SipTrunkFailure,
    DisconnectReason::ConnectionTimeout => pb::DisconnectReason::ConnectionTimeout,
    DisconnectReason::MediaFailure => pb::DisconnectReason::MediaFailure,
    DisconnectReason::AgentError => pb::DisconnectReason::AgentError,
});

enum_map!(connection_quality_to_proto, connection_quality_from_proto, ConnectionQuality, pb::ConnectionQuality, ConnectionQuality::Poor, {
    ConnectionQuality::Poor => pb::ConnectionQuality::Poor,
    ConnectionQuality::Good => pb::ConnectionQuality::Good,
    ConnectionQuality::Excellent => pb::ConnectionQuality::Excellent,
    ConnectionQuality::Lost => pb::ConnectionQuality::Lost,
});

enum_map!(stream_state_to_proto, stream_state_from_proto, StreamState, pb::StreamState, StreamState::Active, {
    StreamState::Active => pb::StreamState::Active,
    StreamState::Paused => pb::StreamState::Paused,
});

enum_map!(room_end_reason_to_proto, room_end_reason_from_proto, RoomEndReason, pb::RoomEndReason, RoomEndReason::Unknown, {
    RoomEndReason::Unknown => pb::RoomEndReason::RoomEndUnknown,
    RoomEndReason::ApiDelete => pb::RoomEndReason::RoomEndApiDelete,
    RoomEndReason::IdleTimeout => pb::RoomEndReason::RoomEndIdleTimeout,
    RoomEndReason::ServerShutdown => pb::RoomEndReason::RoomEndServerShutdown,
    RoomEndReason::Superseded => pb::RoomEndReason::RoomEndSuperseded,
    RoomEndReason::OpenFailed => pb::RoomEndReason::RoomEndOpenFailed,
});

/// Open enum identity is preserved until the transport chooses a channel.
pub fn data_kind_to_proto(kind: DataKind) -> i32 {
    kind.as_i32()
}
pub fn data_kind_from_proto(value: i32) -> DataKind {
    DataKind::from_i32(value)
}

enum_map!(subscription_error_to_proto, subscription_error_from_proto, SubscriptionError, pb::SubscriptionError, SubscriptionError::Unknown, {
    SubscriptionError::Unknown => pb::SubscriptionError::SeUnknown,
    SubscriptionError::CodecUnsupported => pb::SubscriptionError::SeCodecUnsupported,
    SubscriptionError::TrackNotFound => pb::SubscriptionError::SeTrackNotfound,
});

pub fn sdk_to_proto(v: &SdkKind) -> i32 {
    use pb::client_info::Sdk;
    (match v {
        SdkKind::Unknown => Sdk::Unknown,
        SdkKind::Js => Sdk::Js,
        SdkKind::Swift => Sdk::Swift,
        SdkKind::Android => Sdk::Android,
        SdkKind::Flutter => Sdk::Flutter,
        SdkKind::Go => Sdk::Go,
        SdkKind::Unity => Sdk::Unity,
        SdkKind::ReactNative => Sdk::ReactNative,
        SdkKind::Rust => Sdk::Rust,
        SdkKind::Python => Sdk::Python,
        SdkKind::Cpp => Sdk::Cpp,
        SdkKind::UnityWeb => Sdk::UnityWeb,
        SdkKind::Node => Sdk::Node,
        SdkKind::Unreal => Sdk::Unreal,
        SdkKind::Esp32 => Sdk::Esp32,
        SdkKind::DotNet => Sdk::Dotnet,
    }) as i32
}

pub fn sdk_from_proto(v: i32) -> SdkKind {
    use pb::client_info::Sdk;
    match Sdk::try_from(v) {
        Ok(Sdk::Js) => SdkKind::Js,
        Ok(Sdk::Swift) => SdkKind::Swift,
        Ok(Sdk::Android) => SdkKind::Android,
        Ok(Sdk::Flutter) => SdkKind::Flutter,
        Ok(Sdk::Go) => SdkKind::Go,
        Ok(Sdk::Unity) => SdkKind::Unity,
        Ok(Sdk::ReactNative) => SdkKind::ReactNative,
        Ok(Sdk::Rust) => SdkKind::Rust,
        Ok(Sdk::Python) => SdkKind::Python,
        Ok(Sdk::Cpp) => SdkKind::Cpp,
        Ok(Sdk::UnityWeb) => SdkKind::UnityWeb,
        Ok(Sdk::Node) => SdkKind::Node,
        Ok(Sdk::Unreal) => SdkKind::Unreal,
        Ok(Sdk::Esp32) => SdkKind::Esp32,
        Ok(Sdk::Dotnet) => SdkKind::DotNet,
        _ => SdkKind::Unknown,
    }
}

/// `sdk=` query parameter of `/rtc` (upstream `ParseClientInfo`, utils.go:201-229).
/// Case-sensitive; unrecognised values (including `unreal`/`dotnet`, which the
/// upstream switch does not list) map to UNKNOWN.
pub fn sdk_from_query(s: &str) -> SdkKind {
    match s {
        "js" => SdkKind::Js,
        "ios" | "swift" => SdkKind::Swift,
        "android" => SdkKind::Android,
        "flutter" => SdkKind::Flutter,
        "go" => SdkKind::Go,
        "unity" => SdkKind::Unity,
        "reactnative" => SdkKind::ReactNative,
        "rust" => SdkKind::Rust,
        "python" => SdkKind::Python,
        "cpp" => SdkKind::Cpp,
        "unityweb" => SdkKind::UnityWeb,
        "node" => SdkKind::Node,
        "esp32" => SdkKind::Esp32,
        _ => SdkKind::Unknown,
    }
}

/// Lowercase `ParticipantInfo.Kind` name as used in the JWT `kind` claim
/// (`grants.go:816-825`).
pub fn participant_kind_to_claim(kind: ParticipantKind) -> &'static str {
    match kind {
        ParticipantKind::Standard => "standard",
        ParticipantKind::Ingress => "ingress",
        ParticipantKind::Egress => "egress",
        ParticipantKind::Sip => "sip",
        ParticipantKind::Agent => "agent",
        ParticipantKind::Connector => "connector",
        ParticipantKind::Bridge => "bridge",
    }
}

/// Inverse of [`participant_kind_to_claim`]; upstream upper-cases the claim and
/// looks it up in the enum value map, so matching is case-insensitive and unknown
/// values become STANDARD.
pub fn participant_kind_from_claim(s: &str) -> ParticipantKind {
    match s.to_ascii_lowercase().as_str() {
        "ingress" => ParticipantKind::Ingress,
        "egress" => ParticipantKind::Egress,
        "sip" => ParticipantKind::Sip,
        "agent" => ParticipantKind::Agent,
        "connector" => ParticipantKind::Connector,
        "bridge" => ParticipantKind::Bridge,
        _ => ParticipantKind::Standard,
    }
}

/// Lowercase `TrackSource` name as used in `canPublishSources` (`grants.go:805-807`).
pub fn track_source_to_claim(source: TrackSource) -> std::borrow::Cow<'static, str> {
    rtc_core::permission::canonical_source(source)
}

/// `canPublishSources` entry → engine source. Upstream's publish check
/// (`GetCanPublishSource`) is an exact, case-sensitive match against the lowercase
/// name, so anything else (e.g. `"CAMERA"`) can never authorise a source; it is
/// mapped to `Unknown`, which keeps the list non-empty (still restricting) without
/// granting a real source.
pub fn track_source_from_claim(s: &str) -> TrackSource {
    match s {
        "camera" => TrackSource::Camera,
        "microphone" => TrackSource::Microphone,
        "screen_share" => TrackSource::ScreenShare,
        "screen_share_audio" => TrackSource::ScreenShareAudio,
        _ => TrackSource::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_round_trip() {
        for s in [
            TrackSource::Unknown,
            TrackSource::Camera,
            TrackSource::Microphone,
            TrackSource::ScreenShare,
            TrackSource::ScreenShareAudio,
        ] {
            assert_eq!(track_source_from_proto(track_source_to_proto(s)), s);
            assert_eq!(track_source_from_claim(&track_source_to_claim(s)), s);
        }
        for k in [
            ParticipantKind::Standard,
            ParticipantKind::Ingress,
            ParticipantKind::Egress,
            ParticipantKind::Sip,
            ParticipantKind::Agent,
            ParticipantKind::Connector,
            ParticipantKind::Bridge,
        ] {
            assert_eq!(participant_kind_from_proto(participant_kind_to_proto(k)), k);
            assert_eq!(participant_kind_from_claim(participant_kind_to_claim(k)), k);
        }
        assert_eq!(participant_kind_to_proto(ParticipantKind::Connector), 7);
        assert_eq!(participant_kind_to_proto(ParticipantKind::Bridge), 8);
        assert_eq!(disconnect_reason_from_proto(999), DisconnectReason::Unknown);
        assert_eq!(room_end_reason_to_proto(RoomEndReason::ApiDelete), 1);
        assert_eq!(
            sdk_from_proto(sdk_to_proto(&SdkKind::DotNet)),
            SdkKind::DotNet
        );
        assert_eq!(audio_feature_from_proto_opt(42), None);
    }

    #[test]
    fn claim_strings() {
        assert_eq!(participant_kind_from_claim("AGENT"), ParticipantKind::Agent);
        assert_eq!(
            participant_kind_from_claim("robot"),
            ParticipantKind::Standard
        );
        assert_eq!(track_source_from_claim("CAMERA"), TrackSource::Unknown);
        assert_eq!(sdk_from_query("ios"), SdkKind::Swift);
        assert_eq!(sdk_from_query("JS"), SdkKind::Unknown);
    }
}

#[cfg(test)]
mod video_quality_open_tests {
    use super::*;
    #[test]
    fn video_quality_open_int32_survives_layer_and_subscribed_quality_consumers() {
        for raw in [i32::MIN, -1, 0, 1, 2, 3, 77, i32::MAX] {
            let layer = pb::VideoLayer {
                quality: raw,
                width: 1280,
                height: 720,
                spatial_layer: 2,
                ..Default::default()
            };
            let core = super::super::models::video_layer_from_proto(&layer);
            assert_eq!(core.quality.as_i32(), raw);
            assert_eq!(super::super::models::video_layer_to_proto(&core), layer);
            let demand = pb::SubscribedQuality {
                quality: raw,
                enabled: true,
            };
            let core = super::super::models::subscribed_quality_from_proto(&demand);
            assert_eq!(
                super::super::models::subscribed_quality_to_proto(&core),
                demand
            );
        }
    }
}
