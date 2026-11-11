//! Engine model ⇄ protobuf message mappings.
//!
//! Every engine field has a protobuf counterpart, so engine → proto → engine is
//! lossless. The reverse direction drops protobuf-only fields the engine does not
//! model (`ParticipantInfo.kind_details/capabilities`,
//! `TrackInfo.packet_trailer_features`, `Room.turn_password`, ...).

use std::collections::{BTreeMap, HashMap};

use lk_protocol::livekit as pb;
use rtc_core::ids::*;
use rtc_core::model::*;
use rtc_core::permission::ParticipantPermission;
use rtc_core::session::ClientMeta;

use super::enums::*;

// ---------------------------------------------------------------- versions

pub fn timed_version_to_proto(v: &TimedVersion) -> pb::TimedVersion {
    pb::TimedVersion {
        unix_micro: v.unix_micro,
        ticks: v.ticks,
    }
}

pub fn timed_version_from_proto(v: &pb::TimedVersion) -> TimedVersion {
    TimedVersion {
        unix_micro: v.unix_micro,
        ticks: v.ticks,
    }
}

// ---------------------------------------------------------------- codecs / layers

pub fn codec_spec_to_proto(c: &CodecSpec) -> pb::Codec {
    pb::Codec {
        mime: c.mime.clone(),
        fmtp_line: c.fmtp_line.clone(),
    }
}

pub fn codec_spec_from_proto(c: &pb::Codec) -> CodecSpec {
    CodecSpec {
        mime: c.mime.clone(),
        fmtp_line: c.fmtp_line.clone(),
    }
}

pub fn video_layer_to_proto(l: &VideoLayer) -> pb::VideoLayer {
    pb::VideoLayer {
        quality: video_quality_to_proto(l.quality),
        width: l.width,
        height: l.height,
        bitrate: l.bitrate,
        ssrc: l.ssrc,
        spatial_layer: l.spatial_layer,
        rid: l.rid.clone(),
        repair_ssrc: l.repair_ssrc,
    }
}

pub fn video_layer_from_proto(l: &pb::VideoLayer) -> VideoLayer {
    VideoLayer {
        quality: video_quality_from_proto(l.quality),
        width: l.width,
        height: l.height,
        bitrate: l.bitrate,
        ssrc: l.ssrc,
        spatial_layer: l.spatial_layer,
        rid: l.rid.clone(),
        repair_ssrc: l.repair_ssrc,
    }
}

pub fn track_codec_to_proto(c: &TrackCodec) -> pb::SimulcastCodecInfo {
    pb::SimulcastCodecInfo {
        mime_type: c.mime_type.clone(),
        mid: c.mid.clone(),
        cid: c.cid.clone(),
        layers: c.layers.iter().map(video_layer_to_proto).collect(),
        video_layer_mode: layer_mode_to_proto(c.layer_mode),
        sdp_cid: c.sdp_cid.clone(),
    }
}

pub fn track_codec_from_proto(c: &pb::SimulcastCodecInfo) -> TrackCodec {
    TrackCodec {
        mime_type: c.mime_type.clone(),
        mid: c.mid.clone(),
        cid: c.cid.clone(),
        sdp_cid: c.sdp_cid.clone(),
        layers: c.layers.iter().map(video_layer_from_proto).collect(),
        layer_mode: layer_mode_from_proto(c.video_layer_mode),
    }
}

// ---------------------------------------------------------------- tracks

pub fn track_info_to_proto(t: &TrackInfo) -> pb::TrackInfo {
    pb::TrackInfo {
        sid: t.sid.0.clone(),
        r#type: track_kind_to_proto(t.kind),
        name: t.name.clone(),
        muted: t.muted,
        width: t.width,
        height: t.height,
        simulcast: t.simulcast,
        disable_dtx: t.disable_dtx,
        source: track_source_to_proto(t.source),
        layers: t.layers.iter().map(video_layer_to_proto).collect(),
        mime_type: t.mime_type.clone(),
        mid: t.mid.clone(),
        codecs: t.codecs.iter().map(track_codec_to_proto).collect(),
        stereo: t.stereo,
        disable_red: t.disable_red,
        encryption: encryption_to_proto(t.encryption),
        packet_trailer_features: t.packet_trailer_features.clone(),
        stream: t.stream.clone(),
        version: Some(timed_version_to_proto(&t.version)),
        audio_features: t
            .audio_features
            .iter()
            .map(|f| audio_feature_to_proto(*f))
            .collect(),
        backup_codec_policy: backup_policy_to_proto(t.backup_codec_policy),
    }
}

pub fn track_info_from_proto(t: &pb::TrackInfo) -> TrackInfo {
    TrackInfo {
        sid: TrackSid(t.sid.clone()),
        kind: track_kind_from_proto(t.r#type),
        name: t.name.clone(),
        muted: t.muted,
        width: t.width,
        height: t.height,
        simulcast: t.simulcast,
        source: track_source_from_proto(t.source),
        layers: t.layers.iter().map(video_layer_from_proto).collect(),
        mime_type: t.mime_type.clone(),
        mid: t.mid.clone(),
        codecs: t.codecs.iter().map(track_codec_from_proto).collect(),
        disable_red: t.disable_red,
        disable_dtx: t.disable_dtx,
        stereo: t.stereo,
        encryption: encryption_from_proto(t.encryption),
        packet_trailer_features: t.packet_trailer_features.clone(),
        stream: t.stream.clone(),
        version: t
            .version
            .as_ref()
            .map(timed_version_from_proto)
            .unwrap_or_default(),
        audio_features: t
            .audio_features
            .iter()
            .map(|f| audio_feature_from_proto(*f))
            .collect(),
        backup_codec_policy: backup_policy_from_proto(t.backup_codec_policy),
    }
}

// ---------------------------------------------------------------- participants

pub fn permission_to_proto(p: &ParticipantPermission) -> pb::ParticipantPermission {
    pb::ParticipantPermission {
        can_subscribe: p.can_subscribe,
        can_publish: p.can_publish,
        can_publish_data: p.can_publish_data,
        can_publish_sources: p
            .can_publish_sources
            .iter()
            .map(|s| track_source_to_proto(*s))
            .collect(),
        hidden: p.hidden,
        recorder: p.recorder,
        can_update_metadata: p.can_update_metadata,
        agent: p.agent,
        can_subscribe_metrics: p.can_subscribe_metrics,
        can_manage_agent_session: p.can_manage_agent_session,
    }
}

pub fn permission_from_proto(p: &pb::ParticipantPermission) -> ParticipantPermission {
    ParticipantPermission {
        can_subscribe: p.can_subscribe,
        can_publish: p.can_publish,
        can_publish_data: p.can_publish_data,
        can_publish_sources: p
            .can_publish_sources
            .iter()
            .map(|s| track_source_from_proto(*s))
            .collect(),
        hidden: p.hidden,
        recorder: p.recorder,
        can_update_metadata: p.can_update_metadata,
        agent: p.agent,
        can_subscribe_metrics: p.can_subscribe_metrics,
        can_manage_agent_session: p.can_manage_agent_session,
    }
}

pub fn attributes_to_proto(a: &BTreeMap<String, String>) -> HashMap<String, String> {
    a.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

pub fn attributes_from_proto(a: &HashMap<String, String>) -> BTreeMap<String, String> {
    a.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

pub fn participant_info_to_proto(p: &ParticipantInfo) -> pb::ParticipantInfo {
    pb::ParticipantInfo {
        sid: p.sid.0.clone(),
        identity: p.identity.0.clone(),
        state: participant_state_to_proto(p.state),
        tracks: p.tracks.iter().map(track_info_to_proto).collect(),
        data_tracks: p.data_tracks.iter().map(data_track_info_to_proto).collect(),
        metadata: p.metadata.clone(),
        joined_at: p.joined_at_ms.div_euclid(1000),
        joined_at_ms: p.joined_at_ms,
        name: p.name.clone(),
        version: p.version,
        permission: Some(permission_to_proto(&p.permission)),
        region: p.region.clone(),
        is_publisher: p.is_publisher,
        kind: participant_kind_to_proto(p.kind),
        attributes: attributes_to_proto(&p.attributes),
        disconnect_reason: disconnect_reason_to_proto(p.disconnect_reason),
        client_protocol: p.client_protocol,
        ..Default::default()
    }
}

pub fn participant_info_from_proto(p: &pb::ParticipantInfo) -> ParticipantInfo {
    let joined_at_ms = if p.joined_at_ms != 0 {
        p.joined_at_ms
    } else {
        p.joined_at.saturating_mul(1000)
    };
    ParticipantInfo {
        sid: ParticipantSid(p.sid.clone()),
        identity: Identity(p.identity.clone()),
        name: p.name.clone(),
        state: participant_state_from_proto(p.state),
        tracks: p.tracks.iter().map(track_info_from_proto).collect(),
        data_tracks: p
            .data_tracks
            .iter()
            .map(data_track_info_from_proto)
            .collect(),
        metadata: p.metadata.clone(),
        attributes: attributes_from_proto(&p.attributes),
        joined_at_ms,
        version: p.version,
        permission: p
            .permission
            .as_ref()
            .map(permission_from_proto)
            .unwrap_or_default(),
        region: p.region.clone(),
        is_publisher: p.is_publisher,
        kind: participant_kind_from_proto(p.kind),
        disconnect_reason: disconnect_reason_from_proto(p.disconnect_reason),
        client_protocol: p.client_protocol,
    }
}

// ---------------------------------------------------------------- rooms

/// Upstream rooms carry the random `turn_password` chosen at creation and never
/// set `Room.version` (no producer in the fixed server), so it stays absent.
pub fn room_info_to_proto(r: &RoomInfo) -> pb::Room {
    pb::Room {
        sid: r.sid.0.clone(),
        name: r.name.0.clone(),
        empty_timeout: r.empty_timeout_s,
        departure_timeout: r.departure_timeout_s,
        max_participants: r.max_participants,
        creation_time: r.creation_time_ms.div_euclid(1000),
        creation_time_ms: r.creation_time_ms,
        turn_password: r.turn_password.clone(),
        enabled_codecs: r.enabled_codecs.iter().map(codec_spec_to_proto).collect(),
        metadata: r.metadata.clone(),
        num_participants: r.num_participants,
        num_publishers: r.num_publishers,
        active_recording: r.active_recording,
        version: None,
    }
}

pub fn room_info_from_proto(r: &pb::Room) -> RoomInfo {
    let creation_time_ms = if r.creation_time_ms != 0 {
        r.creation_time_ms
    } else {
        r.creation_time.saturating_mul(1000)
    };
    RoomInfo {
        sid: RoomSid(r.sid.clone()),
        name: RoomName(r.name.clone()),
        empty_timeout_s: r.empty_timeout,
        departure_timeout_s: r.departure_timeout,
        max_participants: r.max_participants,
        creation_time_ms,
        metadata: r.metadata.clone(),
        num_participants: r.num_participants,
        num_publishers: r.num_publishers,
        active_recording: r.active_recording,
        enabled_codecs: r.enabled_codecs.iter().map(codec_spec_from_proto).collect(),
        version: r
            .version
            .as_ref()
            .map(timed_version_from_proto)
            .unwrap_or_default(),
        turn_password: r.turn_password.clone(),
    }
}

/// Room as included in `track_published`/`track_unpublished` webhooks when
/// `room.enable_full_room_in_webhooks` is off (`room.go:2045-2054`).
pub fn minimal_room_proto(r: &RoomInfo) -> pb::Room {
    pb::Room {
        sid: r.sid.0.clone(),
        name: r.name.0.clone(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------- broadcasts

pub fn speaker_info_to_proto(s: &SpeakerInfo) -> pb::SpeakerInfo {
    pb::SpeakerInfo {
        sid: s.sid.0.clone(),
        level: s.level,
        active: s.active,
    }
}

pub fn speaker_info_from_proto(s: &pb::SpeakerInfo) -> SpeakerInfo {
    SpeakerInfo {
        sid: ParticipantSid(s.sid.clone()),
        level: s.level,
        active: s.active,
    }
}

pub fn connection_quality_info_to_proto(c: &ConnectionQualityInfo) -> pb::ConnectionQualityInfo {
    pb::ConnectionQualityInfo {
        participant_sid: c.participant_sid.0.clone(),
        quality: connection_quality_to_proto(c.quality),
        score: c.score,
    }
}

pub fn connection_quality_info_from_proto(c: &pb::ConnectionQualityInfo) -> ConnectionQualityInfo {
    ConnectionQualityInfo {
        participant_sid: ParticipantSid(c.participant_sid.clone()),
        quality: connection_quality_from_proto(c.quality),
        score: c.score,
    }
}

pub fn stream_state_info_to_proto(s: &StreamStateInfo) -> pb::StreamStateInfo {
    pb::StreamStateInfo {
        participant_sid: s.participant_sid.0.clone(),
        track_sid: s.track_sid.0.clone(),
        state: stream_state_to_proto(s.state),
    }
}

pub fn stream_state_info_from_proto(s: &pb::StreamStateInfo) -> StreamStateInfo {
    StreamStateInfo {
        participant_sid: ParticipantSid(s.participant_sid.clone()),
        track_sid: TrackSid(s.track_sid.clone()),
        state: stream_state_from_proto(s.state),
    }
}

pub fn subscribed_quality_to_proto(q: &SubscribedQuality) -> pb::SubscribedQuality {
    pb::SubscribedQuality {
        quality: video_quality_to_proto(q.quality),
        enabled: q.enabled,
    }
}

pub fn subscribed_quality_from_proto(q: &pb::SubscribedQuality) -> SubscribedQuality {
    SubscribedQuality {
        quality: video_quality_from_proto(q.quality),
        enabled: q.enabled,
    }
}

pub fn subscribed_codec_to_proto(c: &SubscribedCodec) -> pb::SubscribedCodec {
    pb::SubscribedCodec {
        codec: c.codec.clone(),
        qualities: c
            .qualities
            .iter()
            .map(subscribed_quality_to_proto)
            .collect(),
    }
}

pub fn subscribed_codec_from_proto(c: &pb::SubscribedCodec) -> SubscribedCodec {
    SubscribedCodec {
        codec: c.codec.clone(),
        qualities: c
            .qualities
            .iter()
            .map(subscribed_quality_from_proto)
            .collect(),
    }
}

pub fn track_permission_to_proto(t: &TrackPermission) -> pb::TrackPermission {
    pb::TrackPermission {
        participant_sid: t
            .participant_sid
            .as_ref()
            .map(|s| s.0.clone())
            .unwrap_or_default(),
        all_tracks: t.all_tracks,
        track_sids: t.track_sids.iter().map(|s| s.0.clone()).collect(),
        participant_identity: t
            .participant_identity
            .as_ref()
            .map(|s| s.0.clone())
            .unwrap_or_default(),
    }
}

/// Empty strings mean "not set" on the wire.
pub fn track_permission_from_proto(t: &pb::TrackPermission) -> TrackPermission {
    TrackPermission {
        participant_sid: (!t.participant_sid.is_empty())
            .then(|| ParticipantSid(t.participant_sid.clone())),
        participant_identity: (!t.participant_identity.is_empty())
            .then(|| Identity(t.participant_identity.clone())),
        all_tracks: t.all_tracks,
        track_sids: t.track_sids.iter().map(|s| TrackSid(s.clone())).collect(),
    }
}

pub fn subscription_permission_to_proto(s: &SubscriptionPermission) -> pb::SubscriptionPermission {
    pb::SubscriptionPermission {
        all_participants: s.all_participants,
        track_permissions: s
            .track_permissions
            .iter()
            .map(track_permission_to_proto)
            .collect(),
    }
}

pub fn subscription_permission_from_proto(
    s: &pb::SubscriptionPermission,
) -> SubscriptionPermission {
    SubscriptionPermission {
        all_participants: s.all_participants,
        track_permissions: s
            .track_permissions
            .iter()
            .map(track_permission_from_proto)
            .collect(),
    }
}

// ---------------------------------------------------------------- client info

const CAP_PACKET_TRAILER: i32 = pb::client_info::Capability::CapPacketTrailer as i32;

pub fn client_info_to_meta(ci: &pb::ClientInfo) -> ClientMeta {
    ClientMeta {
        sdk: sdk_from_proto(ci.sdk),
        version: ci.version.clone(),
        protocol: ci.protocol,
        client_protocol: ci.client_protocol,
        os: ci.os.clone(),
        os_version: ci.os_version.clone(),
        device_model: ci.device_model.clone(),
        browser: ci.browser.clone(),
        browser_version: ci.browser_version.clone(),
        address: ci.address.clone(),
        network: ci.network.clone(),
        packet_trailer: ci.capabilities.contains(&CAP_PACKET_TRAILER),
    }
}

pub fn meta_to_client_info(m: &ClientMeta) -> pb::ClientInfo {
    pb::ClientInfo {
        sdk: sdk_to_proto(&m.sdk),
        version: m.version.clone(),
        protocol: m.protocol,
        os: m.os.clone(),
        os_version: m.os_version.clone(),
        device_model: m.device_model.clone(),
        browser: m.browser.clone(),
        browser_version: m.browser_version.clone(),
        address: m.address.clone(),
        network: m.network.clone(),
        client_protocol: m.client_protocol,
        capabilities: if m.packet_trailer {
            vec![CAP_PACKET_TRAILER]
        } else {
            Vec::new()
        },
        ..Default::default()
    }
}

pub fn data_track_frame_encoding_from_proto(
    encoding: &pb::DataTrackFrameEncoding,
) -> DataTrackEncoding {
    match &encoding.value {
        None => DataTrackEncoding::Unset,
        Some(pb::data_track_frame_encoding::Value::WellKnown(value)) => {
            DataTrackEncoding::WellKnown(*value)
        }
        Some(pb::data_track_frame_encoding::Value::Custom(value)) => {
            DataTrackEncoding::Custom(value.clone())
        }
    }
}

pub fn data_track_frame_encoding_to_proto(
    encoding: &DataTrackEncoding,
) -> pb::DataTrackFrameEncoding {
    pb::DataTrackFrameEncoding {
        value: match encoding {
            DataTrackEncoding::Unset => None,
            DataTrackEncoding::WellKnown(value) => {
                Some(pb::data_track_frame_encoding::Value::WellKnown(*value))
            }
            DataTrackEncoding::Custom(value) => {
                Some(pb::data_track_frame_encoding::Value::Custom(value.clone()))
            }
        },
    }
}

pub fn data_track_schema_from_proto(schema: &pb::DataTrackSchemaId) -> DataTrackSchema {
    DataTrackSchema {
        name: schema.name.clone(),
        encoding: schema
            .encoding
            .as_ref()
            .map(|encoding| match &encoding.value {
                None => DataTrackEncoding::Unset,
                Some(pb::data_track_schema_encoding::Value::WellKnown(value)) => {
                    DataTrackEncoding::WellKnown(*value)
                }
                Some(pb::data_track_schema_encoding::Value::Custom(value)) => {
                    DataTrackEncoding::Custom(value.clone())
                }
            }),
    }
}

pub fn data_track_schema_to_proto(schema: &DataTrackSchema) -> pb::DataTrackSchemaId {
    pb::DataTrackSchemaId {
        name: schema.name.clone(),
        encoding: schema
            .encoding
            .as_ref()
            .map(|encoding| pb::DataTrackSchemaEncoding {
                value: match encoding {
                    DataTrackEncoding::Unset => None,
                    DataTrackEncoding::WellKnown(value) => {
                        Some(pb::data_track_schema_encoding::Value::WellKnown(*value))
                    }
                    DataTrackEncoding::Custom(value) => {
                        Some(pb::data_track_schema_encoding::Value::Custom(value.clone()))
                    }
                },
            }),
    }
}

pub fn data_track_info_to_proto(track: &DataTrackInfo) -> pb::DataTrackInfo {
    pb::DataTrackInfo {
        sid: track.sid.0.clone(),
        pub_handle: track.pub_handle,
        name: track.name.clone(),
        encryption: encryption_to_proto(track.encryption),
        frame_encoding: track
            .frame_encoding
            .as_ref()
            .map(data_track_frame_encoding_to_proto),
        schema: track.schema.as_ref().map(data_track_schema_to_proto),
    }
}

pub fn data_track_info_from_proto(track: &pb::DataTrackInfo) -> DataTrackInfo {
    DataTrackInfo {
        sid: TrackSid(track.sid.clone()),
        pub_handle: track.pub_handle,
        name: track.name.clone(),
        encryption: encryption_from_proto(track.encryption),
        frame_encoding: track
            .frame_encoding
            .as_ref()
            .map(data_track_frame_encoding_from_proto),
        schema: track.schema.as_ref().map(data_track_schema_from_proto),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer() -> VideoLayer {
        VideoLayer {
            quality: VideoQuality::Medium,
            width: 640,
            height: 360,
            bitrate: 500_000,
            ssrc: 1234,
            spatial_layer: 1,
            rid: "h".into(),
            repair_ssrc: 99,
        }
    }

    fn track() -> TrackInfo {
        TrackInfo {
            sid: TrackSid("TR_x".into()),
            kind: TrackKind::Video,
            name: "cam".into(),
            muted: true,
            width: 1280,
            height: 720,
            simulcast: true,
            source: TrackSource::Camera,
            layers: vec![layer()],
            mime_type: "video/VP8".into(),
            mid: "1".into(),
            codecs: vec![TrackCodec {
                mime_type: "video/VP8".into(),
                mid: "1".into(),
                cid: "c".into(),
                sdp_cid: "s".into(),
                layers: vec![layer()],
                layer_mode: VideoLayerMode::OneSpatialLayerPerStream,
            }],
            disable_red: true,
            disable_dtx: true,
            stereo: true,
            encryption: EncryptionType::Gcm,
            stream: "camera".into(),
            version: TimedVersion {
                unix_micro: 5,
                ticks: 2,
            },
            audio_features: vec![AudioFeature::Stereo, AudioFeature::PreconnectBuffer],
            backup_codec_policy: BackupCodecPolicy::Simulcast,
            packet_trailer_features: vec![],
        }
    }

    #[test]
    fn track_round_trip() {
        let t = track();
        assert_eq!(track_info_from_proto(&track_info_to_proto(&t)), t);
    }

    fn data_track() -> DataTrackInfo {
        DataTrackInfo {
            sid: TrackSid::from("DTR_example"),
            pub_handle: 65535,
            name: "position.x".into(),
            encryption: EncryptionType::Gcm,
            frame_encoding: Some(DataTrackEncoding::WellKnown(934)),
            schema: Some(DataTrackSchema {
                name: "schema".into(),
                encoding: Some(DataTrackEncoding::Custom("custom-schema".into())),
            }),
        }
    }

    #[test]
    fn data_track_optional_encoding_and_schema_presence_roundtrip() {
        for frame in [
            None,
            Some(DataTrackEncoding::Unset),
            Some(DataTrackEncoding::Custom("json".into())),
            Some(DataTrackEncoding::WellKnown(934)),
        ] {
            for schema in [
                None,
                Some(DataTrackSchema {
                    name: String::new(),
                    encoding: None,
                }),
                Some(DataTrackSchema {
                    name: "x".into(),
                    encoding: Some(DataTrackEncoding::Unset),
                }),
            ] {
                let track = DataTrackInfo {
                    frame_encoding: frame.clone(),
                    schema,
                    ..data_track()
                };
                assert_eq!(
                    data_track_info_from_proto(&data_track_info_to_proto(&track)),
                    track
                );
            }
        }
    }

    #[test]
    fn participant_round_trip() {
        let mut attributes = BTreeMap::new();
        attributes.insert("a".to_string(), "b".to_string());
        let p = ParticipantInfo {
            sid: ParticipantSid("PA_x".into()),
            identity: Identity("alice".into()),
            name: "Alice".into(),
            state: ParticipantState::Active,
            tracks: vec![track()],
            data_tracks: vec![data_track()],
            metadata: "m".into(),
            attributes,
            joined_at_ms: 1_700_000_000_123,
            version: 7,
            permission: ParticipantPermission {
                can_publish_sources: vec![TrackSource::Microphone],
                hidden: true,
                ..Default::default()
            },
            region: "r".into(),
            is_publisher: true,
            kind: ParticipantKind::Agent,
            disconnect_reason: DisconnectReason::RoomDeleted,
            client_protocol: 1,
        };
        let pb = participant_info_to_proto(&p);
        assert_eq!(pb.joined_at, 1_700_000_000);
        assert_eq!(participant_info_from_proto(&pb), p);
    }

    #[test]
    fn room_round_trip() {
        let r = RoomInfo {
            sid: RoomSid("RM_x".into()),
            name: RoomName("room".into()),
            empty_timeout_s: 300,
            departure_timeout_s: 20,
            max_participants: 3,
            creation_time_ms: 1_700_000_000_999,
            metadata: "md".into(),
            num_participants: 2,
            num_publishers: 1,
            active_recording: false,
            enabled_codecs: vec![CodecSpec {
                mime: "audio/opus".into(),
                fmtp_line: String::new(),
            }],
            version: TimedVersion {
                unix_micro: 1,
                ticks: 0,
            },
            turn_password: "fixed-secret".into(),
        };
        let pb = room_info_to_proto(&r);
        assert_eq!(pb.creation_time, 1_700_000_000);
        assert_eq!(pb.turn_password, "fixed-secret");
        assert!(
            pb.version.is_none(),
            "the fixed server never sets Room.version"
        );
        assert_eq!(
            room_info_from_proto(&pb),
            RoomInfo {
                version: TimedVersion::default(),
                ..r.clone()
            }
        );
        let min = minimal_room_proto(&r);
        assert_eq!(min.name, "room");
        assert_eq!(min.empty_timeout, 0);
    }

    #[test]
    fn subscription_permission_round_trip() {
        let s = SubscriptionPermission {
            all_participants: false,
            track_permissions: vec![TrackPermission {
                participant_sid: None,
                participant_identity: Some(Identity("bob".into())),
                all_tracks: false,
                track_sids: vec![TrackSid("TR_a".into())],
            }],
        };
        assert_eq!(
            subscription_permission_from_proto(&subscription_permission_to_proto(&s)),
            s
        );
    }

    #[test]
    fn client_meta_round_trip() {
        let m = ClientMeta {
            sdk: rtc_core::session::SdkKind::Js,
            version: "2.0".into(),
            protocol: 16,
            client_protocol: 1,
            os: "macOS".into(),
            browser: "Chrome".into(),
            packet_trailer: true,
            ..Default::default()
        };
        assert_eq!(client_info_to_meta(&meta_to_client_info(&m)), m);
    }
}
