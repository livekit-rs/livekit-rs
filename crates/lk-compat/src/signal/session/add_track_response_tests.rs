//! Original public APIs only: wire decode/cache → actual room owner → wire response.
use super::*;
use rtc_core::{
    ids::{Identity, ProjectId, RoomKey},
    permission::{Grants, VideoGrant},
    session::{JoinParams, SdkKind, SessionRequest},
};

fn params(key: &RoomKey, grants: Grants) -> JoinParams {
    let client = ClientMeta {
        sdk: SdkKind::Rust,
        protocol: 10,
        ..Default::default()
    };
    JoinParams {
        room: key.clone(),
        identity: Identity(grants.identity.clone()),
        name: grants.name.clone(),
        metadata: grants.metadata.clone(),
        attributes: grants.attributes.clone(),
        grants,
        capabilities: crate::profile::capabilities(&client),
        client,
        auto_subscribe: true,
        auto_subscribe_data_track: true,
        adaptive_stream: false,
        single_pc: false,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}

// Nondefault known fields, including packet trailer features that the neutral
// AddTrack model does not carry: the adapter must echo its original request.
#[allow(deprecated)]
fn original(cid: &str, kind: i32) -> pb::AddTrackRequest {
    let layer = pb::VideoLayer {
        quality: 2,
        width: 1280,
        height: 720,
        bitrate: 900_001,
        ssrc: 1234,
        spatial_layer: 1,
        rid: "h".into(),
        repair_ssrc: 4321,
    };
    pb::AddTrackRequest {
        cid: cid.into(),
        name: "diagnostic original".into(),
        r#type: kind,
        width: 1280,
        height: 720,
        muted: true,
        disable_dtx: true,
        source: pb::TrackSource::Microphone as i32,
        layers: vec![layer.clone()],
        simulcast_codecs: vec![pb::SimulcastCodec {
            codec: "opus".into(),
            cid: "nested-codec-cid".into(),
            layers: vec![layer],
            video_layer_mode: 2,
        }],
        sid: "TR_echo_not_lookup".into(),
        stereo: true,
        disable_red: true,
        encryption: 1,
        stream: "echo-stream".into(),
        backup_codec_policy: 1,
        audio_features: vec![1, 4, 1],
        packet_trailer_features: vec![2, 0, 2],
    }
}

async fn rejected_response(
    handle: &mut SessionHandle,
    ctx: &mut SessionCtx,
    original: &pb::AddTrackRequest,
) -> pb::RequestResponse {
    let incoming = pb::SignalRequest {
        message: Some(pb::signal_request::Message::AddTrack(original.clone())),
    };
    let incoming = pb::SignalRequest::decode(incoming.encode_to_vec().as_slice()).unwrap();
    let Inbound::Forward(request @ SessionRequest::AddTrack(_)) =
        decode_request(incoming, &mut ctx.cache)
    else {
        panic!("actual adapter must forward AddTrack")
    };
    let SessionRequest::AddTrack(ref add) = request else {
        unreachable!()
    };
    let expected_token = add.token;
    assert!(handle.apply(request).await);
    let event = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle
                .output
                .recv()
                .await
                .expect("actual owner remains open")
            {
                SessionOutput::Signal(event @ SignalEvent::RequestResponse { token, .. }) => {
                    assert_eq!(
                        token, expected_token,
                        "response must correlate original cache entry"
                    );
                    return event;
                }
                SessionOutput::Signal(SignalEvent::TrackPublished { cid, .. })
                    if cid == original.cid =>
                {
                    panic!("rejected AddTrack must not publish")
                }
                SessionOutput::Close => panic!("owner closed before rejection"),
                _ => {}
            }
        }
    })
    .await
    .expect("actual owner rejection deadline");
    let response = ctx.compose(event, &handle.participant_sid).unwrap();
    let response = pb::SignalResponse::decode(response.encode_to_vec().as_slice()).unwrap();
    let Some(Resp::RequestResponse(response)) = response.message else {
        panic!("actual encoded RequestResponse")
    };
    response
}

async fn responses(
    allowed: bool,
    kinds: &[i32],
) -> Vec<(pb::AddTrackRequest, pb::RequestResponse)> {
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "add-track-response-diagnostics");
    let grants = Grants {
        identity: "response-owner".into(),
        video: VideoGrant {
            room_join: true,
            room: key.name.0.clone(),
            can_publish: Some(allowed),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut handle = state
        .manager
        .join(params(&key, grants.clone()))
        .await
        .unwrap();
    let mut ctx = SessionCtx {
        state: state.clone(),
        grants,
        client: ClientMeta {
            sdk: SdkKind::Rust,
            protocol: 10,
            ..Default::default()
        },
        cache: Default::default(),
    };
    let mut results = Vec::new();
    for (index, kind) in kinds.iter().enumerate() {
        let original = original(&format!("diagnostic-{index}"), *kind);
        let response = rejected_response(&mut handle, &mut ctx, &original).await;
        results.push((original, response));
    }
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
    results
}

fn assert_original_rejection(
    original: pb::AddTrackRequest,
    actual: pb::RequestResponse,
    reason: pb::request_response::Reason,
) {
    assert_eq!(actual.reason, reason as i32);
    assert_eq!(actual.request_id, 0, "AddTrack has no request_id field");
    assert_eq!(
        actual.request,
        Some(pb::request_response::Request::AddTrack(original)),
        "complete known typed original request must survive owner projection and response encoding"
    );
    assert_eq!(
        actual.message, "",
        "fixed Go AddTrack omits Message in this rejection"
    );
}

#[tokio::test]
async fn actual_owner_denied_add_track_preserves_source_response_and_permission_precedence() {
    for (original, actual) in responses(
        false,
        &[pb::TrackType::Audio as i32, pb::TrackType::Data as i32],
    )
    .await
    {
        assert_original_rejection(original, actual, pb::request_response::Reason::NotAllowed);
    }
}

#[tokio::test]
async fn actual_owner_unsupported_add_track_preserves_source_response() {
    for (original, actual) in responses(true, &[pb::TrackType::Data as i32]).await {
        assert_original_rejection(
            original,
            actual,
            pb::request_response::Reason::UnsupportedType,
        );
    }
}

// Append-only old-API regressions for every non-AUDIO/VIDEO numeric TrackType.
// None references the candidate opaque enum or its new methods.
#[test]
fn old_public_add_track_kind_binary_projection_retains_signed_i32() {
    use prost::Message as _;
    for value in [i32::MIN, -1, 0, 1, 2, 99, i32::MAX] {
        let input = pb::SignalRequest {
            message: Some(pb::signal_request::Message::AddTrack(pb::AddTrackRequest {
                cid: "kind-projection".into(),
                r#type: value,
                ..Default::default()
            })),
        };
        let decoded = pb::SignalRequest::decode(input.encode_to_vec().as_slice()).unwrap();
        let mut cache = RequestCache::default();
        let Inbound::Forward(SessionRequest::AddTrack(request)) =
            decode_request(decoded, &mut cache)
        else {
            panic!("existing decoder must forward AddTrack")
        };
        assert_eq!(
            crate::convert::track_kind_to_proto(request.kind),
            value,
            "legal open TrackType must not become AUDIO before owner validation"
        );
    }
}

async fn unknown_kind_responses(
    allowed: bool,
    kinds: &[i32],
) -> Vec<(pb::AddTrackRequest, pb::RequestResponse)> {
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "add-track-open-kind-diagnostics");
    let grants = Grants {
        identity: "open-kind-owner".into(),
        video: VideoGrant {
            room_join: true,
            room: key.name.0.clone(),
            can_publish: Some(allowed),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut handle = state
        .manager
        .join(params(&key, grants.clone()))
        .await
        .unwrap();
    let mut ctx = SessionCtx {
        state: state.clone(),
        grants,
        client: ClientMeta {
            sdk: SdkKind::Rust,
            protocol: 10,
            ..Default::default()
        },
        cache: Default::default(),
    };
    let mut results = Vec::new();
    for (index, kind) in kinds.iter().enumerate() {
        let mut original = original(&format!("open-kind-{index}"), *kind);
        // Empty SID exercises new publication, exposing any unknown→Audio alias.
        original.sid.clear();
        let response = rejected_response(&mut handle, &mut ctx, &original).await;
        results.push((original, response));
    }
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
    results
}

#[tokio::test]
async fn old_public_owner_rejects_every_unknown_track_kind_with_exact_echo() {
    for (original, actual) in unknown_kind_responses(true, &[99, -1, i32::MIN, i32::MAX]).await {
        assert_original_rejection(
            original,
            actual,
            pb::request_response::Reason::UnsupportedType,
        );
    }
}

#[tokio::test]
async fn old_public_owner_unknown_kind_still_checks_publish_permission_first() {
    for (original, actual) in unknown_kind_responses(false, &[99, -1, i32::MIN, i32::MAX]).await {
        assert_original_rejection(original, actual, pb::request_response::Reason::NotAllowed);
    }
}

// Old public APIs: successful owner response, never a new-field compile premise.
async fn metadata_positive_response(input: pb::AddTrackRequest) -> pb::TrackInfo {
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "add-track-positive-metadata");
    let grants = Grants {
        identity: "positive-metadata-owner".into(),
        video: VideoGrant {
            room_join: true,
            room: key.name.0.clone(),
            can_publish: Some(true),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut handle = state
        .manager
        .join(params(&key, grants.clone()))
        .await
        .unwrap();
    let mut ctx = SessionCtx {
        state: state.clone(),
        grants,
        client: ClientMeta {
            sdk: SdkKind::Rust,
            protocol: 10,
            ..Default::default()
        },
        cache: Default::default(),
    };
    let wire = pb::SignalRequest {
        message: Some(pb::signal_request::Message::AddTrack(input.clone())),
    };
    let wire = pb::SignalRequest::decode(wire.encode_to_vec().as_slice()).unwrap();
    let Inbound::Forward(request @ SessionRequest::AddTrack(_)) =
        decode_request(wire, &mut ctx.cache)
    else {
        panic!("real decoder forwards AddTrack")
    };
    assert!(handle.apply(request).await);
    let track = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle.output.recv().await.expect("owner alive") {
                SessionOutput::Signal(event @ SignalEvent::TrackPublished { .. }) => {
                    let response = ctx.compose(event, &handle.participant_sid).unwrap();
                    let response =
                        pb::SignalResponse::decode(response.encode_to_vec().as_slice()).unwrap();
                    let Some(Resp::TrackPublished(actual)) = response.message else {
                        panic!("actual typed publication")
                    };
                    assert_eq!(actual.cid, input.cid);
                    return actual.track.expect("actual TrackInfo");
                }
                SessionOutput::Signal(SignalEvent::RequestResponse { .. }) => {
                    panic!("valid AUDIO must succeed")
                }
                SessionOutput::Close => panic!("owner closed"),
                _ => {}
            }
        }
    })
    .await
    .expect("owner response deadline");
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
    track
}

#[tokio::test]
async fn old_public_positive_add_track_preserves_open_encryption() {
    for value in [0, 1, 2, 99, -1, i32::MIN, i32::MAX] {
        let input = pb::AddTrackRequest {
            cid: format!("encryption-{value}"),
            encryption: value,
            ..Default::default()
        };
        let track = metadata_positive_response(input).await;
        assert_eq!(
            track.encryption, value,
            "fixed source copies raw signed encryption"
        );
    }
}

#[tokio::test]
async fn old_public_positive_add_track_sorts_dedupes_and_retains_audio_features() {
    for (features, stereo, disable_dtx, expected) in [
        (vec![4, 1, 4, 0, 1], false, false, vec![0, 1, 4]),
        (
            vec![99, -1, 6, 99, i32::MIN, i32::MAX],
            false,
            false,
            vec![i32::MIN, -1, 6, 99, i32::MAX],
        ),
        (vec![99, 4, 99], true, true, vec![4, 99, 0, 1]),
    ] {
        let input = pb::AddTrackRequest {
            cid: "features".into(),
            audio_features: features,
            stereo,
            disable_dtx,
            ..Default::default()
        };
        let track = metadata_positive_response(input).await;
        assert_eq!(
            track.audio_features, expected,
            "Go sorts/dedupes before appending legacy flags"
        );
        assert_eq!(track.stereo, stereo);
        assert_eq!(track.disable_dtx, disable_dtx);
    }
}

#[tokio::test]
async fn old_public_positive_add_track_retains_sorted_packet_trailer_features() {
    for (features, expected) in [
        (vec![2, 0, 2, 1], vec![0, 1, 2]),
        (
            vec![99, -1, i32::MIN, 2, 99, i32::MAX],
            vec![i32::MIN, -1, 2, 99, i32::MAX],
        ),
    ] {
        let input = pb::AddTrackRequest {
            cid: "trailers".into(),
            packet_trailer_features: features,
            ..Default::default()
        };
        let track = metadata_positive_response(input).await;
        assert_eq!(
            track.packet_trailer_features, expected,
            "positive metadata is separate from rejection echo"
        );
    }
}

#[test]
fn old_public_add_track_actual_expect_track_uses_nonzero_encryption() {
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::media_api::{DataKind, MediaCommand};
    use rtc_core::room::{Effect, Room};
    struct DropData;
    impl DataHandler for DropData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }
    for value in [0, 1, 2, 99, -1, i32::MIN, i32::MAX] {
        let config = crate::test_support::config();
        let now = std::time::Instant::now();
        let key = RoomKey::new(ProjectId::local(), "actual-encrypted-command");
        let grants = Grants {
            identity: "command-owner".into(),
            video: VideoGrant {
                room_join: true,
                room: key.name.0.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut room = Room::new(
            key.clone(),
            std::sync::Arc::new((&config).into()),
            std::sync::Arc::new(DropData),
            &Default::default(),
            now,
        );
        let joined = room.join(params(&key, grants), now).unwrap();
        room.take_effects();
        let input = pb::AddTrackRequest {
            cid: "actual-command".into(),
            encryption: value,
            ..Default::default()
        };
        let request = crate::signal::params::add_track_from_proto(&input, 1);
        room.handle_request(
            &joined.sid,
            joined.generation,
            SessionRequest::AddTrack(request),
            now,
        );
        let commands = room.take_effects();
        let track = commands
            .iter()
            .find_map(|effect| match effect {
                Effect::Media(MediaCommand::ExpectTrack { track, .. }) => Some(track),
                _ => None,
            })
            .expect("real AddTrack produces actual media command");
        assert_eq!(
            track.encrypted,
            value != 0,
            "actual PublishedTrackSpec matches Go IsEncrypted; not a native-media claim"
        );
    }
}

#[test]
fn old_public_track_info_roundtrip_preserves_all_open_metadata_without_owner_normalization() {
    let input = pb::TrackInfo {
        encryption: -1,
        audio_features: vec![99, 4, 99, -1],
        packet_trailer_features: vec![2, 99, 2, -1],
        ..Default::default()
    };
    let output =
        crate::convert::track_info_to_proto(&crate::convert::track_info_from_proto(&input));
    assert_eq!(output.encryption, input.encryption);
    assert_eq!(output.audio_features, input.audio_features);
    assert_eq!(
        output.packet_trailer_features,
        input.packet_trailer_features
    );
}
