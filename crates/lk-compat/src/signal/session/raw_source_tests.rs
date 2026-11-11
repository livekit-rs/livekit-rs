use super::*;
use rtc_core::{
    ids::{Identity, ProjectId, RoomKey},
    model::TrackSource,
    permission::Grants,
    session::{JoinParams, SessionOutput, SessionRequest, SignalEvent},
};
use rtc_runtime::api::ParticipantUpdate;
use std::collections::BTreeMap;

fn params(key: &RoomKey, grants: Grants) -> JoinParams {
    JoinParams {
        room: key.clone(),
        identity: Identity(grants.identity.clone()),
        name: grants.name.clone(),
        metadata: grants.metadata.clone(),
        attributes: grants.attributes.clone(),
        grants,
        client: Default::default(),
        capabilities: Default::default(),
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
async fn next_claims(handle: &mut SessionHandle) -> SignalEvent {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle
                .output
                .recv()
                .await
                .expect("actual owner output remains open")
            {
                SessionOutput::Signal(event @ SignalEvent::ClaimsChanged(_)) => return event,
                SessionOutput::Signal(_) => {}
                SessionOutput::Close => panic!("owner closed before claims"),
            }
        }
    })
    .await
    .expect("actual owner claims deadline")
}
fn decoded_refresh(
    ctx: &mut SessionCtx,
    event: SignalEvent,
    sid: &rtc_core::ids::ParticipantSid,
) -> Grants {
    let response = ctx.compose(event, sid).expect("actual refresh response");
    let Some(Resp::RefreshToken(token)) = response.message else {
        panic!("expected refresh token")
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    crate::auth::verify_token(&ctx.state.keys, &token, now)
        .unwrap()
        .grants
}
fn context(state: &AppState, grants: Grants) -> SessionCtx {
    SessionCtx {
        state: state.clone(),
        grants,
        client: Default::default(),
        cache: Default::default(),
    }
}
fn raw_grants(room: &str, sources: serde_json::Value) -> Grants {
    let value = serde_json::json!({"video": {"roomJoin": true, "room": room,
        "canUpdateOwnMetadata": true, "canPublishSources": sources}});
    let decoded = crate::auth::claims::decode_grants(value.as_object().unwrap()).unwrap();
    Grants {
        identity: "raw-source-owner".into(),
        video: decoded.video,
        ..Default::default()
    }
}
fn video(grants: &Grants) -> serde_json::Value {
    crate::auth::minter::video_grant_json(&grants.video, grants.video.can_manage_agent_session)
}
#[test]
fn original_public_auth_functions_keep_raw_strings_and_distinct_permission_projection() {
    let raw = serde_json::json!([
        "camera",
        "future_source",
        "CAMERA",
        "microphone",
        "future_source"
    ]);
    let initial = raw_grants("owned", raw.clone());
    assert_eq!(
        video(&initial)["canPublishSources"],
        raw,
        "raw refresh must not canonicalize original strings"
    );
    let permission = initial.video.to_permission();
    assert_eq!(
        permission.can_publish_sources,
        vec![
            TrackSource::Camera,
            TrackSource::Unknown,
            TrackSource::Camera,
            TrackSource::Microphone,
            TrackSource::Unknown
        ]
    );
    let mut changed = permission.clone();
    changed.can_manage_agent_session = true;
    let rewritten = crate::auth::claims::video_grant_with_permission(&initial.video, &changed);
    let canonical = crate::auth::minter::video_grant_json(&rewritten, Some(true));
    assert_eq!(
        canonical["canPublishSources"],
        serde_json::json!(["camera", "unknown", "camera", "microphone", "unknown"])
    );
}
#[tokio::test]
async fn actual_owner_exact_admission_survives_equal_permission_until_real_materialization() {
    use rtc_core::{
        model::{BackupCodecPolicy, EncryptionType, TrackKind},
        session::{AddTrack, RequestOutcome},
    };
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "raw-source-three-consumers");
    let initial = raw_grants(&key.name.0, serde_json::json!(["CAMERA"]));
    let mut join = params(&key, initial.clone());
    join.capabilities.request_response = true;
    let mut handle = state.manager.join(join).await.unwrap();
    let mut ctx = context(&state, initial.clone());
    let event = next_claims(&mut handle).await;
    let first = decoded_refresh(&mut ctx, event, &handle.participant_sid);
    assert_eq!(
        video(&first)["canPublishSources"],
        serde_json::json!(["CAMERA"])
    );
    let before = state
        .manager
        .get_participant(&key, &handle.identity)
        .await
        .unwrap();
    assert_eq!(
        before.permission.can_publish_sources,
        vec![TrackSource::Camera]
    );
    let same = state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                permission: Some(before.permission.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        same.version, before.version,
        "MatchesPermission must not rewrite raw spelling"
    );
    state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                attributes: BTreeMap::from([("k".into(), "v".into())]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let event = next_claims(&mut handle).await;
    assert_eq!(
        video(&decoded_refresh(&mut ctx, event, &handle.participant_sid))["canPublishSources"],
        serde_json::json!(["CAMERA"])
    );
    let request = |token, cid: &str| {
        SessionRequest::AddTrack(AddTrack {
            token,
            cid: cid.into(),
            name: String::new(),
            kind: TrackKind::Video,
            width: 0,
            height: 0,
            muted: false,
            disable_dtx: false,
            source: TrackSource::Camera,
            layers: vec![],
            simulcast_codecs: vec![],
            sid: None,
            stereo: false,
            disable_red: false,
            encryption: EncryptionType::None,
            stream: String::new(),
            backup_codec_policy: BackupCodecPolicy::PreferRegression,
            audio_features: vec![],
            packet_trailer_features: vec![],
        })
    };
    assert!(handle.apply(request(901, "raw-uppercase-denied")).await);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle.output.recv().await.expect("actual owner output") {
                SessionOutput::Signal(SignalEvent::RequestResponse {
                    token: 901,
                    outcome,
                    message,
                    ..
                }) => {
                    assert_eq!(outcome, RequestOutcome::NotAllowed);
                    assert_eq!(message, "");
                    break;
                }
                SessionOutput::Signal(SignalEvent::TrackPublished { cid, .. })
                    if cid == "raw-uppercase-denied" =>
                {
                    panic!("public enum projection must not grant raw uppercase media admission")
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    let mut permission = same.permission;
    permission.can_manage_agent_session = true;
    state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                permission: Some(permission),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let event = next_claims(&mut handle).await;
    let materialized = decoded_refresh(&mut ctx, event, &handle.participant_sid);
    assert_eq!(
        video(&materialized)["canPublishSources"],
        serde_json::json!(["camera"])
    );
    assert_eq!(materialized.video.can_manage_agent_session, Some(true));
    assert!(handle.apply(request(902, "canonical-accepted")).await);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle.output.recv().await.expect("actual owner output") {
                SessionOutput::Signal(SignalEvent::TrackPublished { cid, .. })
                    if cid == "canonical-accepted" =>
                {
                    break;
                }
                SessionOutput::Signal(SignalEvent::RequestResponse {
                    token: 902,
                    outcome,
                    ..
                }) => panic!("materialized canonical source denied: {outcome:?}"),
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    drop(handle);
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}
async fn publication_accepted(
    handle: &mut SessionHandle,
    token: u32,
    cid: &str,
    source: TrackSource,
) -> bool {
    use rtc_core::{
        model::{BackupCodecPolicy, EncryptionType, TrackKind},
        session::{AddTrack, RequestOutcome},
    };
    assert!(
        handle
            .apply(SessionRequest::AddTrack(AddTrack {
                token,
                cid: cid.into(),
                name: String::new(),
                kind: TrackKind::Video,
                width: 0,
                height: 0,
                muted: false,
                disable_dtx: false,
                source,
                layers: vec![],
                simulcast_codecs: vec![],
                sid: None,
                stereo: false,
                disable_red: false,
                encryption: EncryptionType::None,
                stream: String::new(),
                backup_codec_policy: BackupCodecPolicy::PreferRegression,
                audio_features: vec![],
                packet_trailer_features: vec![],
            }))
            .await
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle
                .output
                .recv()
                .await
                .expect("actual owner publication output")
            {
                SessionOutput::Signal(SignalEvent::TrackPublished { cid: actual, .. })
                    if actual == cid =>
                {
                    return true;
                }
                SessionOutput::Signal(SignalEvent::RequestResponse {
                    token: actual,
                    outcome,
                    message,
                    ..
                }) if actual == token => {
                    assert_eq!(outcome, RequestOutcome::NotAllowed);
                    assert_eq!(message, "");
                    return false;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
#[ignore = "requires independently executed original Go raw-source observer JSONL and cases"]
async fn actual_go_raw_sources_three_consumers() {
    use serde_json::Value;
    let inputs: Vec<Value> = serde_json::from_str(
        &std::fs::read_to_string(std::env::var_os("RAW_SOURCE_GO_CASES").unwrap()).unwrap(),
    )
    .unwrap();
    let text = std::fs::read_to_string(std::env::var_os("RAW_SOURCE_GO_RESULTS").unwrap()).unwrap();
    let rows: Vec<Value> = text
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(rows.len(), inputs.len());
    assert_eq!(rows.len(), 16);
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "owned");
    let mut seen = std::collections::BTreeSet::new();
    let expected_ids = [
        "unknown",
        "upper",
        "mixedcase",
        "duplicateknown",
        "duplicateunknown",
        "emptystring",
        "mixed",
        "reverse",
        "canonicalunknown",
        "long-s",
        "dotless-i",
        "null-element",
        "empty-list",
        "null-list",
        "publish-false",
        "mixed-all",
    ];
    for ((input, row), id) in inputs.iter().zip(rows).zip(expected_ids) {
        assert_eq!(input["id"], id, "fixed input inventory");
        assert_eq!(input["id"], row["id"]);
        assert!(seen.insert(row["id"].as_str().unwrap().to_owned()));
        let value = serde_json::json!({"video":{"roomJoin":true,"room":"owned","canPublishSources":input["sources"],"canPublish":input["can_publish"]}});
        let decoded = crate::auth::claims::decode_grants(value.as_object().unwrap()).unwrap();
        let permission =
            crate::auth::claims::permission_from_grants(&decoded.video, &decoded.extra);
        let actual = crate::auth::minter::video_grant_json(
            &decoded.video,
            decoded.video.can_manage_agent_session,
        );
        assert_eq!(
            actual, row["video_before"],
            "original raw video {}",
            input["id"]
        );
        assert_eq!(
            serde_json::json!(crate::convert::permission_to_proto(&permission).can_publish_sources),
            row["permission_sources"]
        );
        let initial = Grants {
            identity: "source-row-owner".into(),
            video: decoded.video,
            ..Default::default()
        };
        let mut join = params(&key, initial.clone());
        join.capabilities.request_response = true;
        let mut handle = state.manager.join(join).await.unwrap();
        let mut ctx = context(&state, initial);
        let event = next_claims(&mut handle).await;
        let refreshed = decoded_refresh(&mut ctx, event, &handle.participant_sid);
        assert_eq!(video(&refreshed), row["refresh_video"]);
        let before = state
            .manager
            .get_participant(&key, &handle.identity)
            .await
            .unwrap();
        let same = state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    permission: Some(permission.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(before.version, same.version);
        assert_eq!(row["same_matches"], true);
        // Observe the actual same-permission owner grant via an attributes-only callback.
        state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    attributes: BTreeMap::from([("source-observer".into(), "actual".into())]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let event = next_claims(&mut handle).await;
        assert_eq!(
            video(&decoded_refresh(&mut ctx, event, &handle.participant_sid)),
            row["same_video"]
        );
        let sources = [
            TrackSource::Unknown,
            TrackSource::Camera,
            TrackSource::Microphone,
            TrackSource::ScreenShare,
            TrackSource::ScreenShareAudio,
        ];
        let mut admission = Vec::new();
        for (i, source) in sources.into_iter().enumerate() {
            admission.push(
                publication_accepted(&mut handle, 1000 + i as u32, &format!("before-{i}"), source)
                    .await,
            );
        }
        assert_eq!(serde_json::json!(admission), row["admission_before"]);
        let mut updated = permission;
        updated.can_manage_agent_session = !updated.can_manage_agent_session;
        updated.can_publish_sources.reverse();
        state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    permission: Some(updated),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let event = next_claims(&mut handle).await;
        assert_eq!(
            video(&decoded_refresh(&mut ctx, event, &handle.participant_sid)),
            row["changed_refresh_video"]
        );
        assert_eq!(row["changed_refresh_video"], row["changed_video"]);
        assert_eq!(row["changed_matches"], false);
        let mut admission = Vec::new();
        for (i, source) in sources.into_iter().enumerate() {
            admission.push(
                publication_accepted(&mut handle, 2000 + i as u32, &format!("after-{i}"), source)
                    .await,
            );
        }
        assert_eq!(serde_json::json!(admission), row["admission_after"]);
        drop(handle);
        state.manager.delete_room(&key).await.unwrap();
    }
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}
