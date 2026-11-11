use super::*;
use rtc_core::{
    ids::{Identity, ProjectId, RoomKey},
    permission::Grants,
    session::{JoinParams, SessionOutput, SessionRequest, SignalEvent},
};
use rtc_runtime::api::ParticipantUpdate;

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
#[test]
fn old_public_binary_add_track_and_model_adapters_preserve_open_source_i32() {
    use prost::Message as _;
    for value in [i32::MIN, -1, 0, 1, 2, 3, 4, 99, i32::MAX] {
        let wire = pb::SignalRequest {
            message: Some(pb::signal_request::Message::AddTrack(pb::AddTrackRequest {
                cid: "raw-open".into(),
                r#type: 1,
                source: value,
                ..Default::default()
            })),
        }
        .encode_to_vec();
        let parsed = pb::SignalRequest::decode(wire.as_slice()).unwrap();
        let Some(pb::signal_request::Message::AddTrack(request)) = parsed.message else {
            panic!("actual protobuf AddTrack")
        };
        let neutral = crate::signal::params::add_track_from_proto(&request, 1);
        assert_eq!(
            convert::track_source_to_proto(neutral.source),
            value,
            "binary source is open i32"
        );
        let info = pb::TrackInfo {
            source: value,
            ..Default::default()
        };
        assert_eq!(
            convert::track_info_to_proto(&convert::track_info_from_proto(&info)).source,
            value
        );
        if !(0..=4).contains(&value) {
            let encoded = lk_protocol::json::encode_default(&convert::track_info_to_proto(
                &convert::track_info_from_proto(&info),
            ))
            .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(
                json["source"],
                serde_json::json!(value),
                "existing public ProtoJSON source stays numeric"
            );
        }
        let permission = pb::ParticipantPermission {
            can_publish_sources: vec![value],
            ..Default::default()
        };
        assert_eq!(
            convert::permission_to_proto(&convert::permission_from_proto(&permission))
                .can_publish_sources,
            vec![value]
        );
    }
}
fn grants(room: &str, sources: Vec<String>) -> Grants {
    let value =
        serde_json::json!({"video":{"roomJoin":true,"room":room,"canPublishSources":sources}});
    let decoded = crate::auth::claims::decode_grants(value.as_object().unwrap()).unwrap();
    Grants {
        identity: "open-numeric-owner".into(),
        video: decoded.video,
        ..Default::default()
    }
}
async fn add(
    handle: &mut SessionHandle,
    ctx: &mut SessionCtx,
    value: i32,
    token: u32,
    cid: &str,
) -> (bool, Option<i32>) {
    let request = pb::AddTrackRequest {
        cid: cid.into(),
        r#type: 1,
        source: value,
        ..Default::default()
    };
    assert!(
        handle
            .apply(SessionRequest::AddTrack(
                crate::signal::params::add_track_from_proto(&request, token)
            ))
            .await
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match handle.output.recv().await.expect("actual owner output") {
                SessionOutput::Signal(event @ SignalEvent::TrackPublished { .. }) => {
                    let response = ctx.compose(event, &handle.participant_sid).unwrap();
                    let Some(Resp::TrackPublished(value)) = response.message else {
                        panic!("actual wire track published")
                    };
                    if value.cid == cid {
                        return (true, Some(value.track.unwrap().source));
                    }
                }
                SessionOutput::Signal(SignalEvent::RequestResponse {
                    token: actual,
                    outcome,
                    message,
                    ..
                }) if actual == token => {
                    assert_eq!(outcome, rtc_core::session::RequestOutcome::NotAllowed);
                    assert_eq!(message, "");
                    return (false, None);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("unchanged owner output3s")
}
#[tokio::test]
async fn old_public_owner_addtrack_numeric_source_authorization_and_wire_output() {
    let (state, net) = crate::test_support::state().await;
    for (index, sources, expected) in [
        (0, vec!["99".into()], true),
        (1, vec!["unknown".into()], false),
        (2, vec![], true),
    ] {
        let key = RoomKey::new(ProjectId::local(), format!("open-source-{index}"));
        let initial = grants(&key.name.0, sources);
        let mut join = params(&key, initial.clone());
        join.capabilities.request_response = true;
        let mut handle = state.manager.join(join).await.unwrap();
        let mut ctx = context(&state, initial);
        let _ = next_claims(&mut handle).await;
        let (allowed, source) = add(&mut handle, &mut ctx, 99, 70, "open-numeric").await;
        assert_eq!(allowed, expected);
        assert_eq!(source, expected.then_some(99));
        drop(handle);
        state.manager.delete_room(&key).await.unwrap();
    }
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
async fn old_public_permission_numeric_source_materializes_raw_but_public_projection_is_unknown() {
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "open-source-permission");
    let initial = grants(&key.name.0, vec![]);
    let mut join = params(&key, initial.clone());
    join.capabilities.request_response = true;
    let mut handle = state.manager.join(join).await.unwrap();
    let mut ctx = context(&state, initial);
    let _ = next_claims(&mut handle).await;
    let before = state
        .manager
        .get_participant(&key, &handle.identity)
        .await
        .unwrap();
    let mut incoming = convert::permission_to_proto(&before.permission);
    incoming.can_publish_sources = vec![99];
    let permission = convert::permission_from_proto(&incoming);
    let first = state
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
    assert_eq!(
        convert::permission_to_proto(&first.permission).can_publish_sources,
        vec![0],
        "Participant.ToProto uses grant.ToPermission, not last input enum"
    );
    let event = next_claims(&mut handle).await;
    let token = decoded_refresh(&mut ctx, event, &handle.participant_sid);
    assert_eq!(token.video.can_publish_sources, vec!["99".to_owned()]);
    assert_eq!(
        add(&mut handle, &mut ctx, 99, 71, "after-admin").await,
        (true, Some(99))
    );
    let second = state
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
    assert!(
        second.version > first.version,
        "raw99 projectsUNKNOWN, so repeated incoming99 still fails MatchesPermission"
    );
    let event = next_claims(&mut handle).await;
    assert_eq!(
        decoded_refresh(&mut ctx, event, &handle.participant_sid)
            .video
            .can_publish_sources,
        vec!["99".to_owned()]
    );
    drop(handle);
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
#[ignore = "requires independent actual Go source12; runtime owner and signed refresh, no RTP claim"]
async fn actual_go_open_numeric_source12_all_columns() {
    let cases: Vec<serde_json::Value> = serde_json::from_str(
        &std::fs::read_to_string(std::env::var_os("OPEN_SOURCE_GO_CASES").unwrap()).unwrap(),
    )
    .unwrap();
    let raw = std::fs::read_to_string(std::env::var_os("OPEN_SOURCE_GO_RESULTS").unwrap()).unwrap();
    let rows: Vec<serde_json::Value> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(cases.len(), 12);
    assert_eq!(rows.len(), 12);
    let (state, net) = crate::test_support::state().await;
    let mut seen = std::collections::BTreeSet::new();
    for (index, (input, row)) in cases.iter().zip(rows).enumerate() {
        assert_eq!(input["id"], row["id"]);
        assert!(seen.insert(input["id"].as_str().unwrap().to_owned()));
        let number: i32 = input["source"].as_i64().unwrap().try_into().unwrap();
        let key = RoomKey::new(ProjectId::local(), format!("open-source-reader-{index}"));
        let value = serde_json::json!({"video":{"roomJoin":true,"room":key.name.0,"canPublish":input["can_publish"],"canPublishSources":input["sources"]}});
        let decoded = crate::auth::claims::decode_grants(value.as_object().unwrap()).unwrap();
        assert_eq!(
            serde_json::json!(
                convert::permission_to_proto(&decoded.video.to_permission()).can_publish_sources
            ),
            row["permission_sources"]
        );
        assert_eq!(
            convert::track_source_to_proto(convert::track_source_from_proto(number)),
            row["request_source"].as_i64().unwrap() as i32
        );
        let initial = Grants {
            identity: "reader".into(),
            video: decoded.video,
            ..Default::default()
        };
        let mut join = params(&key, initial.clone());
        join.capabilities.request_response = true;
        let mut handle = state.manager.join(join).await.unwrap();
        let mut ctx = context(&state, initial);
        let _ = next_claims(&mut handle).await;
        let (accepted, source) = add(
            &mut handle,
            &mut ctx,
            number,
            100 + index as u32,
            "reader-open",
        )
        .await;
        assert_eq!(serde_json::json!(accepted), row["allowed"]);
        assert_eq!(serde_json::json!(source), row["track_source"]);
        assert_eq!(
            rtc_core::permission::canonical_source(convert::track_source_from_proto(number))
                .as_ref(),
            row["canonical_source"].as_str().unwrap()
        );
        let before = state
            .manager
            .get_participant(&key, &handle.identity)
            .await
            .unwrap();
        let mut incoming = convert::permission_to_proto(&before.permission);
        incoming.can_publish_sources = vec![number];
        let changed = state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    permission: Some(convert::permission_from_proto(&incoming)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::json!(changed.version == before.version),
            row["api_matches"]
        );
        assert_eq!(
            serde_json::json!(
                convert::permission_to_proto(&changed.permission).can_publish_sources
            ),
            row["api_permission_sources"]
        );
        state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    attributes: std::collections::BTreeMap::from([(
                        "read".into(),
                        "authoritative".into(),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let event = next_claims(&mut handle).await;
        let actual = decoded_refresh(&mut ctx, event, &handle.participant_sid);
        assert_eq!(
            serde_json::json!(actual.video.can_publish_sources),
            row["api_raw_sources"]
        );
        let (allowed, retained) = add(
            &mut handle,
            &mut ctx,
            number,
            300 + index as u32,
            "reader-after-admin",
        )
        .await;
        assert_eq!(serde_json::json!(allowed), row["api_allowed"]);
        assert_eq!(retained, allowed.then_some(number));
        let current = state
            .manager
            .get_participant(&key, &handle.identity)
            .await
            .unwrap();
        let again = state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    permission: Some(convert::permission_from_proto(&incoming)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::json!(again.version == current.version),
            row["api_repeated_matches"]
        );
        drop(handle);
        state.manager.delete_room(&key).await.unwrap();
    }
    assert_eq!(seen.len(), 12);
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}
