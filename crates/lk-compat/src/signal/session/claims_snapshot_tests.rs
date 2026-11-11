use super::*;
use rtc_core::{
    ids::{Identity, ProjectId, RoomKey},
    permission::{Grants, VideoGrant},
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
fn original(room: &str) -> Grants {
    Grants {
        identity: "claims-video-owner".into(),
        video: VideoGrant {
            room_join: true,
            room: room.into(),
            can_update_own_metadata: Some(true),
            room_admin: true,
            destination_room: "claims-destination".into(),
            can_publish_sources: vec!["camera".into(), "microphone".into()],
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn real_owner_initial_noop_and_attributes_preserve_implicit_video_refresh() {
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "claims-video-noop");
    let initial = original(&key.name.0);
    let mut handle = state
        .manager
        .join(params(&key, initial.clone()))
        .await
        .unwrap();
    let mut ctx = context(&state, initial.clone());
    let event = next_claims(&mut handle).await;
    let first = decoded_refresh(&mut ctx, event.clone(), &handle.participant_sid);
    assert_eq!(
        first.video, initial.video,
        "initial refresh must not materialize defaults"
    );
    // The same real owner snapshot also drives the actual periodic refresh helper.
    let SignalEvent::ClaimsChanged(snapshot) = event else {
        unreachable!()
    };
    let periodic = ctx.refresh_token(&snapshot).expect("periodic mint");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    assert_eq!(
        crate::auth::verify_token(&state.keys, &periodic, now)
            .unwrap()
            .grants
            .video,
        initial.video
    );
    let before = state
        .manager
        .get_participant(&key, &handle.identity)
        .await
        .unwrap();
    let noop = state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                permission: Some(initial.video.to_permission()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        noop.version, before.version,
        "same permission alone is not a mutation"
    );
    for attrs in [
        BTreeMap::from([("k".into(), "v".into())]),
        BTreeMap::from([("k".into(), "v".into())]),
        BTreeMap::from([("absent".into(), String::new())]),
    ] {
        state
            .manager
            .update_participant(
                &key,
                &handle.identity,
                ParticipantUpdate {
                    attributes: attrs,
                    permission: Some(initial.video.to_permission()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let event = next_claims(&mut handle).await;
        let refreshed = decoded_refresh(&mut ctx, event, &handle.participant_sid);
        assert_eq!(
            refreshed.video, initial.video,
            "same permission plus nonempty attribute update"
        );
        assert_eq!(refreshed.attributes.get("k").map(String::as_str), Some("v"));
    }
    assert!(
        handle
            .apply(SessionRequest::UpdateMetadata {
                token: 99,
                request_id: 99,
                name: String::new(),
                metadata: "actual-self-metadata".into(),
                attributes: BTreeMap::new(),
            })
            .await
    );
    let event = next_claims(&mut handle).await;
    let own = decoded_refresh(&mut ctx, event, &handle.participant_sid);
    assert_eq!(own.video, initial.video);
    assert_eq!(own.metadata, "actual-self-metadata");
    drop(handle);
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn real_owner_pending_permissions_and_old_jwt_resume_keep_authoritative_video() {
    let (state, net) = crate::test_support::state().await;
    let key = RoomKey::new(ProjectId::local(), "claims-video-resume");
    let initial = original(&key.name.0);
    let mut handle = state
        .manager
        .join(params(&key, initial.clone()))
        .await
        .unwrap();
    let mut ctx = context(&state, initial.clone());
    let first = next_claims(&mut handle).await;
    assert_eq!(
        decoded_refresh(&mut ctx, first, &handle.participant_sid).video,
        initial.video
    );
    // Queue three real owner snapshots before consuming them. Each must carry
    // its own grant state, rather than consulting latest state while encoding.
    state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                attributes: BTreeMap::from([("queued".into(), "before-permission".into())]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut changed = initial.video.to_permission();
    changed.can_publish = false;
    changed.can_manage_agent_session = true;
    changed.can_publish_sources.reverse();
    state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                permission: Some(changed.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    state
        .manager
        .update_participant(
            &key,
            &handle.identity,
            ParticipantUpdate {
                permission: Some(initial.video.to_permission()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let before = next_claims(&mut handle).await;
    assert_eq!(
        decoded_refresh(&mut ctx, before, &handle.participant_sid).video,
        initial.video
    );
    let event = next_claims(&mut handle).await;
    let actual = decoded_refresh(&mut ctx, event, &handle.participant_sid).video;
    assert_eq!(actual.to_permission(), changed);
    assert_eq!(actual.can_manage_agent_session, Some(true));
    assert_eq!(actual.can_subscribe_metrics, Some(false));
    assert_eq!(actual.can_publish, Some(false));
    let event = next_claims(&mut handle).await;
    let reverted = decoded_refresh(&mut ctx, event, &handle.participant_sid).video;
    assert_eq!(reverted.to_permission(), initial.video.to_permission());
    assert_eq!(reverted.can_publish, Some(true));
    assert_eq!(reverted.can_subscribe_metrics, Some(false));
    assert_eq!(reverted.can_manage_agent_session, Some(false));
    assert_eq!(reverted.destination_room, initial.video.destination_room);
    let old_sid = handle.participant_sid.clone();
    let mut resume = params(&key, initial.clone());
    resume.reconnect = true;
    resume.participant_sid = Some(old_sid.clone());
    let mut resumed = state.manager.join(resume).await.unwrap();
    assert_eq!(resumed.participant_sid, old_sid);
    let mut resumed_ctx = context(&state, initial.clone()); // unchanged old JWT
    let event = next_claims(&mut resumed).await;
    assert_eq!(
        decoded_refresh(&mut resumed_ctx, event, &old_sid).video,
        reverted
    );
    drop(handle);
    drop(resumed);
    state.manager.close_all().await;
    state.agents.shutdown().await;
    net.shutdown_and_wait().await;
}
