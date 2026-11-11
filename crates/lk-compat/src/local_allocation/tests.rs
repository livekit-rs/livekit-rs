use super::*;
use crate::{
    auth::{ExtraGrants, RequestAuth, VerifiedToken},
    state::AppState,
    twirp::{codec::Codec, error::Code},
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use lk_protocol::livekit as pb;
use prost::Message;
use std::sync::Mutex;
use tower::ServiceExt;

#[derive(Debug)]
struct ControlledSample(Arc<Mutex<pb::NodeStats>>);
impl crate::routing::NodeStatsProvider for ControlledSample {
    fn poll_system(&self) -> Result<(), String> {
        Ok(())
    }
    fn sample(&self, started_at: i64, now: i64) -> Result<pb::NodeStats, String> {
        let mut s = self.0.lock().unwrap().clone();
        s.started_at = started_at;
        s.updated_at = now;
        Ok(s)
    }
}
fn config(tracks: i32, bytes: f32) -> Config {
    Config {
        kind: "any".into(),
        sort_by: "random".into(),
        algorithm: "random".into(),
        cpu_limit: 0.,
        sysload_limit: 0.,
        region: String::new(),
        regions: vec![],
        track_limit: tracks,
        bytes_limit: bytes,
    }
}
async fn state(
    tracks: i32,
    bytes: f32,
) -> (
    AppState,
    rtc_runtime::net::NetRouter,
    Arc<LocalNode>,
    Arc<Mutex<pb::NodeStats>>,
) {
    let (mut state, net) = crate::test_support::state().await;
    let node = Arc::new(
        LocalNode::serving(
            "ND_local-limit".into(),
            "127.0.0.1".into(),
            String::new(),
            1,
            rtc_core::util::unix_s(),
        )
        .unwrap(),
    );
    let sample = Arc::new(Mutex::new(pb::NodeStats::default()));
    node.set_stats_provider(Arc::new(ControlledSample(sample.clone())));
    assert!(node.update_stats(rtc_core::util::unix_s()));
    state.enable_node(node.clone());
    state
        .enable_local_allocation_selector(config(tracks, bytes))
        .unwrap();
    (state, net, node, sample)
}
fn token(room: &str) -> VerifiedToken {
    let mut token = VerifiedToken::default();
    token.grants.video.room_create = true;
    token.grants.video.room_admin = true;
    token.grants.video.room = room.into();
    token
}
async fn create(state: &AppState, room: &str) -> Result<pb::Room, crate::twirp::error::TwirpError> {
    let req = pb::CreateRoomRequest {
        name: room.into(),
        ..Default::default()
    };
    let response = crate::twirp::room_service::call(
        state,
        Some(&token(room)),
        "CreateRoom",
        Codec::Protobuf,
        &req.encode_to_vec(),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(pb::Room::decode(to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap())
}
async fn finish(state: &AppState, net: rtc_runtime::net::NetRouter) {
    state.manager().close_all().await;
    state.agents().shutdown().await;
    state.shutdown_whip().await;
    net.shutdown();
}
#[tokio::test]
async fn actual_public_create_and_auto_dispatch_enforce_single_node_limits_then_recover() {
    for (track_limit, bytes_limit) in [(2, 0.), (0, 100.)] {
        let (state, net, node, sample) = state(track_limit, bytes_limit).await;
        let first = create(&state, "already-existing").await.unwrap();
        {
            let mut stats = sample.lock().unwrap();
            stats.num_tracks_in = 1;
            stats.num_tracks_out = 1;
            stats.rates = vec![pb::NodeStatsRate {
                bytes_in: 40.,
                bytes_out: 60.,
                ..Default::default()
            }];
        }
        node.update_stats(rtc_core::util::unix_s());
        for name in ["already-existing", "not-created-while-full"] {
            let error = create(&state, name).await.unwrap_err();
            assert_eq!(error.code, Code::Unknown);
            assert_eq!(error.msg, "reached configured limit for node");
        }
        assert_eq!(
            state
                .manager()
                .debug_rooms()
                .into_iter()
                .find(|room| room.name.0 == "already-existing")
                .unwrap()
                .sid
                .0,
            first.sid
        );
        assert!(
            !state
                .manager()
                .room_exists(&state.room_key("not-created-while-full"))
        );
        let request = pb::CreateAgentDispatchRequest {
            room: "agent-blocked".into(),
            agent_name: "actual-agent".into(),
            ..Default::default()
        };
        let error = crate::twirp::other_services::agent_dispatch::call(
            &state,
            Some(&token(&request.room)),
            "CreateDispatch",
            Codec::Protobuf,
            &request.encode_to_vec(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.msg, "reached configured limit for node");
        assert!(!state.manager().room_exists(&state.room_key(&request.room)));
        *sample.lock().unwrap() = pb::NodeStats::default();
        node.update_stats(rtc_core::util::unix_s());
        let recovered = create(&state, "not-created-while-full").await.unwrap();
        assert!(!recovered.sid.is_empty());
        let response = crate::twirp::other_services::agent_dispatch::call(
            &state,
            Some(&token(&request.room)),
            "CreateDispatch",
            Codec::Protobuf,
            &request.encode_to_vec(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let dispatch =
            pb::AgentDispatch::decode(to_bytes(response.into_body(), 1 << 20).await.unwrap())
                .unwrap();
        assert_eq!(dispatch.room, request.room);
        assert!(!dispatch.id.is_empty());
        assert!(dispatch.state.unwrap().jobs.is_empty());
        assert!(state.manager().room_exists(&state.room_key(&request.room)));
        finish(&state, net).await;
    }
}
#[tokio::test]
async fn single_node_stale_fallback_and_invalid_selector_follow_real_constructor() {
    let (mut state, net, node, sample) = state(0, 0.).await;
    let mut invalid = config(0, 0.);
    invalid.kind = "not-a-selector".into();
    let error = state.enable_local_allocation_selector(invalid).unwrap_err();
    assert_eq!(error.message, "unsupported node selector");
    let now = node.snapshot().stats.unwrap().updated_at;
    let policy = state.local_allocation.as_ref().unwrap();
    assert!(policy.check_at(now + 4).is_ok());
    assert_eq!(
        policy.check_at(now + 5).unwrap_err().message,
        "could not find any available nodes"
    );
    // Fixed LimitsReached checks >0 only; negative/zero do not invent hard caps.
    state
        .enable_local_allocation_selector(config(-1, -1.))
        .unwrap();
    sample.lock().unwrap().num_tracks_in = i32::MAX;
    node.update_stats(rtc_core::util::unix_s());
    assert!(state.check_local_room_allocation().is_ok());
    finish(&state, net).await;
}
#[tokio::test]
async fn actual_whip_validation_precedes_local_quota_and_quota_creates_no_room() {
    let (state, net, node, sample) = state(1, 0.).await;
    sample.lock().unwrap().num_tracks_in = 1;
    node.update_stats(rtc_core::util::unix_s());
    for (offer, expected) in [
        ("bad SDP", StatusCode::BAD_REQUEST),
        (
            "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n",
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let mut auth = token("quota-whip");
        auth.grants.identity = "whip-peer".into();
        auth.grants.video.room_join = true;
        auth.extra = ExtraGrants {
            has_video: true,
            ..Default::default()
        };
        let mut request = Request::builder()
            .method("POST")
            .uri("/whip/v1")
            .header("content-type", "application/sdp")
            .body(Body::from(offer))
            .unwrap();
        request
            .extensions_mut()
            .insert(RequestAuth(Some(Arc::new(auth))));
        let response = crate::whip::routes(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        if expected == StatusCode::INTERNAL_SERVER_ERROR {
            assert!(
                std::str::from_utf8(&bytes)
                    .unwrap()
                    .contains("reached configured limit for node")
            );
        }
        assert!(!state.manager().room_exists(&state.room_key("quota-whip")));
    }
    finish(&state, net).await;
}
