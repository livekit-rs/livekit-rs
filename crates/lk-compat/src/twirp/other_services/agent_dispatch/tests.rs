use super::*;
use crate::state::CompatServices;
use axum::body::to_bytes;
use prost::Message;
use rtc_core::events::NullSink;
use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
use std::sync::Arc;

fn admin(room: &str) -> VerifiedToken {
    let mut token = VerifiedToken::default();
    token.grants.video.room_admin = true;
    token.grants.video.room = room.into();
    token
}

#[test]
fn deployment_contract_uses_utf8_byte_lengths_and_ascii_control_indices() {
    for valid in ["", "production-1", "東京", "a\u{a0}b"] {
        validate_deployment(valid).unwrap();
    }
    validate_deployment(&"é".repeat(32)).unwrap();
    assert_eq!(
        validate_deployment(&"é".repeat(33)).unwrap_err().msg,
        "deployment exceeds 64 bytes"
    );
    assert_eq!(
        validate_deployment("prod_blue").unwrap_err().msg,
        "deployment contains reserved character '_'"
    );
    for invalid in ["é\t", "é ", "é\0", "é\u{7f}"] {
        assert_eq!(
            validate_deployment(invalid).unwrap_err().msg,
            "deployment contains whitespace or control byte at position 2"
        );
    }
}

#[test]
fn typed_decode_auth_and_creation_validation_follow_upstream_order() {
    let malformed = prepare(
        None,
        "CreateDispatch",
        Codec::Json,
        br#"{"room":42}"#,
        None,
        None,
    );
    assert_eq!(malformed.err().unwrap().code, Code::Malformed);
    let invalid = br#"{"room":"r","deployment":"bad_name","metadata":"too long"}"#;
    assert_eq!(
        prepare(None, "CreateDispatch", Codec::Json, invalid, Some(1), None)
            .err()
            .unwrap()
            .code,
        Code::Unauthenticated
    );
    let token = admin("r");
    assert_eq!(
        prepare(
            Some(&token),
            "CreateDispatch",
            Codec::Json,
            invalid,
            Some(1),
            None
        )
        .err()
        .unwrap()
        .msg,
        "deployment contains reserved character '_'"
    );
    assert_eq!(
        prepare(
            Some(&admin("elsewhere")),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"r"}"#,
            None,
            None
        )
        .err()
        .unwrap()
        .code,
        Code::Unauthenticated
    );
    assert_eq!(
        prepare(
            Some(&token),
            "DeleteDispatch",
            Codec::Protobuf,
            &[0xff],
            None,
            None
        )
        .err()
        .unwrap()
        .code,
        Code::Malformed
    );
}

#[test]
fn metadata_and_attributes_limits_count_utf8_bytes_and_zero_means_unlimited() {
    let token = admin("r");
    let req = pb::CreateAgentDispatchRequest {
        room: "r".into(),
        metadata: "é".into(),
        attributes: [("é".into(), "字".into())].into_iter().collect(),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        prepare(
            Some(&token),
            "CreateDispatch",
            Codec::Protobuf,
            &req,
            Some(1),
            Some(4)
        )
        .err()
        .unwrap()
        .msg,
        "metadata size exceeds limits"
    );
    assert_eq!(
        prepare(
            Some(&token),
            "CreateDispatch",
            Codec::Protobuf,
            &req,
            Some(2),
            Some(4)
        )
        .err()
        .unwrap()
        .msg,
        "attribute size exceeds limits"
    );
    prepare(
        Some(&token),
        "CreateDispatch",
        Codec::Protobuf,
        &req,
        Some(2),
        Some(5),
    )
    .unwrap();
    prepare(
        Some(&token),
        "CreateDispatch",
        Codec::Protobuf,
        &req,
        Some(0),
        Some(0),
    )
    .unwrap();
    prepare(
        Some(&token),
        "CreateDispatch",
        Codec::Protobuf,
        &req,
        None,
        None,
    )
    .unwrap();
}

async fn state(auto_create: bool) -> (AppState, NetRouter) {
    let mut config = crate::test_support::config();
    config.room.auto_create = auto_create;
    state_with_config(config).await
}

async fn state_with_config(config: rtc_core::config::ServerConfig) -> (AppState, NetRouter) {
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: std::net::Ipv4Addr::LOCALHOST.into(),
            advertise: std::net::Ipv4Addr::LOCALHOST.into(),
        }],
    })
    .await
    .unwrap();
    (
        CompatServices::new(Arc::new(config)).assemble(net.clone(), Arc::new(NullSink)),
        net,
    )
}

async fn response<T>(value: Response, codec: Codec) -> T
where
    T: Message + prost::Name + Default + serde::de::DeserializeOwned,
{
    assert_eq!(value.status(), axum::http::StatusCode::OK);
    let bytes = to_bytes(value.into_body(), usize::MAX).await.unwrap();
    decode(codec, &bytes).unwrap()
}

#[tokio::test]
async fn real_room_dispatch_crud_is_shared_cloned_and_filtered_across_codecs() {
    let (state, net) = state(true).await;
    let token = admin("room");
    let create = br#"{"room":"room","agentName":"assistant","metadata":"context","restartPolicy":"JRP_NEVER","deployment":"blue","attributes":{"k":"v"}}"#;
    let first: pb::AgentDispatch = response(
        call(&state, Some(&token), "CreateDispatch", Codec::Json, create)
            .await
            .unwrap(),
        Codec::Json,
    )
    .await;
    assert!(first.id.starts_with("AD_"));
    assert_eq!(first.agent_name, "assistant");
    assert_eq!(first.room, "room");
    assert_eq!(first.metadata, "context");
    assert_eq!(first.restart_policy, 1);
    assert_eq!(first.deployment, "blue");
    assert_eq!(first.attributes.get("k").map(String::as_str), Some("v"));
    let first_state = first.state.as_ref().unwrap();
    assert!(first_state.created_at > 1_000_000_000_000_000_000);
    assert_eq!(first_state.deleted_at, 0);
    assert!(
        first_state.jobs.is_empty(),
        "dispatch acceptance must not invent a worker/job"
    );
    let all: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"room"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    assert_eq!(
        all.agent_dispatches.len(),
        2,
        "auto-create initializes unnamed then adds explicit dispatch"
    );
    assert_eq!(
        all.agent_dispatches
            .iter()
            .filter(|record| record.agent_name.is_empty())
            .count(),
        1
    );
    let copy = state.clone();
    let list = pb::ListAgentDispatchRequest {
        room: "room".into(),
        dispatch_id: first.id.clone(),
    }
    .encode_to_vec();
    let mut listed: pb::ListAgentDispatchResponse = response(
        call(&copy, Some(&token), "ListDispatch", Codec::Protobuf, &list)
            .await
            .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(listed.agent_dispatches, vec![first.clone()]);
    listed.agent_dispatches[0].metadata = "client-side mutation".into();
    let unchanged: pb::ListAgentDispatchResponse = response(
        call(&state, Some(&token), "ListDispatch", Codec::Protobuf, &list)
            .await
            .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(unchanged.agent_dispatches[0].metadata, "context");
    let missing = pb::ListAgentDispatchRequest {
        room: "room".into(),
        dispatch_id: "AD_missing".into(),
    }
    .encode_to_vec();
    let empty: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Protobuf,
            &missing,
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert!(empty.agent_dispatches.is_empty());
    let delete = pb::DeleteAgentDispatchRequest {
        room: "room".into(),
        dispatch_id: first.id.clone(),
    }
    .encode_to_vec();
    let removed: pb::AgentDispatch = response(
        call(
            &copy,
            Some(&token),
            "DeleteDispatch",
            Codec::Protobuf,
            &delete,
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(removed, first);
    let err = call(
        &state,
        Some(&token),
        "DeleteDispatch",
        Codec::Protobuf,
        &delete,
    )
    .await
    .unwrap_err();
    assert_eq!(
        (err.code, err.msg.as_str()),
        (Code::NotFound, "dispatch ID not found")
    );
    state.manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn disabled_auto_create_requires_existing_room_for_all_operations() {
    let (state, net) = state(false).await;
    let token = admin("missing");
    for method in ["CreateDispatch", "ListDispatch", "DeleteDispatch"] {
        assert_eq!(
            call(
                &state,
                Some(&token),
                method,
                Codec::Json,
                br#"{"room":"missing"}"#
            )
            .await
            .unwrap_err()
            .code,
            Code::Unavailable
        );
    }
    state
        .manager
        .create_room(state.room_key("missing"), Default::default())
        .await
        .unwrap();
    let created: pb::AgentDispatch = response(
        call(
            &state,
            Some(&token),
            "CreateDispatch",
            Codec::Json,
            br#"{"room":"missing"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    assert!(created.state.unwrap().jobs.is_empty());
    state.manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn deleting_and_recreating_room_does_not_resurrect_dispatches() {
    let (state, net) = state(true).await;
    let token = admin("reused");
    let _: pb::AgentDispatch = response(
        call(
            &state,
            Some(&token),
            "CreateDispatch",
            Codec::Json,
            br#"{"room":"reused","agentName":"old"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    state
        .manager
        .delete_room(&state.room_key("reused"))
        .await
        .unwrap();
    assert_eq!(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"reused"}"#
        )
        .await
        .unwrap_err()
        .code,
        Code::Unavailable
    );
    state
        .manager
        .create_room(state.room_key("reused"), Default::default())
        .await
        .unwrap();
    let listed: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"reused"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    assert_eq!(listed.agent_dispatches.len(), 1);
    assert!(listed.agent_dispatches[0].agent_name.is_empty());
    state.manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn create_room_projects_explicit_and_preset_agents_and_skips_invalid_deployment() {
    use rtc_core::permission::{RoomAgentDispatchOptions, RoomConfig};
    // Assemble adapters and the room owner from one immutable configured graph.
    let mut config = crate::test_support::config();
    config.room.auto_create = true;
    config.room.presets.insert(
        "agent-preset".into(),
        Some(RoomConfig {
            agent_dispatches: Some(vec![RoomAgentDispatchOptions {
                agent_name: "preset-agent".into(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
    );
    let (state, net) = state_with_config(config).await;
    let mut token = admin("configured");
    token.grants.video.room_create = true;
    let _: pb::Room = response(crate::twirp::room_service::call(&state, Some(&token), "CreateRoom", Codec::Json,
        br#"{"name":"configured","agents":[{"agentName":"named","metadata":"context","deployment":"prod","attributes":{"k":"v"}},{"deployment":"bad_name"}]}"#).await.unwrap(), Codec::Json).await;
    let records: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"configured"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    assert_eq!(records.agent_dispatches.len(), 1);
    assert_eq!(records.agent_dispatches[0].agent_name, "named");
    assert_eq!(records.agent_dispatches[0].metadata, "context");
    assert_eq!(
        records.agent_dispatches[0]
            .attributes
            .get("k")
            .map(String::as_str),
        Some("v")
    );
    token.grants.video.room = "preset-room".into();
    let _: pb::Room = response(
        crate::twirp::room_service::call(
            &state,
            Some(&token),
            "CreateRoom",
            Codec::Protobuf,
            &pb::CreateRoomRequest {
                name: "preset-room".into(),
                room_preset: "agent-preset".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    let records: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"preset-room"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    assert_eq!(records.agent_dispatches.len(), 1);
    assert_eq!(records.agent_dispatches[0].agent_name, "preset-agent");
    state.manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn standalone_auto_create_rejects_closed_owner_request_before_dispatch_mutation() {
    let (state, net) = state(true).await;
    let token = admin("accepted-owner-policy");
    // This is the actual F150 local request admission boundary, not an injected
    // synthetic timeout or mocked room/dispatch response.
    state.room_creates.close_admission();
    let error = call(
        &state,
        Some(&token),
        "CreateDispatch",
        Codec::Json,
        br#"{"room":"accepted-owner-policy","agentName":"named","metadata":"must not mutate"}"#,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, Code::Canceled);
    assert_eq!(error.msg, "client is closed");
    assert!(
        !state
            .manager
            .room_exists(&state.room_key("accepted-owner-policy"))
    );
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn standalone_auto_create_still_validates_before_closed_request_boundary() {
    let (state, net) = state(true).await;
    state.room_creates.close_admission();
    let body = br#"{"room":"ordering","deployment":"bad_name"}"#;
    let unauth = call(&state, None, "CreateDispatch", Codec::Json, body)
        .await
        .unwrap_err();
    assert_eq!(unauth.code, Code::Unauthenticated);
    let error = call(
        &state,
        Some(&admin("ordering")),
        "CreateDispatch",
        Codec::Json,
        body,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert_eq!(error.msg, "deployment contains reserved character '_'");
    assert!(!state.manager.room_exists(&state.room_key("ordering")));
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn disabled_auto_create_existing_room_does_not_call_closed_create_client() {
    let (state, net) = state(false).await;
    let key = state.room_key("no-auto-create");
    let room = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    state.room_creates.close_admission();
    let token = admin("no-auto-create");
    let first: pb::AgentDispatch = response(call(&state, Some(&token), "CreateDispatch", Codec::Json,
        br#"{"room":"no-auto-create","agentName":"named","metadata":"real metadata","attributes":{"k":"v"}}"#)
        .await.unwrap(), Codec::Json).await;
    assert_eq!(first.metadata, "real metadata");
    assert_eq!(first.attributes.get("k").map(String::as_str), Some("v"));
    assert!(first.state.as_ref().unwrap().jobs.is_empty());
    let again: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&token),
            "ListDispatch",
            Codec::Protobuf,
            &pb::ListAgentDispatchRequest {
                room: key.name.0.clone(),
                dispatch_id: first.id.clone(),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(again.agent_dispatches, vec![first.clone()]);
    let removed: pb::AgentDispatch = response(
        call(
            &state,
            Some(&token),
            "DeleteDispatch",
            Codec::Protobuf,
            &pb::DeleteAgentDispatchRequest {
                room: key.name.0.clone(),
                dispatch_id: first.id.clone(),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(removed, first);
    assert_eq!(
        state
            .manager
            .list_rooms(&key.project, std::slice::from_ref(&key.name))[0]
            .sid,
        room.sid
    );
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

/// Public adapter -> common local wire RPC -> exact room actor registry.
/// No registered worker is manufactured: jobs must remain empty.
#[tokio::test]
async fn standalone_local_topics_are_ready_with_owner_and_old_sid_close_cannot_retire_replacement()
{
    use rtc_runtime::RoomOwnerLifecycle;
    let (state, net) = state(false).await;
    let key = state.room_key("local-generation");
    let first = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let auth = admin("local-generation");
    let old:pb::AgentDispatch=response(call(&state,Some(&auth),"CreateDispatch",Codec::Json,br#"{"room":"local-generation","agentName":"old","metadata":"first","attributes":{"generation":"one"}}"#).await.unwrap(),Codec::Json).await;
    assert_eq!(old.agent_name, "old");
    assert_eq!(old.metadata, "first");
    assert!(old.state.as_ref().unwrap().jobs.is_empty());
    state.manager.delete_room(&key).await.unwrap();
    let second = state
        .manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    assert_ne!(first.sid, second.sid);
    // Exact old-generation lifecycle callback cannot unsubscribe fresh groups.
    state
        .local_dispatch
        .closed(key.clone(), first.sid.clone())
        .await;
    let fresh:pb::AgentDispatch=response(call(&state,Some(&auth),"CreateDispatch",Codec::Json,br#"{"room":"local-generation","agentName":"fresh","metadata":"second","attributes":{"generation":"two"}}"#).await.unwrap(),Codec::Json).await;
    assert_ne!(fresh.id, old.id);
    assert_eq!(fresh.metadata, "second");
    assert_eq!(fresh.attributes.get("generation").unwrap(), "two");
    assert!(fresh.state.as_ref().unwrap().jobs.is_empty());
    let listed: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&auth),
            "ListDispatch",
            Codec::Json,
            br#"{"room":"local-generation"}"#,
        )
        .await
        .unwrap(),
        Codec::Json,
    )
    .await;
    assert!(listed.agent_dispatches.iter().any(|d| d.id == fresh.id));
    assert!(!listed.agent_dispatches.iter().any(|d| d.id == old.id));
    let removed: pb::AgentDispatch = response(
        call(
            &state,
            Some(&auth),
            "DeleteDispatch",
            Codec::Protobuf,
            &pb::DeleteAgentDispatchRequest {
                room: "local-generation".into(),
                dispatch_id: fresh.id.clone(),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(removed.id, fresh.id);
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn real_standalone_agent_crud_plain_traffic_is_not_a_gzip_expansion() {
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: std::net::Ipv4Addr::LOCALHOST.into(),
            advertise: std::net::Ipv4Addr::LOCALHOST.into(),
        }],
    })
    .await
    .unwrap();
    let state = CompatServices::new(Arc::new(crate::test_support::config()))
        .with_local_rpc_compression(
            crate::service_bus::wire::CompressionPolicy {
                quality: 0,
                threshold: 1,
            },
            1,
        )
        .assemble(net.clone(), Arc::new(NullSink));
    let auth = admin("plain-cap-one");
    let created:pb::AgentDispatch=response(call(&state,Some(&auth),"CreateDispatch",Codec::Json,br#"{"room":"plain-cap-one","agentName":"actual-owner","metadata":"plain body and response larger than gzip cap"}"#).await.unwrap(),Codec::Json).await;
    assert_eq!(created.agent_name, "actual-owner");
    assert_eq!(
        created.metadata,
        "plain body and response larger than gzip cap"
    );
    assert!(created.state.as_ref().unwrap().jobs.is_empty());
    let listed: pb::ListAgentDispatchResponse = response(
        call(
            &state,
            Some(&auth),
            "ListDispatch",
            Codec::Protobuf,
            &pb::ListAgentDispatchRequest {
                room: "plain-cap-one".into(),
                dispatch_id: created.id.clone(),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(listed.agent_dispatches, vec![created.clone()]);
    let removed: pb::AgentDispatch = response(
        call(
            &state,
            Some(&auth),
            "DeleteDispatch",
            Codec::Protobuf,
            &pb::DeleteAgentDispatchRequest {
                room: "plain-cap-one".into(),
                dispatch_id: created.id.clone(),
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
        Codec::Protobuf,
    )
    .await;
    assert_eq!(removed, created);
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

// Same existing public constructor/request API on B7 and proposed local RPC.
// No new LocalTransport/LocalDispatch/RoomOwnerLifecycle types referenced.
mod v8_public_missing_topic_baseline {
    #[tokio::test]
    async fn public_disabled_auto_create_missing_owner_uses_actual_bus_failure_family() {
        use prost::Message;
        let ip = std::net::Ipv4Addr::LOCALHOST.into();
        let net = rtc_runtime::net::NetRouter::start(rtc_runtime::net::NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![rtc_runtime::net::LocalInterface {
                bind: ip,
                advertise: ip,
            }],
        })
        .await
        .unwrap();
        let mut cfg = crate::test_support::config();
        cfg.room.auto_create = false;
        let policy = crate::workers::retry::RpcPolicy::from_raw(1, 3_000_000_000, 0, 1000);
        let state = crate::state::CompatServices::new(std::sync::Arc::new(cfg))
            .with_whip_policy(policy)
            .assemble(net.clone(), std::sync::Arc::new(rtc_core::events::NullSink));
        let mut token = crate::auth::VerifiedToken::default();
        token.grants.video.room_admin = true;
        token.grants.video.room = "v8-public-missing-owner".into();
        let request = lk_protocol::livekit::CreateAgentDispatchRequest {
            room: token.grants.video.room.clone(),
            agent_name: "registered-topic-required".into(),
            ..Default::default()
        };
        // Ten seconds bounds a hung test; it does not shorten or assert the source
        // 3s request/1s affinity policy. No fabricated successful worker effect.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            crate::twirp::other_services::agent_dispatch(
                &state,
                Some(&token),
                "CreateDispatch",
                crate::twirp::codec::Codec::Protobuf,
                &request.encode_to_vec(),
            ),
        )
        .await;
        state.manager.close_all().await;
        state.shutdown_workers().await;
        net.shutdown_and_wait().await;
        let error = result
            .expect("actual configured RPC must terminate")
            .expect_err("absent owner must not create a dispatch or report business success");
        assert_eq!(error.code, crate::twirp::error::Code::Unavailable);
        assert_eq!(error.msg, "no response from servers");
    }
}

/// HTTP-level integration tests exercising the full Twirp router middleware
/// (auth, body limit, CORS, path normalization, recovery) before dispatch.
#[cfg(test)]
mod http_integration_tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn make_admin_jwt(room: &str) -> String {
        crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({
                "iss": "devkey",
                "sub": "admin",
                "exp": 99999999999_u64,
                "video": {
                    "roomAdmin": true,
                    "room": room,
                    "canManageAgentSession": true
                }
            })
            .to_string(),
            "secret",
        )
    }

    async fn invoke_dispatch(
        state: &AppState,
        token: &str,
        method: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post(format!("/twirp/livekit.AgentDispatchService/{method}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        (status, json)
    }

    #[tokio::test]
    async fn create_dispatch_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.AgentDispatchService/CreateDispatch")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn create_dispatch_bad_deployment_returns_400() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_admin_jwt("testroom");
        let (status, json) = invoke_dispatch(
            &state,
            &jwt,
            "CreateDispatch",
            serde_json::json!({
                "room": "testroom",
                "deployment": "bad_name",
                "agentName": "agent-1"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            json["msg"]
                .as_str()
                .is_some_and(|m| m.contains("'_'") || m.contains("underscore")),
            "expected deployment error, got: {json}"
        );
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn create_dispatch_wrong_room_admin_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_admin_jwt("room-a");
        let (status, _json) = invoke_dispatch(
            &state,
            &jwt,
            "CreateDispatch",
            serde_json::json!({
                "room": "room-b",
                "deployment": "prod-1",
                "agentName": "agent-1"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn create_and_list_dispatch_lifecycle() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_admin_jwt("lifecycle-room");
        // Create
        let (status, created) = invoke_dispatch(
            &state,
            &jwt,
            "CreateDispatch",
            serde_json::json!({
                "room": "lifecycle-room",
                "deployment": "prod-1",
                "agentName": "test-agent"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "create: {created}");
        assert!(created["id"].as_str().is_some(), "must have dispatch id");
        assert_eq!(created["room"].as_str(), Some("lifecycle-room"));
        assert_eq!(created["agent_name"].as_str(), Some("test-agent"));
        // List
        let (status, listed) = invoke_dispatch(
            &state,
            &jwt,
            "ListDispatch",
            serde_json::json!({"room": "lifecycle-room"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "list: {listed}");
        let dispatches = listed["agent_dispatches"].as_array();
        assert!(dispatches.is_some());
        assert!(
            !dispatches.unwrap().is_empty(),
            "must contain the created dispatch"
        );
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn create_and_delete_dispatch_lifecycle() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_admin_jwt("delete-room");
        // Create
        let (status, created) = invoke_dispatch(
            &state,
            &jwt,
            "CreateDispatch",
            serde_json::json!({
                "room": "delete-room",
                "deployment": "prod-1",
                "agentName": "test-agent"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "create: {created}");
        let id = created["id"].as_str().unwrap().to_string();
        // Delete
        let (status, deleted) = invoke_dispatch(
            &state,
            &jwt,
            "DeleteDispatch",
            serde_json::json!({
                "room": "delete-room",
                "dispatch_id": id
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "delete: {deleted}");
        // Deleted dispatch must echo the same id
        assert_eq!(deleted["id"].as_str(), Some(id.as_str()));
        // Delete again must fail (not found)
        let (status, _json) = invoke_dispatch(
            &state,
            &jwt,
            "DeleteDispatch",
            serde_json::json!({
                "room": "delete-room",
                "dispatch_id": id
            }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_dispatch_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.AgentDispatchService/ListDispatch")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn delete_dispatch_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.AgentDispatchService/DeleteDispatch")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }
}
