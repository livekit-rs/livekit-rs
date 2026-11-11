//! Actual public AgentDispatch adapter → real owner topic/claim/grant → actor.
//! No worker/job assignment is inferred from the real dispatch record.
use super::*;
use crate::{auth::VerifiedToken, state::CompatServices, twirp::codec::Codec};
use prost::Message;
use rtc_core::events::NullSink;
use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};

async fn response<T: Message + Default>(response: axum::response::Response) -> T {
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    T::decode(bytes).unwrap()
}
#[tokio::test]
async fn actual_production_constructor_profiles_reach_public_crud_and_retire_real_owner_topics() {
    for (configured, effective) in [(1000, 1000), (0, 100), (3, 3)] {
        let local_ip = std::net::Ipv4Addr::LOCALHOST.into();
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: local_ip,
                advertise: local_ip,
            }],
        })
        .await
        .unwrap();
        let mut cfg = crate::test_support::config();
        cfg.room.auto_create = false;
        let policy = RpcPolicy::from_raw(1, 3_000_000_000, 0, configured);
        let state = CompatServices::new(Arc::new(cfg))
            .with_whip_policy(policy)
            .with_local_rpc_compression(
                CompressionPolicy {
                    quality: 0,
                    threshold: 1,
                },
                1,
            )
            .assemble(net.clone(), Arc::new(NullSink));
        let room = format!("actual-configured-{configured}");
        let key = state.room_key(&room);
        let first = state
            .manager
            .create_room(key.clone(), Default::default())
            .await
            .unwrap();
        let mut token = VerifiedToken::default();
        token.grants.video.room_admin = true;
        token.grants.video.room = room.clone();
        let request = pb::CreateAgentDispatchRequest {
            room: room.clone(),
            agent_name: "real-owner".into(),
            metadata: "plain-larger-than-one".into(),
            ..Default::default()
        };
        let dispatch: pb::AgentDispatch = response(
            crate::twirp::other_services::agent_dispatch(
                &state,
                Some(&token),
                "CreateDispatch",
                Codec::Protobuf,
                &request.encode_to_vec(),
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(dispatch.room, room);
        assert_eq!(dispatch.metadata, request.metadata);
        assert!(dispatch.state.as_ref().unwrap().jobs.is_empty());
        let list = pb::ListAgentDispatchRequest {
            room: room.clone(),
            dispatch_id: dispatch.id.clone(),
        };
        let listed: pb::ListAgentDispatchResponse = response(
            crate::twirp::other_services::agent_dispatch(
                &state,
                Some(&token),
                "ListDispatch",
                Codec::Protobuf,
                &list.encode_to_vec(),
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(listed.agent_dispatches, vec![dispatch.clone()]);
        let delete = pb::DeleteAgentDispatchRequest {
            room: room.clone(),
            dispatch_id: dispatch.id.clone(),
        };
        let removed: pb::AgentDispatch = response(
            crate::twirp::other_services::agent_dispatch(
                &state,
                Some(&token),
                "DeleteDispatch",
                Codec::Protobuf,
                &delete.encode_to_vec(),
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(removed, dispatch);
        assert_eq!(state.local_dispatch.bus.limits().inbox_messages, effective);
        assert_eq!(
            state
                .local_dispatch
                .owners
                .lock()
                .await
                .get(&key)
                .unwrap()
                .sid(),
            &first.sid
        );
        let channel = crate::owner_dispatch::method(&room, "CreateDispatch").request_channel();
        // Opaque malformed data probes only real membership, not business
        // success. Typed forwarding must discard it; its queue is then retired.
        assert_eq!(
            state
                .local_dispatch
                .bus
                .publish_encoded(&channel, b"invalid-membership-probe")
                .await
                .unwrap(),
            1
        );
        state.manager.delete_room(&key).await.unwrap();
        assert_eq!(
            state
                .local_dispatch
                .bus
                .publish_encoded(&channel, b"invalid-after-owner-close")
                .await
                .unwrap(),
            0
        );
        assert!(state.local_dispatch.owners.lock().await.get(&key).is_none());
        let second = state
            .manager
            .create_room(key.clone(), Default::default())
            .await
            .unwrap();
        assert_ne!(first.sid, second.sid);
        let empty: pb::ListAgentDispatchResponse = response(
            crate::twirp::other_services::agent_dispatch(
                &state,
                Some(&token),
                "ListDispatch",
                Codec::Protobuf,
                &list.encode_to_vec(),
            )
            .await
            .unwrap(),
        )
        .await;
        assert!(empty.agent_dispatches.is_empty());
        state.manager.close_all().await;
        state.shutdown_workers().await;
        assert_eq!(
            state
                .local_dispatch
                .bus
                .publish_encoded(&channel, b"invalid-after-service-close")
                .await
                .unwrap(),
            0
        );
        net.shutdown_and_wait().await;
    }
}
