use super::*;
use crate::{
    auth::VerifiedToken,
    twirp::codec::{Codec, decode},
};
use axum::{body::to_bytes, response::Response};
use prost::Message;
use rtc_core::{
    events::NullSink,
    permission::{Grants, VideoGrant},
    session::JoinParams,
};
use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};

async fn state(node: &str) -> (AppState, NetRouter) {
    state_profile(node, true).await
}
async fn state_profile(node: &str, live: bool) -> (AppState, NetRouter) {
    let mut cfg = crate::test_support::config();
    cfg.node_id = node.into();
    cfg.enable_psrpc_for_get_list_participants = live;
    let local = std::net::Ipv4Addr::LOCALHOST.into();
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: local,
            advertise: local,
        }],
    })
    .await
    .unwrap();
    (
        crate::state::CompatServices::new(Arc::new(cfg)).assemble(net.clone(), Arc::new(NullSink)),
        net,
    )
}
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn config(node: &str, url: &str) -> Config {
    use redis::IntoConnectionInfo;
    Config {
        startup_gate: None,
        node: pb::Node {
            id: node.into(),
            state: pb::NodeState::Serving as i32,
            num_cpus: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
            stats: Some(pb::NodeStats {
                started_at: unix_now(),
                updated_at: unix_now(),
                ..Default::default()
            }),
            ..Default::default()
        },
        redis: url.into_connection_info().unwrap(),
        timeout: Duration::from_secs(2),
        max_protobuf_bytes: 1 << 20,
        max_pending: 32,
        selector: selector::Config {
            kind: "any".into(),
            sort_by: "clients".into(),
            algorithm: "lowest".into(),
            cpu_limit: 0.9,
            sysload_limit: 0.9,
            region: String::new(),
            regions: vec![],
            track_limit: 0,
            bytes_limit: 0.0,
        },
        limits: ServerLimits {
            concurrent: 16,
            max_request_duration: Duration::from_secs(10),
            allow_skip_claim: false,
        },
    }
}
async fn response<T: Message + prost::Name + Default + serde::de::DeserializeOwned>(
    response: Response,
) -> T {
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    decode(
        Codec::Protobuf,
        &to_bytes(response.into_body(), 1 << 20).await.unwrap(),
    )
    .unwrap()
}
async fn call<T: Message + prost::Name + Default + serde::de::DeserializeOwned, Req: Message>(
    state: &AppState,
    auth: &VerifiedToken,
    method: &str,
    request: &Req,
) -> T {
    response(
        crate::twirp::room_service::call(
            state,
            Some(auth),
            method,
            Codec::Protobuf,
            &request.encode_to_vec(),
        )
        .await
        .unwrap(),
    )
    .await
}
#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL, actual two-entry/one-owner PSRPC; not SDK media"]
async fn real_two_entry_public_control_routes_actual_owner_crud_and_live_participants() {
    use crate::service_bus::{redis_transport::TransportLimits, wire};
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let bus = RedisTransport::connect(
        &url,
        TransportLimits {
            wire: wire::WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: wire::CompressionPolicy::default(),
            inbox_messages: 128,
            subscriptions: 128,
            commands: 32,
            io_timeout: Duration::from_secs(2),
        },
    )
    .await
    .unwrap();
    let node_a = rtc_core::ids::new_sid("ND_");
    let node_b = rtc_core::ids::new_sid("ND_");
    let (mut a, net_a) = state(&node_a).await;
    let (mut b, net_b) = state(&node_b).await;
    let control_a = a
        .enable_room_control_component(bus.clone(), config(&node_a, &url))
        .await
        .unwrap();
    let control_b = b
        .enable_room_control_component(bus, config(&node_b, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("control-room-");
    let key = a.room_key(&room);
    let mut auth = VerifiedToken::default();
    auth.grants.video.room_create = true;
    auth.grants.video.room_list = true;
    auth.grants.video.room_admin = true;
    auth.grants.video.room = room.clone();
    let created: pb::Room = call(
        &b,
        &auth,
        "CreateRoom",
        &pb::CreateRoomRequest {
            name: room.clone(),
            node_id: node_a.clone(),
            ..Default::default()
        },
    )
    .await;
    assert!(a.manager.room_exists(&key));
    assert!(!b.manager.room_exists(&key));
    assert_eq!(a.manager.list_rooms(&key.project, &[]).len(), 1);
    let listed: pb::ListRoomsResponse = call(
        &b,
        &auth,
        "ListRooms",
        &pb::ListRoomsRequest {
            names: vec![room.clone()],
        },
    )
    .await;
    assert_eq!(listed.rooms.len(), 1);
    assert_eq!(listed.rooms[0].sid, created.sid);
    let changed: pb::Room = call(
        &b,
        &auth,
        "UpdateRoomMetadata",
        &pb::UpdateRoomMetadataRequest {
            room: room.clone(),
            metadata: "actual-remote-owner".into(),
        },
    )
    .await;
    assert_eq!(changed.sid, created.sid);
    assert_eq!(changed.metadata, "actual-remote-owner");
    assert_eq!(
        a.manager.list_rooms(&key.project, &[])[0].metadata,
        "actual-remote-owner"
    );
    let identity = "real-runtime-participant";
    let session = a
        .manager
        .join(JoinParams {
            room: key.clone(),
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: room.clone(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: Identity(identity.into()),
            name: "actual-participant".into(),
            metadata: String::new(),
            attributes: Default::default(),
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
        })
        .await
        .unwrap();
    let participants: pb::ListParticipantsResponse = call(
        &b,
        &auth,
        "ListParticipants",
        &pb::ListParticipantsRequest { room: room.clone() },
    )
    .await;
    assert_eq!(participants.participants.len(), 1);
    assert_eq!(participants.participants[0].identity, identity);
    let participant: pb::ParticipantInfo = call(
        &b,
        &auth,
        "GetParticipant",
        &pb::RoomParticipantIdentity {
            revoke_token_ts: 0,
            room: room.clone(),
            identity: identity.into(),
        },
    )
    .await;
    assert_eq!(participant.sid, participants.participants[0].sid);
    assert_eq!(participant.name, "actual-participant");
    let request = pb::CreateAgentDispatchRequest {
        room: room.clone(),
        agent_name: "routed-dispatch".into(),
        ..Default::default()
    };
    let dispatch: pb::AgentDispatch = response(
        crate::twirp::other_services::agent_dispatch(
            &b,
            Some(&auth),
            "CreateDispatch",
            Codec::Protobuf,
            &request.encode_to_vec(),
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(
        a.manager
            .list_agent_dispatches(&key, Some(&dispatch.id))
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(!b.manager.room_exists(&key));
    let _: pb::AgentDispatch = response(
        crate::twirp::other_services::agent_dispatch(
            &b,
            Some(&auth),
            "DeleteDispatch",
            Codec::Protobuf,
            &pb::DeleteAgentDispatchRequest {
                room: room.clone(),
                dispatch_id: dispatch.id,
            }
            .encode_to_vec(),
        )
        .await
        .unwrap(),
    )
    .await;
    drop(session);
    let _: pb::DeleteRoomResponse = call(
        &b,
        &auth,
        "DeleteRoom",
        &pb::DeleteRoomRequest { room: room.clone() },
    )
    .await;
    assert!(!a.manager.room_exists(&key));
    assert!(!b.manager.room_exists(&key));
    let empty: pb::ListRoomsResponse = call(
        &a,
        &auth,
        "ListRooms",
        &pb::ListRoomsRequest {
            names: vec![room.clone()],
        },
    )
    .await;
    assert!(empty.rooms.is_empty());
    control_b.shutdown().await.unwrap();
    control_a.shutdown().await.unwrap();
    a.manager.close_all().await;
    b.manager.close_all().await;
    net_a.shutdown();
    net_b.shutdown();
}

#[test]
fn actual_generated_room_manager_and_room_contracts_are_not_private_protocols() {
    let create = create_spec("ND_selected");
    assert_eq!(create.service, "RoomManager");
    assert_eq!(create.method, "CreateRoom");
    assert_eq!(create.topics, ["ND_selected"]);
    assert!(create.require_claim && create.queue);
    for name in [
        "DeleteRoom",
        "UpdateRoomMetadata",
        "ListParticipants",
        "GetParticipant",
    ] {
        let spec = room_spec("room", name);
        assert_eq!(spec.service, "Room");
        assert_eq!(spec.method, name);
        assert_eq!(spec.topics, ["room"]);
        assert!(spec.require_claim && spec.queue);
    }
}

async fn join_actual(
    state: &AppState,
    key: rtc_core::ids::RoomKey,
    identity: &str,
) -> rtc_runtime::SessionHandle {
    state
        .manager
        .join(JoinParams {
            room: key.clone(),
            grants: Grants {
                identity: identity.into(),
                video: VideoGrant {
                    room_join: true,
                    room: key.name.0.clone(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: Identity(identity.into()),
            name: "cached-original".into(),
            metadata: String::new(),
            attributes: Default::default(),
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
        })
        .await
        .unwrap()
}
async fn actual_bus(url: &str) -> Arc<RedisTransport> {
    use crate::service_bus::{redis_transport::TransportLimits, wire};
    RedisTransport::connect(
        url,
        TransportLimits {
            wire: wire::WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: wire::CompressionPolicy::default(),
            inbox_messages: 128,
            subscriptions: 128,
            commands: 32,
            io_timeout: Duration::from_secs(2),
        },
    )
    .await
    .unwrap()
}
async fn wait_cached(
    control: &RoomControl,
    room: &str,
    identity: &str,
    name: &str,
) -> pb::ParticipantInfo {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(row) = control
                .get_participant(pb::RoomParticipantIdentity {
                    revoke_token_ts: 0,
                    room: room.into(),
                    identity: identity.into(),
                })
                .await
                && row.name == name
            {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual Redis cached row must converge, not a fabricated local fallback")
}

#[tokio::test]
#[ignore = "requires EXCLUSIVE broker CLUSTER_CONTROL_REDIS_URL; CLIENT PAUSE affects the entire Redis server"]
async fn actual_cached_inventory_keeps_dirty_revisions_through_real_redis_failure() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let bus = actual_bus(&url).await;
    let na = rtc_core::ids::new_sid("ND_");
    let nb = rtc_core::ids::new_sid("ND_");
    let (mut a, net_a) = state_profile(&na, false).await;
    let (mut b, net_b) = state_profile(&nb, false).await;
    let ca = a
        .enable_room_control_component(bus.clone(), config(&na, &url))
        .await
        .unwrap();
    let cb = b
        .enable_room_control_component(bus, config(&nb, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("cached-dirty-");
    let key = a.room_key(&room);
    let created = cb
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            node_id: na,
            ..Default::default()
        })
        .await
        .unwrap();
    let inherited = WireValue::from_message(pb::ParticipantInfo {
        sid: "PA_inherited".into(),
        identity: "inherited".into(),
        ..Default::default()
    });
    let old_cached_session = WireValue::from_message(pb::ParticipantInfo {
        sid: "PA_previous_session".into(),
        identity: "cached-participant".into(),
        ..Default::default()
    });
    for participant in [&inherited, &old_cached_session] {
        ca.allocator
            .catalog
            .store_participant(&room, participant)
            .await
            .unwrap();
    }
    let session = join_actual(&a, key.clone(), "cached-participant").await;
    let initial = wait_cached(&cb, &room, "cached-participant", "cached-original").await;
    assert_eq!(initial.sid, session.participant_sid.0);
    assert_ne!(initial.sid, old_cached_session.message.sid);
    let rc = redis::Client::open(url.as_str()).unwrap();
    let mut commands = rc.get_multiplexed_async_connection().await.unwrap();
    // Real six-second command pause exceeds actual two-second transport timeout.
    // Existing owner mutation must not await Redis; this is a functional
    // separation bound relative to that withheld external service, not BWE/SLA.
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(6000)
        .arg("ALL")
        .query_async::<()>(&mut commands)
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        a.manager.update_participant(
            &key,
            &Identity("cached-participant".into()),
            rtc_runtime::ParticipantUpdate {
                name: "retained-after-outage".into(),
                metadata: "not-lost".into(),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("owner must not block on external Redis")
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !a.routing_events.failed() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real failed Redis reconciliation must be observed");
    assert!(
        a.routing_events.unresolved() > 0,
        "failed revision must remain owned"
    );
    let healed = wait_cached(&cb, &room, "cached-participant", "retained-after-outage").await;
    assert_eq!(healed.sid, initial.sid);
    assert_eq!(healed.metadata, "not-lost");
    tokio::time::timeout(Duration::from_secs(5), async {
        while a.routing_events.unresolved() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let listed = cb
        .list_participants(pb::ListParticipantsRequest { room: room.clone() })
        .await
        .unwrap();
    assert_eq!(listed.participants.len(), 2);
    assert!(listed.participants.iter().any(|p| p.sid == initial.sid));
    assert!(listed.participants.iter().any(|p| p == &inherited.message));
    a.manager
        .remove_participant(&key, &Identity("cached-participant".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let listed = cb
                .list_participants(pb::ListParticipantsRequest { room: room.clone() })
                .await
                .unwrap();
            if listed.participants == [inherited.message.clone()] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let missing = cb
        .get_participant(pb::RoomParticipantIdentity {
            revoke_token_ts: 0,
            room: room.clone(),
            identity: "cached-participant".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code, "not_found");
    drop(session);
    // The real source publishes node statistics periodically. This component
    // fixture drives that public heartbeat after the six-second broker outage,
    // rather than fabricating future availability or changing selector rules.
    for control in [&ca, &cb] {
        let mut stats = control.node.lock().await.message.stats.clone().unwrap();
        stats.updated_at = unix_now();
        control.heartbeat(stats).await.unwrap();
    }
    cb.delete_room(pb::DeleteRoomRequest { room: room.clone() })
        .await
        .unwrap();
    assert!(
        cb.list_participants(pb::ListParticipantsRequest { room: room.clone() })
            .await
            .unwrap()
            .participants
            .is_empty()
    );
    // Simulate an orphaned per-room hash after a previous catalog generation
    // vanished. Only a new catalog SID may clear such a hash at creation.
    ca.allocator
        .catalog
        .store_participant(&room, &inherited)
        .await
        .unwrap();
    let next = cb
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_ne!(next.sid, created.sid);
    assert!(
        cb.list_participants(pb::ListParticipantsRequest { room: room.clone() })
            .await
            .unwrap()
            .participants
            .is_empty()
    );
    cb.delete_room(pb::DeleteRoomRequest { room })
        .await
        .unwrap();
    a.manager.close_all().await;
    b.manager.close_all().await;
    cb.shutdown().await.unwrap();
    ca.shutdown().await.unwrap();
    net_a.shutdown();
    net_b.shutdown();
}

#[tokio::test]
#[ignore = "requires coordinator-owned exclusive CLUSTER_CONTROL_REDIS_URL; real idle owner and topic cleanup"]
async fn actual_idle_cleanup_retires_old_sid_and_next_create_allocates_a_new_sid() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let bus = actual_bus(&url).await;
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state_profile(&node, false).await;
    let control = state
        .enable_room_control_component(bus, config(&node, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("idle-catalog-");
    let key = state.room_key(&room);
    let old = control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            empty_timeout: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if matches!(
                control.allocator.catalog.load(&room).await,
                Err(catalog::CatalogError::NotFound)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real terminal projection must remove catalog and assignment");
    assert!(!state.manager.room_exists(&key));
    assert!(!control.generations.lock().await.contains_key(&room));
    let next = control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_ne!(old.sid, next.sid);
    let dispatch = control
        .create_dispatch(pb::AgentDispatch {
            id: rtc_core::ids::new_sid("AD_"),
            room: room.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dispatch.room, room);
    control
        .delete_room(pb::DeleteRoomRequest { room })
        .await
        .unwrap();
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires coordinator-owned exclusive CLUSTER_CONTROL_REDIS_URL; real post-CAS barrier and later actor turn"]
async fn actual_post_cas_old_inventory_cannot_ack_or_erase_new_join_revision() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let bus = actual_bus(&url).await;
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state_profile(&node, false).await;
    let control = state
        .enable_room_control_component(bus, config(&node, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("cas-inventory-");
    let key = state.room_key(&room);
    let created = control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while state.routing_events.unresolved() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let gate = Arc::new(ProjectionGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    *control.after_projection_cas.lock().unwrap() = Some(gate.clone());
    state
        .manager
        .update_room_metadata(&key, "barrier-old-projection".into())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let joined = join_actual(&state, key.clone(), "new-during-old-cas").await;
    assert!(state.routing_events.unresolved() > 0);
    gate.release.add_permits(1);
    let participant = wait_cached(&control, &room, "new-during-old-cas", "cached-original").await;
    assert_eq!(participant.sid, joined.participant_sid.0);
    let dispatch = control
        .create_dispatch(pb::AgentDispatch {
            id: rtc_core::ids::new_sid("AD_"),
            room: room.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dispatch.room, room);
    assert_eq!(
        control
            .allocator
            .catalog
            .load(&room)
            .await
            .unwrap()
            .room
            .message
            .sid,
        created.sid
    );
    drop(joined);
    control
        .delete_room(pb::DeleteRoomRequest { room })
        .await
        .unwrap();
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires EXCLUSIVE broker CLUSTER_CONTROL_REDIS_URL; pauses every Redis connection"]
async fn actual_redis_outage_shutdown_retires_local_topics_for_all_callers() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let bus = actual_bus(&url).await;
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state(&node).await;
    let control = state
        .enable_room_control_component(bus.clone(), config(&node, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("shutdown-room-");
    control
        .create_room(pb::CreateRoomRequest {
            name: room,
            node_id: node,
            ..Default::default()
        })
        .await
        .unwrap();
    state.manager.close_all().await;
    let gate = Arc::new(ProjectionGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    *control.before_shutdown.lock().unwrap() = Some(gate.clone());
    let first = tokio::spawn({
        let c = control.clone();
        async move { c.shutdown().await }
    });
    gate.entered.notified().await;
    let second = tokio::spawn({
        let c = control.clone();
        async move { c.shutdown().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if control
                .shutdown_run
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .completion
                .receiver_count()
                >= 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!first.is_finished() && !second.is_finished());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let rc = redis::Client::open(url.as_str()).unwrap();
    let mut connection = rc.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(10000)
        .arg("ALL")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    gate.release.add_permits(1);
    let failure = tokio::time::timeout(Duration::from_secs(30), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        control.shutdown().await.unwrap_err(),
        failure,
        "late caller must see same first failure, not an early success"
    );
    assert!(control.node_server.lock().await.is_none());
    assert!(control.generations.lock().await.is_empty());
    assert_eq!(
        bus.active_subscriptions(),
        0,
        "Redis failure must not leave request/claim subscriptions locally alive"
    );
    assert!(
        control
            .client
            .unary::<_, pb::Room>(
                &create_spec("missing"),
                &pb::CreateRoomRequest::default(),
                CallOptions::new(Duration::from_secs(3))
            )
            .await
            .unwrap_err()
            .origin
            == crate::service_bus::rpc::RpcErrorOrigin::Closed
    );
    assert!(state.routing_events.detach().await.is_err() || state.routing_events.unresolved() == 0);
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; actual startup capacity failure and retry"]
async fn actual_failed_start_does_not_poison_standalone_or_retry_authority() {
    use crate::service_bus::{redis_transport::TransportLimits, wire};
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state(&node).await;
    let bus = RedisTransport::connect(
        &url,
        TransportLimits {
            wire: wire::WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: wire::CompressionPolicy::default(),
            inbox_messages: 8,
            subscriptions: 1,
            commands: 4,
            io_timeout: Duration::from_secs(2),
        },
    )
    .await
    .unwrap();
    let occupied = bus
        .subscribe(
            &create_spec("reserved-only-for-fault").request_channel(),
            false,
        )
        .await
        .unwrap();
    assert!(
        state
            .enable_room_control_component(bus.clone(), config(&node, &url))
            .await
            .is_err()
    );
    assert!(state.room_control.is_none());
    assert_eq!(state.routing_events.unresolved(), 0);
    assert!(!rtc_core::events::EventSink::wants_room_projection(
        state.routing_events.as_ref()
    ));
    drop(occupied);
    let good = actual_bus(&url).await;
    let control = state
        .enable_room_control_component(good, config(&node, &url))
        .await
        .unwrap();
    let name = rtc_core::ids::new_sid("after-retry-");
    let room = control
        .create_room(pb::CreateRoomRequest {
            name: name.clone(),
            node_id: node,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!room.sid.is_empty());
    assert!(state.manager.room_exists(&state.room_key(&name)));
    control
        .delete_room(pb::DeleteRoomRequest { room: name })
        .await
        .unwrap();
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; source one-second selection distinct from three-second overall"]
async fn actual_missing_owner_is_selection_no_response_not_overall_deadline() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state(&node).await;
    state.rpc_policy = RpcPolicy::from_raw(1, 3_000_000_000, 0, 100);
    let control = state
        .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    let failure = control
        .configured::<_, pb::Room>(
            &create_spec("no-such-owner"),
            &pb::CreateRoomRequest::default(),
        )
        .await
        .unwrap_err();
    assert!(
        failure.is_local_no_response(),
        "a missing claimant must finish the independent selection window, not hit total request deadline: {failure}"
    );
    assert_eq!(control.client.pending_count(), 0);
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; actual publish-before-close cancellation"]
async fn actual_configured_missing_owner_close_cleans_pending_after_real_publish() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state(&node).await;
    state.rpc_policy = RpcPolicy::from_raw(1, 3_000_000_000, 0, 100);
    let bus = actual_bus(&url).await;
    let control = state
        .enable_room_control_component(bus.clone(), config(&node, &url))
        .await
        .unwrap();
    let spec = create_spec("controlled-missing-cancel-owner");
    let mut observer = bus.subscribe(&spec.request_channel(), false).await.unwrap();
    let call = tokio::spawn({
        let control = control.clone();
        async move {
            control
                .configured::<_, pb::Room>(&spec, &pb::CreateRoomRequest::default())
                .await
        }
    });
    assert!(
        !tokio::time::timeout(Duration::from_secs(3), observer.recv())
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    control.client.close();
    let failure = call.await.unwrap().unwrap_err();
    assert_eq!(
        failure.origin,
        crate::service_bus::rpc::RpcErrorOrigin::Closed
    );
    assert_eq!(control.client.pending_count(), 0);
    drop(observer);
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; actual ACK-ready topic then factory caller cancellation"]
async fn actual_cancel_before_factory_return_rolls_back_only_its_attachment() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let node = rtc_core::ids::new_sid("ND_");
    let (state, net) = state(&node).await;
    let bus = actual_bus(&url).await;
    let gate = Arc::new(ProjectionGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut options = config(&node, &url);
    options.startup_gate = Some(gate.clone());
    let started = tokio::spawn({
        let state = state.clone();
        let bus = bus.clone();
        async move { RoomControl::start(state, bus, options).await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    assert!(
        bus.active_subscriptions() > 0,
        "actual node request/claim subscriptions must be ACK-ready before cancellation"
    );
    started.abort();
    assert!(matches!(started.await,Err(error) if error.is_cancelled()));
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if bus.active_subscriptions() == 0
                && !rtc_core::events::EventSink::wants_room_projection(
                    state.routing_events.as_ref(),
                )
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("tracked factory must finish actual cleanup despite canceled waiter");
    assert!(state.room_control.is_none());
    let control = RoomControl::start(state.clone(), actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("after-canceled-start-");
    control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            node_id: node,
            ..Default::default()
        })
        .await
        .unwrap();
    control
        .delete_room(pb::DeleteRoomRequest { room })
        .await
        .unwrap();
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires dedicated CLUSTER_CONTROL_REDIS_URL; duplicate bootstrap must not mutate first actual owner"]
async fn actual_concurrent_clone_bootstrap_preserves_first_authority_budget_and_topics() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let node = rtc_core::ids::new_sid("ND_");
    let (state, net) = state(&node).await;
    let bus = actual_bus(&url).await;
    let gate = Arc::new(ProjectionGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut first_options = config(&node, &url);
    first_options.startup_gate = Some(gate.clone());
    let first = tokio::spawn({
        let state = state.clone();
        let bus = bus.clone();
        async move { RoomControl::start(state, bus, first_options).await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let expected_budget = state.routing_events.drain_budget();
    let subscriptions = bus.active_subscriptions();
    let mut duplicate = state.clone();
    duplicate.rpc_policy = RpcPolicy::from_raw(1, 9_000_000_000, 0, 100);
    let other = rtc_core::ids::new_sid("ND_");
    let result = RoomControl::start(duplicate, actual_bus(&url).await, config(&other, &url)).await;
    assert!(matches!(result,Err(ref error) if error.code=="already_exists"));
    assert_eq!(
        state.routing_events.drain_budget(),
        expected_budget,
        "rejected second attach must not change the live authority timeout"
    );
    assert_eq!(
        bus.active_subscriptions(),
        subscriptions,
        "rejected second bootstrap must not detach first topics"
    );
    gate.release.add_permits(1);
    let control = first.await.unwrap().unwrap();
    let room = rtc_core::ids::new_sid("first-bootstrap-owner-");
    let created = control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            node_id: node,
            ..Default::default()
        })
        .await
        .unwrap();
    let dispatch = control
        .create_dispatch(pb::AgentDispatch {
            id: "AD_same_actual_owner".into(),
            room: room.clone(),
            agent_name: "real-control".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dispatch.id, "AD_same_actual_owner");
    assert_eq!(
        state
            .manager
            .list_rooms(&state.room_key(&room).project, &[])[0]
            .sid
            .0,
        created.sid
    );
    control
        .delete_room(pb::DeleteRoomRequest { room })
        .await
        .unwrap();
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}

#[tokio::test]
#[ignore = "requires EXCLUSIVE broker CLUSTER_CONTROL_REDIS_URL; genuine delayed HSET/HDEL with unknown reply"]
async fn actual_delayed_unregister_quarantines_same_authority_retry_before_and_after_recovery() {
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").unwrap();
    let node = rtc_core::ids::new_sid("ND_");
    let (state, net) = state(&node).await;
    let bus = actual_bus(&url).await;
    let gate = Arc::new(ProjectionGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut options = config(&node, &url);
    options.timeout = Duration::from_millis(100);
    options.startup_gate = Some(gate.clone());
    let starting = tokio::spawn({
        let state = state.clone();
        let bus = bus.clone();
        async move { RoomControl::start(state, bus, options).await }
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let rc = redis::Client::open(url.as_str()).unwrap();
    let mut commands = rc.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(2000)
        .arg("ALL")
        .query_async::<()>(&mut commands)
        .await
        .unwrap();
    gate.release.add_permits(1);
    let failed = tokio::time::timeout(Duration::from_secs(5), starting)
        .await
        .unwrap()
        .unwrap();
    assert!(
        failed.is_err(),
        "real registration and removal replies are withheld beyond their configured timeout"
    );
    assert_eq!(
        bus.active_subscriptions(),
        0,
        "local stop must finish despite uncertain registry commands"
    );
    assert!(state.room_control.is_none());
    for after_recovery in [false, true] {
        if after_recovery {
            // This PING is a real Redis response after the pause. Recovery does
            // not prove that the prior timed-out HDEL executed or did not run.
            let pong: String = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match redis::cmd("PING")
                        .query_async::<String>(&mut commands)
                        .await
                    {
                        Ok(pong) => break pong,
                        Err(error) if error.is_timeout() => {
                            // Preserve the client's 500ms per-command timeout
                            // and original five-second recovery bound. A known
                            // two-second CLIENT PAUSE must actually end before
                            // a successful response proves broker recovery.
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        Err(error) => panic!("unexpected broker recovery error: {error}"),
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(pong, "PONG");
        }
        let retry = RoomControl::start(state.clone(), bus.clone(), config(&node, &url)).await;
        assert!(
            matches!(retry,Err(ref error) if error.code=="unavailable" && error.message=="room authority quarantined after uncertain node unregister"),
            "neither uncertain timeout nor later network health may manufacture a safe same-ID retry"
        );
        let key = state.room_key(&rtc_core::ids::new_sid("quarantine-no-room-"));
        assert!(
            state
                .manager
                .create_room(key.clone(), Default::default())
                .await
                .is_err()
        );
        assert!(!state.manager.room_exists(&key));
    }
    // Reconcile fixture-owned external residue explicitly, not by clearing the
    // production quarantine or registering a fake new node over an old command.
    let _: usize = redis::cmd("HDEL")
        .arg(registry::NODES_KEY)
        .arg(&node)
        .query_async(&mut commands)
        .await
        .unwrap();
    state.manager.close_all().await;
    net.shutdown();
}

mod create_room_policy;

mod create_room_context;

mod create_room_claimed_egress;

#[tokio::test]
#[ignore = "requires coordinator-owned CLUSTER_CONTROL_REDIS_URL; real initial store conflict and request-pump admission"]
async fn actual_superseded_initial_catalog_projection_does_not_admit_rtc_requests() {
    use futures_util::FutureExt;

    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive broker");
    let node = rtc_core::ids::new_sid("ND_");
    let (mut state, net) = state_profile(&node, false).await;
    let control = state
        .enable_room_control_component(actual_bus(&url).await, config(&node, &url))
        .await
        .unwrap();
    let room = rtc_core::ids::new_sid("first-store-conflict-");
    let key = state.room_key(&room);
    control
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let gate = || {
        Arc::new(ProjectionGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        })
    };
    let obsolete = gate();
    *control.before_participant_store.lock().unwrap() = Some(obsolete.clone());
    let identity = "first-store-conflict-participant";
    let joined = join_actual(&state, key.clone(), identity).await;
    tokio::time::timeout(Duration::from_secs(3), obsolete.entered.notified())
        .await
        .expect("actual topics registered before the initial catalog write");

    let current = gate();
    *control.before_participant_store.lock().unwrap() = Some(current.clone());
    state
        .manager
        .update_room_metadata(&key, "supersede-first-store".into())
        .await
        .unwrap();
    obsolete.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), current.entered.notified())
        .await
        .expect("obsolete attempt rejected and actual current retry reached the store");
    let lookup = || pb::RoomParticipantIdentity {
        room: room.clone(),
        identity: identity.into(),
        revoke_token_ts: 0,
    };
    assert_eq!(
        control.get_participant(lookup()).await.unwrap_err().code,
        "not_found"
    );
    assert!(
        state
            .routing_events
            .wait_participant_admitted(&room, identity, &joined.participant_sid.0)
            .now_or_never()
            .is_none(),
        "a superseded store attempt must not start the RTC input pump while the actual catalog row is absent"
    );
    current.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(3),
        state
            .routing_events
            .wait_participant_admitted(&room, identity, &joined.participant_sid.0),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        control.get_participant(lookup()).await.unwrap().sid,
        joined.participant_sid.0
    );
    drop(joined);
    control
        .delete_room(pb::DeleteRoomRequest { room })
        .await
        .unwrap();
    state.manager.close_all().await;
    control.shutdown().await.unwrap();
    net.shutdown();
}
