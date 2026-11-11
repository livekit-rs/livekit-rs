use super::*;
use crate::service_bus::{local_transport::LocalTransport, redis_transport::TransportLimits, wire};
use crate::{
    state::{AppState, CompatServices},
    workers::retry::RpcPolicy,
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures_util::{pin_mut, poll};
use rtc_core::{
    events::NullSink,
    permission::{Grants, VideoGrant},
    session::{ClientCapabilities, ClientMeta, JoinParams},
};
use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
use rtc_runtime::{RoomOwnerLifecycle, RoomOwnerOperation};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::BTreeMap, task::Poll};
use tower::ServiceExt;
struct Admission {
    bus: Arc<LocalTransport>,
    armed: AtomicBool,
    entered: AtomicBool,
}
impl Admission {
    fn new() -> Self {
        Self {
            bus: LocalTransport::new(TransportLimits {
                wire: wire::WireLimits {
                    encoded_bytes: 1 << 20,
                    decompressed_bytes: 1 << 20,
                },
                compression: wire::CompressionPolicy::default(),
                inbox_messages: 100,
                subscriptions: 100,
                commands: 100,
                io_timeout: Duration::from_secs(2),
            })
            .unwrap(),
            armed: AtomicBool::new(false),
            entered: AtomicBool::new(false),
        }
    }
}
struct Lifecycle {
    local: Arc<crate::local_dispatch::LocalDispatch>,
    admission: Arc<Admission>,
}
impl RoomOwnerLifecycle for Lifecycle {
    fn opened(&self, owner: RoomOwnerHandle) -> RoomOwnerOperation<'_, Result<(), String>> {
        self.local.opened(owner)
    }
    fn closed(&self, key: RoomKey, sid: RoomSid) -> RoomOwnerOperation<'_, ()> {
        self.local.closed(key, sid)
    }
    fn participants(
        &self,
        owner: RoomOwnerHandle,
        participants: Vec<ParticipantInfo>,
    ) -> RoomOwnerOperation<'_, Result<(), String>> {
        Box::pin(async move {
            self.local.participants(owner, participants).await?;
            if self.admission.armed.swap(false, Ordering::SeqCst) {
                self.admission.entered.store(true, Ordering::SeqCst);
                let channel = crate::service_bus::channels::Channel {
                    legacy: "actual-extra-lifecycle-ack".into(),
                    ..Default::default()
                };
                let sub = self.admission.bus.subscribe_sized(&channel, true, 1).await;
                sub.unsubscribe(&channel)
                    .await
                    .map_err(|e| format!("{e}"))?;
            }
            Ok(())
        })
    }
}
const METHODS: [&str; 6] = [
    "UpdateRoomMetadata",
    "UpdateParticipant",
    "MutePublishedTrack",
    "UpdateSubscriptions",
    "RemoveParticipant",
    "DeleteRoom",
];
fn params(room: &str, id: &str) -> JoinParams {
    JoinParams {
        room: RoomKey::new(rtc_core::ids::ProjectId::local(), room),
        identity: Identity(id.into()),
        name: String::new(),
        metadata: String::new(),
        attributes: BTreeMap::new(),
        grants: Grants {
            identity: id.into(),
            video: VideoGrant {
                room_join: true,
                room: room.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        client: ClientMeta::default(),
        capabilities: ClientCapabilities::default(),
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc: true,
        disable_ice_lite: false,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: vec![],
        publisher_offer: None,
    }
}
async fn state(policy_yaml: &str) -> (AppState, NetRouter, Arc<Admission>) {
    state_with_read_policy(policy_yaml, false).await
}
async fn state_with_read_policy(
    policy_yaml: &str,
    enable_psrpc: bool,
) -> (AppState, NetRouter, Arc<Admission>) {
    state_profile(policy_yaml, enable_psrpc, true).await
}
async fn state_profile(
    policy_yaml: &str,
    enable_psrpc: bool,
    auto_create: bool,
) -> (AppState, NetRouter, Arc<Admission>) {
    let mut config = crate::test_support::config();
    config.room.auto_create = auto_create;
    config.enable_psrpc_for_get_list_participants = enable_psrpc;
    config.node_id = rtc_core::ids::new_sid("ND_mutation_");
    let args = vec![
        "local-mutation".into(),
        "--dev".into(),
        "--node-ip".into(),
        "127.0.0.1".into(),
        "--config-body".into(),
        policy_yaml.into(),
    ];
    let info = lk_config::ProgramInfo {
        binary_name: "local-mutation".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let lk_config::EntryAction::RunServer(loaded) =
        lk_config::parse(args, &lk_config::Env::default(), &info).unwrap()
    else {
        panic!("source config")
    };
    let p = &loaded.official.psrpc;
    let policy = RpcPolicy::from_raw(
        p.max_attempts,
        p.timeout.nanos(),
        p.backoff.nanos(),
        p.buffer_size,
    );
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: "127.0.0.1".parse().unwrap(),
            advertise: "127.0.0.1".parse().unwrap(),
        }],
    })
    .await
    .unwrap();
    let admission = Arc::new(Admission::new());
    let gate = admission.clone();
    (
        CompatServices::new(Arc::new(config))
            .with_whip_policy(policy)
            .with_test_lifecycle_decorator(move |local| {
                Arc::new(Lifecycle {
                    local,
                    admission: gate,
                })
            })
            .assemble(net.clone(), Arc::new(NullSink)),
        net,
        admission,
    )
}
async fn public(state: &AppState, room: &str, method: &str) -> (StatusCode, serde_json::Value) {
    let key = &state.config.keys[0];
    let token=crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#,&serde_json::json!({"iss":key.0,"sub":"actual-mutation-api","exp":99999999999u64,"video":{"roomAdmin":true,"room":room,"roomCreate":true,"roomList":true}}).to_string(),&key.1);
    let body = match method {
        "DeleteRoom" | "ListParticipants" => serde_json::json!({"room":room}),
        "CreateRoom" => serde_json::json!({"name":room,"node_id":state.config.node_id}),
        "GetParticipant" => serde_json::json!({"room":room,"identity":"alice"}),
        "UpdateRoomMetadata" => serde_json::json!({"room":room,"metadata":"actual-typed-room"}),
        "UpdateParticipant" => {
            serde_json::json!({"room":room,"identity":"alice","name":"typed-name","metadata":"typed-metadata","attributes":{"actual":"yes"}})
        }
        "MutePublishedTrack" => {
            serde_json::json!({"room":room,"identity":"alice","track_sid":"TR_nonexistent_source_nil_branch","muted":true})
        }
        "UpdateSubscriptions" => {
            serde_json::json!({"room":room,"identity":"alice","track_sids":[],"subscribe":true})
        }
        "RemoveParticipant" => serde_json::json!({"room":room,"identity":"alice"}),
        _ => panic!("unknown fixture"),
    };
    let response = crate::http::router(state.clone())
        .oneshot(
            Request::post(format!("/twirp/livekit.RoomService/{method}"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let raw = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&raw).unwrap())
}
async fn until(mut check: impl FnMut() -> bool, label: &str, seconds: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
    while !check() {
        assert!(std::time::Instant::now() < deadline, "{label}");
        tokio::task::yield_now().await;
    }
}
async fn verify_effect(state: &AppState, room: &str, method: &str) {
    let key = state.room_key(room);
    match method {
        "DeleteRoom" => {
            until(
                || !state.manager.room_exists(&key),
                "real captured room closed",
                3,
            )
            .await
        }
        "RemoveParticipant" => assert!(
            state
                .manager
                .get_participant(&key, &Identity("alice".into()))
                .await
                .is_err()
        ),
        "UpdateRoomMetadata" => assert_eq!(
            state.manager.list_rooms(&key.project, &[key.name.clone()])[0].metadata,
            "actual-typed-room"
        ),
        "UpdateParticipant" => {
            let p = state
                .manager
                .get_participant(&key, &Identity("alice".into()))
                .await
                .unwrap();
            assert_eq!(p.name, "typed-name");
            assert_eq!(p.metadata, "typed-metadata");
            assert_eq!(p.attributes["actual"], "yes");
        }
        // Source Go SetTrackMuted returns nil when track absent and UpdateSubscriptions
        // with no tracks is a valid successful no-op. These branches verify typed RPC
        // completion only; real mute/subscription media remains the unchanged API4 gate.
        "MutePublishedTrack" | "UpdateSubscriptions" => {}
        _ => unreachable!(),
    }
}
#[tokio::test]
async fn all_six_public_mutations_reach_original_local_typed_server_consumers() {
    for method in METHODS {
        let (state, net, _gate) = state("{}").await;
        let room = rtc_core::ids::new_sid("local_mut_");
        let session = state.manager.join(params(&room, "alice")).await.unwrap();
        let (status, body) = public(&state, &room, method).await;
        assert_eq!(status, StatusCode::OK, "{method}: {body}");
        verify_effect(&state, &room, method).await;
        let observed = state
            .local_dispatch
            .mutations
            .observed
            .lock()
            .unwrap()
            .clone();
        assert_eq!(
            observed.len(),
            1,
            "must enter actual typed consumer, not direct manager shortcut"
        );
        assert_eq!(observed[0].0, method);
        assert!(
            observed[0].2 - observed[0].1 > 2_900_000_000
                && observed[0].2 - observed[0].1 <= 3_000_000_000
        );
        state.manager.close_all().await;
        drop(session);
        state.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
}
#[tokio::test]
async fn all_six_public_mutations_preserve_admitted_work_after_real_actor_inbox_expiry() {
    for method in METHODS {
        let (state, net, gate) =
            state("psrpc:\n  max_attempts: 1\n  timeout: 1s\n  backoff: 2s\n").await;
        let room = rtc_core::ids::new_sid("local_expiry_");
        let session = state.manager.join(params(&room, "alice")).await.unwrap();
        let key = state.room_key(&room);
        let sid = state.manager.list_rooms(&key.project, &[key.name.clone()])[0]
            .sid
            .clone();
        let owner = state.manager.capture_room_owner(&key, &sid).unwrap();
        let admission = gate.bus.clone();
        let held = crate::service_bus::channels::Channel {
            legacy: "local-mutation-expiry-held".into(),
            ..Default::default()
        };
        let sub = admission.subscribe_sized(&held, true, 1).await;
        admission.publish_encoded(&held, b"one").await.unwrap();
        let publishing = admission.publish_encoded(&held, b"two");
        pin_mut!(publishing);
        assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
        let registering = admission.subscribe_sized(&held, true, 1);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        gate.armed.store(true, Ordering::SeqCst);
        let joining = state.manager.join(params(&room, "registration-gate"));
        pin_mut!(joining);
        assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
        for _ in 0..1025 {
            let read = tokio::task::unconstrained(owner.list_participants());
            pin_mut!(read);
            assert!(matches!(poll!(read.as_mut()), Poll::Pending));
        }
        assert_eq!(
            owner.control_capacity(),
            0,
            "actual bounded actor inbox must be full; cooperative Pending is not proof"
        );
        until(
            || gate.entered.load(Ordering::SeqCst),
            "actual owner awaits independent subscription ACK",
            3,
        )
        .await;
        let caller = {
            let state = state.clone();
            let room = room.clone();
            tokio::spawn(async move { public(&state, &room, method).await })
        };
        until(
            || state.local_dispatch.mutations.accepted.pending() == 1,
            "actual handler must enter despite a full actor queue",
            3,
        )
        .await;
        let (status, body) = caller.await.unwrap();
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{method}: {body}");
        assert_eq!(body["code"], "deadline_exceeded");
        assert_eq!(state.local_dispatch.mutations.accepted.pending(), 1);
        let (closed, sent, extra) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
        closed.unwrap();
        sent.unwrap();
        extra.unsubscribe(&held).await.unwrap();
        let extra = joining.await.ok();
        until(
            || state.local_dispatch.mutations.accepted.pending() == 0,
            "actual admitted mutation must finish after wire expiry",
            3,
        )
        .await;
        assert_eq!(
            state.local_dispatch.mutations.accepted.outcomes(),
            vec![None],
            "no fabricated success/error after actual actor release"
        );
        verify_effect(&state, &room, method).await;
        let seen = state
            .local_dispatch
            .mutations
            .observed
            .lock()
            .unwrap()
            .clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, method);
        assert!(seen[0].2 - seen[0].1 > 990_000_000 && seen[0].2 - seen[0].1 <= 1_000_000_000);
        state.manager.close_all().await;
        drop(session);
        drop(extra);
        state.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
}
#[tokio::test]
async fn actual_public_metadata_mutation_default_client_retries_three_then_five_second_wire_budgets()
 {
    let (state, net, gate) = state("{}").await;
    let room = rtc_core::ids::new_sid("local_default_retry_");
    let session = state.manager.join(params(&room, "alice")).await.unwrap();
    let key = state.room_key(&room);
    let sid = state.manager.list_rooms(&key.project, &[key.name.clone()])[0]
        .sid
        .clone();
    let owner = state.manager.capture_room_owner(&key, &sid).unwrap();
    let admission = gate.bus.clone();
    let held = crate::service_bus::channels::Channel {
        legacy: "local-mutation-retry-held".into(),
        ..Default::default()
    };
    let sub = admission.subscribe_sized(&held, true, 1).await;
    admission.publish_encoded(&held, b"one").await.unwrap();
    let publishing = admission.publish_encoded(&held, b"two");
    pin_mut!(publishing);
    assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
    let registering = admission.subscribe_sized(&held, true, 1);
    pin_mut!(registering);
    assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
    gate.armed.store(true, Ordering::SeqCst);
    let joining = state.manager.join(params(&room, "registration-gate"));
    pin_mut!(joining);
    assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
    for _ in 0..1025 {
        let read = tokio::task::unconstrained(owner.list_participants());
        pin_mut!(read);
        assert!(matches!(poll!(read.as_mut()), Poll::Pending));
    }
    assert_eq!(
        owner.control_capacity(),
        0,
        "actual bounded actor inbox must be full; cooperative Pending is not proof"
    );
    until(
        || gate.entered.load(Ordering::SeqCst),
        "actual owner awaits independent subscription ACK",
        3,
    )
    .await;
    let caller = {
        let state = state.clone();
        let room = room.clone();
        tokio::spawn(async move { public(&state, &room, "UpdateRoomMetadata").await })
    };
    until(
        || {
            state
                .local_dispatch
                .mutations
                .observed
                .lock()
                .unwrap()
                .len()
                == 2
        },
        "actual first3s expiry must start real second5s attempt",
        6,
    )
    .await;
    assert_eq!(
        state.local_dispatch.mutations.accepted.pending(),
        2,
        "both actually admitted attempts outlive first caller expiry"
    );
    let (closed, sent, extra) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
    closed.unwrap();
    sent.unwrap();
    extra.unsubscribe(&held).await.unwrap();
    let extra = joining.await.unwrap();
    let (status, body) = caller.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    verify_effect(&state, &room, "UpdateRoomMetadata").await;
    let seen = state
        .local_dispatch
        .mutations
        .observed
        .lock()
        .unwrap()
        .clone();
    assert_eq!(seen.len(), 2);
    assert!(seen[0].2 - seen[0].1 > 2_900_000_000 && seen[0].2 - seen[0].1 <= 3_000_000_000);
    assert!(seen[1].2 - seen[1].1 > 4_900_000_000 && seen[1].2 - seen[1].1 <= 5_000_000_000);
    state.manager.close_all().await;
    drop(session);
    drop(extra);
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

// Go LocalStore reads are synchronous stored snapshots by default. Opting into
// PSRPC deliberately changes this: the original typed Room client must consume
// its configured wire deadline even while the real actor mailbox is full.
#[tokio::test]
async fn public_cached_reads_bypass_full_actor_but_opt_in_uses_original_typed_deadlines() {
    for typed in [false, true] {
        let (state, net, gate) = state_with_read_policy(
            "psrpc:\n  max_attempts: 1\n  timeout: 1s\n  backoff: 2s\n",
            typed,
        )
        .await;
        let room = rtc_core::ids::new_sid("local_read_");
        let session = state.manager.join(params(&room, "alice")).await.unwrap();
        let key = state.room_key(&room);
        let sid = state.manager.list_rooms(&key.project, &[key.name.clone()])[0]
            .sid
            .clone();
        let owner = state.manager.capture_room_owner(&key, &sid).unwrap();
        let stored = state
            .local_dispatch
            .mutations
            .stored_participant(&key, &Identity("alice".into()))
            .unwrap();
        assert_eq!(
            stored.sid,
            state
                .manager
                .get_participant(&key, &Identity("alice".into()))
                .await
                .unwrap()
                .sid
        );
        let held = crate::service_bus::channels::Channel {
            legacy: "read-policy-held".into(),
            ..Default::default()
        };
        let sub = gate.bus.subscribe_sized(&held, true, 1).await;
        gate.bus.publish_encoded(&held, b"one").await.unwrap();
        let publishing = gate.bus.publish_encoded(&held, b"two");
        pin_mut!(publishing);
        assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
        let registering = gate.bus.subscribe_sized(&held, true, 1);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        gate.armed.store(true, Ordering::SeqCst);
        let joining = state.manager.join(params(&room, "registration-gate"));
        pin_mut!(joining);
        assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
        until(
            || gate.entered.load(Ordering::SeqCst),
            "actual independent lifecycle ACK",
            3,
        )
        .await;
        for _ in 0..1025 {
            let fill = tokio::task::unconstrained(owner.list_participants());
            pin_mut!(fill);
            assert!(matches!(poll!(fill.as_mut()), Poll::Pending));
        }
        assert_eq!(
            owner.control_capacity(),
            0,
            "actual Sender capacity, not cooperative-budget Pending"
        );
        for method in ["ListParticipants", "GetParticipant"] {
            let (status, body) = tokio::time::timeout(
                Duration::from_secs(if typed { 3 } else { 1 }),
                public(&state, &room, method),
            )
            .await
            .expect(
                "cached reads cannot await this blocked actor; typed reads must honor 1s policy",
            );
            if typed {
                assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{method}: {body}");
                assert_eq!(body["code"], "deadline_exceeded");
            } else {
                assert_eq!(status, StatusCode::OK, "{method}: {body}");
                let participant = if method == "GetParticipant" {
                    &body
                } else {
                    &body["participants"][0]
                };
                // Local cached order is unspecified; registration-gate may already be projected.
                if method == "GetParticipant" {
                    assert_eq!(participant["sid"], stored.sid.0);
                } else {
                    assert!(
                        body["participants"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|p| p["sid"] == stored.sid.0)
                    );
                }
            }
            assert_eq!(owner.control_capacity(), 0);
        }
        let seen = state
            .local_dispatch
            .mutations
            .observed
            .lock()
            .unwrap()
            .clone();
        if typed {
            assert_eq!(
                seen.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                vec!["ListParticipants", "GetParticipant"]
            );
            for (_, sent, expiry) in seen {
                assert!(expiry - sent > 990_000_000 && expiry - sent <= 1_000_000_000);
            }
            assert_eq!(state.local_dispatch.mutations.accepted.pending(), 2);
        } else {
            assert!(
                seen.is_empty(),
                "cached reads must not dispatch typed server requests"
            );
        }
        let (closed, sent, extra) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
        closed.unwrap();
        sent.unwrap();
        extra.unsubscribe(&held).await.unwrap();
        let extra_session = joining.await.unwrap();
        until(
            || state.local_dispatch.mutations.accepted.pending() == 0,
            "accepted typed read cleanup",
            3,
        )
        .await;
        state.manager.close_all().await;
        drop(session);
        drop(extra_session);
        state.shutdown_workers().await;
        assert!(
            state
                .local_dispatch
                .mutations
                .stored_participants(&key)
                .is_empty(),
            "retirement must remove cached generation"
        );
        net.shutdown_and_wait().await;
    }
}

#[tokio::test]
#[ignore = "requires exclusive CLUSTER_CONTROL_REDIS_URL; actual two-entry Redis mutation expiry and full actor admission"]
async fn remote_six_public_mutations_survive_true_full_actor_inbox_and_wire_expiry() {
    use crate::service_bus::rpc::ServerLimits;
    use crate::{
        room_control::{Config, selector},
        service_bus::redis_transport::RedisTransport,
    };
    use redis::IntoConnectionInfo;
    let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive real Redis broker");
    let options = |node: &str| Config {
        startup_gate: None,
        node: pb::Node {
            id: node.into(),
            state: pb::NodeState::Serving as i32,
            num_cpus: 1,
            stats: Some(pb::NodeStats {
                started_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                ..Default::default()
            }),
            ..Default::default()
        },
        redis: url.as_str().into_connection_info().unwrap(),
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
    };
    for method in METHODS {
        let (mut a, net_a, gate) = state("psrpc:\n  max_attempts: 1\n  timeout: 1s\n").await;
        let (mut b, net_b, _) = state("psrpc:\n  max_attempts: 1\n  timeout: 1s\n").await;
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
        let config_a = options(&a.config.node_id);
        let config_b = options(&b.config.node_id);
        let ac = a
            .enable_room_control_component(bus.clone(), config_a)
            .await
            .unwrap();
        let bc = b
            .enable_room_control_component(bus, config_b)
            .await
            .unwrap();
        let room = rtc_core::ids::new_sid("remote_full_mut_");
        let created = bc
            .create_room(pb::CreateRoomRequest {
                name: room.clone(),
                node_id: a.config.node_id.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        let session = a.manager.join(params(&room, "alice")).await.unwrap();
        let key = a.room_key(&room);
        let owner = a
            .manager
            .capture_room_owner(&key, &RoomSid(created.sid))
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if public(&b, &room, "GetParticipant").await.0 == StatusCode::OK {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "actual remote inventory/topic projection"
            );
            tokio::task::yield_now().await;
        }
        let held = crate::service_bus::channels::Channel {
            legacy: "remote-mutation-real-ack".into(),
            ..Default::default()
        };
        let sub = gate.bus.subscribe_sized(&held, true, 1).await;
        gate.bus.publish_encoded(&held, b"one").await.unwrap();
        let publishing = gate.bus.publish_encoded(&held, b"two");
        pin_mut!(publishing);
        assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
        let registering = gate.bus.subscribe_sized(&held, true, 1);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        gate.armed.store(true, Ordering::SeqCst);
        let joining = a.manager.join(params(&room, "registration-gate"));
        pin_mut!(joining);
        assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
        until(
            || gate.entered.load(Ordering::SeqCst),
            "actual lifecycle subscription ACK blocks actor",
            3,
        )
        .await;
        for _ in 0..1025 {
            let fill = tokio::task::unconstrained(owner.list_participants());
            pin_mut!(fill);
            assert!(matches!(poll!(fill.as_mut()), Poll::Pending));
        }
        assert_eq!(
            owner.control_capacity(),
            0,
            "actual mpsc capacity, not cooperative Pending"
        );
        let caller = {
            let b = b.clone();
            let room = room.clone();
            tokio::spawn(async move { public(&b, &room, method).await })
        };
        until(
            || ac.accepted_pending_for_test() == 1,
            "actual remote consumer has admitted its generation-owned body before queue admission",
            3,
        )
        .await;
        let (status, body) = caller.await.unwrap();
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{method}: {body}");
        assert_eq!(body["code"], "deadline_exceeded");
        assert_eq!(ac.accepted_pending_for_test(), 1);
        assert_eq!(owner.control_capacity(), 0);
        let (closed, sent, extra) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
        closed.unwrap();
        sent.unwrap();
        extra.unsubscribe(&held).await.unwrap();
        let extra_session = joining.await.ok();
        until(
            || ac.accepted_pending_for_test() == 0,
            "actual remote application completes beyond caller wire expiry",
            5,
        )
        .await;
        assert_eq!(
            ac.accepted_outcomes_for_test(),
            vec![None],
            "real application outcome, not fabricated wire success"
        );
        verify_effect(&a, &room, method).await;
        assert!(
            !b.manager.room_exists(&key),
            "entry node must never create a shadow owner"
        );
        a.manager.close_all().await;
        drop(session);
        drop(extra_session);
        bc.shutdown().await.unwrap();
        ac.shutdown().await.unwrap();
        a.shutdown_workers().await;
        b.shutdown_workers().await;
        net_a.shutdown_and_wait().await;
        net_b.shutdown_and_wait().await;
    }
}

async fn dispatch_public(
    state: &AppState,
    room: &str,
    method: &str,
    id: &str,
) -> (StatusCode, serde_json::Value) {
    let key = &state.config.keys[0];
    let token=crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#,&serde_json::json!({"iss":key.0,"sub":"actual-dispatch-expiry","exp":99999999999u64,"video":{"roomAdmin":true,"room":room}}).to_string(),&key.1);
    let body = match method {
        "CreateDispatch" => {
            serde_json::json!({"room":room,"agent_name":"actual-accepted-agent","metadata":"after-wire-expiry"})
        }
        "DeleteDispatch" => serde_json::json!({"room":room,"dispatch_id":id}),
        _ => unreachable!(),
    };
    let response = crate::http::router(state.clone())
        .oneshot(
            Request::post(format!("/twirp/livekit.AgentDispatchService/{method}"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let raw = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&raw).unwrap())
}
async fn dispatch_full_inbox_campaign(remote: bool, auto_create: bool) {
    // Cover both source profiles: auto_create=true performs a separate
    // CreateRoom preflight, whose existing-room ToProto must remain a cached
    // read independent of this actor's full control inbox. False isolates only
    // the following original typed AgentDispatch consumer.
    // Catalog/admission scope only: no fake worker is declared to have terminated.
    // Fixed Go deletes dispatch under lock, then its separate background worker
    // termination continues independently; actual child exit remains Agent12 scope.
    for method in ["CreateDispatch", "DeleteDispatch"] {
        let (mut owner_state, net, gate) = state_profile(
            "psrpc:\n  max_attempts: 1\n  timeout: 1s\n",
            false,
            auto_create,
        )
        .await;
        let room = rtc_core::ids::new_sid("dispatch_full_");
        let mut remote_fixture = None;
        if remote {
            let url = std::env::var("CLUSTER_CONTROL_REDIS_URL").expect("exclusive Redis broker");
            let (mut entry, entry_net, _) = state_profile(
                "psrpc:\n  max_attempts: 1\n  timeout: 1s\n",
                false,
                auto_create,
            )
            .await;
            let bus = crate::service_bus::redis_transport::RedisTransport::connect(
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
            // Reuse the exact original-component config, no alternate authority.
            let owner_config = remote_dispatch_config(&owner_state.config.node_id, &url);
            let entry_config = remote_dispatch_config(&entry.config.node_id, &url);
            let control = owner_state
                .enable_room_control_component(bus.clone(), owner_config)
                .await
                .unwrap();
            let entry_control = entry
                .enable_room_control_component(bus, entry_config)
                .await
                .unwrap();
            entry_control
                .create_room(pb::CreateRoomRequest {
                    name: room.clone(),
                    node_id: owner_state.config.node_id.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            remote_fixture = Some((entry, entry_net, control, entry_control));
        }
        if !auto_create {
            // A disabled implicit-create profile must start with an actual
            // explicitly created room, through the authenticated public API.
            let (status, body) = public(&owner_state, &room, "CreateRoom").await;
            assert_eq!(
                status,
                StatusCode::OK,
                "actual explicit public room setup: {body}"
            );
        }
        let session = owner_state
            .manager
            .join(params(&room, "alice"))
            .await
            .unwrap();
        let entry = remote_fixture
            .as_ref()
            .map(|f| &f.0)
            .unwrap_or(&owner_state);
        let key = owner_state.room_key(&room);
        let sid = owner_state
            .manager
            .list_rooms(&key.project, &[key.name.clone()])[0]
            .sid
            .clone();
        let owner = owner_state.manager.capture_room_owner(&key, &sid).unwrap();
        let mut deleting_id = String::new();
        if method == "DeleteDispatch" {
            let (status, body) = dispatch_public(entry, &room, "CreateDispatch", "").await;
            assert_eq!(status, StatusCode::OK, "actual dispatch setup: {body}");
            deleting_id = body["id"].as_str().unwrap().to_owned();
        }
        let before = owner.list_agent_dispatches(None).await.unwrap();
        let held = crate::service_bus::channels::Channel {
            legacy: "dispatch-owned-admission".into(),
            ..Default::default()
        };
        let sub = gate.bus.subscribe_sized(&held, true, 1).await;
        gate.bus.publish_encoded(&held, b"one").await.unwrap();
        let publishing = gate.bus.publish_encoded(&held, b"two");
        pin_mut!(publishing);
        assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
        let registering = gate.bus.subscribe_sized(&held, true, 1);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        gate.armed.store(true, Ordering::SeqCst);
        let joining = owner_state.manager.join(params(&room, "registration-gate"));
        pin_mut!(joining);
        assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
        until(
            || gate.entered.load(Ordering::SeqCst),
            "real lifecycle ACK blocks actor",
            3,
        )
        .await;
        for _ in 0..1025 {
            let fill = tokio::task::unconstrained(owner.list_participants());
            pin_mut!(fill);
            assert!(matches!(poll!(fill.as_mut()), Poll::Pending));
        }
        assert_eq!(owner.control_capacity(), 0, "actual actor admission full");
        let caller = {
            let entry = entry.clone();
            let room = room.clone();
            let id = deleting_id.clone();
            tokio::spawn(async move { dispatch_public(&entry, &room, method, &id).await })
        };
        let pending = || {
            remote_fixture
                .as_ref()
                .map(|f| f.2.accepted_pending_for_test())
                .unwrap_or_else(|| owner_state.local_dispatch.mutations.accepted.pending())
        };
        until(
            || pending() == 1,
            "actual dispatch handler admitted beyond full actor capacity",
            3,
        )
        .await;
        let (status, body) = caller.await.unwrap();
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{method}: {body}");
        assert_eq!(body["code"], "deadline_exceeded");
        assert_eq!(pending(), 1);
        assert_eq!(owner.control_capacity(), 0);
        let (closed, sent, extra) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
        closed.unwrap();
        sent.unwrap();
        extra.unsubscribe(&held).await.unwrap();
        let extra_session = joining.await.unwrap();
        until(
            || pending() == 0,
            "actual dispatch mutation completes after original wire expiry",
            5,
        )
        .await;
        let after = owner.list_agent_dispatches(None).await.unwrap();
        if method == "CreateDispatch" {
            assert_eq!(after.len(), before.len() + 1);
            assert!(
                after.iter().any(|d| d.agent_name == "actual-accepted-agent"
                    && d.metadata == "after-wire-expiry")
            );
        } else {
            assert_eq!(after.len() + 1, before.len());
            assert!(!after.iter().any(|d| d.id == deleting_id));
        }
        if let Some((entry, _, _, _)) = &remote_fixture {
            assert!(!entry.manager.room_exists(&key), "no entry shadow owner");
        }
        owner_state.manager.close_all().await;
        drop(session);
        drop(extra_session);
        if let Some((entry, entry_net, control, entry_control)) = remote_fixture {
            entry_control.shutdown().await.unwrap();
            control.shutdown().await.unwrap();
            entry.shutdown_workers().await;
            entry_net.shutdown_and_wait().await;
        }
        owner_state.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
}
fn remote_dispatch_config(node: &str, url: &str) -> crate::room_control::Config {
    use redis::IntoConnectionInfo;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    crate::room_control::Config {
        startup_gate: None,
        node: pb::Node {
            id: node.into(),
            state: pb::NodeState::Serving as i32,
            num_cpus: 1,
            stats: Some(pb::NodeStats {
                started_at: now,
                updated_at: now,
                ..Default::default()
            }),
            ..Default::default()
        },
        redis: url.into_connection_info().unwrap(),
        timeout: Duration::from_secs(2),
        max_protobuf_bytes: 1 << 20,
        max_pending: 32,
        selector: crate::room_control::selector::Config {
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
        limits: crate::service_bus::rpc::ServerLimits {
            concurrent: 16,
            max_request_duration: Duration::from_secs(10),
            allow_skip_claim: false,
        },
    }
}
#[tokio::test]
async fn actual_local_dispatch_mutations_preserve_full_actor_admission_after_wire_expiry() {
    dispatch_full_inbox_campaign(false, true).await;
    dispatch_full_inbox_campaign(false, false).await;
}
#[tokio::test]
#[ignore = "requires exclusive CLUSTER_CONTROL_REDIS_URL; true remote dispatch actor admission, not original-worker shutdown certification"]
async fn actual_remote_dispatch_mutations_preserve_full_actor_admission_after_wire_expiry() {
    dispatch_full_inbox_campaign(true, true).await;
    dispatch_full_inbox_campaign(true, false).await;
}

#[tokio::test]
async fn actual_public_existing_create_room_reads_same_generation_info_without_actor_admission() {
    let (state, net, gate) = state("{}").await;
    let room = rtc_core::ids::new_sid("existing_cached_create_");
    let session = state.manager.join(params(&room, "alice")).await.unwrap();
    let key = state.room_key(&room);
    let owner = state
        .manager
        .capture_room_owner(
            &key,
            &state.manager.list_rooms(&key.project, &[key.name.clone()])[0].sid,
        )
        .unwrap();
    let updated = owner
        .update_metadata("actual-applied-before-cache-read".into())
        .await
        .unwrap();
    // Strict immediate cache visibility: no timer/poll lets a later owner turn
    // conceal reply-before-snapshot bugs observed in the real candidate29 run.
    assert_eq!(
        state.manager.list_rooms(&key.project, &[key.name.clone()])[0].metadata,
        updated.metadata
    );
    let held = crate::service_bus::channels::Channel {
        legacy: "existing-create-cache-real-ack".into(),
        ..Default::default()
    };
    let sub = gate.bus.subscribe_sized(&held, true, 1).await;
    gate.bus.publish_encoded(&held, b"one").await.unwrap();
    let publishing = gate.bus.publish_encoded(&held, b"two");
    pin_mut!(publishing);
    assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
    let registering = gate.bus.subscribe_sized(&held, true, 1);
    pin_mut!(registering);
    assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
    gate.armed.store(true, Ordering::SeqCst);
    let joining = state.manager.join(params(&room, "registration-gate"));
    pin_mut!(joining);
    assert!(matches!(poll!(joining.as_mut()), Poll::Pending));
    until(
        || gate.entered.load(Ordering::SeqCst),
        "actual existing-owner lifecycle ACK",
        3,
    )
    .await;
    for _ in 0..1025 {
        let fill = tokio::task::unconstrained(owner.list_participants());
        pin_mut!(fill);
        assert!(matches!(poll!(fill.as_mut()), Poll::Pending));
    }
    assert_eq!(owner.control_capacity(), 0);
    let (status, body) =
        tokio::time::timeout(Duration::from_secs(1), public(&state, &room, "CreateRoom"))
            .await
            .expect(
                "fixed Go existing owner CreateRoom is cached ToProto, not actor queue admission",
            );
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["sid"], updated.sid.0);
    assert_eq!(body["metadata"], updated.metadata);
    assert_eq!(
        owner.control_capacity(),
        0,
        "cached CreateRoom cannot enqueue or drain a control slot"
    );
    let (closed, sent, extra) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
    closed.unwrap();
    sent.unwrap();
    extra.unsubscribe(&held).await.unwrap();
    let extra_session = joining.await.unwrap();
    state.manager.close_all().await;
    drop(session);
    drop(extra_session);
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}

/// A successful nil TrackInfo response must still deliver the original admin
/// command to the real publisher session, including AddTrack/retirement races.
#[tokio::test]
async fn public_admin_mute_unknown_track_signals_publisher_without_inventing_track() {
    use rtc_core::session::{SessionOutput, SignalEvent};
    let (state, net, _) = state("{}").await;
    let room = "actual-admin-mute-unknown-public";
    let join = params(room, "alice");
    let mut session = state.manager.join(join.clone()).await.unwrap();
    let (status, body) = public(&state, room, "MutePublishedTrack").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("track").is_none_or(serde_json::Value::is_null),
        "{body}"
    );
    let signal = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match session
                .output
                .recv()
                .await
                .expect("actual publisher session remains open")
            {
                SessionOutput::Signal(event @ SignalEvent::Mute { .. }) => break event,
                SessionOutput::Signal(_) => {}
                SessionOutput::Close => panic!("admin mute must not close publisher session"),
            }
        }
    })
    .await
    .expect("nil TrackInfo must not suppress actual publisher Mute signal");
    let mut ctx = crate::signal::session::SessionCtx {
        state: state.clone(),
        grants: join.grants,
        client: join.client,
        cache: crate::signal::encode::RequestCache::default(),
    };
    let response = ctx
        .compose(signal, &session.participant_sid)
        .expect("actual signal wire projection");
    let encoded = prost::Message::encode_to_vec(&response);
    let decoded: pb::SignalResponse = prost::Message::decode(encoded.as_slice()).unwrap();
    match decoded.message {
        Some(pb::signal_response::Message::Mute(command)) => {
            assert_eq!(command.sid, "TR_nonexistent_source_nil_branch");
            assert!(command.muted);
        }
        other => panic!("expected original protobuf Mute command, got {other:?}"),
    }
    let participant = state
        .manager
        .get_participant(&state.room_key(room), &Identity("alice".into()))
        .await
        .unwrap();
    assert!(
        participant.tracks.is_empty(),
        "nil response must not manufacture publication"
    );
    let observed = state
        .local_dispatch
        .mutations
        .observed
        .lock()
        .unwrap()
        .clone();
    assert_eq!(
        observed.len(),
        1,
        "must traverse actual typed LocalPSRPC consumer"
    );
    assert_eq!(observed[0].0, "MutePublishedTrack");
    state.manager.close_all().await;
    drop(session);
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}
