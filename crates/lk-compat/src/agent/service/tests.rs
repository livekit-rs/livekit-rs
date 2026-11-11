use super::super::registry::Registration;
use super::*;
use rtc_core::{
    data::{DataHandler, DataSender, DataVerdict},
    events::NullSink,
    ids::{ProjectId, RoomKey},
    media_api::DataKind,
};
use rtc_runtime::{
    RuntimeDeps,
    api::AgentDispatchOptions,
    net::{LocalInterface, NetConfig, NetRouter},
};
use std::{net::Ipv4Addr, time::Duration};
struct DropData;
impl DataHandler for DropData {
    fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
        DataVerdict::Drop
    }
}
async fn receive(
    rx: &mut mpsc::Receiver<super::super::registry::Outbound>,
) -> super::super::registry::Outbound {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test(start_paused = true)]
async fn room_dispatch_only_stores_a_real_accepted_assignment_and_delete_sends_termination() {
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: Ipv4Addr::LOCALHOST.into(),
            advertise: Ipv4Addr::LOCALHOST.into(),
        }],
    })
    .await
    .unwrap();
    let manager = RoomManager::new(RuntimeDeps {
        config: Arc::new(crate::test_support::config()),
        net: net.clone(),
        events: Arc::new(NullSink),
        data: Arc::new(DropData),
    });
    let keys = Arc::new(KeyStore::new(vec![(
        "unit-worker-key".into(),
        "public-worker-unit-fixture-secret".into(),
    )]));
    let tokens = Arc::new(TokenMinter::new(keys.clone()));
    let service = AgentService::new(
        manager.clone(),
        keys.clone(),
        tokens,
        pb::ServerInfo::default(),
        ServiceConfig {
            enable_recording: true,
            enable_redaction: true,
            ..Default::default()
        },
    )
    .unwrap();
    let mut registration = Registration::from_request(
        "AW_unit".into(),
        1,
        &pb::RegisterWorkerRequest {
            agent_name: "unit-agent".into(),
            ..Default::default()
        },
    )
    .unwrap();
    registration.api_key = "unit-worker-key".into();
    let (tx, mut outgoing) = mpsc::channel(2);
    service.registry.register(registration, tx).unwrap();
    let key = RoomKey::new(ProjectId::local(), "agent-bridge-unit");
    manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let dispatch = manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_unit".into(),
                agent_name: "unit-agent".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(dispatch.jobs.is_empty());
    let offer = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Availability(availability)) = offer.message.message
    else {
        panic!("availability required")
    };
    let job = availability.job.unwrap();
    assert!(job.enable_recording);
    assert!(job.enable_redaction);
    assert_eq!(job.dispatch_id, "AD_unit");
    let _ = offer.written.send(Ok(()));
    service
        .registry
        .availability(
            "AW_unit",
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: "unit-agent-identity".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Assignment(assigned)) = assignment.message.message else {
        panic!("assignment required")
    };
    let auth = crate::auth::authenticate_token(&keys, Some(&assigned.token))
        .unwrap()
        .unwrap();
    assert!(auth.grants.video.agent);
    assert_eq!(auth.grants.video.room, "agent-bridge-unit");
    let _ = assignment.written.send(Ok(()));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let values = manager
                .list_agent_dispatches(&key, Some("AD_unit"))
                .await
                .unwrap();
            if values[0].jobs.len() == 1 {
                assert_eq!(values[0].jobs[0].id, job.id);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager
        .delete_agent_dispatch(&key, "AD_unit")
        .await
        .unwrap();
    let termination = tokio::time::timeout(Duration::from_secs(11), outgoing.recv())
        .await
        .expect("a job without an RTC session still terminates after the startup deadline")
        .unwrap();
    assert!(
        matches!(termination.message.message,Some(pb::server_message::Message::Termination(t)) if t.job_id==job.id)
    );
    let _ = termination.written.send(Ok(()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.registry.job_state("AW_unit", &job.id).is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        manager
            .list_agent_dispatches(&key, Some("AD_unit"))
            .await
            .unwrap()
            .is_empty()
    );
    manager.delete_room(&key).await.unwrap();
    service.shutdown().await;
    assert!(service.registry.registration("AW_unit").is_none());
    net.shutdown();
}

async fn fixture() -> (
    RoomManager,
    NetRouter,
    Arc<AgentService>,
    mpsc::Receiver<super::super::registry::Outbound>,
) {
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: Ipv4Addr::LOCALHOST.into(),
            advertise: Ipv4Addr::LOCALHOST.into(),
        }],
    })
    .await
    .unwrap();
    let manager = RoomManager::new(RuntimeDeps {
        config: Arc::new(crate::test_support::config()),
        net: net.clone(),
        events: Arc::new(NullSink),
        data: Arc::new(DropData),
    });
    let keys = Arc::new(KeyStore::new(vec![(
        "worker-unit-key".into(),
        "public-unit-secret".into(),
    )]));
    let service = AgentService::new_with_capacity(
        manager.clone(),
        keys.clone(),
        Arc::new(TokenMinter::new(keys)),
        pb::ServerInfo::default(),
        ServiceConfig::default(),
        2,
    )
    .unwrap();
    let mut registration = Registration::from_request(
        "AW_fixture".into(),
        1,
        &pb::RegisterWorkerRequest {
            agent_name: "fixture-agent".into(),
            ..Default::default()
        },
    )
    .unwrap();
    registration.api_key = "worker-unit-key".into();
    let (tx, rx) = mpsc::channel(2);
    service.registry.register(registration, tx).unwrap();
    (manager, net, service, rx)
}

#[tokio::test]
async fn saturated_launch_queue_keeps_owner_control_live_and_eventually_commits_every_real_assignment()
 {
    let (manager, net, service, mut outgoing) = fixture().await;
    let key = RoomKey::new(ProjectId::local(), "agent-overload-unit");
    let agents = (0..80)
        .map(|_| rtc_core::permission::RoomAgentDispatchOptions {
            agent_name: "fixture-agent".into(),
            ..Default::default()
        })
        .collect();
    manager
        .create_room(
            key.clone(),
            rtc_runtime::api::CreateRoomOptions {
                config: rtc_core::permission::RoomConfig {
                    agent_dispatches: Some(agents),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    // More effects than both the bridge launch queue and runtime channel can
    // hold. No availability writer is drained, so accepted jobs cannot free it.
    let records = tokio::time::timeout(
        Duration::from_secs(5),
        manager.list_agent_dispatches(&key, None),
    )
    .await
    .expect("owner/control must not wait on a cyclic bridge RPC")
    .unwrap();
    assert_eq!(records.len(), 80);
    assert!(records.iter().all(|record| record.jobs.is_empty()));
    // Release actual writer receipts and worker availability for every queued
    // launch. A successful channel send is never counted as an assigned job.
    let mut assigned = 0;
    while assigned < 80 {
        let frame = receive(&mut outgoing).await;
        match frame.message.message {
            Some(pb::server_message::Message::Availability(offer)) => {
                let job = offer.job.unwrap();
                frame.written.send(Ok(())).unwrap();
                service
                    .registry
                    .availability(
                        "AW_fixture",
                        pb::AvailabilityResponse {
                            job_id: job.id.clone(),
                            available: true,
                            participant_identity: format!("agent-{}", job.id),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            Some(pb::server_message::Message::Assignment(_)) => {
                frame.written.send(Ok(())).unwrap();
                assigned += 1;
            }
            _ => panic!("unexpected worker frame while assigning"),
        }
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let records = manager.list_agent_dispatches(&key, None).await.unwrap();
            if records
                .iter()
                .map(|record| record.jobs.len())
                .sum::<usize>()
                == 80
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all 80 accepted requests must retain a real completed assignment");
    tokio::time::timeout(Duration::from_secs(2), service.shutdown())
        .await
        .unwrap();
    manager.close_all().await;
    net.shutdown();
}

pub(in crate::agent) async fn join_agent(
    manager: &RoomManager,
    key: &RoomKey,
    identity: &str,
) -> rtc_runtime::api::SessionHandle {
    manager
        .join(agent_join_params(key, identity))
        .await
        .unwrap()
}

fn agent_join_params(key: &RoomKey, identity: &str) -> rtc_core::session::JoinParams {
    use rtc_core::{
        ids::Identity,
        model::ParticipantKind,
        permission::{Grants, VideoGrant},
        session::JoinParams,
    };
    JoinParams {
        room: key.clone(),
        grants: Grants {
            identity: identity.into(),
            kind: ParticipantKind::Agent,
            video: VideoGrant {
                room_join: true,
                agent: true,
                room: key.name.0.clone(),
                ..Default::default()
            },
            ..Default::default()
        },
        identity: Identity(identity.into()),
        name: String::new(),
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
        add_tracks: Vec::new(),
        publisher_offer: None,
    }
}

#[tokio::test]
async fn deletion_waits_for_cold_agent_to_enter_before_terminating_its_job() {
    let (manager, net, service, mut outgoing) = fixture().await;
    let key = RoomKey::new(ProjectId::local(), "cold-agent");
    manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let dispatch = manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_cold".into(),
                agent_name: "fixture-agent".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Availability(value)) = offer.message.message else {
        panic!("worker availability request")
    };
    let job_id = value.job.unwrap().id;
    offer.written.send(Ok(())).unwrap();
    service
        .registry
        .availability(
            "AW_fixture",
            pb::AvailabilityResponse {
                job_id: job_id.clone(),
                available: true,
                participant_identity: "cold-agent".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = receive(&mut outgoing).await;
    assert!(matches!(
        assignment.message.message,
        Some(pb::server_message::Message::Assignment(_))
    ));
    assignment.written.send(Ok(())).unwrap();
    manager
        .delete_agent_dispatch(&key, &dispatch.id)
        .await
        .unwrap();
    assert!(
        manager
            .list_agent_dispatches(&key, Some(&dispatch.id))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), outgoing.recv())
            .await
            .is_err(),
        "a cold worker can discard termination before its job process enters"
    );
    let _session = join_agent(&manager, &key, "cold-agent").await;
    let termination = receive(&mut outgoing).await;
    assert!(matches!(
        termination.message.message,
        Some(pb::server_message::Message::Termination(value)) if value.job_id == job_id
    ));
    termination.written.send(Ok(())).unwrap();
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn dispatch_deleted_while_assignment_write_is_pending_removes_actual_agent_after_grace() {
    use rtc_core::ids::Identity;
    let (manager, net, service, mut outgoing) = fixture().await;
    let key = RoomKey::new(ProjectId::local(), "late-agent-unit");
    manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_late".into(),
                agent_name: "fixture-agent".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Availability(availability)) = offer.message.message
    else {
        panic!("offer")
    };
    let job = availability.job.unwrap();
    offer.written.send(Ok(())).unwrap();
    service
        .registry
        .availability(
            "AW_fixture",
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: "late-agent".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = receive(&mut outgoing).await; // deliberately hold writer receipt
    let _session = join_agent(&manager, &key, "late-agent").await;
    manager
        .delete_agent_dispatch(&key, "AD_late")
        .await
        .unwrap();
    assignment.written.send(Ok(())).unwrap();
    let termination = receive(&mut outgoing).await;
    assert!(
        matches!(termination.message.message,Some(pb::server_message::Message::Termination(t)) if t.job_id==job.id)
    );
    termination.written.send(Ok(())).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .get_participant(&key, &Identity("late-agent".into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a genuine late assignment must use the same guarded removal lifecycle");
    assert!(
        manager
            .list_agent_dispatches(&key, Some("AD_late"))
            .await
            .unwrap()
            .is_empty()
    );
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}

async fn enable_local_agent_bus(service: &AgentService) {
    use crate::service_bus::{
        local_transport::LocalTransport,
        redis_transport::TransportLimits,
        wire::{CompressionPolicy, WireLimits},
    };
    let bus = LocalTransport::new(TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: 128,
        subscriptions: 4096,
        commands: 4096,
        io_timeout: Duration::from_secs(3),
    })
    .unwrap();
    service.enable_cluster_rpc(bus).await.unwrap();
}

#[tokio::test]
async fn actual_local_psrpc_discovery_finishes_before_deleted_dispatch_terminates_late_availability()
 {
    use rtc_core::ids::Identity;
    let (manager, net, service, mut outgoing) = fixture().await;
    enable_local_agent_bus(&service).await;
    let key = RoomKey::new(ProjectId::local(), "late-availability-real-discovery");
    manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_held_availability".into(),
                agent_name: "fixture-agent".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Availability(request)) = offer.message.message else {
        panic!("real AgentInternal availability")
    };
    let job = request.job.unwrap();
    offer.written.send(Ok(())).unwrap();
    let delete = manager
        .delete_agent_dispatch(&key, "AD_held_availability")
        .await
        .unwrap();
    assert_eq!(delete.id, "AD_held_availability");
    assert!(
        manager
            .list_agent_dispatches(&key, Some(&delete.id))
            .await
            .unwrap()
            .is_empty(),
        "public catalog deletion does not wait for worker availability"
    );
    service
        .registry
        .availability(
            "AW_fixture",
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: "late-discovery-agent".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = receive(&mut outgoing).await;
    assert!(matches!(
        assignment.message.message,
        Some(pb::server_message::Message::Assignment(_))
    ));
    assignment.written.send(Ok(())).unwrap();
    let _session = join_agent(&manager, &key, "late-discovery-agent").await;
    assert!(
        manager
            .get_participant(&key, &Identity("late-discovery-agent".into()))
            .await
            .is_ok()
    );
    // Real CheckEnabled multi-response discovery remains in flight after its
    // first worker response. No production pause hook or invented sleep gate.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), outgoing.recv())
            .await
            .is_err(),
        "termination must not overtake still-accepted native discovery/launch completion"
    );
    let termination = tokio::time::timeout(Duration::from_secs(6), outgoing.recv())
        .await
        .expect("source CheckEnabled discovery has an existing finite five-second deadline")
        .unwrap();
    assert!(
        matches!(termination.message.message,Some(pb::server_message::Message::Termination(value))if value.job_id==job.id)
    );
    termination.written.send(Ok(())).unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        while manager
            .get_participant(&key, &Identity("late-discovery-agent".into()))
            .await
            .is_ok()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("same original three-second guarded participant cleanup must complete");
    assert!(
        manager
            .list_agent_dispatches(&key, Some("AD_held_availability"))
            .await
            .unwrap()
            .is_empty()
    );
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn held_availability_room_close_does_not_terminate_same_name_new_room_participant() {
    use rtc_core::ids::Identity;
    let (manager, net, service, mut outgoing) = fixture().await;
    let key = RoomKey::new(ProjectId::local(), "late-job-room-replacement");
    let old_room = manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_replaced".into(),
                agent_name: "fixture-agent".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Availability(value)) = offer.message.message else {
        panic!("actual held availability")
    };
    let job = value.job.unwrap();
    offer.written.send(Ok(())).unwrap();
    manager.delete_room(&key).await.unwrap();
    let new_room = manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    assert_ne!(old_room.sid, new_room.sid);
    let replacement = join_agent(&manager, &key, "same-agent-identity").await;
    let replacement_sid = replacement.participant_sid.clone();
    service
        .registry
        .availability(
            "AW_fixture",
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: "same-agent-identity".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = receive(&mut outgoing).await;
    assert!(matches!(
        assignment.message.message,
        Some(pb::server_message::Message::Assignment(_))
    ));
    assignment.written.send(Ok(())).unwrap();
    let termination = receive(&mut outgoing).await;
    assert!(
        matches!(termination.message.message,Some(pb::server_message::Message::Termination(value))if value.job_id==job.id)
    );
    termination.written.send(Ok(())).unwrap();
    tokio::time::sleep(Duration::from_millis(3100)).await;
    let actual = manager
        .get_participant(&key, &Identity("same-agent-identity".into()))
        .await
        .unwrap();
    assert_eq!(
        actual.sid, replacement_sid,
        "old assigned job cleanup cannot cross the captured original RoomSID"
    );
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn terminated_agent_signal_receipt_cannot_retire_a_resumed_session() {
    use rtc_core::ids::Identity;
    let (manager, net, service, _outgoing) = fixture().await;
    let key = RoomKey::new(ProjectId::local(), "agent-termination-signal-generation");
    let room = manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let identity = Identity("same-agent-participant".into());
    let initial = join_agent(&manager, &key, &identity.0).await;
    let mut old = manager
        .agent_participant_scope(&key, room.sid.clone(), &identity)
        .await
        .unwrap()
        .unwrap();
    assert!(!old.signal_retired);
    assert_eq!(old.participant_sid, initial.participant_sid);

    let mut resume = agent_join_params(&key, &identity.0);
    resume.reconnect = true;
    resume.participant_sid = Some(initial.participant_sid.clone());
    let resumed = manager.join(resume).await.unwrap();
    assert_eq!(resumed.participant_sid, old.participant_sid);
    assert_ne!(resumed.generation, old.signal_generation);
    drop(initial); // Its actual stale SignalClosed must not retire the new signal.
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            old.session_retirement.take().unwrap().changed()
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(
        !manager
            .remove_agent_participant(
                &key,
                room.sid.clone(),
                identity.clone(),
                old.participant_sid.clone(),
                Some(old.signal_generation),
            )
            .await
            .unwrap(),
        "same-SID resume must not admit early old-signal cleanup"
    );
    assert_eq!(
        manager.get_participant(&key, &identity).await.unwrap().sid,
        resumed.participant_sid
    );

    let mut current = manager
        .agent_participant_scope(&key, room.sid.clone(), &identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.signal_generation, resumed.generation);
    assert!(!current.signal_retired);
    drop(resumed);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            current.session_retirement.take().unwrap().changed()
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(
        manager.get_participant(&key, &identity).await.is_err(),
        "real current SignalClosed retires this pre-ICE participant immediately"
    );
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn closed_room_keeps_accepted_discovery_until_old_job_can_be_terminated() {
    use rtc_core::ids::Identity;
    let (manager, net, service, mut outgoing) = fixture().await;
    enable_local_agent_bus(&service).await;
    let key = RoomKey::new(ProjectId::local(), "closed-room-accepted-discovery");
    let original = manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    manager
        .create_agent_dispatch(
            &key,
            AgentDispatchOptions {
                id: "AD_closing".into(),
                agent_name: "fixture-agent".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let offer = receive(&mut outgoing).await;
    let Some(pb::server_message::Message::Availability(value)) = offer.message.message else {
        panic!("actual AgentInternal availability")
    };
    let job = value.job.unwrap();
    assert_eq!(job.room.as_ref().unwrap().sid, original.sid.0);
    offer.written.send(Ok(())).unwrap();

    // The API and a same-name replacement progress while the old official
    // availability request is still held by the worker fixture.
    manager.delete_room(&key).await.unwrap();
    let replacement = manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    assert_ne!(original.sid, replacement.sid);
    let identity = Identity("same-identity-after-close".into());
    let sentinel = join_agent(&manager, &key, &identity.0).await;
    service
        .registry
        .availability(
            "AW_fixture",
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: identity.0.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = receive(&mut outgoing).await;
    assert!(matches!(assignment.message.message,
        Some(pb::server_message::Message::Assignment(value))
        if value.job.as_ref().is_some_and(|assigned| assigned.id == job.id)));
    assignment.written.send(Ok(())).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), outgoing.recv())
            .await
            .is_err(),
        "closing the original room must not terminate during its still-live real discovery"
    );
    let termination = tokio::time::timeout(Duration::from_secs(6), outgoing.recv())
        .await
        .expect("actual original discovery and launch are bounded")
        .unwrap();
    assert!(matches!(termination.message.message,
        Some(pb::server_message::Message::Termination(value)) if value.job_id == job.id));
    termination.written.send(Ok(())).unwrap();
    assert_eq!(
        manager.get_participant(&key, &identity).await.unwrap().sid,
        sentinel.participant_sid
    );
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}

#[tokio::test]
async fn terminated_agent_waits_for_actual_signal_closed_after_output_slot_retires() {
    use rtc_core::{
        ids::Identity,
        session::{SessionOutput, SessionRequest, Simulate},
    };
    let (manager, net, service, _outgoing) = fixture().await;
    let key = RoomKey::new(ProjectId::local(), "agent-output-closed-before-signal");
    let room = manager
        .create_room(key.clone(), Default::default())
        .await
        .unwrap();
    let identity = Identity("agent-output-receipt".into());
    let mut handle = join_agent(&manager, &key, &identity.0).await;
    let mut before = manager
        .agent_participant_scope(&key, room.sid.clone(), &identity)
        .await
        .unwrap()
        .unwrap()
        .session_retirement
        .unwrap();

    // This is an actual room operation that closes the outgoing signal slot
    // while the handle and its current generation still belong to the client.
    // (NodeFailure no longer qualifies: like fixed Go it now closes the
    // participant itself, so its retirement would legitimately change.)
    assert!(
        handle
            .apply(SessionRequest::Simulate(Simulate::DisconnectSignalOnResume))
            .await
    );
    let close_seen = tokio::time::timeout(Duration::from_secs(1), async {
        let mut close_seen = false;
        while let Some(output) = handle.output.recv().await {
            close_seen |= matches!(output, SessionOutput::Close);
        }
        close_seen
    })
    .await
    .expect("room output slot must actually retire");
    assert!(
        close_seen,
        "room sent an actual Close before retiring its outgoing slot"
    );
    tokio::select! {
        biased;
        _ = before.changed() => panic!("output retirement must not stand in for SignalClosed"),
        _ = std::future::ready(()) => {}
    }
    let after = manager
        .agent_participant_scope(&key, room.sid.clone(), &identity)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.participant_sid, handle.participant_sid);
    assert_eq!(after.signal_generation, handle.generation);
    assert!(!after.signal_retired);
    let mut after = after
        .session_retirement
        .expect("actual actor must still be able to observe current SignalClosed");

    drop(handle);
    tokio::time::timeout(Duration::from_secs(1), async {
        assert!(before.changed().await.is_err());
        assert!(after.changed().await.is_err());
    })
    .await
    .expect("actual matching SignalClosed completes both actor receipts");
    assert!(manager.get_participant(&key, &identity).await.is_err());
    service.shutdown().await;
    manager.close_all().await;
    net.shutdown();
}
