use super::*;
#[test]
fn signal_sequence_deduplicates_retried_prefix_and_rejects_gap() {
    let make = |id: u64| pb::SignalRequest {
        message: Some(pb::signal_request::Message::Ping(id as i64)),
    };
    let raw = vec![make(1).encode_to_vec(), make(2).encode_to_vec()];
    let mut expected = 0;
    let batch = codec::request_batch(0, false, &raw).unwrap();
    assert_eq!(
        codec::read_batch(
            "rpc.RelaySignalRequest",
            "requests",
            &batch,
            0,
            &mut expected
        )
        .unwrap(),
        raw
    );
    assert_eq!(expected, 2);
    assert!(
        codec::read_batch(
            "rpc.RelaySignalRequest",
            "requests",
            &batch,
            0,
            &mut expected
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(expected, 2);
    assert!(
        codec::read_batch(
            "rpc.RelaySignalRequest",
            "requests",
            &batch,
            3,
            &mut expected
        )
        .is_err()
    );
    assert_eq!(expected, 2);
}
#[test]
fn relay_keeps_unknown_request_and_blob_children() {
    let mut raw = pb::SignalRequest {
        message: Some(pb::signal_request::Message::GetDataBlobRequest(
            pb::GetDataBlobRequest {
                request_id: 7,
                ..Default::default()
            },
        )),
    }
    .encode_to_vec();
    raw.extend_from_slice(&[0xa0, 0x06, 0x07]);
    let batch = codec::request_batch(0, false, &[raw.clone()]).unwrap();
    let mut seq = 0;
    let output =
        codec::read_batch("rpc.RelaySignalRequest", "requests", &batch, 0, &mut seq).unwrap();
    assert_eq!(output.len(), 1);
    let parsed = PreservedMessage::decode("livekit.SignalRequest", &output[0]).unwrap();
    assert_eq!(parsed.unknown_wire(), &[0xa0, 0x06, 0x07]);
}

fn relay_config() -> RelayConfig {
    RelayConfig {
        retry_timeout: Duration::from_secs(3),
        min_retry_interval: Duration::from_millis(100),
        max_retry_interval: Duration::from_millis(500),
        stream_buffer_size: 64,
        connect_attempts: 2,
    }
}
pub(super) fn join(
    config: &rtc_core::config::ServerConfig,
    room: &str,
    identity: &str,
) -> JoinParams {
    let grants = rtc_core::permission::Grants {
        identity: identity.into(),
        name: "caller".into(),
        metadata: "source metadata".into(),
        attributes: [("region".into(), "hk".into())].into_iter().collect(),
        expires_at: rtc_core::util::unix_s() + 3600,
        video: rtc_core::permission::VideoGrant {
            room: room.into(),
            room_join: true,
            can_publish: Some(true),
            can_subscribe: Some(false),
            can_publish_data: Some(true),
            can_update_own_metadata: Some(true),
            ..Default::default()
        },
        ..Default::default()
    };
    crate::signal::params::parse_join(
        config,
        Some(grants),
        &[
            ("room".into(), room.into()),
            ("protocol".into(), "16".into()),
            ("sdk".into(), "js".into()),
        ]
        .into_iter()
        .collect(),
        &Default::default(),
        None,
        false,
        false,
    )
    .unwrap()
    .params
}
#[test]
fn fixed_start_session_round_trip_keeps_permissions_metadata_reconnect_and_offer() {
    let config = crate::test_support::config();
    let mut params = join(&config, "start-room", "participant");
    params.reconnect = true;
    params.reconnect_reason = 3;
    params.participant_sid = Some("PA_resume".into());
    params.adaptive_stream = true;
    params.single_pc = true;
    params.subscriber_allow_pause = Some(false);
    params.disable_ice_lite = true;
    params.publisher_offer = Some(("v=0\r\n".into(), 8));
    let track = pb::AddTrackRequest {
        cid: "initial-track".into(),
        name: "microphone".into(),
        ..Default::default()
    };
    let start = codec::start_session(&params, vec![track.clone()]).unwrap();
    assert!(start.connection_id.starts_with("CO_"));
    let restored = codec::join_params("api-key", &start).unwrap();
    assert_eq!(restored.grants.api_key, "api-key");
    assert_eq!(restored.grants.video, params.grants.video);
    assert_eq!(restored.metadata, params.metadata);
    assert_eq!(restored.attributes, params.attributes);
    assert_eq!(restored.client, params.client);
    assert_eq!(restored.reconnect, params.reconnect);
    assert_eq!(restored.reconnect_reason, params.reconnect_reason);
    assert_eq!(restored.participant_sid, params.participant_sid);
    assert_eq!(restored.publisher_offer, params.publisher_offer);
    assert_eq!(restored.single_pc, params.single_pc);
    assert_eq!(
        restored.subscriber_allow_pause,
        params.subscriber_allow_pause
    );
    assert_eq!(restored.add_tracks.len(), 1);
    assert_eq!(restored.add_tracks[0].cid, track.cid);
    assert_eq!(restored.grants.expires_at, params.grants.expires_at);
}

/// Full component integration against one explicitly supplied Redis. No process
/// is launched; coordinator owns the fixture. Nodes/rooms use unique IDs, no
/// fixture-wide deletion is permitted. Production binary+Go paired tests remain
/// separate acceptance gates.
async fn two_node_remote_join_case(
    endpoint: crate::redis_endpoint::RedisEndpoint,
    fail_initial_store: bool,
) {
    use crate::room_control::{ManagedConfig, registry::RedisRegistry, selector};
    use crate::service_bus::{
        redis_transport::TransportLimits,
        rpc::ServerLimits,
        wire::{CompressionPolicy, WireLimits},
    };
    use rtc_core::events::NullSink;
    let mut states = Vec::new();
    let mut nets = Vec::new();
    let mut nodes = Vec::new();
    let mut controls = Vec::new();
    let registry = Arc::new(
        RedisRegistry::connect_endpoint(endpoint.clone(), Duration::from_secs(2), 1 << 20)
            .await
            .unwrap(),
    );
    for _ in 0..2 {
        let (template, net) = crate::test_support::state().await;
        let mut config = (*template.config).clone();
        config.node_id = rtc_core::ids::new_sid("ND_");
        // The owner room must admit participant Blob stores so the cross-node
        // provenance round-trip below exercises the real relay, not NotAllowed.
        config.enable_participant_data_blob = true;
        let mut state = crate::state::CompatServices::new(Arc::new(config))
            .assemble(net.clone(), Arc::new(NullSink));
        let node = Arc::new(
            crate::routing::LocalNode::serving(
                state.config.node_id.clone(),
                "127.0.0.1".into(),
                "hk".into(),
                1,
                rtc_core::util::unix_s(),
            )
            .unwrap(),
        );
        state.enable_node(node.clone());
        registry.register_node(&node.wire_value()).await.unwrap();
        let bus = RedisTransport::connect_endpoint(
            endpoint.clone(),
            TransportLimits {
                wire: WireLimits {
                    encoded_bytes: 1 << 20,
                    decompressed_bytes: 1 << 20,
                },
                compression: CompressionPolicy::default(),
                inbox_messages: 128,
                subscriptions: 512,
                commands: 128,
                io_timeout: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
        let control = state
            .enable_managed_room_control_component(
                bus.clone(),
                ManagedConfig {
                    timeout: Duration::from_secs(2),
                    max_protobuf_bytes: 1 << 20,
                    max_pending: 32,
                    selector: selector::Config {
                        kind: "any".into(),
                        sort_by: "clients".into(),
                        algorithm: "lowest".into(),
                        cpu_limit: 0.9,
                        sysload_limit: 0.9,
                        region: "hk".into(),
                        regions: vec![],
                        track_limit: 0,
                        bytes_limit: 0.0,
                    },
                    limits: ServerLimits {
                        concurrent: 16,
                        max_request_duration: Duration::from_secs(10),
                        allow_skip_claim: false,
                    },
                },
                endpoint.clone(),
                node.clone(),
            )
            .await
            .unwrap();
        state
            .enable_cluster_signal(bus, relay_config())
            .await
            .unwrap();
        states.push(state);
        nets.push(net);
        nodes.push(node);
        controls.push(control);
    }
    let stop = Cancellation::default();
    let stopping = stop.clone();
    let produce = nodes.clone();
    let producer = registry.clone();
    let refreshing = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {biased;_=stopping.cancelled()=>break,_=tick.tick()=>for node in &produce{node.update_stats(rtc_core::util::unix_s());producer.register_node(&node.wire_value()).await.unwrap();}}
        }
    });
    let room = rtc_core::ids::new_sid("remote_");
    let identity = rtc_core::ids::new_sid("EG_");
    controls[0]
        .create_room(pb::CreateRoomRequest {
            name: room.clone(),
            node_id: nodes[1].id(),
            ..Default::default()
        })
        .await
        .unwrap();
    let participant_key = format!("room_participants:{room}");
    let fault_connection = if fail_initial_store {
        let connection = endpoint
            .connect(&redis::AsyncConnectionConfig::new())
            .await
            .unwrap();
        let mut commands = connection.begin();
        let _: () = redis::cmd("SET")
            .arg(&participant_key)
            .arg("owned-initial-store-wrongtype")
            .query_async(&mut commands)
            .await
            .unwrap();
        drop(commands);
        Some(connection)
    } else {
        None
    };
    let connection = states[0]
        .cluster_signal
        .as_ref()
        .unwrap()
        .connect(join(&states[0].config, &room, &identity), vec![])
        .await
        .unwrap();
    let SignalConnection::Remote(mut remote) = connection else {
        panic!("entry node must not own the participant")
    };
    let raw = tokio::time::timeout(Duration::from_secs(3), remote.incoming.recv())
        .await
        .unwrap()
        .unwrap();
    let response: pb::SignalResponse = lk_protocol::wire::decode(&raw).unwrap();
    assert!(matches!(
        response.message,
        Some(pb::signal_response::Message::Join(_))
    ));
    assert!(!states[0].manager.room_exists(&rtc_core::ids::RoomKey::new(
        rtc_core::ids::ProjectId::local(),
        room.clone()
    )));
    assert!(states[1].manager.room_exists(&rtc_core::ids::RoomKey::new(
        rtc_core::ids::ProjectId::local(),
        room.clone()
    )));
    let update = pb::SignalRequest {
        message: Some(pb::signal_request::Message::UpdateMetadata(
            pb::UpdateParticipantMetadata {
                request_id: 77,
                metadata: "applied-on-owner".into(),
                name: "remote-name".into(),
                ..Default::default()
            },
        )),
    };
    assert!(remote.send(update.encode_to_vec()).await);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let raw = remote.incoming.recv().await.expect("signal response");
            let response: pb::SignalResponse = lk_protocol::wire::decode(&raw).unwrap();
            if matches!(
                response.message,
                Some(pb::signal_response::Message::RequestResponse(response)) if response.request_id==77&&response.reason==0
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let participant = states[1]
        .manager
        .get_participant(
            &rtc_core::ids::RoomKey::new(rtc_core::ids::ProjectId::local(), room.clone()),
            &rtc_core::ids::Identity(identity.clone()),
        )
        .await
        .unwrap();
    assert_eq!(participant.metadata, "applied-on-owner");
    // A successful RTC request response is produced by the request pump.
    // Fixed Go starts that pump only after initial topic registration and
    // StoreParticipant attempt; on a healthy broker the cached participant
    // must already be visible. Do not retry the subsequent layout mutator.
    if let Some(connection) = &fault_connection {
        // The RTC metadata request succeeded despite a real WRONGTYPE on the
        // initial store: Go logs that storage error and still starts its pump.
        // Prove the fault remained in place until after the actual response.
        let mut commands = connection.begin();
        let value: String = redis::cmd("GET")
            .arg(&participant_key)
            .query_async(&mut commands)
            .await
            .unwrap();
        assert_eq!(value, "owned-initial-store-wrongtype");
        let _: usize = redis::cmd("DEL")
            .arg(&participant_key)
            .query_async(&mut commands)
            .await
            .unwrap();
        drop(commands);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if controls[0]
                    .get_participant(pb::RoomParticipantIdentity {
                        room: room.clone(),
                        identity: identity.clone(),
                        ..Default::default()
                    })
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real catalog recovers after removing only owned WRONGTYPE key");
    }
    let stored = controls[0]
        .get_participant(pb::RoomParticipantIdentity {
            room: room.clone(),
            identity: identity.clone(),
            ..Default::default()
        })
        .await
        .expect("RTC metadata ACK must follow healthy initial participant store");
    assert_eq!(stored.sid, participant.sid.0);
    // Cross-node participant Blob provenance through BOTH real PSRPC stream
    // directions. The entry node is not the owner: the raw unknown subtrees in
    // the key and blob must survive the relay to the owner room and back, not be
    // erased by a typed decode/re-encode at either hop.
    {
        let mut key_raw = pb::DataBlobKey {
            key: Some(pb::data_blob_key::Key::Generic("cross-node-blob".into())),
        }
        .encode_to_vec();
        key_raw.extend_from_slice(&[0xa0, 0x06, 7]); // unknown field 100, varint 7
        let key = PreservedMessage::decode("livekit.DataBlobKey", &key_raw).unwrap();
        let mut blob = PreservedMessage::from_message(&pb::DataBlob {
            contents: vec![0, 255, 1],
            ..Default::default()
        });
        blob.replace_nested("key", Some(&key));
        let mut blob_raw = blob.encode_to_vec();
        blob_raw.extend_from_slice(&[0xa8, 0x06, 9]); // unknown field 101, varint 9
        let blob = PreservedMessage::decode("livekit.DataBlob", &blob_raw).unwrap();

        let mut store = PreservedMessage::from_message(&pb::StoreDataBlobRequest {
            request_id: 91,
            ..Default::default()
        });
        store.replace_nested("blob", Some(&blob));
        let mut root = PreservedMessage::from_message(&pb::SignalRequest {
            message: Some(pb::signal_request::Message::StoreDataBlobRequest(
                Default::default(),
            )),
        });
        root.replace_nested("store_data_blob_request", Some(&store));
        assert!(remote.send(root.encode_to_vec()).await);
        let store_resp = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let raw = remote.incoming.recv().await.expect("store blob response");
                let resp = PreservedMessage::decode("livekit.SignalResponse", &raw).unwrap();
                if resp.nested("store_data_blob_response").is_some() {
                    break resp;
                }
            }
        })
        .await
        .unwrap();
        let typed: pb::SignalResponse = store_resp.project();
        let Some(pb::signal_response::Message::StoreDataBlobResponse(sr)) = typed.message else {
            panic!("cross-node Blob store must succeed, got {typed:?}");
        };
        assert_eq!(sr.request_id, 91);
        let resp_key = store_resp
            .nested("store_data_blob_response")
            .unwrap()
            .nested("key")
            .unwrap();
        assert_eq!(
            resp_key.unknown_wire(),
            &[0xa0, 0x06, 7],
            "store response key unknown cohort must survive the relay"
        );
        assert_eq!(
            resp_key.project::<pb::DataBlobKey>().key,
            Some(pb::data_blob_key::Key::Generic("cross-node-blob".into()))
        );

        let mut get = PreservedMessage::from_message(&pb::GetDataBlobRequest {
            request_id: 92,
            participant_identity: identity.clone(),
            ..Default::default()
        });
        get.replace_nested("key", Some(&key));
        let mut root = PreservedMessage::from_message(&pb::SignalRequest {
            message: Some(pb::signal_request::Message::GetDataBlobRequest(
                Default::default(),
            )),
        });
        root.replace_nested("get_data_blob_request", Some(&get));
        assert!(remote.send(root.encode_to_vec()).await);
        let get_resp = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let raw = remote.incoming.recv().await.expect("get blob response");
                let resp = PreservedMessage::decode("livekit.SignalResponse", &raw).unwrap();
                if resp.nested("get_data_blob_response").is_some() {
                    break resp;
                }
            }
        })
        .await
        .unwrap();
        let typed: pb::SignalResponse = get_resp.project();
        let Some(pb::signal_response::Message::GetDataBlobResponse(gr)) = typed.message else {
            panic!("cross-node Blob get must succeed, got {typed:?}");
        };
        assert_eq!(gr.request_id, 92);
        let resp_blob = get_resp
            .nested("get_data_blob_response")
            .unwrap()
            .nested("blob")
            .unwrap();
        assert_eq!(
            resp_blob.unknown_wire(),
            &[0xa8, 0x06, 9],
            "get response blob unknown cohort must survive the relay"
        );
        assert_eq!(resp_blob.project::<pb::DataBlob>().contents, vec![0, 255, 1]);
        assert_eq!(
            resp_blob.nested("key").unwrap().unknown_wire(),
            &[0xa0, 0x06, 7],
            "get response nested key unknown cohort must survive the relay"
        );
    }
    // These live signal observations precede the helper's genuine public
    // RemoveParticipant, which intentionally retires this exact owner.
    super::signal_activity_tests::remote_owner_activity(
        &states[0],
        &states[1],
        &mut remote,
        &room,
        &identity,
    )
    .await;
    crate::worker_services::cluster_rooms_tests::remote_consumers(
        &states[0],
        &states[1],
        endpoint.clone(),
        &room,
        &identity,
    )
    .await;
    remote.close().await;
    controls[0]
        .delete_room(pb::DeleteRoomRequest { room: room.clone() })
        .await
        .unwrap();
    for state in &states {
        state.shutdown_cluster_signal().await;
    }
    // Stop the actual producer before auditing authority: a still-running
    // heartbeat could mask an illicit HDEL or overwrite by the component.
    stop.cancel();
    refreshing.await.unwrap();
    let mut rows = Vec::new();
    for node in &nodes {
        rows.push(registry.get_node(&node.id()).await.unwrap().encode());
    }
    for control in &controls {
        control.shutdown().await.unwrap();
    }
    for (node, raw) in nodes.iter().zip(rows) {
        assert_eq!(
            registry.get_node(&node.id()).await.unwrap().encode(),
            raw,
            "managed room authority cannot write/delete producer node records"
        );
    }
    for node in &nodes {
        registry.unregister_node(&node.id()).await.unwrap();
    }
    for net in nets {
        net.shutdown();
    }
}

#[test]
fn relay_preserves_nested_blob_schema_unknown_cohort() {
    let mut schema = pb::DataTrackSchemaId::default().encode_to_vec();
    schema.extend_from_slice(&[0xa0, 0x06, 7]);
    let schema = PreservedMessage::decode("livekit.DataTrackSchemaId", &schema).unwrap();
    let mut key = PreservedMessage::from_message(&pb::DataBlobKey {
        key: Some(pb::data_blob_key::Key::SchemaId(Default::default())),
    });
    key.replace_nested("schema_id", Some(&schema));
    let mut request = PreservedMessage::from_message(&pb::GetDataBlobRequest {
        request_id: 1,
        key: Some(Default::default()),
        ..Default::default()
    });
    request.replace_nested("key", Some(&key));
    let mut root = PreservedMessage::from_message(&pb::SignalRequest {
        message: Some(pb::signal_request::Message::GetDataBlobRequest(
            Default::default(),
        )),
    });
    root.replace_nested("get_data_blob_request", Some(&request));
    let batch = codec::request_batch(0, false, &[root.encode_to_vec()]).unwrap();
    let mut seq = 0;
    let raw = codec::read_batch("rpc.RelaySignalRequest", "requests", &batch, 0, &mut seq).unwrap();
    let root = PreservedMessage::decode("livekit.SignalRequest", &raw[0]).unwrap();
    let schema = root
        .nested("get_data_blob_request")
        .unwrap()
        .nested("key")
        .unwrap()
        .nested("schema_id")
        .unwrap();
    assert_eq!(schema.unknown_wire(), &[0xa0, 0x06, 7]);
}

#[tokio::test]
#[ignore = "requires LIVEKIT_TEST_REDIS_URL externally owned fixture"]
async fn real_redis_two_node_remote_join_signal_and_single_node_authority() {
    let url = std::env::var("LIVEKIT_TEST_REDIS_URL").expect("external isolated Redis URL");
    let info = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection_info()
        .clone();
    two_node_remote_join_case(crate::redis_endpoint::RedisEndpoint::simple(info), false).await;
}
#[tokio::test]
#[ignore = "requires LIVEKIT_TEST_REDIS_CLUSTER_NODES comma-separated externally owned fixtures"]
async fn real_redis_cluster_two_node_remote_join_signal_and_single_node_authority() {
    let nodes = std::env::var("LIVEKIT_TEST_REDIS_CLUSTER_NODES")
        .expect("comma-separated Cluster seed URLs");
    let infos = nodes
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(|url| {
            redis::Client::open(url)
                .unwrap()
                .get_connection_info()
                .clone()
        })
        .collect();
    two_node_remote_join_case(
        crate::redis_endpoint::RedisEndpoint::cluster(infos, 8),
        false,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires LIVEKIT_TEST_REDIS_URL externally owned fixture"]
async fn real_redis_initial_participant_store_error_still_starts_request_pump() {
    let url = std::env::var("LIVEKIT_TEST_REDIS_URL").expect("external isolated Redis URL");
    let info = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection_info()
        .clone();
    two_node_remote_join_case(crate::redis_endpoint::RedisEndpoint::simple(info), true).await;
}
#[tokio::test]
#[ignore = "requires LIVEKIT_TEST_REDIS_CLUSTER_NODES externally owned fixtures"]
async fn real_redis_cluster_initial_participant_store_error_still_starts_request_pump() {
    let infos = std::env::var("LIVEKIT_TEST_REDIS_CLUSTER_NODES")
        .unwrap()
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(|url| {
            redis::Client::open(url)
                .unwrap()
                .get_connection_info()
                .clone()
        })
        .collect();
    two_node_remote_join_case(
        crate::redis_endpoint::RedisEndpoint::cluster(infos, 8),
        true,
    )
    .await;
}
