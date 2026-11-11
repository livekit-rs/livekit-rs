//! Real cross-entry consumer proof; the external worker acknowledges STARTING
//! only. This fixture does not claim that media recording has occurred.
use super::*;
use crate::service_bus::rpc::{MethodSpec, RpcServer};
use lk_protocol::{livekit as pb, rpc as internal};
pub(crate) async fn remote_consumers(
    entry: &crate::state::AppState,
    owner: &crate::state::AppState,
    endpoint: crate::redis_endpoint::RedisEndpoint,
    room: &str,
    identity: &str,
) {
    let control = entry.room_control.as_ref().unwrap();
    let services = WorkerServices::connect(
        WorkerConfig {
            redis: endpoint.clone(),
            rpc: crate::workers::retry::RpcPolicy::default(),
            compression: CompressionPolicy::default(),
            max_decompressed_size: 1 << 20,
            ingress: crate::workers::ingress::IngressConfig::default(),
            ingress_legacy_psrpc: false,
        },
        entry.manager.clone(),
        None,
        Arc::new(NoRecordingNotifications),
    )
    .await
    .unwrap();
    services.bind_room_control(control).unwrap();
    let expected_room = owner
        .manager
        .list_rooms(&ProjectId::local(), &[RoomName(room.into())])[0]
        .sid
        .0
        .clone();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let handler = RpcServer::register::<internal::StartEgressRequest, pb::EgressInfo, _, _>(
        services.message_bus(),
        MethodSpec {
            service: "EgressInternal".into(),
            method: "StartEgress".into(),
            topics: vec![String::new()],
            require_claim: true,
            queue: false,
        },
        ServerLimits {
            concurrent: 1,
            // Original Egress NewEgressInternalServer installs observability,
            // not an extra handler-lifetime cap. Its caller's Start timeout is
            // at least 10s even when ordinary CallOptions was constructed at3s.
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        },
        move |context, request| {
            let sink = sink.clone();
            async move {
                assert!(
                    context.expiry_ns.saturating_sub(context.sent_at_ns) > 5_000_000_000,
                    "real Egress minimum Start budget must reach the worker"
                );
                sink.lock()
                    .unwrap()
                    .push((request.room_id.clone(), request.egress_id.clone()));
                let room_name = match request.request {
                    Some(internal::start_egress_request::Request::RoomComposite(value)) => {
                        value.room_name
                    }
                    _ => {
                        return Err(RpcError::new(
                            "invalid_argument",
                            "expected real composite request",
                        ));
                    }
                };
                Ok(pb::EgressInfo {
                    egress_id: request.egress_id,
                    room_id: request.room_id,
                    room_name,
                    status: pb::EgressStatus::EgressStarting as i32,
                    ..Default::default()
                })
            }
        },
        Some(Arc::new(|_| 1.0)),
    )
    .await
    .unwrap();
    let mut auth = crate::auth::VerifiedToken::default();
    auth.grants.video.room_record = true;
    let started = services
        .egress
        .start(
            Some(&auth),
            internal::StartEgressRequest {
                egress_id: identity.into(),
                request: Some(internal::start_egress_request::Request::RoomComposite(
                    pb::RoomCompositeEgressRequest {
                        room_name: room.into(),
                        audio_only: true,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
            CallOptions::new(Duration::from_secs(3)),
        )
        .await
        .unwrap();
    assert_eq!(started.room_id, expected_room);
    assert_eq!(started.status, pb::EgressStatus::EgressStarting as i32);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[(expected_room, identity.to_owned())]
    );
    // The actual shared egress record now drives the full layout consumer,
    // which must use fixed Participant.UpdateParticipant on the remote actor.
    services
        .egress
        .update_layout(
            Some(&auth),
            pb::UpdateLayoutRequest {
                egress_id: identity.into(),
                layout: "grid".into(),
            },
        )
        .await
        .unwrap();
    let live = owner
        .manager
        .get_participant(&owner.room_key(room), &Identity(identity.into()))
        .await
        .unwrap();
    assert_eq!(live.metadata, r#"{"layout":"grid"}"#);
    assert!(!entry.manager.room_exists(&entry.room_key(room)));
    let read = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match services.rooms.participant(room, identity).await {
                Ok(value) if value.metadata == live.metadata => break value,
                Ok(_) => {}
                Err(error) if error.code == "not_found" => {}
                Err(error) => panic!("actual remote SIP participant lookup: {error:?}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual cached owner projection reaches SIP room consumer");
    assert_eq!(read.sid, live.sid.0);
    assert_eq!(read.metadata, live.metadata);
    let mut admin = crate::auth::VerifiedToken::default();
    admin.grants.video.room_admin = true;
    admin.grants.video.room = room.into();
    use prost::Message;
    let public = crate::twirp::room_service::call(
        entry,
        Some(&admin),
        "UpdateParticipant",
        crate::twirp::codec::Codec::Protobuf,
        &pb::UpdateParticipantRequest {
            room: room.into(),
            identity: identity.into(),
            metadata: "public-remote".into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert!(public.status().is_success());
    assert_eq!(
        owner
            .manager
            .get_participant(&owner.room_key(room), &Identity(identity.into()))
            .await
            .unwrap()
            .metadata,
        "public-remote"
    );
    // Public entry is non-owner: these positive replies require the actual
    // fixed remote participant/room subscribers, not a local manager shortcut.
    let call = |method: &'static str, body: Vec<u8>| {
        let admin = admin.clone();
        async move {
            crate::twirp::room_service::call(
                entry,
                Some(&admin),
                method,
                crate::twirp::codec::Codec::Protobuf,
                &body,
            )
            .await
        }
    };
    let muted = call(
        "MutePublishedTrack",
        pb::MuteRoomTrackRequest {
            room: room.into(),
            identity: identity.into(),
            track_sid: "TR_missing-fixture".into(),
            muted: true,
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert!(
        muted.status().is_success(),
        "fixed owner returns nil track for unknown track, not missing local participant"
    );
    let updated = call(
        "UpdateSubscriptions",
        pb::UpdateSubscriptionsRequest {
            room: room.into(),
            identity: identity.into(),
            subscribe: true,
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert!(updated.status().is_success());
    let sent = call(
        "SendData",
        pb::SendDataRequest {
            room: room.into(),
            data: b"cross-entry-public-data".to_vec(),
            destination_identities: vec![identity.into()],
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert!(sent.status().is_success());
    let removed = call(
        "RemoveParticipant",
        pb::RoomParticipantIdentity {
            room: room.into(),
            identity: identity.into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert!(removed.status().is_success());
    assert!(
        owner
            .manager
            .get_participant(&owner.room_key(room), &Identity(identity.into()))
            .await
            .is_err(),
        "actual owner actor removed the participant"
    );
    handler.shutdown().await;
    services.shutdown().await;
    let connection = endpoint
        .connect(&redis::AsyncConnectionConfig::new())
        .await
        .unwrap();
    let mut connection = connection.begin();
    // Delete only the external fixture's unique egress rows, never shared keys.
    let _: usize = redis::cmd("HDEL")
        .arg("egress")
        .arg(identity)
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: usize = redis::cmd("SREM")
        .arg(format!("egress:room:{room}"))
        .arg(identity)
        .query_async(&mut connection)
        .await
        .unwrap();
}
