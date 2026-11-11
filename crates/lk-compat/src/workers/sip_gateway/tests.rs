use super::*;
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Rooms(AtomicUsize);
impl SipRooms for Rooms {
    fn participant(
        &self,
        _: &str,
        _: &str,
    ) -> BoxFuture<'_, Result<pb::ParticipantInfo, RpcError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(RpcError::new("not_found", "participant does not exist")) })
    }
}
fn token() -> VerifiedToken {
    let mut token = VerifiedToken::default();
    token.extra.sip.call = true;
    token.grants.video.room_admin = true;
    token.grants.video.room = "room".into();
    token
}
fn request() -> WireValue<pb::CreateSipParticipantRequest> {
    let mut req = pb::CreateSipParticipantRequest {
        sip_number: "123".into(),
        sip_call_to: "456".into(),
        room_name: "room".into(),
        ..Default::default()
    };
    upgrade(&mut req);
    WireValue::from_message(req)
}
#[test]
fn create_mapping_uses_real_trunk_fields_random_choice_and_protected_overwrites() {
    let mut req = request();
    req.message.sip_number.clear();
    req.message.sip_trunk_id = "ST_fixture".into();
    req.message.krisp_enabled = true;
    req.message
        .participant_attributes
        .insert("sip.callID".into(), "spoof".into());
    req.message
        .headers
        .insert("X-Test".into(), "request".into());
    req.message.display_name = Some(String::new());
    let trunk = Stored::typed(pb::SipOutboundTrunkInfo {
        sip_trunk_id: "ST_fixture".into(),
        address: "fixture.twilio.com".into(),
        from_host: "from.example".into(),
        numbers: vec!["111".into(), "222".into()],
        auth_username: "fixture-user".into(),
        auth_password: "fixture-secret".into(),
        include_headers: 999,
        headers: [("X-Test".into(), "trunk".into())].into(),
        media: Some(pb::SipMediaConfig {
            encryption: Some(2),
            ..Default::default()
        }),
        ..Default::default()
    });
    let out = map_create(&req, Some(&trunk), "SCL_fixture".into(), |n| {
        assert_eq!(n, 2);
        Ok(1)
    })
    .unwrap()
    .message;
    assert_eq!(out.number, "+222");
    assert_eq!(out.hostname, "from.example");
    assert_eq!(out.username, "fixture-user");
    assert_eq!(out.password, "fixture-secret");
    assert_eq!(out.headers["X-Test"], "request");
    assert_eq!(out.include_headers, 999);
    assert_eq!(out.participant_attributes["sip.callID"], "SCL_fixture");
    assert_eq!(out.participant_attributes["sip.trunkID"], "ST_fixture");
    assert_eq!(out.participant_attributes["sip.trunkPhoneNumber"], "+222");
    assert_eq!(out.participant_identity, "sip_456");
    assert_eq!(out.enabled_features, [1]);
    assert_eq!(out.display_name, Some(String::new()));
    assert_eq!(out.media_encryption, 2);
    assert_eq!(out.media.unwrap().encryption, Some(2));
}
#[test]
fn hidden_source_semantics_do_not_delete_user_attributes_and_inline_is_not_catalog() {
    let mut req = request();
    req.message.hide_phone_number = true;
    req.message
        .participant_attributes
        .insert("sip.phoneNumber".into(), "caller-owned".into());
    req.message.trunk = Some(pb::SipOutboundConfig {
        hostname: "destination.example".into(),
        from_host: "from.example".into(),
        ..Default::default()
    });
    let out = map_create(&req, None, "SCL_fixture".into(), |_| {
        panic!("explicit number")
    })
    .unwrap()
    .message;
    assert_eq!(
        out.participant_attributes["sip.phoneNumber"],
        "caller-owned"
    );
    assert!(!out.participant_attributes.contains_key("sip.hostname"));
    assert_eq!(out.participant_identity, "sip_456");
    req.message.sip_number.clear();
    let err = map_create(&req, None, "SCL_fixture".into(), |_| {
        panic!("no trunk numbers")
    })
    .unwrap_err();
    assert_eq!(
        (err.code.as_str(), err.message.as_str()),
        ("failed_precondition", "no numbers on outbound trunk")
    );
}
#[test]
fn copy_only_true_child_unknowns_never_cross_type_root() {
    let req = pb::CreateSipParticipantRequest {
        sip_number: "123".into(),
        sip_call_to: "456".into(),
        room_name: "room".into(),
        media: Some(pb::SipMediaConfig::default()),
        ..Default::default()
    };
    let marker = [0xc0, 0xa3, 0x09, 0x2a];
    let mut root = req.encode_to_vec();
    root.extend_from_slice(&marker);
    let mut value = WireValue::<pb::CreateSipParticipantRequest>::decode(&root).unwrap();
    let mut media = pb::SipMediaConfig::default().encode_to_vec();
    media.extend_from_slice(&marker);
    let source = PreservedMessage::decode("livekit.SIPMediaConfig", &media).unwrap();
    value.wire.replace_nested("media", Some(&source));
    let out = map_create(&value, None, "SCL_fixture".into(), |_| panic!()).unwrap();
    assert!(
        out.wire
            .nested("media")
            .unwrap()
            .encode_to_vec()
            .ends_with(&marker)
    );
    let without_media = out.message.clone();
    let mut projected = out.wire.clone();
    projected.replace_nested("media", None);
    let mut typed = without_media;
    typed.media = None;
    assert!(
        !projected
            .overlay(&typed)
            .encode_to_vec()
            .windows(4)
            .any(|x| x == marker)
    );
}
#[test]
fn preflight_auth_is_not_uniform_and_grants_exact_room() {
    let mut req = pb::TransferSipParticipantRequest::default();
    assert_eq!(
        transfer_preflight(None, &req).unwrap_err().message,
        "Missing room name"
    );
    req.room_name = "room".into();
    assert_eq!(
        transfer_preflight(None, &req).unwrap_err().message,
        "Missing participant identity"
    );
    req.participant_identity = "person".into();
    assert_eq!(
        transfer_preflight(None, &req).unwrap_err().code,
        "unauthenticated"
    );
    let mut auth = token();
    transfer_preflight(Some(&auth), &req).unwrap();
    auth.grants.video.room = "other".into();
    assert!(transfer_preflight(Some(&auth), &req).is_err());
    auth.extra.sip.call = false;
    assert!(call_permission(Some(&auth)).is_err());
}
#[tokio::test]
async fn disconnected_create_checks_grant_then_store_before_request_validation() {
    let rooms = Arc::new(Rooms(AtomicUsize::new(0)));
    let gateway = SipGateway::new(None, None, rooms.clone());
    let error = gateway
        .create_wire(
            None,
            WireValue::from_message(Default::default()),
            CallOptions::new(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "unauthenticated");
    let error = gateway
        .create_wire(
            Some(&token()),
            WireValue::from_message(Default::default()),
            CallOptions::new(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.message, "sip not connected (redis required)");
    assert_eq!(rooms.0.load(Ordering::SeqCst), 0);
}
#[test]
fn timeout_call_policy_no_generic_three_second_override_or_retry() {
    let now = Instant::now();
    let mut options = CallOptions::new(Duration::from_secs(3));
    assert_eq!(
        resolve_create_timeout(&mut options, Duration::from_secs(80), now).unwrap(),
        80_000_000_000
    );
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.deadline = now.checked_add(Duration::from_secs(90));
    assert_eq!(
        resolve_create_timeout(&mut options, Duration::from_secs(30), now).unwrap(),
        90_000_000_000
    );
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.deadline = now.checked_add(Duration::from_secs(10));
    assert_eq!(
        resolve_transfer_timeout(&mut options, 30_000_000_000, now).unwrap(),
        10_000_000_000
    );
    for nanos in [0, -1, -30_000_000_000] {
        let mut options = CallOptions::new(Duration::from_secs(3));
        assert_eq!(
            resolve_transfer_timeout(&mut options, nanos, now)
                .unwrap_err()
                .code,
            "deadline_exceeded"
        );
    }
    let mut options = CallOptions::new(Duration::from_secs(3));
    options.cancel.cancel();
    assert_eq!(
        resolve_create_timeout(&mut options, Duration::from_secs(30), now)
            .unwrap_err()
            .code,
        "canceled"
    );
    let create = method("CreateSIPParticipant", "");
    assert!(create.require_claim && create.queue);
    assert_eq!(create.topics, [""]);
    let transfer = method("TransferSIPParticipant", "SCL_fixture");
    assert!(transfer.require_claim && transfer.queue);
    assert_eq!(transfer.topics, ["SCL_fixture"]);
}
#[tokio::test]
#[ignore = "requires coordinator-owned SIP_GATEWAY_REDIS_URL isolated Redis; not a SIP/RTC data-plane test"]
async fn actual_redis_claim_queue_routes_no_retry_and_no_fabricated_success() {
    use crate::service_bus::{
        redis_transport::{RedisTransport, TransportLimits},
        rpc::{RpcServer, ServerLimits},
        wire::{CompressionPolicy, WireLimits},
    };
    use std::sync::Mutex;
    assert_eq!(
        std::env::var("SIP_GATEWAY_REDIS_ISOLATED").as_deref(),
        Ok("1")
    );
    let url = std::env::var("SIP_GATEWAY_REDIS_URL").expect("coordinator isolated broker");
    let limits = TransportLimits {
        wire: WireLimits {
            encoded_bytes: 1 << 20,
            decompressed_bytes: 1 << 20,
        },
        compression: CompressionPolicy::default(),
        inbox_messages: 100,
        subscriptions: 32,
        commands: 32,
        io_timeout: Duration::from_secs(2),
    };
    let bus = RedisTransport::connect(&url, limits).await.unwrap();
    let client = Arc::new(RpcClient::new(bus.clone(), 8).unwrap());
    let captured = Arc::new(Mutex::new(Vec::new()));
    let seen = captured.clone();
    let server = RpcServer::register::<
        rpc::InternalCreateSipParticipantRequest,
        rpc::InternalCreateSipParticipantResponse,
        _,
        _,
    >(
        bus,
        method("CreateSIPParticipant", ""),
        ServerLimits {
            concurrent: 4,
            max_request_duration: Duration::from_secs(100),
            allow_skip_claim: false,
        },
        move |_, req| {
            seen.lock().unwrap().push(req.clone());
            async move {
                if req.call_to == "error" {
                    return Err(RpcError::new(
                        "unavailable",
                        "fixture actual worker rejection",
                    ));
                }
                Ok(rpc::InternalCreateSipParticipantResponse {
                    participant_id: "PA_worker".into(),
                    participant_identity: "worker-identity".into(),
                    sip_call_id: "worker-must-not-override-callid".into(),
                })
            }
        },
        None,
    )
    .await
    .unwrap();
    let catalog = SipCatalog::connect(&url, Duration::from_secs(2), 8)
        .await
        .unwrap();
    let gateway = SipGateway::new(
        Some(client),
        Some(catalog),
        Arc::new(Rooms(AtomicUsize::new(0))),
    );
    let response = gateway
        .create_wire(Some(&token()), request(), {
            let mut options = CallOptions::new(Duration::from_secs(80));
            options.deadline = Some(Instant::now() + Duration::from_secs(2));
            options
        })
        .await
        .unwrap();
    assert_eq!(response.message.participant_id, "PA_worker");
    assert_eq!(response.message.participant_identity, "worker-identity");
    assert_eq!(response.message.room_name, "room");
    assert!(response.message.sip_call_id.starts_with("SCL_"));
    assert_eq!(
        response.message.sip_call_id,
        captured.lock().unwrap()[0].sip_call_id
    );
    let mut rejected = request();
    rejected.message.sip_call_to = "error".into();
    let error = gateway
        .create_wire(Some(&token()), rejected, {
            let mut options = CallOptions::new(Duration::from_secs(80));
            options.deadline = Some(Instant::now() + Duration::from_secs(2));
            options
        })
        .await
        .unwrap_err();
    assert_eq!(error.message, "fixture actual worker rejection");
    assert_eq!(error.code, "unavailable");
    assert_eq!(
        captured.lock().unwrap().len(),
        2,
        "one external call per request, never generic retry"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn actual_held_room_lookup_is_bound_by_caller_cancel_and_deadline() {
    struct Held(Arc<tokio::sync::Notify>);
    impl SipRooms for Held {
        fn participant(
            &self,
            _: &str,
            _: &str,
        ) -> BoxFuture<'_, Result<pb::ParticipantInfo, RpcError>> {
            let started = self.0.clone();
            Box::pin(async move {
                started.notify_one();
                std::future::pending().await
            })
        }
    }
    for cancel in [true, false] {
        let started = Arc::new(tokio::sync::Notify::new());
        let gateway = SipGateway::new(None, None, Arc::new(Held(started.clone())));
        let mut options = CallOptions::new(Duration::from_secs(80));
        if !cancel {
            options.deadline = Some(Instant::now() + Duration::from_millis(50));
        }
        let cancel_token = options.cancel.clone();
        let task = tokio::spawn(async move {
            let auth = token();
            gateway
                .transfer_wire(
                    Some(&auth),
                    WireValue::from_message(pb::TransferSipParticipantRequest {
                        room_name: "room".into(),
                        participant_identity: "held".into(),
                        transfer_to: "sip:target@example.invalid".into(),
                        ..Default::default()
                    }),
                    options,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        if cancel {
            cancel_token.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.origin,
            if cancel {
                RpcErrorOrigin::Canceled
            } else {
                RpcErrorOrigin::Deadline
            }
        );
        assert_eq!(
            error.message,
            if cancel {
                "context canceled"
            } else {
                "context deadline exceeded"
            }
        );
    }
}

#[tokio::test]
async fn assembled_sip_client_delivers_burst_independent_of_generic_handler_buffer() {
    use crate::service_bus::{channels, wire::CompressionPolicy};
    use crate::worker_services::{NoRecordingNotifications, WorkerPolicy, WorkerServices};
    use crate::workers::{ingress::IngressConfig, retry::RpcPolicy};
    use lk_protocol::psrpc::internal::Response;

    let (state, net) = crate::test_support::state().await;
    let services = WorkerServices::connect_local(
        WorkerPolicy {
            rpc: RpcPolicy::from_raw(3, 3_000_000_000, 2_000_000_000, 1),
            compression: CompressionPolicy {
                quality: 0,
                threshold: 1024,
            },
            max_decompressed_size: isize::MAX as usize - 1,
            ingress: IngressConfig::default(),
            ingress_legacy_psrpc: false,
        },
        state.manager().clone(),
        None,
        Arc::new(NoRecordingNotifications),
    )
    .await
    .unwrap();
    let sender = services.message_bus();
    assert_eq!(
        sender.limits().inbox_messages,
        1,
        "generic worker configuration remains active"
    );
    let sip = services.sip.client.as_ref().unwrap().message_bus();
    let channel = channels::response("SIPInternal", "actual_sip_response_burst_fixture");
    let mut subscription = sip.subscribe::<Response>(&channel, false).await.unwrap();
    let received_burst = tokio::time::timeout(Duration::from_secs(1), async {
        // No SIP consumer drains the subscription during this real bus burst.
        // A generic size-one view stalls after the local raw and typed queues
        // fill; native SIP size 100 admits and retains the entire burst.
        for n in 0..8 {
            let response = Response {
                request_id: format!("response-{n}"),
                ..Default::default()
            };
            assert_eq!(sender.publish(&channel, &response).await.unwrap(), 1);
        }
    })
    .await;
    if received_burst.is_ok() {
        for n in 0..8 {
            let bytes = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
                .await
                .unwrap()
                .unwrap();
            let actual = sip.decode::<Response>(&bytes).unwrap().message;
            assert_eq!(actual.request_id, format!("response-{n}"));
        }
    }
    subscription.unsubscribe(&channel).await.unwrap();
    services.shutdown().await;
    state.agents().shutdown().await;
    net.shutdown();
    assert!(
        received_burst.is_ok(),
        "actual assembled SIP client could not admit the burst without draining"
    );
    assert_eq!(sip.limits().inbox_messages, 100);
}
