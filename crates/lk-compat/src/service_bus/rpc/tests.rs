use super::*;
fn claim(server: &str, affinity: f32) -> pb::ClaimRequest {
    pb::ClaimRequest {
        server_id: server.into(),
        affinity,
        ..Default::default()
    }
}

#[test]
fn selection_preserves_best_bid_minimum_early_error_and_empty_outcomes() {
    let mut state = SelectionState::default();
    let options = SelectionOptions {
        minimum_affinity: 0.5,
        ..Default::default()
    };
    assert!(state.claim(claim("low", 0.1), options).is_none());
    assert!(state.claim(claim("best", 0.9), options).is_none());
    assert!(state.claim(claim("less", 0.7), options).is_none());
    state.remote_error = Some(RpcError::new("invalid_argument", "early but outbid"));
    assert_eq!(state.finish().unwrap().server, "best");
    let mut state = SelectionState::default();
    state.claim(claim("low", 0.1), options);
    assert_eq!(
        state.finish().err().unwrap().message,
        "no servers available (received 1 responses)"
    );
    assert_eq!(
        SelectionState::default().finish().err().unwrap().message,
        "no response from servers"
    );
    let state = SelectionState {
        remote_error: Some(RpcError::new("not_found", "missing")),
        ..Default::default()
    };
    assert_eq!(state.finish().err().unwrap().code, "not_found");
}

#[test]
fn selection_max_first_handling_and_zero_affinity_match_fixed_go() {
    let mut state = SelectionState::default();
    let picked = state
        .claim(
            claim("max", 1.0),
            SelectionOptions {
                maximum_affinity: 1.0,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(picked.server, "max");
    assert!(!picked.handling);
    let picked = SelectionState::default()
        .claim(
            claim("first", 0.01),
            SelectionOptions {
                accept_first: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(picked.server, "first");
    let picked = SelectionState::default()
        .claim(
            pb::ClaimRequest {
                handling: true,
                ..claim("running", 0.0)
            },
            SelectionOptions::default(),
        )
        .unwrap();
    assert!(picked.handling);
    let mut state = SelectionState::default();
    state.claim(claim("zero", 0.0), SelectionOptions::default());
    assert_eq!(state.finish().err().unwrap().code, "unavailable");
}

#[tokio::test]
async fn cancellation_observes_both_existing_and_later_notifications() {
    let cancel = Cancellation::default();
    cancel.cancel();
    tokio::time::timeout(Duration::from_millis(100), cancel.cancelled())
        .await
        .unwrap();
    let cancel = Cancellation::default();
    let copy = cancel.clone();
    let waiting = tokio::spawn(async move {
        copy.cancelled().await;
    });
    cancel.cancel();
    tokio::time::timeout(Duration::from_millis(100), waiting)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn pending_and_claim_guards_release_real_registry_entries_on_drop() {
    let pending = Arc::new(Mutex::new(HashSet::from(["pending".into()])));
    let guard = PendingGuard {
        pending: pending.clone(),
        id: "pending".into(),
    };
    drop(guard);
    assert!(pending.lock().unwrap().is_empty());
    let (tx, _) = oneshot::channel();
    let claims = Arc::new(Mutex::new(HashMap::from([("claim".into(), tx)])));
    let guard = ClaimGuard {
        claims: claims.clone(),
        id: "claim".into(),
    };
    drop(guard);
    assert!(claims.lock().unwrap().is_empty());
}

#[test]
fn remote_equal_code_and_text_cannot_impersonate_local_no_response_or_retry_origin() {
    let local = SelectionState::default().finish().err().unwrap();
    let details = vec![pbjson_types::Any {
        type_url: "fixture.invalid/Detail".into(),
        value: vec![8, 7].into(),
    }];
    let remote = RpcError::remote(pb::Response {
        code: local.code.clone(),
        error: local.message.clone(),
        error_details: details.clone(),
        ..Default::default()
    });
    assert_eq!(local.code, remote.code);
    assert_eq!(local.message, remote.message);
    assert!(local.is_local_no_response());
    assert!(!local.is_remote());
    assert!(remote.is_remote());
    assert!(!remote.is_local_no_response());
    assert_eq!(remote.details, details);
    let application = RpcError::new(&local.code, local.message.clone());
    assert_eq!(application.origin, RpcErrorOrigin::Application);
    assert!(!application.is_local_no_response());
    // A caller's local-no-responder retry branch must inspect provenance, not
    // matching an untrusted worker's identical wire text and status code.
    let retry_no_responder = |error: &RpcError| error.is_local_no_response();
    assert!(retry_no_responder(&local));
    assert!(!retry_no_responder(&remote));
    assert!(!retry_no_responder(&application));
    let state = SelectionState {
        remote_error: Some(remote.clone()),
        ..Default::default()
    };
    assert_eq!(state.finish().err().unwrap(), remote);
}

#[test]
fn local_lifecycle_and_transport_origins_preserve_existing_wire_codes_and_text() {
    for (error, origin, code, text) in [
        (
            RpcError::canceled(),
            RpcErrorOrigin::Canceled,
            "canceled",
            "request canceled",
        ),
        (
            RpcError::closed(),
            RpcErrorOrigin::Closed,
            "canceled",
            "client is closed",
        ),
        (
            RpcError::timed_out(),
            RpcErrorOrigin::Deadline,
            "deadline_exceeded",
            "request timed out",
        ),
        (
            RpcError::from(TransportError::Disconnected),
            RpcErrorOrigin::Transport,
            "unavailable",
            "Redis subscription disconnected",
        ),
    ] {
        assert_eq!(error.origin, origin);
        assert_eq!(error.code, code);
        assert_eq!(error.message, text);
        let remote = RpcError::remote(pb::Response {
            code: error.code.clone(),
            error: error.message.clone(),
            ..Default::default()
        });
        assert!(remote.is_remote());
        assert!(!remote.is_local_no_response());
        assert!(!remote.is_transport());
        assert_eq!(error.is_transport(), origin == RpcErrorOrigin::Transport);
        assert_eq!(error.details, remote.details);
    }
    assert!(RpcError::malformed_response("invalid response envelope").is_transport());
}

#[test]
fn actual_raw_request_projection_retains_noncanonical_unknown_and_known_number_wrong_wire() {
    let mut raw = lk_protocol::livekit::SipInboundTrunkInfo {
        name: "actual-known-value".into(),
        ..Default::default()
    }
    .encode_to_vec();
    raw.extend([0xa0, 6, 0x81, 0]);
    raw.extend([0x10, 7]);
    let decode: RequestDecoder<lk_protocol::livekit::SipInboundTrunkInfo> = Arc::new(|raw| {
        lk_protocol::wire::decode(raw)
            .map_err(|error| RpcError::new("malformed_request", error.to_string()))
    });
    let actual = decode_request(raw.clone(), &decode).unwrap();
    assert_eq!(actual.raw.as_ref(), raw);
    assert_eq!(actual.message.name, "actual-known-value");
    // Known projection intentionally cannot recreate the received unknown wire.
    assert_ne!(actual.message.encode_to_vec(), raw);
    let clone = actual.clone();
    assert!(Arc::ptr_eq(&actual.raw, &clone.raw));
    drop(actual);
    assert_eq!(Arc::strong_count(&clone.raw), 1);
    assert_eq!(clone.raw.as_ref(), raw);
}

#[test]
fn actual_response_wire_survives_typed_projection_and_rejects_stale_known_fields() {
    type Response = lk_protocol::livekit::SipInboundTrunkInfo;
    let mut raw = Response {
        name: "latest".into(),
        ..Default::default()
    }
    .encode_to_vec();
    raw.extend([0xb0, 6, 3, 0xa0, 6, 0x81, 0, 0x10, 7]);
    let actual: RawResponse<Response> = decode_response(raw.clone(), |raw| {
        lk_protocol::wire::decode(raw)
            .map_err(|error| RpcError::malformed_response(error.to_string()))
    })
    .unwrap();
    assert_eq!(actual.raw, raw);
    assert_eq!(actual.message.name, "latest");
    assert_eq!(encode_raw_response(actual.clone()).unwrap(), raw);
    let mut stale = actual;
    stale.message.name = "stale".into();
    let error = encode_raw_response(stale).unwrap_err();
    assert_eq!(error.origin, RpcErrorOrigin::Application);
    assert_eq!(error.code, "internal");
    assert_eq!(error.message, "raw response and typed projection disagree");
}

#[test]
fn preserved_outbound_request_validates_known_values_without_reencoding_unknown_children() {
    type Request = lk_protocol::livekit::SipInboundTrunkInfo;
    let mut raw = Request {
        name: "current".into(),
        ..Default::default()
    }
    .encode_to_vec();
    raw.extend([0xb0, 6, 3, 0xa0, 6, 0x81, 0, 0x10, 7]);
    let typed: Request = lk_protocol::wire::decode(&raw).unwrap();
    let mut request = RawRequest {
        message: typed,
        raw: raw.clone().into(),
    };
    validate_raw_request(&request).unwrap();
    assert_eq!(request.raw.as_ref(), raw);
    request.message.name = "stale".into();
    let error = validate_raw_request(&request).unwrap_err();
    assert_eq!(error.code, "invalid_argument");
    assert_eq!(error.origin, RpcErrorOrigin::Application);
    assert_eq!(error.message, "raw request and typed projection disagree");
    assert_eq!(request.raw.as_ref(), raw);
}

#[test]
fn local_application_metadata_never_becomes_peer_metadata() {
    let error = RpcError::new("invalid_argument", "metadata size exceeds limits")
        .with_application_metadata("argument", "max");
    assert_eq!(
        error
            .application_metadata
            .get("argument")
            .map(String::as_str),
        Some("max")
    );
    assert_eq!(error.origin, RpcErrorOrigin::Application);
    let peer = RpcError::remote(pb::Response {
        code: error.code.clone(),
        error: error.message.clone(),
        error_details: error.details.clone(),
        ..Default::default()
    });
    assert!(peer.application_metadata.is_empty());
    assert_eq!(peer.code, error.code);
    assert_eq!(peer.message, error.message);
    assert_eq!(peer.origin, RpcErrorOrigin::Remote);
}

/// An explicit dedicated Redis fixture is required; ordinary unit tests never
/// start external services. The coordinator runs this against a private broker.
#[tokio::test]
#[ignore = "requires dedicated IO_RPC_TEST_REDIS_URL Redis broker"]
async fn actual_redis_shutdown_drains_held_handler_while_close_kills() {
    use super::super::redis_transport::TransportLimits;
    let url = std::env::var("IO_RPC_TEST_REDIS_URL").expect("dedicated broker URL");
    let bus = RedisTransport::connect(
        &url,
        TransportLimits {
            wire: wire::WireLimits {
                encoded_bytes: 1024 * 1024,
                decompressed_bytes: 1024 * 1024,
            },
            compression: wire::CompressionPolicy::default(),
            inbox_messages: 64,
            subscriptions: 32,
            commands: 16,
            io_timeout: Duration::from_secs(2),
        },
    )
    .await
    .unwrap();
    for kill in [false, true] {
        let spec = MethodSpec {
            service: "HeldShutdownFixture".into(),
            method: "Call".into(),
            topics: vec![rtc_core::ids::new_sid("run_")],
            require_claim: true,
            queue: false,
        };
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server = RpcServer::register::<String, String, _, _>(
            bus.clone(),
            spec.clone(),
            ServerLimits {
                concurrent: 1,
                max_request_duration: Duration::from_secs(5),
                allow_skip_claim: false,
            },
            {
                let started = started.clone();
                let release = release.clone();
                let completed = completed.clone();
                move |_, value| {
                    let started = started.clone();
                    let release = release.clone();
                    let completed = completed.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        completed.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(value)
                    }
                }
            },
            None,
        )
        .await
        .unwrap();
        let client = Arc::new(RpcClient::new(bus.clone(), 2).unwrap());
        let call = tokio::spawn({
            let client = client.clone();
            async move {
                let mut options = CallOptions::new(Duration::from_secs(2));
                options.selection.accept_first = true;
                client
                    .unary::<String, String>(&spec, &"actual held request".to_owned(), options)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(3), started.notified())
            .await
            .unwrap();
        if kill {
            tokio::time::timeout(Duration::from_secs(1), server.close())
                .await
                .unwrap();
            assert!(!completed.load(std::sync::atomic::Ordering::SeqCst));
            client.close();
            assert!(call.await.unwrap().is_err());
        } else {
            let mut shutdown = tokio::spawn(server.shutdown());
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
                    .await
                    .is_err()
            );
            release.notify_one();
            assert_eq!(call.await.unwrap().unwrap(), "actual held request");
            tokio::time::timeout(Duration::from_secs(1), shutdown)
                .await
                .unwrap()
                .unwrap();
            assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
        }
    }
}

#[test]
fn inherited_absolute_expiry_is_never_extended_by_clock_conversion() {
    let now = Instant::now();
    let original = 10_000_000_000i64;
    // Exact deterministic reproduction of positive wall/mono conversion drift.
    assert_eq!(
        wire_expiry_ns(
            now,
            9_000_000_001,
            now + Duration::from_secs(1),
            Some(original)
        ),
        original
    );
    // A shorter child timeout still wins; a past inherited expiry stays past.
    assert_eq!(
        wire_expiry_ns(
            now,
            9_000_000_000,
            now + Duration::from_millis(500),
            Some(original)
        ),
        9_500_000_000
    );
    assert_eq!(
        wire_expiry_ns(
            now,
            original,
            now + Duration::from_secs(1),
            Some(original - 1)
        ),
        original - 1
    );
    assert_eq!(
        wire_expiry_ns(now, 9_000_000_000, now + Duration::from_secs(1), None),
        original
    );
}

#[test]
fn inherited_selected_deadline_uses_original_wire_value_across_retry_clock_samples() {
    let now = Instant::now();
    let parent_deadline = now + Duration::from_secs(1);
    let parent_expiry = 10_000_000_000;
    for jitter in [-200, -1, 0, 1, 200] {
        assert_eq!(
            call_expiry_ns(
                now,
                9_000_000_000 + jitter,
                now + Duration::from_secs(2),
                Some(parent_deadline),
                Some(parent_expiry)
            ),
            parent_expiry
        );
    }
    // A truly shorter child timeout must still win, not gain its parent's time.
    assert_eq!(
        call_expiry_ns(
            now,
            9_000_000_000,
            now + Duration::from_millis(500),
            Some(parent_deadline),
            Some(parent_expiry)
        ),
        9_500_000_000
    );
    // A raw upper bound without a paired monotonic deadline remains a cap.
    assert_eq!(
        call_expiry_ns(
            now,
            9_000_000_001,
            now + Duration::from_secs(1),
            None,
            Some(parent_expiry)
        ),
        parent_expiry
    );
    // Already expired inherited context is never converted back into a future.
    assert_eq!(
        call_expiry_ns(
            now,
            9_000_000_000,
            now + Duration::from_secs(1),
            Some(now),
            Some(8_999_999_999)
        ),
        8_999_999_999
    );
}

#[test]
fn caller_monotonic_deadline_without_wire_cap_still_limits_published_expiry() {
    let now = Instant::now();
    let wall = 9_000_000_000;
    let timeout = now + Duration::from_secs(1);
    assert_eq!(
        call_expiry_ns(
            now,
            wall,
            timeout,
            Some(now + Duration::from_millis(100)),
            None
        ),
        9_100_000_000
    );
    assert_eq!(
        call_expiry_ns(now, wall, timeout, Some(now + Duration::from_secs(2)), None),
        10_000_000_000
    );
    assert_eq!(
        call_expiry_ns(now, wall, timeout, None, None),
        10_000_000_000
    );
    assert_eq!(call_expiry_ns(now, wall, timeout, Some(now), None), wall);
}
