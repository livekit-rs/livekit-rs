//! Supplemental LiveKit services. Agent dispatch CRUD is owned by the actual
//! runtime room; external worker calls use the genuine worker service graph.
//! Worker execution is separate from dispatch CRUD and is never fabricated.

pub mod agent_dispatch;
pub mod egress;
pub mod ingress_api;
pub mod sip_calls;

use axum::response::Response;

use super::codec::{Codec, decode};
use super::error::TwirpError;
use crate::auth::VerifiedToken;
use crate::auth::permissions::*;
use lk_protocol::livekit as pb;

pub const AGENT_DISPATCH_SERVICE: &str = "livekit.AgentDispatchService";
pub const EGRESS_SERVICE: &str = "livekit.Egress";
pub const INGRESS_SERVICE: &str = "livekit.Ingress";
pub const SIP_SERVICE: &str = "livekit.SIP";

pub const AGENT_DISPATCH_METHODS: &[&str] = &["CreateDispatch", "DeleteDispatch", "ListDispatch"];

// One registry owns both public routes and their pinned request types. A route
// cannot be registered without also receiving real protobuf/protojson decoding.
macro_rules! typed_methods {
    ($registry:ident, $decode:ident, {$($method:literal => $request:ident),+ $(,)?}) => {
        pub const $registry: &[&str] = &[$($method),+];
        fn $decode(method: &str, codec: Codec, body: &[u8]) -> Result<(), TwirpError> {
            match method {
                $($method => { let _: pb::$request = decode(codec, body)?; Ok(()) }),+
                _ => Err(TwirpError::new(super::error::Code::BadRoute, "no handler for method")),
            }
        }
    };
}

typed_methods!(EGRESS_METHODS, decode_egress, {
    "StartEgress" => StartEgressRequest,
    "UpdateLayout" => UpdateLayoutRequest,
    "UpdateStream" => UpdateStreamRequest,
    "ListEgress" => ListEgressRequest,
    "StopEgress" => StopEgressRequest,
    "StartRoomCompositeEgress" => RoomCompositeEgressRequest,
    "StartWebEgress" => WebEgressRequest,
    "StartParticipantEgress" => ParticipantEgressRequest,
    "StartTrackCompositeEgress" => TrackCompositeEgressRequest,
    "StartTrackEgress" => TrackEgressRequest,
});

typed_methods!(INGRESS_METHODS, decode_ingress, {
    "CreateIngress" => CreateIngressRequest,
    "UpdateIngress" => UpdateIngressRequest,
    "ListIngress" => ListIngressRequest,
    "DeleteIngress" => DeleteIngressRequest,
});

typed_methods!(SIP_METHODS, decode_sip, {
    "ListSIPTrunk" => ListSipTrunkRequest,
    "CreateSIPInboundTrunk" => CreateSipInboundTrunkRequest,
    "CreateSIPOutboundTrunk" => CreateSipOutboundTrunkRequest,
    "UpdateSIPInboundTrunk" => UpdateSipInboundTrunkRequest,
    "UpdateSIPOutboundTrunk" => UpdateSipOutboundTrunkRequest,
    "GetSIPInboundTrunk" => GetSipInboundTrunkRequest,
    "GetSIPOutboundTrunk" => GetSipOutboundTrunkRequest,
    "ListSIPInboundTrunk" => ListSipInboundTrunkRequest,
    "ListSIPOutboundTrunk" => ListSipOutboundTrunkRequest,
    "DeleteSIPTrunk" => DeleteSipTrunkRequest,
    "CreateSIPDispatchRule" => CreateSipDispatchRuleRequest,
    "UpdateSIPDispatchRule" => UpdateSipDispatchRuleRequest,
    "ListSIPDispatchRule" => ListSipDispatchRuleRequest,
    "DeleteSIPDispatchRule" => DeleteSipDispatchRuleRequest,
    "CreateSIPParticipant" => CreateSipParticipantRequest,
    "TransferSIPParticipant" => TransferSipParticipantRequest,
});

type Auth<'a> = Option<&'a VerifiedToken>;

pub async fn agent_dispatch(
    state: &crate::state::AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> Result<Response, TwirpError> {
    agent_dispatch::call(state, auth, method, codec, body).await
}

#[cfg(test)]
pub async fn egress(
    state: &crate::state::AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> Result<Response, TwirpError> {
    egress_with_deadline(state, auth, method, codec, body, None).await
}

pub async fn egress_with_deadline(
    state: &crate::state::AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
    deadline: Option<std::time::Instant>,
) -> Result<Response, TwirpError> {
    decode_egress(method, codec, body)?;
    ensure_record(auth)?;
    egress::call(state, auth, method, codec, body, deadline).await
}

#[cfg(test)]
fn ingress(
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> Result<Response, TwirpError> {
    decode_ingress(method, codec, body)?;
    ensure_ingress_admin(auth)?;
    Err(TwirpError::internal(
        "ingress not connected (redis required)",
    ))
}

pub async fn ingress_with_deadline(
    state: &crate::state::AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
    deadline: Option<std::time::Instant>,
) -> Result<Response, TwirpError> {
    decode_ingress(method, codec, body)?;
    ingress_api::call(state, auth, method, codec, body, deadline).await
}

pub async fn sip(
    state: &crate::state::AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> Result<Response, TwirpError> {
    sip_with_deadline(state, auth, method, codec, body, None).await
}
pub async fn sip_with_deadline(
    state: &crate::state::AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
    deadline: Option<std::time::Instant>,
) -> Result<Response, TwirpError> {
    if matches!(method, "CreateSIPParticipant" | "TransferSIPParticipant") {
        sip_calls::call(state, auth, method, codec, body, deadline).await
    } else {
        decode_sip(method, codec, body)?;
        crate::sip::api::call(state.sip_catalog.as_deref(), auth, method, codec, body).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::Code;
    use super::*;
    use crate::test_support::state as test_state;

    #[tokio::test]
    async fn redis_services() {
        let (state, net) = test_state().await;
        let mut t = VerifiedToken::default();
        t.grants.video.room_record = true;
        let e = egress(&state, Some(&t), "StartEgress", Codec::Json, b"{}")
            .await
            .unwrap_err();
        assert_eq!(
            (e.code, e.msg.as_str()),
            (Code::Internal, "egress not connected (redis required)")
        );
        assert_eq!(
            ingress(Some(&t), "CreateIngress", Codec::Json, b"{}")
                .unwrap_err()
                .code,
            Code::Unauthenticated
        );
        t.extra.sip.call = true;
        assert_eq!(
            sip(&state, Some(&t), "ListSIPTrunk", Codec::Json, b"{}")
                .await
                .unwrap_err()
                .code,
            Code::Unauthenticated
        );
        assert_eq!(
            sip(&state, Some(&t), "CreateSIPParticipant", Codec::Json, b"{}")
                .await
                .unwrap_err()
                .msg,
            "sip not connected (redis required)"
        );
        assert_eq!(
            egress(&state, Some(&t), "StartEgress", Codec::Json, b"")
                .await
                .unwrap_err()
                .code,
            Code::Malformed
        );
        net.shutdown();
    }

    #[derive(Clone, Copy)]
    enum Service {
        Egress,
        Ingress,
        Sip,
    }
    impl Service {
        async fn call(
            self,
            state: &crate::state::AppState,
            auth: Auth<'_>,
            method: &str,
            codec: Codec,
            body: &[u8],
        ) -> Result<Response, TwirpError> {
            match self {
                Self::Egress => egress(state, auth, method, codec, body).await,
                Self::Ingress => ingress(auth, method, codec, body),
                Self::Sip => sip(state, auth, method, codec, body).await,
            }
        }
    }

    #[tokio::test]
    async fn all_thirty_routes_decode_their_request_before_service_auth() {
        let (state, net) = test_state().await;
        let mut auth = VerifiedToken::default();
        auth.grants.video.room_record = true;
        auth.grants.video.ingress_admin = true;
        auth.extra.sip.admin = true;
        auth.extra.sip.call = true;
        let services: &[(&[&str], Service, &str)] = &[
            (EGRESS_METHODS, Service::Egress, "egress"),
            (INGRESS_METHODS, Service::Ingress, "ingress"),
            (SIP_METHODS, Service::Sip, "sip"),
        ];
        assert_eq!(services.iter().map(|(m, _, _)| m.len()).sum::<usize>(), 30);
        for &(methods, call, label) in services {
            for method in methods {
                // Transfer has explicit missing-field preflight before grants.
                let empty_code = if *method == "TransferSIPParticipant" {
                    Code::InvalidArgument
                } else {
                    Code::Unauthenticated
                };
                // A valid empty message must reach method-specific preflight,
                // auth or backend, never success.
                for (codec, body) in [(Codec::Json, &b"{}"[..]), (Codec::Protobuf, &b""[..])] {
                    assert_eq!(
                        call.call(&state, None, method, codec, body)
                            .await
                            .unwrap_err()
                            .code,
                        empty_code,
                        "{method}"
                    );
                    let error = call
                        .call(&state, Some(&auth), method, codec, body)
                        .await
                        .unwrap_err();
                    if *method == "TransferSIPParticipant" {
                        assert_eq!(error.code, Code::InvalidArgument);
                        assert_eq!(error.msg, "Missing room name");
                    } else {
                        assert_eq!(error.code, Code::Internal, "{method}");
                        assert_eq!(error.msg, format!("{label} not connected (redis required)"));
                    }
                }
                // Both generic JSON values and arbitrary protobuf used to pass
                // check_body. Neither may now reach the service's auth check.
                for (codec, body) in [
                    (Codec::Json, &b"[]"[..]),
                    (Codec::Json, &b"42"[..]),
                    (Codec::Protobuf, &b"\xff"[..]),
                    (Codec::Protobuf, &b"\x0a\xff"[..]),
                ] {
                    assert_eq!(
                        call.call(&state, None, method, codec, body)
                            .await
                            .unwrap_err()
                            .code,
                        Code::Malformed,
                        "{method} {codec:?}"
                    );
                }
                // Official protojson discards unknown fields; binary decoders
                // also retain compatibility with future field numbers.
                assert_eq!(
                    call.call(
                        &state,
                        None,
                        method,
                        Codec::Json,
                        br#"{"future_field":true} trailing"#
                    )
                    .await
                    .unwrap_err()
                    .code,
                    empty_code
                );
                assert_eq!(
                    call.call(&state, None, method, Codec::Protobuf, &[0xf8, 0x07, 0x01])
                        .await
                        .unwrap_err()
                        .code,
                    empty_code
                );
            }
            assert_eq!(
                call.call(&state, None, "UnknownMethod", Codec::Json, b"{}")
                    .await
                    .unwrap_err()
                    .code,
                Code::BadRoute
            );
        }
        net.shutdown();
    }

    async fn invoke_transfer(
        state: &crate::state::AppState,
        request: &pb::TransferSipParticipantRequest,
        auth: Auth<'_>,
        codec: Codec,
    ) -> TwirpError {
        let bytes = match codec {
            Codec::Json => serde_json::to_vec(request).unwrap(),
            Codec::Protobuf => prost::Message::encode_to_vec(request),
        };
        sip(state, auth, "TransferSIPParticipant", codec, &bytes)
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn transfer_preflight_preserves_presence_grant_and_uri_order() {
        let (state, net) = test_state().await;
        let mut token = VerifiedToken::default();
        for codec in [Codec::Json, Codec::Protobuf] {
            let mut request = pb::TransferSipParticipantRequest::default();
            assert_eq!(
                invoke_transfer(&state, &request, None, codec).await.msg,
                "Missing room name"
            );
            request.room_name = "r".into();
            assert_eq!(
                invoke_transfer(&state, &request, None, codec).await.msg,
                "Missing participant identity"
            );
            request.participant_identity = "p".into();
            assert_eq!(
                invoke_transfer(&state, &request, None, codec).await.code,
                Code::Unauthenticated
            );
            token.extra.sip.call = true;
            token.grants.video.room_admin = false;
            assert_eq!(
                invoke_transfer(&state, &request, Some(&token), codec)
                    .await
                    .code,
                Code::Unauthenticated
            );
            token.grants.video.room_admin = true;
            token.grants.video.room = "different".into();
            assert_eq!(
                invoke_transfer(&state, &request, Some(&token), codec)
                    .await
                    .code,
                Code::Unauthenticated
            );
            token.grants.video.room = "r".into();
            assert_eq!(
                invoke_transfer(&state, &request, Some(&token), codec)
                    .await
                    .msg,
                "missing transfer to"
            );
            request.transfer_to = "https://example.invalid".into();
            assert_eq!(
                invoke_transfer(&state, &request, Some(&token), codec)
                    .await
                    .msg,
                "transfer_to must be a valid SIP(s) or TEL URI (sip:, sips: or tel:)"
            );
            for uri in [
                "sip:p@example.invalid",
                "<sips:p@example.invalid>",
                "tel:+123",
                "<tel:+123>",
            ] {
                request.transfer_to = uri.into();
                let error = invoke_transfer(&state, &request, Some(&token), codec).await;
                // Actual fixed Go JSON/protobuf and both Redis profiles return this owner error.
                assert_eq!(error.code, Code::NotFound);
                assert_eq!(error.msg, "participant does not exist");
                assert!(error.meta.is_empty());
            }
        }
        net.shutdown();
    }

    #[tokio::test]
    async fn typed_sip_maps_oneofs_enums_and_nested_messages_are_not_generic_json() {
        let (state, net) = test_state().await;
        let valid = br#"{"sipTrunkId":"trunk","replace":{"headers":{"X-Test":"v"}}}"#;
        let request: pb::UpdateSipInboundTrunkRequest = decode(Codec::Json, valid).unwrap();
        assert_eq!(request.sip_trunk_id, "trunk");
        assert!(request.action.is_some());
        for body in [
            &br#"{"replace":{},"update":{}}"#[..],
            &br#"{"sip_trunk_id":12}"#[..],
            &br#"{"replace":{"headers":{"X-Test":12}}}"#[..],
        ] {
            assert_eq!(
                sip(&state, None, "UpdateSIPInboundTrunk", Codec::Json, body)
                    .await
                    .unwrap_err()
                    .code,
                Code::Malformed
            );
        }
        for body in [
            &br#"{"participantAttributes":{"k":false}}"#[..],
            &br#"{"ringingTimeout":{}}"#[..],
        ] {
            assert_eq!(
                sip(&state, None, "CreateSIPParticipant", Codec::Json, body)
                    .await
                    .unwrap_err()
                    .code,
                Code::Malformed
            );
        }
        // Fixed Go protojson DiscardUnknown skips unknown symbolic enum
        // values. The literal remains covered, with the official auth error.
        let unknown_enum = sip(
            &state,
            None,
            "CreateSIPParticipant",
            Codec::Json,
            br#"{"includeHeaders":"NOT_A_HEADER_OPTION"}"#,
        )
        .await
        .unwrap_err();
        assert_eq!(unknown_enum.code, Code::Unauthenticated);
        assert_eq!(unknown_enum.msg, "permissions denied");
        let request: pb::CreateSipParticipantRequest = decode(
            Codec::Json,
            br#"{"participant_attributes":{"k":"v"},"ringingTimeout":"1.500s"}"#,
        )
        .unwrap();
        assert_eq!(
            request.participant_attributes.get("k").map(String::as_str),
            Some("v")
        );
        assert_eq!(request.ringing_timeout.as_ref().unwrap().nanos, 500_000_000);
        let wire = prost::Message::encode_to_vec(&request);
        assert_eq!(
            decode::<pb::CreateSipParticipantRequest>(Codec::Protobuf, &wire).unwrap(),
            request
        );
        net.shutdown();
    }

    #[tokio::test]
    async fn typed_egress_and_ingress_reject_schema_errors_before_auth() {
        let (state, net) = test_state().await;
        for body in [
            &br#"{"template":{},"web":{}}"#[..],
            &br#"{"outputs":{}}"#[..],
            &br#"{"roomName":true}"#[..],
        ] {
            assert_eq!(
                egress(&state, None, "StartEgress", Codec::Json, body)
                    .await
                    .unwrap_err()
                    .code,
                Code::Malformed
            );
        }
        for body in [&br#"{"audio":[]}"#[..], &br#"{"enabled":"yes"}"#[..]] {
            assert_eq!(
                ingress(None, "CreateIngress", Codec::Json, body)
                    .unwrap_err()
                    .code,
                Code::Malformed
            );
        }
        let unknown_enum = ingress(
            None,
            "CreateIngress",
            Codec::Json,
            br#"{"inputType":"NOT_AN_INPUT"}"#,
        )
        .unwrap_err();
        assert_eq!(unknown_enum.code, Code::Unauthenticated);
        assert_eq!(unknown_enum.msg, "permissions denied");
        let ingress: pb::CreateIngressRequest = decode(
            Codec::Json,
            br#"{"inputType":"WHIP_INPUT","enabled":false}"#,
        )
        .unwrap();
        assert_eq!(ingress.enabled, Some(false));
        let egress: pb::StartEgressRequest = decode(
            Codec::Json,
            br#"{"room_name":"r","web":{"url":"https://example.invalid"}}"#,
        )
        .unwrap();
        assert!(egress.source.is_some());
        net.shutdown();
    }

    #[test]
    fn service_registry_matches_every_pinned_schema_method() {
        let schemas = [
            (
                EGRESS_METHODS,
                include_str!("../../../lk-protocol/protos/livekit_egress.proto"),
            ),
            (
                INGRESS_METHODS,
                include_str!("../../../lk-protocol/protos/livekit_ingress.proto"),
            ),
            (
                SIP_METHODS,
                include_str!("../../../lk-protocol/protos/livekit_sip.proto"),
            ),
        ];
        for (methods, schema) in schemas {
            let schema_methods: std::collections::BTreeSet<_> = schema
                .lines()
                .filter_map(|line| line.trim().strip_prefix("rpc "))
                .map(|line| line.split('(').next().unwrap().trim())
                .collect();
            assert_eq!(
                methods
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>(),
                schema_methods
            );
        }
    }
}

/// HTTP-level integration tests for Egress/Ingress/SIP through the full Twirp
/// router (auth middleware → body limit → CORS → path normalization → handler).
#[cfg(test)]
mod http_integration_tests {
    use super::*;
    use crate::test_support::state as test_state;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn make_record_jwt() -> String {
        crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({
                "iss": "devkey",
                "sub": "admin",
                "exp": 99999999999_u64,
                "video": {
                    "roomRecord": true
                }
            })
            .to_string(),
            "secret",
        )
    }

    fn make_ingress_jwt() -> String {
        crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({
                "iss": "devkey",
                "sub": "admin",
                "exp": 99999999999_u64,
                "video": {
                    "ingressAdmin": true
                }
            })
            .to_string(),
            "secret",
        )
    }

    fn make_sip_jwt() -> String {
        crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({
                "iss": "devkey",
                "sub": "admin",
                "exp": 99999999999_u64,
                "sip": {
                    "admin": true,
                    "call": true
                }
            })
            .to_string(),
            "secret",
        )
    }

    async fn invoke_service(
        state: &crate::state::AppState,
        service: &str,
        method: &str,
        token: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post(format!("/twirp/{service}/{method}"))
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
    async fn egress_list_unauthenticated_via_http_returns_401() {
        let (state, net) = test_state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.Egress/ListEgress")
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
    async fn egress_list_authenticated_returns_503_redis_required() {
        let (state, net) = test_state().await;
        let jwt = make_record_jwt();
        let (status, json) = invoke_service(
            &state,
            "livekit.Egress",
            "ListEgress",
            &jwt,
            serde_json::json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            json["msg"].as_str(),
            Some("egress not connected (redis required)")
        );
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn ingress_list_unauthenticated_via_http_returns_401() {
        let (state, net) = test_state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.Ingress/ListIngress")
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
    async fn sip_list_trunk_unauthenticated_via_http_returns_401() {
        let (state, net) = test_state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.SIP/ListSIPTrunk")
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
    async fn sip_list_trunk_authenticated_returns_503_redis_required() {
        let (state, net) = test_state().await;
        let jwt = make_sip_jwt();
        let (status, json) = invoke_service(
            &state,
            "livekit.SIP",
            "ListSIPTrunk",
            &jwt,
            serde_json::json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            json["msg"].as_str(),
            Some("sip not connected (redis required)")
        );
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn all_egress_routes_reachable_via_http() {
        let (state, net) = test_state().await;
        let jwt = make_record_jwt();
        for method in EGRESS_METHODS {
            let (status, json) = invoke_service(
                &state,
                "livekit.Egress",
                method,
                &jwt,
                serde_json::json!({}),
            )
            .await;
            // Every method must reach the service layer (not 404/bad route).
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{method} must be routable via HTTP"
            );
            // Without Redis, all methods return 500 "egress not connected".
            assert_eq!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{method}: expected 500 without Redis, got {status}: {json}"
            );
        }
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn all_ingress_routes_reachable_via_http() {
        let (state, net) = test_state().await;
        let jwt = make_ingress_jwt();
        for method in INGRESS_METHODS {
            let (status, json) = invoke_service(
                &state,
                "livekit.Ingress",
                method,
                &jwt,
                serde_json::json!({}),
            )
            .await;
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{method} must be routable via HTTP"
            );
            assert_eq!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{method}: expected 500 without Redis, got {status}: {json}"
            );
        }
        state.manager().close_all().await;
        net.shutdown();
    }

    /// A08: without Redis the public Ingress gateway splits exactly like the fixed
    /// Go service. Create/List hit the `store == nil` guard (`ingress.go:126/364`)
    /// and return a normal twirp 500 on a reusable keepalive connection;
    /// Update/Delete pass the `psrpcClient == nil` guard (the local bus yields a
    /// non-nil client) and dereference the nil store, so twirp's
    /// `ensurePanicResponses` flushes `Internal service panic` and re-panics, and
    /// Go's net/http retires the connection WITHOUT a `Connection: close` header.
    /// The Rust gateway mirrors both halves: `backend!` returns the guard error,
    /// `stored_backend!` returns `panic_response()` carrying
    /// `hyper::ext::CloseAfterResponse` (vendored hyper: "does not add a Connection
    /// header"). This drives the real production `axum::serve` path over a raw TCP
    /// socket to prove the wire-level reuse, not just the response object.
    async fn post_ingress_raw(stream: &mut tokio::net::TcpStream, method: &str, jwt: &str) {
        use tokio::io::AsyncWriteExt;
        let body = b"{}";
        let req = format!(
            "POST /twirp/livekit.Ingress/{method} HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer {jwt}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    }

    async fn read_http_response(stream: &mut tokio::net::TcpStream) -> (u16, String, String) {
        use tokio::io::AsyncReadExt;
        let mut buf: Vec<u8> = Vec::new();
        let mut tmp = [0u8; 4096];
        let header_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "peer closed before a complete response header");
            buf.extend_from_slice(&tmp[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let status = head.split_whitespace().nth(1).unwrap().parse::<u16>().unwrap();
        let lower = head.to_lowercase();
        let content_length: usize = lower
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map(|v| v.trim().parse().unwrap())
            .unwrap_or(0);
        while buf.len() < header_end + content_length {
            let n = stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "peer closed before the response body completed");
            buf.extend_from_slice(&tmp[..n]);
        }
        let body =
            String::from_utf8_lossy(&buf[header_end..header_end + content_length]).into_owned();
        (status, lower, body)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingress_no_redis_wire_reuse_matches_go_silent_panic_close() {
        use tokio::io::AsyncReadExt;
        use tokio::net::{TcpListener, TcpStream};
        let (state, net) = test_state().await;
        let jwt = make_ingress_jwt();
        let router = crate::http::router(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });

        // Create/List: store==nil guard -> normal twirp 500, keepalive reuse. Two
        // complete responses on one socket prove the connection stays reusable,
        // exactly like Go's normal error return (no panic, no close).
        let mut guard = TcpStream::connect(addr).await.unwrap();
        for method in ["CreateIngress", "ListIngress"] {
            post_ingress_raw(&mut guard, method, &jwt).await;
            let (status, head, body) = read_http_response(&mut guard).await;
            assert_eq!(status, 500, "{method}");
            assert_eq!(
                body,
                r#"{"code":"internal","msg":"ingress not connected (redis required)"}"#,
                "{method}"
            );
            assert!(
                !head.contains("connection: close"),
                "{method} must not send Connection: close"
            );
        }
        drop(guard);

        // Update/Delete: nil-store deref panic -> twirp recovery 500, silent close.
        for method in ["UpdateIngress", "DeleteIngress"] {
            let mut panic = TcpStream::connect(addr).await.unwrap();
            post_ingress_raw(&mut panic, method, &jwt).await;
            let (status, head, body) = read_http_response(&mut panic).await;
            assert_eq!(status, 500, "{method}");
            assert_eq!(
                body,
                r#"{"code":"internal","msg":"Internal service panic"}"#,
                "{method}"
            );
            assert!(
                !head.contains("connection: close"),
                "{method} must retire silently without a Connection header (Go net/http re-panic parity)"
            );
            // The server closes after flushing: the next read is EOF, not reuse.
            let mut tmp = [0u8; 64];
            let n = tokio::time::timeout(std::time::Duration::from_secs(5), panic.read(&mut tmp))
                .await
                .expect("server must close the connection after the panic response")
                .unwrap();
            assert_eq!(n, 0, "{method}: connection must be closed (EOF), not reusable");
        }

        server.abort();
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn all_sip_admin_routes_reachable_via_http() {
        let (state, net) = test_state().await;
        let jwt = make_sip_jwt();
        // SIP admin routes (not call routes)
        let admin_methods: Vec<&&str> = SIP_METHODS
            .iter()
            .filter(|m| **m != "CreateSIPParticipant" && **m != "TransferSIPParticipant")
            .collect();
        for method in admin_methods {
            let (status, _json) =
                invoke_service(&state, "livekit.SIP", method, &jwt, serde_json::json!({})).await;
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{method} must be routable via HTTP"
            );
        }
        state.manager().close_all().await;
        net.shutdown();
    }
}
