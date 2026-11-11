//! Type-only measurement, no oversized reference future is constructed on stack.
use super::*;
fn size<A: 'static, F, Fut>(_: F) -> usize
where
    F: FnOnce(&'static A) -> Fut,
    Fut: std::future::Future,
{
    std::mem::size_of::<Fut>()
}
#[test]
fn production_http_twirp_and_worker_future_sizes_without_construction() {
    let handle = size(|state: &'static AppState| {
        handle(
            State(state.clone()),
            Request::new(axum::body::Body::empty()),
        )
    });
    let serve = size(|state: &'static AppState| {
        serve(
            state,
            Service::Room,
            "/twirp/livekit.RoomService/CreateRoom",
            Request::new(axum::body::Body::empty()),
        )
    });
    let room = size(|state: &'static AppState| {
        room_service::call(state, None, "CreateRoom", Codec::Json, b"{}")
    });
    let agent = size(|state: &'static AppState| {
        other_services::agent_dispatch(state, None, "ListDispatch", Codec::Json, b"{}")
    });
    let egress = size(|state: &'static AppState| {
        other_services::egress(state, None, "ListEgress", Codec::Json, b"{}")
    });
    let ingress = size(|state: &'static AppState| {
        other_services::ingress_with_deadline(state, None, "ListIngress", Codec::Json, b"{}", None)
    });
    let sip = size(|state: &'static AppState| {
        other_services::sip_with_deadline(
            state,
            None,
            "ListSIPInboundTrunk",
            Codec::Json,
            b"{}",
            None,
        )
    });
    let typed_create = size(
        |parts: &'static (
            crate::service_bus::rpc::RpcClient,
            crate::service_bus::rpc::MethodSpec,
            lk_protocol::livekit::CreateRoomRequest,
        )| {
            parts.0.unary::<_, lk_protocol::livekit::Room>(
                &parts.1,
                &parts.2,
                crate::service_bus::rpc::CallOptions::new(Duration::from_secs(3)),
            )
        },
    );
    eprintln!(
        "HTTP_FUTURE_SIZES handle={handle} serve={serve} room={room} agent={agent} egress={egress} ingress={ingress} sip={sip} typed_create={typed_create}"
    );
}
/// Exercise the real middleware + full dispatcher + actual actor creation, not
/// a directly invoked branch or altered runtime worker stack.
#[tokio::test]
async fn actual_axum_create_list_delete_room_keeps_default_runtime_stack() {
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;
    let (state, net) = crate::test_support::state().await;
    let key = &state.config.keys[0];
    let token=crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#,&serde_json::json!({"iss":key.0,"sub":"http-stack","exp":99999999999_u64,"video":{"roomCreate":true,"roomList":true,"roomAdmin":true,"room":"http-stack"}}).to_string(),&key.1);
    for (method, body) in [
        ("CreateRoom", r#"{"name":"http-stack"}"#),
        ("ListRooms", "{}"),
        ("DeleteRoom", r#"{"room":"http-stack"}"#),
    ] {
        let response = crate::http::router(state.clone())
            .oneshot(
                Request::post(format!("/twirp/livekit.RoomService/{method}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let payload = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "{method}: {}",
            String::from_utf8_lossy(&payload)
        );
    }
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}
