//! Main-port HTTP router (impl-notes §2.1 and §5).
//!
//! Middleware order (outermost first), as in `server.go:483-516`:
//! path normalization → panic recovery → CORS → request body limit → API key
//! auth → routes. Routes: `/` health, `/twirp/...`, the signaling routes from
//! [`crate::signal::routes`], anything else `404 page not found`.
//!
//! The auth middleware stores [`RequestAuth`] in the request extensions for every
//! route (including the signaling routes); handlers decide what an
//! unauthenticated request means.

mod body_limit;
pub(crate) use body_limit::exceeded_limit as api_body_limit;

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::header::{self, HeaderValue};
use axum::http::uri::{PathAndQuery, Uri};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use futures_util::FutureExt;

use crate::auth::{KeyStore, RequestAuth, authenticate_token, extract_token};
use crate::{routing, state::AppState};

pub const CORS_MAX_AGE: &str = "86400";
pub const CORS_ALLOWED_METHODS: &[&str] = &["OPTIONS", "HEAD", "GET", "POST", "PATCH", "DELETE"];
const PREFLIGHT_VARY: &str =
    "Origin, Access-Control-Request-Method, Access-Control-Request-Headers";
const TEXT_PLAIN: &str = "text/plain; charset=utf-8";

pub fn router(state: AppState) -> Router {
    let routes: Router = Router::new()
        .route("/", any(health))
        .route("/twirp/{*rest}", any(crate::twirp::handle))
        .with_state(state.clone());
    let routes = routes
        .merge(crate::signal::routes(state.clone()))
        .merge(crate::whip::routes(state.clone()))
        .merge(state.agents.routes())
        .fallback(not_found);
    let api = middleware_chain(
        routes,
        state.keys.clone(),
        state.config.http.max_request_body,
    );
    if state.agents.registry.endpoints.disabled() {
        return api.layer(middleware::from_fn(normalize_path_layer));
    }
    dispatch_agent_chain(api, agent_endpoint_router(state.agents.worker_state()))
}

/// The agent endpoint router with its production CORS, auth and recovery layers.
pub fn agent_endpoint_router(state: crate::agent::websocket::WorkerHttpState) -> Router {
    let keys = state.keys.clone();
    crate::agent::endpoint::routes(state)
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(keys, auth_layer))
        .layer(middleware::from_fn(agent_cors_layer))
        .layer(middleware::from_fn(recover_layer))
}

/// Fixed NewHTTPHandler: only an enabled agent front owns this exact escaped
/// prefix; every other request keeps the original bounded API middleware.
fn dispatch_agent_chain(api: Router, agents: Router) -> Router {
    use tower::ServiceExt;
    Router::new()
        .fallback(any(move |req: Request| {
            let router = if req.uri().path().starts_with("/agents/") {
                agents.clone()
            } else {
                api.clone()
            };
            async move {
                router
                    .oneshot(req)
                    .await
                    .unwrap_or_else(|never| match never {})
            }
        }))
        .layer(middleware::from_fn(normalize_path_layer))
}

/// Wrap `routes` so that path normalization runs before route matching.
pub fn with_middleware(routes: Router, keys: Arc<KeyStore>, max_request_body: u64) -> Router {
    middleware_chain(routes, keys, max_request_body)
        .layer(middleware::from_fn(normalize_path_layer))
}
fn middleware_chain(routes: Router, keys: Arc<KeyStore>, max_request_body: u64) -> Router {
    Router::new()
        .fallback_service(routes)
        .layer(middleware::from_fn_with_state(keys, auth_layer))
        .layer(middleware::from_fn_with_state(
            max_request_body,
            body_limit_layer,
        ))
        .layer(middleware::from_fn(cors_layer))
        .layer(middleware::from_fn(recover_layer))
}

/// `LivekitServer.healthCheck` (`server.go:450-464`): 200 "OK" while this node's
/// `stats.updated_at` is within the literal 4 s, otherwise 406 with Go's
/// `time.Time.String()` of that stamp. The stamp advances in `keepaliveWorker`
/// (Redis routing: a psrpc ping that made the round trip; single-node: the local
/// stats tick), so a wedged refresh makes the node report not-ready exactly like
/// upstream. With no node record installed the stamp is Go's zero time, which is
/// 406 rather than a silent 200.
async fn health(State(state): State<AppState>) -> Response {
    let snapshot = state.node().map(|node| node.snapshot());
    let updated_at = snapshot
        .as_ref()
        .and_then(|node| node.stats.as_ref())
        .map(|stats| stats.updated_at);
    let zone = routing::local_zone_at(updated_at.unwrap_or(0));
    match routing::health_body(snapshot.as_ref(), std::time::SystemTime::now(), &zone) {
        Some(body) => (
            StatusCode::NOT_ACCEPTABLE,
            [(header::CONTENT_TYPE, TEXT_PLAIN)],
            body,
        )
            .into_response(),
        None => (StatusCode::OK, [(header::CONTENT_TYPE, TEXT_PLAIN)], "OK").into_response(),
    }
}

/// Go `http.NotFound`.
pub async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        [
            (header::CONTENT_TYPE, TEXT_PLAIN),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        "404 page not found\n",
    )
        .into_response()
}

fn plain(status: StatusCode, body: &'static str) -> Response {
    (status, [(header::CONTENT_TYPE, TEXT_PLAIN)], body).into_response()
}

/// `RemoveDoubleSlashes`: strip one leading `/` when the raw path starts with `//`.
async fn normalize_path_layer(mut req: Request, next: Next) -> Response {
    if let Some(stripped) = strip_double_slash(req.uri()) {
        *req.uri_mut() = stripped;
    }
    next.run(req).await
}

pub fn strip_double_slash(uri: &Uri) -> Option<Uri> {
    let pq = uri.path_and_query()?;
    if !pq.as_str().starts_with("//") {
        return None;
    }
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(PathAndQuery::try_from(&pq.as_str()[1..]).ok()?);
    Uri::from_parts(parts).ok()
}

/// negroni Recovery: a panic anywhere below becomes a bare 500.
async fn recover_layer(req: Request, next: Next) -> Response {
    match AssertUnwindSafe(next.run(req)).catch_unwind().await {
        Ok(resp) => resp,
        Err(_) => {
            tracing::error!("panic while serving HTTP request");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn cors_method_allowed(method: &str) -> bool {
    method == "OPTIONS" || CORS_ALLOWED_METHODS.contains(&method)
}

/// rs/cors v1.11.1 with any origin, the methods above, allowed/exposed headers
/// `*`, max-age 86400 and 204 preflights.
async fn cors_layer(req: Request, next: Next) -> Response {
    cors(req, next, false).await
}
async fn agent_cors_layer(req: Request, next: Next) -> Response {
    cors(req, next, true).await
}
#[derive(Clone)]
pub(crate) struct AgentResponseDefaults(pub(crate) HeaderMap);

async fn cors(mut req: Request, next: Next, agent: bool) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .filter(|v| !v.as_bytes().is_empty())
        .cloned();
    let origin_present = origin.is_some();
    let request_method = req
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .filter(|v| !v.as_bytes().is_empty())
        .cloned();

    if *req.method() == Method::OPTIONS
        && let Some(acrm) = request_method
    {
        let mut resp = StatusCode::NO_CONTENT.into_response();
        let request_headers: Vec<HeaderValue> = req
            .headers()
            .get_all(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .iter()
            .cloned()
            .collect();
        let h = resp.headers_mut();
        h.append(header::VARY, HeaderValue::from_static(PREFLIGHT_VARY));
        let method_ok = acrm
            .to_str()
            .is_ok_and(|method| cors_method_allowed(method) || (agent && method == "PUT"));
        if origin_present && method_ok {
            h.insert(
                header::ACCESS_CONTROL_ALLOW_ORIGIN,
                origin.clone().expect("nonempty origin checked"),
            );
            h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, acrm);
            if request_headers
                .first()
                .is_some_and(|v| !v.as_bytes().is_empty())
            {
                for v in request_headers {
                    h.append(header::ACCESS_CONTROL_ALLOW_HEADERS, v);
                }
            }
            h.insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static(CORS_MAX_AGE),
            );
        }
        return resp;
    }

    let method_ok =
        cors_method_allowed(req.method().as_str()) || (agent && req.method() == Method::PUT);
    let mut defaults = HeaderMap::new();
    defaults.insert(header::VARY, HeaderValue::from_static("Origin"));
    if let Some(origin) = origin.filter(|_| method_ok) {
        defaults.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        defaults.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("*"),
        );
    }
    // rs/cors sets these before invoking the handler. The agent worker's
    // informational heads need that base even before the final response exists.
    if agent {
        req.extensions_mut()
            .insert(AgentResponseDefaults(defaults.clone()));
    }
    let mut resp = next.run(req).await;
    merge_response_defaults(resp.headers_mut(), &defaults);
    resp
}

pub(crate) fn merge_response_defaults(headers: &mut HeaderMap, defaults: &HeaderMap) {
    for (name, value) in defaults {
        headers.entry(name.clone()).or_insert_with(|| value.clone());
    }
}

/// Source RequestBodyLimiter: keep declared-size rejection before auth; install
/// a lazy real-reader bound for chunked/unknown-length bodies. Downstream route
/// validators still decide whether to read the body and how to map read errors.
async fn body_limit_layer(State(max): State<u64>, mut req: Request, next: Next) -> Response {
    if max > 0 {
        let declared = req
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        if declared.is_some_and(|len| len > max) {
            return plain(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
        }
        let body = std::mem::replace(req.body_mut(), axum::body::Body::empty());
        *req.body_mut() = body_limit::wrap(body, max);
    }
    next.run(req).await
}

fn is_validate_path(path: &str) -> bool {
    path == "/rtc/validate" || path == "/rtc/v1/validate"
}

/// `APIKeyAuthMiddleware` (auth.go:63-120).
async fn auth_layer(State(keys): State<Arc<KeyStore>>, mut req: Request, next: Next) -> Response {
    let validate = is_validate_path(req.uri().path());
    let result = extract_token(req.headers(), &req.uri().query())
        .and_then(|token| authenticate_token(&keys, token.as_deref()));
    let mut resp = match result {
        Err(e) => e.into_response(),
        Ok(token) => {
            req.extensions_mut()
                .insert(RequestAuth(token.map(Arc::new)));
            next.run(req).await
        }
    };
    if validate {
        resp.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use tower::Service;

    #[tokio::test]
    async fn enabled_agent_chain_streams_above_api_limit_and_put_cors_is_isolated() {
        use tower::ServiceExt;
        async fn consume(req: Request) -> Response {
            match to_bytes(req.into_body(), 1 << 20).await {
                Ok(bytes) => (StatusCode::OK, bytes.len().to_string()).into_response(),
                Err(_) => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
            }
        }
        let keys = Arc::new(KeyStore::default());
        let api_routes = Router::new().fallback(any(consume));
        let agent_routes = Router::new()
            .fallback(any(consume))
            .layer(middleware::from_fn(agent_cors_layer));
        let enabled = dispatch_agent_chain(
            middleware_chain(api_routes.clone(), keys.clone(), 16),
            agent_routes,
        );
        let large = || {
            Body::from_stream(futures_util::stream::iter([
                Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b'a'; 32])),
                Ok(bytes::Bytes::from(vec![b'b'; 4096])),
            ]))
        };
        let response = enabled
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/agents/worker/_/upload")
                    .body(large())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_bytes(response.into_body(), 32).await.unwrap(), "4128");
        for uri in [
            "/agents",
            "/agentsish/upload",
            "/%61gents/worker/_/upload",
            "/Agents/worker/_/upload",
        ] {
            let response = enabled
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri(uri)
                        .body(large())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::PAYLOAD_TOO_LARGE,
                "{uri} bypassed API limit"
            );
        }
        // Disabled front is exactly the original API chain, even on /agents/.
        let disabled = with_middleware(api_routes, keys, 16);
        let response = disabled
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/agents/worker/_/upload")
                    .body(large())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        for (app, path, allowed) in [
            (enabled.clone(), "/agents/worker/_/upload", true),
            (enabled, "/api", false),
            (disabled, "/agents/worker/_/upload", false),
        ] {
            let response = app
                .oneshot(
                    axum::http::Request::builder()
                        .method("OPTIONS")
                        .uri(path)
                        .header("origin", "https://caller.example")
                        .header("access-control-request-method", "PUT")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert_eq!(
                response
                    .headers()
                    .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN),
                allowed
            );
        }
    }

    /// Middleware fixtures run over the real `/` handler, so the state must carry
    /// a node record: a server with no stamp answers 406, not 200.
    async fn app() -> Router {
        app_with_node(node_stamped_at(now_unix())).await
    }

    async fn app_with_node(node: routing::LocalNode) -> Router {
        let (mut state, net) = crate::test_support::state().await;
        state.enable_node(Arc::new(node));
        let routes = Router::new()
            .route("/", any(health))
            .route(
                "/whoami",
                any(|req: Request| async move {
                    let auth = req
                        .extensions()
                        .get::<RequestAuth>()
                        .cloned()
                        .unwrap_or_default();
                    match auth.0 {
                        Some(t) => t.grants.identity.clone(),
                        None => "anonymous".to_string(),
                    }
                }),
            )
            .route("/boom", any(boom))
            .fallback(not_found)
            .with_state(state);
        // None of these routes touch the media plane, and the fixture's own key
        // store (not the state's) drives the auth assertions below.
        net.shutdown();
        let keys = Arc::new(KeyStore::new(vec![("key".into(), "secret".into())]));
        with_middleware(routes, keys, 16)
    }

    fn node_stamped_at(updated_at: i64) -> routing::LocalNode {
        routing::LocalNode::serving(
            "ND_http_test".into(),
            "127.0.0.1".into(),
            String::new(),
            1,
            updated_at,
        )
        .expect("test node")
    }

    fn now_unix() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    async fn boom() -> &'static str {
        panic!("boom")
    }

    fn run(req: axum::http::Request<Body>) -> (StatusCode, axum::http::HeaderMap, String) {
        run_with(app(), req)
    }

    fn run_with(
        app: impl std::future::Future<Output = Router>,
        req: axum::http::Request<Body>,
    ) -> (StatusCode, axum::http::HeaderMap, String) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut app = app.await;
            let resp = app.call(req).await.unwrap();
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            (status, headers, String::from_utf8_lossy(&body).into_owned())
        })
    }

    fn get(uri: &str) -> axum::http::request::Builder {
        axum::http::Request::builder().uri(uri)
    }

    /// The 4 s rule is the handler's own, so a stale stamp has to reach the client
    /// as Go's exact `Not Ready\nNode Updated At <time.Time>` body. Which zone
    /// text the host prints is the reference binary's live differential, not this
    /// test's; here the wiring (status, Content-Type, prefix, stamp rendering) is.
    #[test]
    fn health_reports_406_when_the_node_stamp_is_stale() {
        let stale_at = now_unix() - 5;
        let node = node_stamped_at(stale_at);
        let snapshot = node.snapshot();
        let (s, h, b) = run_with(app_with_node(node), get("/").body(Body::empty()).unwrap());
        assert_eq!(s, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(h.get(header::CONTENT_TYPE).unwrap(), TEXT_PLAIN);
        let expected = format!(
            "Not Ready\nNode Updated At {}",
            routing::go_time_string(stale_at, &routing::local_zone_at(stale_at))
        );
        assert_eq!(b, expected);
        assert!(
            routing::health_body(
                Some(&snapshot),
                std::time::SystemTime::now(),
                &routing::local_zone_at(stale_at)
            )
            .is_some()
        );
    }

    #[test]
    fn health_and_not_found() {
        let (s, _, b) = run(get("/").body(Body::empty()).unwrap());
        assert_eq!((s, b.as_str()), (StatusCode::OK, "OK"));
        let (s, _, b) = run(get("/nope").body(Body::empty()).unwrap());
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::NOT_FOUND, "404 page not found\n")
        );
        // `//` prefix is normalised before routing
        let (s, _, b) = run(get("//").body(Body::empty()).unwrap());
        assert_eq!((s, b.as_str()), (StatusCode::OK, "OK"));
    }

    #[test]
    fn auth_failures_are_plain_401() {
        let (s, h, b) = run(get("/")
            .header("authorization", "Basic x")
            .body(Body::empty())
            .unwrap());
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_eq!(b, "invalid authorization header. Must start with Bearer ");
        assert_eq!(h.get(header::CONTENT_TYPE).unwrap(), TEXT_PLAIN);
        let (s, _, b) = run(get("/whoami?access_token=garbage")
            .body(Body::empty())
            .unwrap());
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::UNAUTHORIZED, "invalid authorization token")
        );
        let (s, _, b) = run(get("/whoami").body(Body::empty()).unwrap());
        assert_eq!((s, b.as_str()), (StatusCode::OK, "anonymous"));
        let (_, h, _) = run(get("/rtc/validate?access_token=garbage")
            .body(Body::empty())
            .unwrap());
        assert_eq!(h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
    }

    #[test]
    fn verified_token_reaches_handler() {
        let jwt = crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            r#"{"iss":"key","sub":"alice","exp":99999999999}"#,
            "secret",
        );
        let (s, _, b) = run(get("/whoami")
            .header("authorization", format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap());
        assert_eq!((s, b.as_str()), (StatusCode::OK, "alice"));
    }

    #[test]
    fn body_limit() {
        let (s, _, b) = run(axum::http::Request::post("/")
            .header("content-length", "17")
            .body(Body::from(vec![0u8; 17]))
            .unwrap());
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        );
    }

    #[test]
    fn cors() {
        let (s, h, _) = run(axum::http::Request::builder()
            .method("OPTIONS")
            .uri("/twirp/livekit.RoomService/ListRooms")
            .header("origin", "https://app")
            .header("access-control-request-method", "POST")
            .header(
                "access-control-request-headers",
                "authorization, content-type",
            )
            .body(Body::empty())
            .unwrap());
        assert_eq!(s, StatusCode::NO_CONTENT);
        assert_eq!(
            h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://app"
        );
        assert_eq!(h.get(header::ACCESS_CONTROL_ALLOW_METHODS).unwrap(), "POST");
        assert_eq!(
            h.get(header::ACCESS_CONTROL_ALLOW_HEADERS).unwrap(),
            "authorization, content-type"
        );
        assert_eq!(h.get(header::ACCESS_CONTROL_MAX_AGE).unwrap(), "86400");

        let (s, h, _) = run(axum::http::Request::builder()
            .method("OPTIONS")
            .uri("/")
            .header("origin", "https://app")
            .header("access-control-request-method", "PUT")
            .body(Body::empty())
            .unwrap());
        assert_eq!(s, StatusCode::NO_CONTENT);
        assert!(h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());

        let (_, h, _) = run(get("/")
            .header("origin", "https://app")
            .body(Body::empty())
            .unwrap());
        assert_eq!(
            h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://app"
        );
        assert_eq!(h.get(header::ACCESS_CONTROL_EXPOSE_HEADERS).unwrap(), "*");
        let (_, h, _) = run(get("/").body(Body::empty()).unwrap());
        assert!(h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
        assert_eq!(h.get(header::VARY).unwrap(), "Origin");
    }

    #[test]
    fn cors_preserves_handler_override_and_reflects_simple_origin() {
        let routes = Router::new().route(
            "/",
            any(|| async {
                (
                    [
                        (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                        (header::ACCESS_CONTROL_EXPOSE_HEADERS, "*"),
                    ],
                    "explicit WHIP handler",
                )
            }),
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut service =
                with_middleware(routes, Arc::new(KeyStore::new(Default::default())), 0);
            let response = service
                .call(
                    axum::http::Request::builder()
                        .method("OPTIONS")
                        .uri("/")
                        .header("origin", "https://app")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        });
    }

    #[test]
    fn panic_becomes_500() {
        let (s, _, _) = run(get("/boom").body(Body::empty()).unwrap());
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn double_slash() {
        let u: Uri = "//rtc?x=1".parse().unwrap();
        assert_eq!(strip_double_slash(&u).unwrap().to_string(), "/rtc?x=1");
        let u: Uri = "/rtc".parse().unwrap();
        assert!(strip_double_slash(&u).is_none());
    }
}
