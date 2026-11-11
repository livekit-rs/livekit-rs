// Apache-2.0; local HTTP branch of fixed pkg/agent/endpoint/front.go.
// Local worker selection and typed response-reset retry.
use super::super::websocket::WorkerHttpState;
use super::{Registration, RemoteResponseReset, StreamIo, StreamLease};
use crate::auth::{authenticate_token, extract_token};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::any,
};
use bytes::{Buf, Bytes, BytesMut};
use hyper::body::{Body as HttpBody, Frame, Incoming, SizeHint};
use hyper_util::rt::TokioIo;
use lk_protocol::livekit as pb;
use prost::Message;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncWrite as _;

pub(super) fn routes(state: WorkerHttpState) -> Router {
    if state.registry.endpoints.disabled() {
        return Router::new();
    }
    Router::new()
        .route("/agents/{*path}", any(serve))
        .with_state(state)
}
fn plain(code: StatusCode, message: &str) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        format!("{message}\n"),
    )
        .into_response()
}
fn unauthorized() -> Response {
    let mut r = plain(StatusCode::UNAUTHORIZED, "authentication required");
    r.headers_mut()
        .insert(header::WWW_AUTHENTICATE, "Bearer".parse().unwrap());
    r
}
fn unavailable() -> Response {
    unavailable_reason("no workers available for deployment")
}
fn unavailable_reason(reason: &str) -> Response {
    let mut response = plain(StatusCode::SERVICE_UNAVAILABLE, reason);
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, "1".parse().unwrap());
    response
}
fn path_unescape(raw: &str) -> Option<String> {
    // PathUnescape keeps '+' literal, unlike query decoding.
    crate::auth::query_unescape(&raw.replace('+', "%2B"))
}
fn split(uri: &Uri) -> Result<(String, String, String, String), StatusCode> {
    let raw = uri
        .path()
        .strip_prefix("/agents/")
        .ok_or(StatusCode::NOT_FOUND)?;
    let (agent, rest) = raw.split_once('/').ok_or(StatusCode::NOT_FOUND)?;
    let (deployment, path) = rest.split_once('/').unwrap_or((rest, ""));
    if agent.is_empty() || deployment.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    let escaped = format!("/{path}");
    if escaped.len() > 8192 {
        return Err(StatusCode::URI_TOO_LONG);
    }
    let mut agent = path_unescape(agent).ok_or(StatusCode::BAD_REQUEST)?;
    let mut deployment = path_unescape(deployment).ok_or(StatusCode::BAD_REQUEST)?;
    let decoded = path_unescape(&escaped).ok_or(StatusCode::BAD_REQUEST)?;
    if agent == "_" {
        agent.clear();
    }
    if deployment == "_" {
        deployment.clear();
    }
    Ok((agent, deployment, decoded, escaped))
}
// The worker learns identity through the authenticated preamble. Keep
// unrelated query bytes unchanged, including escapes, duplicates and empties.
fn worker_query(raw: &str) -> String {
    raw.split('&')
        .filter(|pair| {
            let key = pair.split_once('=').map_or(*pair, |(key, _)| key);
            crate::auth::query_unescape(key).as_deref() != Some("access_token")
        })
        .collect::<Vec<_>>()
        .join("&")
}
fn hop_headers(headers: &mut HeaderMap) {
    let connection: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|v| header::HeaderName::from_bytes(v.trim().as_bytes()).ok())
        .collect();
    for name in connection {
        headers.remove(name);
    }
    for name in [
        "connection",
        "proxy-connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}
fn reserved(headers: &mut HeaderMap) {
    let names: Vec<_> = headers
        .keys()
        .filter(|k| k.as_str().starts_with("x-lk-"))
        .cloned()
        .collect();
    for name in names {
        headers.remove(name);
    }
}
#[derive(Clone)]
struct Candidate {
    reg: Arc<Registration>,
    raw: String,
    public: bool,
}
fn match_route(
    regs: &[Arc<Registration>],
    path: &str,
    method: &str,
    granted: bool,
) -> (Vec<Candidate>, bool, bool) {
    let mut routes: BTreeMap<(usize, String), Vec<Candidate>> = BTreeMap::new();
    let mut positions: BTreeMap<String, usize> = BTreeMap::new();
    let mut partial = false;
    for r in regs {
        let mut declared = HashSet::new();
        for (idx, ep) in r.manifest.iter().enumerate() {
            if !ep.matches(path) {
                continue;
            }
            if !ep.methods.iter().any(|m| m == method) {
                partial = true;
                continue;
            }
            if !declared.insert(ep.canonical.clone()) {
                continue;
            }
            positions
                .entry(ep.canonical.clone())
                .and_modify(|i| *i = (*i).min(idx))
                .or_insert(idx);
        }
    }
    for r in regs {
        let mut declared = HashSet::new();
        for ep in &r.manifest {
            if !ep.matches(path)
                || !ep.methods.iter().any(|m| m == method)
                || !declared.insert(ep.canonical.clone())
            {
                continue;
            }
            let position = positions[&ep.canonical];
            routes
                .entry((position, ep.canonical.clone()))
                .or_default()
                .push(Candidate {
                    reg: r.clone(),
                    raw: ep.raw.clone(),
                    public: ep.public,
                });
        }
    }
    if let Some((_, workers)) = routes.into_iter().next() {
        let denied = workers.iter().any(|w| !granted && !w.public);
        (
            workers
                .into_iter()
                .filter(|w| granted || w.public)
                .collect(),
            false,
            denied,
        )
    } else {
        (vec![], partial, false)
    }
}
fn pick(mut workers: Vec<Candidate>) -> Option<Candidate> {
    workers.retain(|w| {
        w.reg.has_session() && !w.reg.draining.load(std::sync::atomic::Ordering::Acquire)
    });
    if workers.is_empty() {
        return None;
    }
    if workers.len() == 1 {
        return workers.pop();
    }
    let mut random = [0u8; 16];
    let _ = getrandom::fill(&mut random);
    let i = u64::from_le_bytes(random[..8].try_into().unwrap()) as usize % workers.len();
    let mut j = u64::from_le_bytes(random[8..].try_into().unwrap()) as usize % (workers.len() - 1);
    if j >= i {
        j += 1;
    }
    let choice = if workers[i].reg.inflight() <= workers[j].reg.inflight() {
        i
    } else {
        j
    };
    Some(workers.swap_remove(choice))
}
fn websocket_upgrade(req: &Request) -> bool {
    matches!(
        req.version(),
        hyper::Version::HTTP_10 | hyper::Version::HTTP_11
    ) && req.method() == hyper::Method::GET
        && req.body().size_hint().exact() == Some(0)
        && req
            .headers()
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case("upgrade"))
        && req
            .headers()
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("websocket"))
}
async fn serve(State(state): State<WorkerHttpState>, mut req: Request) -> Response {
    let (agent, deployment, path, mut escaped) = match split(req.uri()) {
        Ok(p) => p,
        Err(code) => {
            return plain(
                code,
                match code {
                    StatusCode::BAD_REQUEST => "bad request path",
                    StatusCode::URI_TOO_LONG => "uri too long",
                    _ => "404 page not found",
                },
            );
        }
    };
    let token = match extract_token(req.headers(), req.uri().query().unwrap_or("")) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let auth = match authenticate_token(&state.keys, token.as_deref()) {
        Ok(a) => a,
        Err(e) => return e.into_response(),
    };
    let (key, credentialed, granted) = if let Some(auth) = auth {
        let allowed = auth.extra.agent_endpoint.allows(&agent, &deployment);
        (auth.grants.api_key, true, allowed)
    } else {
        let key = state
            .keys
            .single_api_key()
            .map(str::to_owned)
            .or_else(|| state.registry.endpoints.single_key());
        let Some(key) = key else {
            return unauthorized();
        };
        (key, false, false)
    };
    if req.headers().get_all("x-request-id").iter().count() > 1 {
        return plain(StatusCode::BAD_REQUEST, "invalid X-Request-Id");
    }
    let request_id = match req.headers().get("x-request-id") {
        Some(v) => match v.to_str() {
            Ok(v)
                if !v.is_empty() && v.len() <= 128 && v.bytes().all(|b| (33..127).contains(&b)) =>
            {
                v.to_string()
            }
            _ => return plain(StatusCode::BAD_REQUEST, "invalid X-Request-Id"),
        },
        None => rtc_core::ids::new_sid("AER_"),
    };
    let scope = state.registry.endpoints.scope(&key, &agent, &deployment);
    if scope.is_empty() {
        return unavailable();
    }
    let (mut workers, mut partial, mut denied) =
        match_route(&scope, &path, req.method().as_str(), granted);
    if workers.is_empty() && !partial && !denied && path != "/" {
        let alt = if path.ends_with('/') && escaped.ends_with('/') {
            Some((
                path.strip_suffix('/').unwrap().to_string(),
                escaped.strip_suffix('/').unwrap().to_string(),
            ))
        } else if !path.ends_with('/') {
            Some((format!("{path}/"), format!("{escaped}/")))
        } else {
            None
        };
        if let Some((p, e)) = alt {
            let matched = match_route(&scope, &p, req.method().as_str(), granted);
            if !matched.0.is_empty() || matched.1 || matched.2 {
                escaped = e;
                (workers, partial, denied) = matched;
            }
        }
    }
    if workers.is_empty() {
        return if denied {
            if credentialed {
                plain(StatusCode::FORBIDDEN, "forbidden")
            } else {
                unauthorized()
            }
        } else if partial {
            plain(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        } else {
            plain(StatusCode::NOT_FOUND, "not found")
        };
    }
    let is_websocket = websocket_upgrade(&req);
    let client_upgrade = is_websocket.then(|| hyper::upgrade::on(&mut req));
    let endpoint_registry = state.registry.endpoints.clone();
    let mut force_tunnels = endpoint_registry.tunnel_force.subscribe();
    if is_websocket {
        workers.retain(|w| {
            !*endpoint_registry.tunnel_draining.borrow()
                && w.reg.tunnels.load(Ordering::Acquire) < w.reg.tunnel_limit
        });
    }
    let client = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|i| i.0.ip().to_string())
        .unwrap_or_default();
    let preamble = pb::agent_http::StreamPreamble {
        kind: 0,
        request_id: request_id.clone(),
        authorized: granted,
        route: String::new(),
        timeout_ms: 0,
        client_addr: client.clone(),
        scheme: if req.extensions().get::<super::TlsTransport>().is_some() {
            "https"
        } else {
            "http"
        }
        .to_string(),
    };
    let host = req.headers().get(header::HOST).cloned();
    hop_headers(req.headers_mut());
    if is_websocket {
        req.headers_mut()
            .insert(header::CONNECTION, "Upgrade".parse().unwrap());
        req.headers_mut()
            .insert(header::UPGRADE, "websocket".parse().unwrap());
    }
    reserved(req.headers_mut());
    let expect_continue = req
        .headers()
        .get_all(header::EXPECT)
        .iter()
        .last()
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"100-continue"));
    req.headers_mut().remove(header::EXPECT);
    if !client.is_empty() {
        let forwarded = req
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.is_empty())
            .map_or_else(|| client.clone(), |v| format!("{v}, {client}"));
        if let Ok(value) = forwarded.parse() {
            req.headers_mut().insert("x-forwarded-for", value);
        }
    }
    if req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split_once(' ')
                .is_some_and(|(s, _)| s.eq_ignore_ascii_case("Bearer"))
        })
    {
        req.headers_mut().remove(header::AUTHORIZATION);
    }
    req.headers_mut().insert(
        "x-request-id",
        match request_id.parse() {
            Ok(v) => v,
            Err(_) => return plain(StatusCode::BAD_REQUEST, "invalid X-Request-Id"),
        },
    );
    if let Some(host) = host {
        req.headers_mut().insert(header::HOST, host);
    }
    let query = worker_query(req.uri().query().unwrap_or_default());
    let target = if query.is_empty() {
        escaped
    } else {
        format!("{escaped}?{query}")
    };
    // Hyper only serializes request trailers named in the local Trailer
    // declaration. Never take that declaration or its values from the caller.
    let chunked_upload = !req.headers().contains_key(header::CONTENT_LENGTH)
        && req.body().size_hint().exact().is_none();
    if chunked_upload {
        req.headers_mut().insert(
            header::TRANSFER_ENCODING,
            header::HeaderValue::from_static("chunked"),
        );
        req.headers_mut().insert(
            header::TRAILER,
            header::HeaderValue::from_static("x-lk-completion, x-lk-error"),
        );
    }
    let size = req.method().as_str().len()
        + target.len()
        + 32
        + req
            .headers()
            .iter()
            .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 4)
            .sum::<usize>();
    if size > 1 << 20 {
        return plain(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request head too large",
        );
    }
    *req.uri_mut() = match target.parse() {
        Ok(uri) => uri,
        Err(_) => return plain(StatusCode::BAD_REQUEST, "bad request"),
    };
    *req.version_mut() = hyper::Version::HTTP_11;
    let (parts, body) = req.into_parts();
    let bodyless = body.size_hint().exact() == Some(0)
        && parts
            .headers
            .get(header::CONTENT_LENGTH)
            .is_none_or(|value| value == "0");
    let mut exchange = Exchange {
        parts,
        body: Some(body),
        expect_continue,
        chunked_upload,
        preamble,
        client_upgrade,
        is_websocket,
    };
    for _ in 0..3 {
        let Some(worker) = pick(workers.clone()) else {
            break;
        };
        workers.retain(|candidate| !Arc::ptr_eq(&candidate.reg, &worker.reg));
        let tunnel_lease = if is_websocket {
            let Some(lease) = endpoint_registry.acquire_tunnel(&worker.reg) else {
                continue;
            };
            Some(lease)
        } else {
            None
        };
        let opened = tokio::select! {
            result = worker.reg.open() => result,
            _ = force_tunnels.changed(), if is_websocket => return unavailable(),
        };
        let Ok((io, lease)) = opened else {
            continue;
        };
        match exchange
            .bridge(
                worker,
                io,
                lease,
                tunnel_lease,
                &endpoint_registry,
                &mut force_tunnels,
            )
            .await
        {
            Ok(response) => return response,
            Err(reset) if bodyless && retryable_reset(reset, &exchange.parts.method) => {}
            Err(reset) => return reset_gateway_error(reset),
        }
    }
    unavailable_reason("no worker could serve the request")
}

fn retryable_reset(reset: RemoteResponseReset, method: &axum::http::Method) -> bool {
    use axum::http::Method;
    use pb::agent_http::HttpStreamResetCode::{HsrInternal, HsrRefused};
    match reset.0 {
        code if code == HsrRefused as u32 => true,
        code if code == HsrInternal as u32 => false,
        _ => matches!(
            *method,
            Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
        ),
    }
}

fn reset_gateway_error(reset: RemoteResponseReset) -> Response {
    if reset.0 == pb::agent_http::HttpStreamResetCode::HsrTimeout as u32 {
        plain(StatusCode::GATEWAY_TIMEOUT, "gateway timeout")
    } else {
        plain(StatusCode::BAD_GATEWAY, "bad gateway")
    }
}

struct Exchange {
    parts: axum::http::request::Parts,
    body: Option<Body>,
    expect_continue: bool,
    chunked_upload: bool,
    preamble: pb::agent_http::StreamPreamble,
    client_upgrade: Option<hyper::upgrade::OnUpgrade>,
    is_websocket: bool,
}

impl Exchange {
    async fn bridge(
        &mut self,
        worker: Candidate,
        io: StreamIo,
        lease: StreamLease,
        tunnel_lease: Option<super::TunnelLease>,
        endpoint_registry: &super::Registry,
        force_tunnels: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<Response, RemoteResponseReset> {
        let is_websocket = self.is_websocket;
        let mut closed = worker.reg.closed();
        self.preamble.route = worker.raw;
        let preamble_size = self.preamble.encoded_len();
        if preamble_size > 64 * 1024 {
            return Ok(plain(StatusCode::BAD_REQUEST, "bad request"));
        }
        let mut prefix = Vec::with_capacity(4 + preamble_size);
        prefix.extend_from_slice(&(preamble_size as u32).to_be_bytes());
        self.preamble
            .encode(&mut prefix)
            .expect("Vec can fit the preamble");
        let head_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        if *closed.borrow() {
            return Ok(plain(StatusCode::BAD_GATEWAY, "bad gateway"));
        }
        let (expect_head, head_ready) = if self.expect_continue {
            let (ready, head_ready) = tokio::sync::oneshot::channel();
            (
                Some(ExpectHead {
                    ready: Some(ready),
                    started: false,
                    queued: BytesMut::new(),
                }),
                Some(head_ready),
            )
        } else {
            (None, None)
        };
        let mut request = hyper::Request::new(WorkerUpload {
            body: self.body.take().unwrap_or_else(Body::empty),
            head_ready,
            chunked: self.chunked_upload,
            ended: false,
        });
        *request.method_mut() = self.parts.method.clone();
        *request.uri_mut() = self.parts.uri.clone();
        *request.version_mut() = self.parts.version;
        *request.headers_mut() = self.parts.headers.clone();
        let info_count = Arc::new(AtomicUsize::new(0));
        let (too_many_info, mut info_failure) = tokio::sync::watch::channel(false);
        // Hyper releases the informational callback when the final head arrives.
        // Keep one sender alive until that head is selected: channel closure is
        // not a ninth informational response or an upstream transport failure.
        let info_guard = too_many_info.clone();
        let counted = info_count.clone();
        let downstream = self
            .parts
            .extensions
            .get::<hyper::ext::InformationalSender>()
            .cloned();
        let defaults = self
            .parts
            .extensions
            .get::<crate::http::AgentResponseDefaults>()
            .cloned();
        hyper::ext::on_informational(&mut request, move |head| {
            let previous = counted.fetch_add(1, Ordering::AcqRel);
            // Go writes the ninth head, then rejects reading another worker
            // head. The existing count also fences retries once a head lands.
            if previous <= 8 {
                if let Some(downstream) = &downstream {
                    let mut headers = head.headers().clone();
                    if let Some(defaults) = &defaults {
                        crate::http::merge_response_defaults(&mut headers, &defaults.0);
                    }
                    hop_headers(&mut headers);
                    reserved(&mut headers);
                    let _ = downstream.try_send(head.status(), headers);
                }
            }
            if previous >= 8 {
                too_many_info.send_replace(true);
            }
        });
        let mut builder = hyper::client::conn::http1::Builder::new();
        builder.max_buf_size(1 << 20).max_headers((1 << 20) / 4);
        let reset = Arc::new(AtomicU64::new(u64::MAX));
        let stream = WorkerStream {
            io,
            prefix: prefix.into(),
            expect_head,
            reset: reset.clone(),
        };
        let (mut sender, connection) = match builder.handshake(TokioIo::new(stream)).await {
            Ok(c) => c,
            Err(_) => return Ok(plain(StatusCode::BAD_GATEWAY, "bad gateway")),
        };
        let task = tokio::spawn(async move {
            if *closed.borrow() {
                return;
            }
            tokio::select! {_=connection.with_upgrades()=>{},_=closed.changed()=>{}}
        });
        let guard = Driver {
            task: Some(task),
            lease: Some(lease),
        };
        let response_result = tokio::select! {
            result = tokio::time::timeout_at(head_deadline, sender.send_request(request)) => result,
            _ = info_failure.changed() => return Ok(plain(StatusCode::BAD_GATEWAY, "bad gateway")),
            _ = force_tunnels.changed(), if is_websocket => return Ok(unavailable()),
        };
        drop(info_guard);
        let mut response = match response_result {
            Ok(Ok(response)) => response,
            Err(_) => return Ok(plain(StatusCode::GATEWAY_TIMEOUT, "gateway timeout")),
            Ok(Err(_)) => {
                if let Ok(code) = u32::try_from(reset.load(Ordering::Acquire)) {
                    let reset = RemoteResponseReset(code);
                    if info_count.load(Ordering::Acquire) == 0 {
                        guard.retire().await;
                        return Err(reset);
                    }
                    return Ok(reset_gateway_error(reset));
                }
                return Ok(plain(StatusCode::BAD_GATEWAY, "bad gateway"));
            }
        };
        if info_count.load(Ordering::Acquire) > 8 || response.status().as_u16() > 599 {
            return Ok(plain(StatusCode::BAD_GATEWAY, "bad gateway"));
        }
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            if !is_websocket
                || !response
                    .headers()
                    .get(header::UPGRADE)
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
            {
                return Ok(plain(StatusCode::BAD_GATEWAY, "bad gateway"));
            }
            let mut tunnel_lease = tunnel_lease.expect("websocket handshake lease");
            tunnel_lease.activate();
            let worker_upgrade = hyper::upgrade::on(&mut response);
            let mut closed = worker.reg.closed();
            let registry = endpoint_registry.clone();
            let (mut parts, _) = response.into_parts();
            hop_headers(&mut parts.headers);
            reserved(&mut parts.headers);
            parts
                .headers
                .insert(header::CONNECTION, "Upgrade".parse().unwrap());
            parts
                .headers
                .insert(header::UPGRADE, "websocket".parse().unwrap());
            let client_upgrade = self.client_upgrade.take();
            let mut force_tunnels = force_tunnels.clone();
            tokio::spawn(async move {
                let _driver = guard;
                let _tunnel = tunnel_lease;
                if *closed.borrow() || *force_tunnels.borrow() {
                    return;
                }
                let upgrades = async {
                    tokio::try_join!(client_upgrade.expect("websocket client"), worker_upgrade)
                };
                let upgraded = tokio::select! {
                    value = upgrades => value,
                    _ = closed.changed() => return,
                    _ = force_tunnels.changed() => return,
                };
                if let Ok((client, endpoint)) = upgraded {
                    super::tunnel::splice(
                        TokioIo::new(client),
                        TokioIo::new(endpoint),
                        worker.reg,
                        registry,
                    )
                    .await;
                }
            });
            return Ok(Response::from_parts(parts, Body::empty()));
        }
        let (mut parts, body) = response.into_parts();
        hop_headers(&mut parts.headers);
        reserved(&mut parts.headers);
        Ok(Response::from_parts(
            parts,
            Body::new(ResponseBody {
                body,
                driver: guard,
                _tunnel: tunnel_lease,
            }),
        ))
    }
}
// Hyper may cancel the request sender while its connection task observes a
// read error. Capture the actual peer reset at the I/O boundary before that
// error crosses the task/channel. Write-side STOP_SENDING cannot prove that
// the worker refused to dispatch a request.
struct WorkerStream {
    io: StreamIo,
    prefix: Bytes,
    expect_head: Option<ExpectHead>,
    reset: Arc<AtomicU64>,
}

// The fixed quic-go v0.61 send stream accepts a final unwritten packet's
// worth of data before Write returns, even if peer credit is exhausted.
// Its request writer first completes the head, then reads the client body.
const GO_QUIC_STREAM_WRITE_BUFFER: usize = 1452;

struct ExpectHead {
    ready: Option<tokio::sync::oneshot::Sender<()>>,
    started: bool,
    queued: BytesMut,
}

impl WorkerStream {
    fn poll_queued(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(poll_write_remaining(&mut self.io, &mut self.prefix, cx))?;
        if let Some(head) = &mut self.expect_head {
            return poll_write_remaining(&mut self.io, &mut head.queued, cx);
        }
        Poll::Ready(Ok(()))
    }

    fn buffer_head(&mut self, bytes: &[u8]) -> Poll<io::Result<usize>> {
        let Some(head) = &mut self.expect_head else {
            return Poll::Pending;
        };
        if head.ready.is_none() {
            return Poll::Pending;
        }
        let room =
            GO_QUIC_STREAM_WRITE_BUFFER.saturating_sub(self.prefix.len() + head.queued.len());
        if room == 0 {
            return Poll::Pending;
        }
        let written = room.min(bytes.len());
        head.queued.extend_from_slice(&bytes[..written]);
        head.started |= written != 0;
        Poll::Ready(Ok(written))
    }
}

fn poll_write_remaining(
    io: &mut StreamIo,
    buf: &mut impl Buf,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    while buf.has_remaining() {
        let written = std::task::ready!(Pin::new(&mut *io).poll_write(cx, buf.chunk()))?;
        if written == 0 {
            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
        }
        buf.advance(written);
    }
    Poll::Ready(Ok(()))
}

impl tokio::io::AsyncRead for WorkerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if let Poll::Ready(Err(error)) = &result
            && let Some(reset) = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<RemoteResponseReset>())
        {
            let _ = self.reset.compare_exchange(
                u64::MAX,
                u64::from(reset.0),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        result
    }
}
impl tokio::io::AsyncWrite for WorkerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        // The prefix and request bytes share the send side. Hyper's duplex
        // driver can poll the response while the prefix is flow-controlled.
        // Report only request-byte progress to Hyper.
        match this.poll_queued(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return this.buffer_head(bytes),
        }
        match Pin::new(&mut this.io).poll_write(cx, bytes) {
            Poll::Ready(Ok(written)) => {
                if let Some(head) = &mut this.expect_head {
                    head.started |= written != 0;
                }
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => this.buffer_head(bytes),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(head) = &mut self.expect_head
            && head.started
        {
            // Hyper flushes this adapter only after all its buffered head
            // bytes were accepted. Allow the body read then, without waiting
            // for the final locally staged packet to cross the QUIC stream.
            if let Some(ready) = head.ready.take() {
                let _ = ready.send(());
            }
            std::task::ready!(self.poll_queued(cx))?;
        }
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

struct Driver {
    task: Option<tokio::task::JoinHandle<()>>,
    lease: Option<StreamLease>,
}
impl Driver {
    async fn retire(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.lease.take();
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.abort();
    }
}
struct WorkerUpload {
    body: Body,
    head_ready: Option<tokio::sync::oneshot::Receiver<()>>,
    chunked: bool,
    ended: bool,
}
impl HttpBody for WorkerUpload {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if let Some(ready) = &mut self.head_ready {
            let result = std::task::ready!(Pin::new(ready).poll(cx));
            self.head_ready = None;
            if result.is_err() {
                self.ended = true;
                return Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "worker request head did not complete",
                )))));
            }
        }
        match Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) if frame.is_trailers() => {
                // Go consumes a byte reader. A caller's trailers never become
                // worker protocol fields and successful framing has no claim.
                self.ended = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) if self.chunked => {
                self.ended = true;
                let mut trailers = HeaderMap::new();
                trailers.insert(
                    "x-lk-completion",
                    header::HeaderValue::from_static("peer_gone"),
                );
                let reason: String = error
                    .to_string()
                    .chars()
                    .map(|c| if c.is_ascii_control() { ' ' } else { c })
                    .collect();
                if !reason.is_empty() {
                    let bytes = reason.as_bytes();
                    if let Ok(value) =
                        header::HeaderValue::from_bytes(&bytes[..bytes.len().min(256)])
                    {
                        trailers.insert("x-lk-error", value);
                    }
                }
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            other => other,
        }
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.ended || (!self.chunked && self.body.is_end_stream())
    }
}

struct ResponseBody {
    body: Incoming,
    driver: Driver,
    _tunnel: Option<super::TunnelLease>,
}
impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
impl HttpBody for ResponseBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        match Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_trailers() {
                Ok(mut headers) => {
                    if headers
                        .get("x-lk-completion")
                        .and_then(|v| v.to_str().ok())
                        .is_some_and(|v| !v.trim().is_empty())
                    {
                        return Poll::Ready(Some(Err(io::Error::other(
                            "agent endpoint body did not complete",
                        ))));
                    }
                    reserved(&mut headers);
                    Poll::Ready(Some(Ok(Frame::trailers(headers))))
                }
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(io::Error::other(e.to_string())))),
            Poll::Ready(None) => {
                self.driver.abort();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
}
