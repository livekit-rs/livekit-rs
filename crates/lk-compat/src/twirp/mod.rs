//! Twirp v8 server for `/twirp/livekit.<Service>/<Method>` (impl-notes §2).
//!
//! Order of checks follows the generated handlers (`livekit_room.twirp.go`):
//! method must be POST → path `[/twirp]/<pkg.Service>/<Method>` → known method →
//! Content-Type → body read/decode → handler (with optional
//! `X-Twirp-Timeout-Ms` deadline). Every failure uses the Twirp JSON envelope.

mod body_reader;
pub mod codec;
pub mod error;
pub mod other_services;
pub mod room_service;

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::FutureExt;

use crate::auth::RequestAuth;
use crate::state::AppState;
use codec::Codec;
use error::{Code, TwirpError, go_quote};

pub const PATH_PREFIX: &str = "/twirp";
pub const TIMEOUT_HEADER: &str = "x-twirp-timeout-ms";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Service {
    Room,
    AgentDispatch,
    Egress,
    Ingress,
    Sip,
}

impl Service {
    const ALL: [Service; 5] = [
        Service::Room,
        Service::AgentDispatch,
        Service::Egress,
        Service::Ingress,
        Service::Sip,
    ];

    fn name(self) -> &'static str {
        match self {
            Service::Room => room_service::SERVICE,
            Service::AgentDispatch => other_services::AGENT_DISPATCH_SERVICE,
            Service::Egress => other_services::EGRESS_SERVICE,
            Service::Ingress => other_services::INGRESS_SERVICE,
            Service::Sip => other_services::SIP_SERVICE,
        }
    }

    fn methods(self) -> &'static [&'static str] {
        match self {
            Service::Room => room_service::METHODS,
            Service::AgentDispatch => other_services::AGENT_DISPATCH_METHODS,
            Service::Egress => other_services::EGRESS_METHODS,
            Service::Ingress => other_services::INGRESS_METHODS,
            Service::Sip => other_services::SIP_METHODS,
        }
    }

    /// Mux registration `/twirp/livekit.<Service>/` (prefix match).
    fn for_path(path: &str) -> Option<Service> {
        Self::ALL.into_iter().find(|s| {
            path.strip_prefix(PATH_PREFIX)
                .and_then(|rest| rest.strip_prefix('/'))
                .and_then(|rest| rest.strip_prefix(s.name()))
                .is_some_and(|rest| rest.starts_with('/'))
        })
    }
}

/// `parseTwirpPath`: (prefix, "pkg.Service", "Method").
pub fn parse_twirp_path(path: &str) -> (String, String, String) {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 2 {
        return (String::new(), String::new(), String::new());
    }
    let method = parts[parts.len() - 1].to_string();
    let service = parts[parts.len() - 2].to_string();
    let prefix = parts[..parts.len() - 2].join("/");
    (prefix, service, method)
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "404 page not found\n",
    )
        .into_response()
}

/// Axum handler for `/twirp/{*rest}` (any method).
pub async fn handle(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    let Some(service) = Service::for_path(&path) else {
        // Not one of the registered prefixes: Go mux default handler.
        return not_found();
    };
    match Box::pin(serve(&state, service, &path, req)).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn serve(
    state: &AppState,
    service: Service,
    path: &str,
    req: Request,
) -> Result<Response, TwirpError> {
    let method = req.method().clone();
    if method != Method::POST {
        return Err(TwirpError::bad_route(
            format!(
                "unsupported method {} (only POST is allowed)",
                go_quote(method.as_str())
            ),
            method.as_str(),
            path,
        ));
    }
    let (prefix, pkg_service, rpc) = parse_twirp_path(path);
    if pkg_service != service.name() {
        return Err(TwirpError::bad_route(
            format!("no handler for path {}", go_quote(path)),
            method.as_str(),
            path,
        ));
    }
    if prefix != PATH_PREFIX {
        return Err(TwirpError::bad_route(
            format!(
                "invalid path prefix {}, expected {}, on path {}",
                go_quote(&prefix),
                go_quote(PATH_PREFIX),
                go_quote(path)
            ),
            method.as_str(),
            path,
        ));
    }
    if !service.methods().contains(&rpc.as_str()) {
        return Err(TwirpError::bad_route(
            format!("no handler for path {}", go_quote(path)),
            method.as_str(),
            path,
        ));
    }

    // `Header.Get("Content-Type")`; non-UTF-8 values cannot match either type.
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let Some(codec) = Codec::from_content_type(&content_type) else {
        return Err(TwirpError::bad_route(
            format!("unexpected Content-Type: {}", go_quote(&content_type)),
            method.as_str(),
            path,
        ));
    };

    let timeout = req
        .headers()
        .get(TIMEOUT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<i64>().ok())
        .map(|ms| Duration::from_millis(ms.max(0) as u64));
    let auth = req
        .extensions()
        .get::<RequestAuth>()
        .and_then(|a| a.0.clone());

    let limit = match state.config.http.max_request_body {
        0 => usize::MAX,
        n => usize::try_from(n).unwrap_or(usize::MAX),
    };
    let body = match codec {
        Codec::Json => body_reader::json(req.into_body(), limit)
            .await
            .map_err(|_| codec.body_read_error())?,
        Codec::Protobuf => to_bytes(req.into_body(), limit)
            .await
            .map_err(|_| codec.body_read_error())?
            .to_vec(),
    };

    let call = async {
        match service {
            Service::Room => {
                Box::pin(room_service::call(
                    state,
                    auth.as_deref(),
                    &rpc,
                    codec,
                    &body,
                ))
                .await
            }
            Service::AgentDispatch => {
                Box::pin(other_services::agent_dispatch(
                    state,
                    auth.as_deref(),
                    &rpc,
                    codec,
                    &body,
                ))
                .await
            }
            Service::Egress => {
                Box::pin(other_services::egress_with_deadline(
                    state,
                    auth.as_deref(),
                    &rpc,
                    codec,
                    &body,
                    timeout.and_then(|duration| std::time::Instant::now().checked_add(duration)),
                ))
                .await
            }
            Service::Ingress => {
                Box::pin(other_services::ingress_with_deadline(
                    state,
                    auth.as_deref(),
                    &rpc,
                    codec,
                    &body,
                    timeout.and_then(|duration| std::time::Instant::now().checked_add(duration)),
                ))
                .await
            }
            Service::Sip => {
                Box::pin(other_services::sip_with_deadline(
                    state,
                    auth.as_deref(),
                    &rpc,
                    codec,
                    &body,
                    timeout.and_then(|duration| std::time::Instant::now().checked_add(duration)),
                ))
                .await
            }
        }
    };
    // `ensurePanicResponses`: a handler panic becomes `internal`.
    let guarded = AssertUnwindSafe(call).catch_unwind();
    // The Go Egress start middleware can report its own cancellation when
    // the caller's deadline ends a retry. Give that consumer the deadline
    // instead of replacing its result with the generic HTTP timeout.
    // Ingress Update/Delete likewise handle their own worker deadline. The
    // fixed service still commits catalog changes after a failed worker RPC.
    let timeout = if (service == Service::Egress && rpc.starts_with("Start"))
        || (service == Service::Ingress
            && matches!(rpc.as_str(), "UpdateIngress" | "DeleteIngress"))
    {
        None
    } else {
        timeout
    };
    let result = match timeout {
        Some(t) => match tokio::time::timeout(t, guarded).await {
            Ok(r) => r,
            Err(_) => {
                return Err(TwirpError::new(
                    Code::DeadlineExceeded,
                    "context deadline exceeded",
                ));
            }
        },
        None => guarded.await,
    };
    match result {
        Ok(r) => r,
        Err(_) => Err(TwirpError::internal("Internal service panic")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_parsing() {
        assert_eq!(
            parse_twirp_path("/twirp/livekit.RoomService/CreateRoom"),
            (
                "/twirp".into(),
                "livekit.RoomService".into(),
                "CreateRoom".into()
            )
        );
        assert_eq!(
            parse_twirp_path("/twirp/livekit.RoomService/a/b"),
            ("/twirp/livekit.RoomService".into(), "a".into(), "b".into())
        );
    }

    #[test]
    fn service_prefixes() {
        assert_eq!(
            Service::for_path("/twirp/livekit.RoomService/X"),
            Some(Service::Room)
        );
        assert_eq!(
            Service::for_path("/twirp/livekit.RoomService/"),
            Some(Service::Room)
        );
        assert_eq!(
            Service::for_path("/twirp/livekit.SIP/ListSIPTrunk"),
            Some(Service::Sip)
        );
        assert_eq!(Service::for_path("/twirp/livekit.RoomService"), None);
        assert_eq!(Service::for_path("/twirp/livekit.RoomServiceX/A"), None);
        assert_eq!(Service::for_path("/twirp/livekit.Unknown/A"), None);
    }
}

pub(crate) mod worker_error;

pub(crate) mod worker_codec;

#[cfg(test)]
#[path = "dispatch_future_size_tests.rs"]
mod dispatch_future_size_tests;
