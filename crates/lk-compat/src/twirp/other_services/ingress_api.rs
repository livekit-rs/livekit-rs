//! Actual management gateway; no successful disconnected or worker fallback.
use super::super::{
    codec::Codec,
    error::{Code, TwirpError},
    worker_codec::{input, output},
    worker_error::failure,
};
use crate::{
    auth::{VerifiedToken, permissions::ensure_ingress_admin},
    state::AppState,
    workers::{ingress::IngressFailure, preserved::WireValue},
};
use axum::response::{IntoResponse, Response};
use lk_protocol::livekit as pb;
use std::time::Instant;
/// Twirp flushes the panic JSON before Go's HTTP/1 writer decides that
/// the connection cannot be reused. Retire after that exact body without
/// adding a Connection header to the already committed response.
fn panic_response() -> Response {
    let mut response = TwirpError::internal("Internal service panic").into_response();
    response
        .extensions_mut()
        .insert(hyper::ext::CloseAfterResponse);
    response
}
fn boundary(error: IngressFailure) -> Result<Response, TwirpError> {
    match error {
        IngressFailure::Rpc(error) => Err(failure(error)),
        // ensurePanicResponses 的 writeError 设置 Content-Length 并 Flush。
        // 真实 HTTP 只发送 JSON；ResponseRecorder 的 PANIC 追加文本不是线缆契约。
        IngressFailure::PanicRecovery => Ok(panic_response()),
    }
}
pub async fn call(
    state: &AppState,
    auth: Option<&VerifiedToken>,
    method: &str,
    codec: Codec,
    body: &[u8],
    deadline: Option<Instant>,
) -> Result<Response, TwirpError> {
    let services = state.worker_services.as_deref();
    // Without Redis the local bus still yields a non-nil ingress client, but
    // getIngressStore returns nil: Update/Delete pass the "not connected"
    // guard and dereference the nil store (Twirp panic recovery).
    macro_rules! stored_backend {
        () => {{
            let Some(services) = services else {
                ensure_ingress_admin(auth)?;
                // ensurePanicResponses writes this before re-panicking; the
                // connection is then closed (see panic_response).
                return Ok(panic_response());
            };
            services
        }};
    }
    macro_rules! backend {
        () => {{
            let Some(services) = services else {
                ensure_ingress_admin(auth)?;
                return Err(TwirpError::internal(
                    "ingress not connected (redis required)",
                ));
            };
            services
        }};
    }
    match method {
        "CreateIngress" => {
            let request: WireValue<pb::CreateIngressRequest> = input(codec, body)?;
            // Fixed source input switch precedes common permission/store checks.
            if !matches!(request.message.input_type, 0..=2) {
                return Err(TwirpError::new(
                    Code::InvalidArgument,
                    "invalid ingress type",
                ));
            }
            let services = backend!();
            let mut options = services.call_options();
            options.deadline = deadline;
            // Open-source public middleware does not set WithRequestID from an
            // HTTP header. Preserve empty context here; direct gateway callers
            // have the real explicit request-id seam for deterministic creation.
            Ok(output(
                codec,
                services
                    .ingress
                    .create(auth, request, "", options)
                    .await
                    .map_err(failure)?,
            ))
        }
        "ListIngress" => {
            let request: WireValue<pb::ListIngressRequest> = input(codec, body)?;
            let services = backend!();
            Ok(output(
                codec,
                services
                    .ingress
                    .list(auth, request.message)
                    .await
                    .map_err(failure)?,
            ))
        }
        "UpdateIngress" => {
            let request: WireValue<pb::UpdateIngressRequest> = input(codec, body)?;
            let services = stored_backend!();
            let mut options = services.call_options();
            options.deadline = deadline;
            match services.ingress.update(auth, request, options).await {
                Ok(value) => Ok(output(codec, value)),
                Err(error) => boundary(error),
            }
        }
        "DeleteIngress" => {
            let request: WireValue<pb::DeleteIngressRequest> = input(codec, body)?;
            let services = stored_backend!();
            let mut options = services.call_options();
            options.deadline = deadline;
            match services.ingress.delete(auth, request, options).await {
                Ok(value) => Ok(output(codec, value)),
                Err(error) => boundary(error),
            }
        }
        _ => Err(TwirpError::new(Code::BadRoute, "no handler for method")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::header};

    #[tokio::test]
    async fn public_panic_boundary_sends_only_the_flushed_twirp_json() {
        let response = boundary(IngressFailure::PanicRecovery).unwrap();
        assert_eq!(response.status(), 500);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert!(response.headers().get(header::CONNECTION).is_none());
        assert!(
            response
                .extensions()
                .get::<hyper::ext::CloseAfterResponse>()
                .is_some()
        );
        assert_eq!(
            to_bytes(response.into_body(), 4096).await.unwrap().as_ref(),
            br#"{"code":"internal","msg":"Internal service panic"}"#,
        );
    }
}
