//! The source API nil-State dereference is recovered by Negroni and is not a
//! ordinary Twirp error. Actual generated recovery writes an Internal JSON prefix
//! then Negroni appends PANIC on the same first line. Preserve this category, without making
//! up Go stack frames or assuming all internal errors should use plain text.
use crate::{service_bus::rpc::RpcError, workers::error::IoError};
#[derive(Debug)]
pub enum IngressFailure {
    Rpc(RpcError),
    PanicRecovery,
}
impl From<RpcError> for IngressFailure {
    fn from(value: RpcError) -> Self {
        Self::Rpc(value)
    }
}
impl From<IoError> for IngressFailure {
    fn from(value: IoError) -> Self {
        Self::Rpc(value.into())
    }
}
impl IngressFailure {
    /// Public route must handle this before normal Twirp mapping.
    /// Go's dynamic runtime stack is intentionally not fabricated in Rust.
    pub fn panic_response(&self) -> Option<axum::response::Response> {
        use axum::response::IntoResponse;
        match self {
            Self::PanicRecovery => Some(
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "application/json",
                    )],
                    "{\"code\":\"internal\",\"msg\":\"Internal service panic\"}PANIC: runtime error: invalid memory address or nil pointer dereference\n",
                )
                    .into_response(),
            ),
            Self::Rpc(_) => None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn panic_category_never_becomes_internal_twirp_json() {
        let response = IngressFailure::PanicRecovery.panic_response().unwrap();
        assert_eq!(response.status(), 500);
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE],
            "application/json"
        );
        assert!(
            IngressFailure::from(RpcError::new("internal", "worker failed"))
                .panic_response()
                .is_none()
        );
    }
}
