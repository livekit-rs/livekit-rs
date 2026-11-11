//! Source-order HTTP policy, intended for actual axum handlers after auth middleware.
//! Pure results are NOT a successful media backend or stand-alone nativeWHIP support.
use axum::http::{HeaderMap, StatusCode};
use rtc_core::{config::Limits, permission::Grants};
#[derive(Clone, Debug)]
pub struct Failure {
    pub status: StatusCode,
    pub message: String,
}
fn fail(status: StatusCode, message: impl Into<String>) -> Failure {
    Failure {
        status,
        message: message.into(),
    }
}
/// Absent/nil video has a different source error than present video lacking roomJoin.
pub fn principal(
    grants: Option<&Grants>,
    has_video: bool,
    create: bool,
    limits: &Limits,
) -> Result<(), Failure> {
    let grants = grants.filter(|_| has_video).ok_or_else(|| {
        fail(
            StatusCode::UNAUTHORIZED,
            "no permissions to access the room",
        )
    })?;
    if !grants.video.room_join {
        return Err(fail(StatusCode::UNAUTHORIZED, "permissions denied"));
    }
    if grants.video.room.is_empty() {
        return Err(fail(StatusCode::UNAUTHORIZED, "room name cannot be empty"));
    }
    if create {
        let max = limits.max_room_name_length;
        if !rtc_core::config::signed_byte_length_allowed(max, grants.video.room.len()) {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                format!("room name length exceeds limits: max length {max}"),
            ));
        }
        if grants.identity.is_empty() {
            return Err(fail(StatusCode::BAD_REQUEST, "identity cannot be empty"));
        }
        let max = limits.max_participant_identity_length;
        if !rtc_core::config::signed_byte_length_allowed(max, grants.identity.len()) {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                format!("participant identity length exceeds limits: max length {max}"),
            ));
        }
    } else if grants.identity.is_empty() {
        return Err(fail(
            StatusCode::UNAUTHORIZED,
            "participant identity cannot be empty",
        ));
    }
    Ok(())
}
pub fn content_type(headers: &HeaderMap, expected: &str) -> Result<(), Failure> {
    let actual = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if actual != expected {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            format!("unsupported content-type: {actual}"),
        ));
    }
    Ok(())
}
pub fn if_match(headers: &HeaderMap) -> Result<String, Failure> {
    let tag = headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if tag.is_empty() {
        return Err(fail(
            StatusCode::PRECONDITION_REQUIRED,
            "missing entity tag",
        ));
    }
    Ok(tag.to_owned())
}
/// Fixed Go MaxBytesReader bound; use actual HTTP streamed read capped at1MiB.
pub const BODY_LIMIT: usize = 1 << 20;
pub fn create_body(body: &[u8]) -> Result<(), Failure> {
    if body.len() > BODY_LIMIT {
        return Err(fail(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("request body exceeds {BODY_LIMIT} bytes"),
        ));
    }
    if body.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "body does not have SDP offer",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod signed_limit_order_tests {
    use super::*;
    #[test]
    fn whip_create_room_length_precedes_identity_but_resource_auth_does_not_recheck_name_lengths() {
        let mut limits = crate::test_support::config().limits;
        limits.max_room_name_length = -1;
        limits.max_participant_identity_length = -1;
        let mut grants = Grants::default();
        grants.video.room_join = true;
        grants.video.room = "a".into();
        assert_eq!(
            principal(Some(&grants), true, true, &limits)
                .unwrap_err()
                .message,
            "room name length exceeds limits: max length -1"
        );
        limits.max_room_name_length = 0;
        assert_eq!(
            principal(Some(&grants), true, true, &limits)
                .unwrap_err()
                .message,
            "identity cannot be empty"
        );
        grants.identity = "a".into();
        assert_eq!(
            principal(Some(&grants), true, true, &limits)
                .unwrap_err()
                .message,
            "participant identity length exceeds limits: max length -1"
        );
        assert!(principal(Some(&grants), true, false, &limits).is_ok());
    }
}
