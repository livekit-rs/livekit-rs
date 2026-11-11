//! Twirp v8 error envelope (`twirp/errors.go`, `writeError`).

use std::collections::BTreeMap;

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use rtc_runtime::ApiError;
use serde::Serialize;

use crate::auth::permissions::PermissionDenied;
use crate::rpc::{RpcErrorClass, RpcFailure};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    Canceled,
    Unknown,
    InvalidArgument,
    Malformed,
    DeadlineExceeded,
    NotFound,
    BadRoute,
    AlreadyExists,
    PermissionDenied,
    Unauthenticated,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
}

impl Code {
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Canceled => "canceled",
            Code::Unknown => "unknown",
            Code::InvalidArgument => "invalid_argument",
            Code::Malformed => "malformed",
            Code::DeadlineExceeded => "deadline_exceeded",
            Code::NotFound => "not_found",
            Code::BadRoute => "bad_route",
            Code::AlreadyExists => "already_exists",
            Code::PermissionDenied => "permission_denied",
            Code::Unauthenticated => "unauthenticated",
            Code::ResourceExhausted => "resource_exhausted",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            Code::OutOfRange => "out_of_range",
            Code::Unimplemented => "unimplemented",
            Code::Internal => "internal",
            Code::Unavailable => "unavailable",
            Code::DataLoss => "data_loss",
        }
    }

    /// `ServerHTTPStatusFromErrorCode` (twirp/errors.go:267-310).
    pub fn http_status(self) -> u16 {
        match self {
            Code::Canceled => 408,
            Code::Unknown => 500,
            Code::InvalidArgument => 400,
            Code::Malformed => 400,
            Code::DeadlineExceeded => 408,
            Code::NotFound => 404,
            Code::BadRoute => 404,
            Code::AlreadyExists => 409,
            Code::PermissionDenied => 403,
            Code::Unauthenticated => 401,
            Code::ResourceExhausted => 429,
            Code::FailedPrecondition => 412,
            Code::Aborted => 409,
            Code::OutOfRange => 400,
            Code::Unimplemented => 501,
            Code::Internal => 500,
            Code::Unavailable => 503,
            Code::DataLoss => 500,
        }
    }
}

/// Maximum `msg` length (twirp truncates to 1e6 bytes).
const MAX_MSG_BYTES: usize = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TwirpError {
    pub code: Code,
    pub msg: String,
    pub meta: BTreeMap<String, String>,
}

impl TwirpError {
    pub fn new(code: Code, msg: impl Into<String>) -> Self {
        Self {
            code,
            msg: msg.into(),
            meta: BTreeMap::new(),
        }
    }

    pub fn with_meta(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.meta.insert(k.into(), v.into());
        self
    }

    /// `twirp.InvalidArgumentError(argument, validationMsg)`: msg is
    /// `argument + " " + validationMsg`, meta `{"argument": argument}`.
    pub fn invalid_argument_arg(argument: &str, validation_msg: &str) -> Self {
        Self::new(
            Code::InvalidArgument,
            format!("{argument} {validation_msg}"),
        )
        .with_meta("argument", argument)
    }

    /// `badRouteError`.
    pub fn bad_route(msg: impl Into<String>, method: &str, path: &str) -> Self {
        Self::new(Code::BadRoute, msg).with_meta("twirp_invalid_route", format!("{method} {path}"))
    }

    pub fn malformed(msg: impl Into<String>) -> Self {
        Self::new(Code::Malformed, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(Code::Internal, msg)
    }

    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.code.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// `{"code":..,"msg":..,"meta":{..}}`, meta omitted when empty.
    pub fn to_json(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Body<'a> {
            code: &'a str,
            msg: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            meta: Option<&'a BTreeMap<String, String>>,
        }
        let mut end = self.msg.len().min(MAX_MSG_BYTES);
        while !self.msg.is_char_boundary(end) {
            end -= 1;
        }
        serde_json::to_vec(&Body {
            code: self.code.as_str(),
            msg: &self.msg[..end],
            meta: (!self.meta.is_empty()).then_some(&self.meta),
        })
        .unwrap_or_else(|_| br#"{"code":"internal","msg":"There was an error but it could not be serialized into JSON"}"#.to_vec())
    }
}

impl IntoResponse for TwirpError {
    fn into_response(self) -> Response {
        (
            self.status(),
            [(header::CONTENT_TYPE, "application/json")],
            self.to_json(),
        )
            .into_response()
    }
}

/// Every Ensure*Permission failure is reported as `unauthenticated` (auth.go:262-264).
impl From<PermissionDenied> for TwirpError {
    fn from(e: PermissionDenied) -> Self {
        Self::new(Code::Unauthenticated, e.to_string())
    }
}

impl From<ApiError> for TwirpError {
    fn from(e: ApiError) -> Self {
        match e {
            ApiError::MetadataLimit(m) => Self::new(Code::Unknown, m),
            ApiError::NotFound(m) => Self::new(Code::NotFound, m),
            ApiError::Unavailable(m) => Self::new(Code::Unavailable, m),
            ApiError::FailedPrecondition(m) => Self::new(Code::FailedPrecondition, m),
            ApiError::InvalidArgument(m) => Self::new(Code::InvalidArgument, m),
            ApiError::Internal(m) => Self::new(Code::Internal, m),
        }
    }
}

impl From<RpcFailure> for TwirpError {
    fn from(e: RpcFailure) -> Self {
        match e {
            RpcFailure::Api(e) => e.into(),
            RpcFailure::Rpc(e) => {
                let code = match e.class() {
                    RpcErrorClass::Internal => Code::Internal,
                    RpcErrorClass::Canceled => Code::Canceled,
                    RpcErrorClass::Unavailable => Code::Unavailable,
                    RpcErrorClass::InvalidArgument => Code::InvalidArgument,
                    RpcErrorClass::NotFound => Code::NotFound,
                    RpcErrorClass::Unimplemented => Code::Unimplemented,
                };
                Self::new(code, e.to_string())
            }
        }
    }
}

/// Go `%q` for the ASCII/UTF-8 strings used in route errors.
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{07}' => out.push_str("\\a"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\u{0B}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::DataChannelRpcError;

    #[test]
    fn envelope() {
        let e = TwirpError::new(Code::NotFound, "requested room does not exist");
        assert_eq!(
            String::from_utf8(e.to_json()).unwrap(),
            r#"{"code":"not_found","msg":"requested room does not exist"}"#
        );
        assert_eq!(e.status(), StatusCode::NOT_FOUND);
        let e = TwirpError::invalid_argument_arg("metadata size exceeds limits", "524288");
        assert_eq!(
            String::from_utf8(e.to_json()).unwrap(),
            r#"{"code":"invalid_argument","msg":"metadata size exceeds limits 524288","meta":{"argument":"metadata size exceeds limits"}}"#
        );
        let e = TwirpError::bad_route(
            format!(
                "unsupported method {} (only POST is allowed)",
                go_quote("GET")
            ),
            "GET",
            "/twirp/x",
        );
        assert_eq!(e.msg, r#"unsupported method "GET" (only POST is allowed)"#);
        assert_eq!(e.meta.get("twirp_invalid_route").unwrap(), "GET /twirp/x");
    }

    #[test]
    fn mappings() {
        let e: TwirpError = PermissionDenied.into();
        assert_eq!(
            (e.code, e.msg.as_str(), e.status().as_u16()),
            (Code::Unauthenticated, "permissions denied", 401)
        );
        let e: TwirpError = ApiError::FailedPrecondition("remote unmute not enabled".into()).into();
        assert_eq!(e.status().as_u16(), 412);
        let e: TwirpError = RpcFailure::Rpc(DataChannelRpcError::built_in(1502)).into();
        assert_eq!(
            (e.code, e.msg.as_str()),
            (Code::Canceled, "RpcError 1502: Response timeout")
        );
        assert_eq!(e.status().as_u16(), 408);
        let e: TwirpError = RpcFailure::Rpc(DataChannelRpcError::built_in(1402)).into();
        assert_eq!(e.status().as_u16(), 400);
    }

    #[test]
    fn quoting() {
        assert_eq!(go_quote("a\"b\\c\n"), r#""a\"b\\c\n""#);
        assert_eq!(go_quote("text/plain"), r#""text/plain""#);
    }
}

#[cfg(test)]
mod actual_owner_metadata_error_oracle {
    use super::*;
    #[test]
    #[ignore = "actual fixed Go signalling errors through xtwirp.ToError; OWNER_METADATA_GO_RESULTS"]
    fn ordinary_owner_metadata_errors_keep_actual_go_unknown_origin() {
        let output = std::fs::read_to_string(
            std::env::var("OWNER_METADATA_GO_RESULTS").expect("actual Go JSONL"),
        )
        .unwrap();
        let rows: Vec<serde_json::Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 3);
        for ((id, message), row) in [
            ("name", "name length exceeds limits"),
            ("metadata", "metadata size exceeds limits"),
            ("attributes", "attributes size exceeds limits"),
        ]
        .iter()
        .zip(rows)
        {
            assert_eq!(row["id"], *id);
            let error: TwirpError = ApiError::MetadataLimit((*message).into()).into();
            let actual: serde_json::Value = serde_json::from_slice(&error.to_json()).unwrap();
            assert_eq!(actual["code"], row["code"]);
            assert_eq!(actual["msg"], row["msg"]);
        }
    }
}
