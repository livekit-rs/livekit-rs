//! Fixed protocol/utils/xtwirp conversion for PSRPC worker failures.
//! Shared worker gateway conversion preserves actual structured peer details.
//! SIPStatus/SIPTransferError are the only ErrorMeta implementations in the
//! pinned protocol source. No arbitrary Any type is invented as custom metadata.
use crate::{
    service_bus::rpc::RpcError,
    twirp::error::{Code, TwirpError},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use lk_protocol::livekit as pb;
use prost::Message;

// Exact google.rpc.Status wire schema. Private encoder only, not an invented
// application protocol/type registration. Oracle marshals the actual Go Status.
#[derive(Clone, PartialEq, Message)]
struct StatusWire {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}
fn code(value: &str) -> (Code, i32) {
    match value {
        "canceled" => (Code::Canceled, 1),
        "invalid_argument" | "malformed_request" | "upstream_client_error" => {
            (Code::InvalidArgument, 3)
        }
        "deadline_exceeded" => (Code::DeadlineExceeded, 4),
        "not_found" => (Code::NotFound, 5),
        "already_exists" => (Code::AlreadyExists, 6),
        "permission_denied" => (Code::PermissionDenied, 7),
        "resource_exhausted" => (Code::ResourceExhausted, 8),
        "failed_precondition" => (Code::FailedPrecondition, 9),
        "aborted" => (Code::Aborted, 10),
        "out_of_range" => (Code::OutOfRange, 11),
        "unimplemented" => (Code::Unimplemented, 12),
        "internal" | "malformed_result" | "upstream_server_error" => (Code::Internal, 13),
        "unavailable" => (Code::Unavailable, 14),
        "data_loss" => (Code::DataLoss, 15),
        "unauthenticated" => (Code::Unauthenticated, 16),
        _ => (Code::Unknown, 2),
    }
}
fn sip_status(meta: &mut std::collections::BTreeMap<String, String>, value: &pb::SipStatus) {
    let status = if value.status.is_empty() {
        pb::SipStatusCode::try_from(value.code)
            .map(|v| v.as_str_name().to_owned())
            .unwrap_or_else(|_| value.code.to_string())
    } else {
        value.status.clone()
    };
    meta.insert("sip_status_code".into(), value.code.to_string());
    meta.insert("sip_status".into(), status);
}
fn custom_detail(
    meta: &mut std::collections::BTreeMap<String, String>,
    detail: &pbjson_types::Any,
) {
    let name = detail.type_url.rsplit('/').next().unwrap_or("");
    match name {
        "livekit.SIPStatus" => {
            if let Ok(value) = lk_protocol::wire::decode::<pb::SipStatus>(&detail.value) {
                sip_status(meta, &value);
            }
        }
        "livekit.SIPTransferError" => {
            if let Ok(value) = lk_protocol::wire::decode::<pb::SipTransferError>(&detail.value) {
                let reason = pb::SipTransferReason::try_from(value.reason)
                    .map(|v| v.as_str_name().to_owned())
                    .unwrap_or_else(|_| value.reason.to_string());
                meta.insert("sip_transfer_reason".into(), reason);
                if !value.transfer_id.is_empty() {
                    meta.insert("sip_transfer_id".into(), value.transfer_id);
                }
                if let Some(status) = value.sip_status {
                    sip_status(meta, &status);
                }
            }
        }
        _ => {}
    }
}
/// Application metadata remains local. Peer detail order determines overwrite
/// order exactly like fixed xtwirp.WithDetailsFromStatus. Unknown/malformed
/// details remain byte-for-byte Any payloads in the full Status metadata.
pub(crate) fn failure(error: RpcError) -> TwirpError {
    let wires = match error.error_detail_wires() {
        Ok(wires) => wires,
        Err(invalid) => return TwirpError::new(Code::Internal, invalid.message),
    };
    let (code, grpc_code) = code(&error.code);
    let mut result = TwirpError::new(code, error.message.clone());
    result.meta = error.application_metadata;
    if !error.details.is_empty() {
        for detail in &error.details {
            custom_detail(&mut result.meta, detail);
        }
        let status = StatusWire {
            code: grpc_code,
            message: error.message,
        };
        let mut bytes = status.encode_to_vec();
        for wire in wires {
            prost::encoding::encode_key(3, prost::encoding::WireType::LengthDelimited, &mut bytes);
            prost::encoding::encode_varint(wire.len() as u64, &mut bytes);
            bytes.extend(wire);
        }
        result
            .meta
            .insert("error_details".into(), STANDARD.encode(bytes));
    }
    result
}
// Expectations come from execution of the pinned Go implementation.
#[cfg(test)]
mod oracle_tests {
    use super::*;
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Detail {
        type_url: String,
        value_hex: String,
    }
    #[derive(Deserialize)]
    struct Row {
        id: String,
        rpc_code: String,
        rpc_message: String,
        details: Vec<Detail>,
        twirp_code: String,
        twirp_message: String,
        meta: std::collections::BTreeMap<String, String>,
    }
    fn bytes(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0);
        hex.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect()
    }
    #[test]
    fn actual_fixed_go_xtwirp_rows_are_exact() {
        // Coordinator sets this to the committed generated Go JSONL fixture,
        // never a Rust-generated expected result.
        let source = include_str!(
            "../../../../tests/fixtures/upstream/evidence/2026-10-25/f07-worker-error-go-expanded-actual.jsonl"
        );
        let mut count = 0;
        for line in source.lines().filter(|l| !l.trim().is_empty()) {
            let row: Row = serde_json::from_str(line).unwrap();
            count += 1;
            let mut error = RpcError::new(&row.rpc_code, row.rpc_message);
            error.origin = crate::service_bus::rpc::RpcErrorOrigin::Remote;
            error.details = row
                .details
                .into_iter()
                .map(|d| pbjson_types::Any {
                    type_url: d.type_url,
                    value: bytes(&d.value_hex).into(),
                })
                .collect();
            let actual = failure(error);
            assert_eq!(actual.code.as_str(), row.twirp_code, "{} code", row.id);
            assert_eq!(actual.msg, row.twirp_message, "{} message", row.id);
            assert_eq!(
                actual.meta, row.meta,
                "{} full metadata including exact base64 Status",
                row.id
            );
        }
        assert_eq!(count, 37, "all actual Go oracle rows retained");
    }
    #[test]
    fn local_application_metadata_is_preserved_without_invented_details() {
        let result = failure(
            RpcError::new("invalid_argument", "source local validation")
                .with_application_metadata("argument", "metadata size exceeds limits"),
        );
        assert_eq!(
            result.meta.get("argument").map(String::as_str),
            Some("metadata size exceeds limits")
        );
        assert!(!result.meta.contains_key("error_details"));
    }
}
