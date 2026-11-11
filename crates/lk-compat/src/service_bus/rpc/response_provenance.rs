//! Request-owned extraction of actual internal.Response error_details children.
//! Private service_bus::rpc child module. No Response root transplant.
use super::{RpcError, pb};
use crate::sip::wire::PreservedMessage;
use prost::{
    Message,
    encoding::{
        DecodeContext, WireType, decode_key, decode_varint, encode_key, encode_varint, skip_field,
    },
};

pub(super) struct ReceivedResponse {
    pub message: pb::Response,
    /// Go-canonical known Any fields plus original canonical-key unknown slices.
    pub error_detail_wire: Vec<Vec<u8>>,
}
fn malformed() -> RpcError {
    RpcError::malformed_response("invalid response envelope")
}
/// Caller already enforces encoded/decompressed bounds. A selected response
/// owns these vectors only until consumed into its local RpcError.
pub(super) fn decode_response_body(raw: &[u8]) -> Result<ReceivedResponse, RpcError> {
    let mut remaining = raw;
    let mut projected = Vec::with_capacity(raw.len());
    let mut error_detail_wire = Vec::new();
    while !remaining.is_empty() {
        let before = remaining;
        let (tag, kind) = decode_key(&mut remaining).map_err(|_| malformed())?;
        // Fixed internal.Response: response#4 is Any, error_details#8 repeated Any.
        if (tag == 8 || tag == 4) && kind == WireType::LengthDelimited {
            let size = usize::try_from(decode_varint(&mut remaining).map_err(|_| malformed())?)
                .map_err(|_| malformed())?;
            if size > remaining.len() {
                return Err(malformed());
            }
            let (body, rest) = remaining.split_at(size);
            remaining = rest;
            let retained =
                PreservedMessage::decode("google.protobuf.Any", body).map_err(|_| malformed())?;
            // Keep each occurrence's original known-field bytes until the
            // enclosing generated Response merges singular response#4 values.
            // Canonicalizing a child here would drop explicit empty scalars,
            // changing a later clear into a no-op during that parent merge.
            let known = lk_protocol::wire::filter_known("google.protobuf.Any", body)
                .map_err(|_| malformed())?;
            // This Any is a typed known projection only; retained independently
            // owns the canonical unknown-preserving repeated detail wrapper.
            pbjson_types::Any::decode(known.as_slice()).map_err(|_| malformed())?;
            encode_key(tag, WireType::LengthDelimited, &mut projected);
            encode_varint(known.len() as u64, &mut projected);
            projected.extend(known);
            if tag == 8 {
                error_detail_wire.push(retained.encode_to_vec());
            }
        } else {
            // Actual prost framing/group checker, depth bounded by DecodeContext.
            // Non-Any fields retain the pre-existing generated Response contract.
            skip_field(kind, tag, &mut remaining, DecodeContext::default())
                .map_err(|_| malformed())?;
            let consumed = before.len() - remaining.len();
            projected.extend_from_slice(&before[..consumed]);
        }
    }
    let message = pb::Response::decode(projected.as_slice()).map_err(|_| malformed())?;
    if message.error_details.len() != error_detail_wire.len() {
        return Err(malformed());
    }
    Ok(ReceivedResponse {
        message,
        error_detail_wire,
    })
}

/// Only actual repeated Any children are copied. Response root unknowns, IDs and
/// unrelated legacy Any data are never transplanted into error metadata.
pub(super) fn encode_response_body(response: &pb::Response, details: &[Vec<u8>]) -> Vec<u8> {
    let mut projected = response.clone();
    projected.error_details.clear();
    let mut output = projected.encode_to_vec();
    for detail in details {
        encode_key(8, WireType::LengthDelimited, &mut output);
        encode_varint(detail.len() as u64, &mut output);
        output.extend_from_slice(detail);
    }
    output
}
