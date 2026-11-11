//! Binary Blob provenance and exact fixed-Go key.String() equality classes.
//! Source: protobuf v1.36.12 prototext/encode.go marshalUnknown; LiveKit sources
//! in F27 SOURCE.json. No Go-formatted text or unstable whitespace is stored.
use super::encode::{self, Inbound};
use crate::sip::wire::{PreservedMessage, WireError};
use lk_protocol::livekit as pb;
use rtc_core::{
    ids::Identity,
    model::{DataBlob, DataBlobKey},
    session::{SessionRequest, SignalEvent},
};

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Inbound, WireError> {
    let request = PreservedMessage::decode("livekit.SignalRequest", bytes)?;
    let typed: pb::SignalRequest = request.project();
    match typed.message {
        Some(pb::signal_request::Message::StoreDataBlobRequest(typed)) => {
            let request = request
                .nested("store_data_blob_request")
                .ok_or(WireError::Traversal("missing Blob request"))?;
            let blob = request
                .nested("blob")
                .map(|blob| -> Result<_, WireError> {
                    let typed: pb::DataBlob = blob.project();
                    Ok(DataBlob {
                        key: blob.nested("key").map(key_from_wire).transpose()?,
                        contents: typed.contents,
                        wire: Some(blob.encode_to_vec()),
                    })
                })
                .transpose()?;
            Ok(Inbound::Forward(SessionRequest::StoreDataBlob {
                request_id: typed.request_id,
                blob,
            }))
        }
        Some(pb::signal_request::Message::GetDataBlobRequest(typed)) => {
            let request = request
                .nested("get_data_blob_request")
                .ok_or(WireError::Traversal("missing Blob request"))?;
            Ok(Inbound::Forward(SessionRequest::GetDataBlob {
                request_id: typed.request_id,
                participant_identity: Identity(typed.participant_identity),
                key: request.nested("key").map(key_from_wire).transpose()?,
            }))
        }
        _ => Err(WireError::Traversal("not a Blob request")),
    }
}
fn key_from_wire(wire: PreservedMessage) -> Result<DataBlobKey, WireError> {
    let typed: pb::DataBlobKey = wire.project();
    let mut key = encode::blob_key_from_proto(&typed);
    let root = unknown_identity(wire.unknown_wire())?;
    let schema = wire.nested("schema_id");
    let schema_unknown = schema
        .as_ref()
        .map(|s| unknown_identity(s.unknown_wire()))
        .transpose()?
        .unwrap_or_default();
    let encoding_unknown = schema
        .and_then(|s| s.nested("encoding"))
        .map(|s| unknown_identity(s.unknown_wire()))
        .transpose()?
        .unwrap_or_default();
    // Known message/oneof presence is already represented in key.value. Only
    // add nonempty unknown extensions; all-known binary and JSON keys must alias.
    if !root.is_empty() || !schema_unknown.is_empty() || !encoding_unknown.is_empty() {
        key.identity_extensions = vec![root, schema_unknown, encoding_unknown];
    }
    key.wire = Some(wire.encode_to_vec());
    Ok(key)
}

pub(super) fn response_wire(event: &SignalEvent) -> Option<Vec<u8>> {
    let (field, body, typed) = match event {
        SignalEvent::DataBlobStored { request_id, key } => {
            let raw = key.wire.as_ref()?;
            let key = PreservedMessage::decode("livekit.DataBlobKey", raw)
                .expect("previously validated Blob key");
            let known: pb::DataBlobKey = key.project();
            let reply = pb::StoreDataBlobResponse {
                request_id: *request_id,
                key: Some(known),
            };
            let mut body = PreservedMessage::from_message(&reply);
            body.replace_nested("key", Some(&key));
            (
                "store_data_blob_response",
                body,
                pb::signal_response::Message::StoreDataBlobResponse(reply),
            )
        }
        SignalEvent::DataBlobRetrieved { request_id, blob } => {
            let raw = blob.wire.as_ref()?;
            let blob = PreservedMessage::decode("livekit.DataBlob", raw)
                .expect("previously validated Blob");
            let known: pb::DataBlob = blob.project();
            let reply = pb::GetDataBlobResponse {
                request_id: *request_id,
                blob: Some(known),
            };
            let mut body = PreservedMessage::from_message(&reply);
            body.replace_nested("blob", Some(&blob));
            (
                "get_data_blob_response",
                body,
                pb::signal_response::Message::GetDataBlobResponse(reply),
            )
        }
        _ => return None,
    };
    let mut response = PreservedMessage::from_message(&pb::SignalResponse {
        message: Some(typed),
    });
    response.replace_nested(field, Some(&body));
    Some(response.encode_to_vec())
}

// Equality of prototext unknown output, not raw bytes or generic protobuf
// semantic equality: fixed32(1) and fixed64(1) BOTH print `N:0x1` in pinned Go.
// Unknown order remains significant, as does the group/message nesting level.
fn unknown_identity(bytes: &[u8]) -> Result<Vec<u8>, WireError> {
    let mut at = 0;
    let value = unknowns(bytes, &mut at, None, 100)?;
    if at != bytes.len() {
        return Err(WireError::Traversal("trailing unknown group"));
    }
    Ok(value)
}
fn varint(bytes: &[u8], at: &mut usize) -> Result<u64, WireError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let b = *bytes
            .get(*at)
            .ok_or(WireError::Traversal("truncated unknown varint"))?;
        *at += 1;
        if shift == 63 && b > 1 {
            return Err(WireError::Traversal("unknown varint overflow"));
        }
        value |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(value);
        }
    }
    Err(WireError::Traversal("unknown varint overflow"))
}
fn take<'a>(bytes: &'a [u8], at: &mut usize, length: usize) -> Result<&'a [u8], WireError> {
    let end = at
        .checked_add(length)
        .ok_or(WireError::Traversal("unknown value overflow"))?;
    let value = bytes
        .get(*at..end)
        .ok_or(WireError::Traversal("truncated unknown value"))?;
    *at = end;
    Ok(value)
}
fn put(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push(value as u8 | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn unknowns(
    bytes: &[u8],
    at: &mut usize,
    end: Option<u64>,
    depth: usize,
) -> Result<Vec<u8>, WireError> {
    if depth == 0 {
        return Err(WireError::Traversal("unknown group depth"));
    }
    let mut out = Vec::new();
    while *at < bytes.len() {
        let tag = varint(bytes, at)?;
        let number = tag >> 3;
        if number == 0 || number > 0x1fff_ffff {
            return Err(WireError::Traversal("unknown field number"));
        }
        let wire = tag & 7;
        if wire == 4 {
            if end == Some(number) {
                return Ok(out);
            }
            return Err(WireError::Traversal("unknown end group mismatch"));
        }
        put(&mut out, number);
        let (kind, value) = match wire {
            0 => {
                let v = varint(bytes, at)?;
                let mut out = Vec::new();
                put(&mut out, v);
                (0u8, out)
            }
            1 => (1, take(bytes, at, 8)?.to_vec()),
            5 => {
                let v = take(bytes, at, 4)?;
                let mut out = v.to_vec();
                out.extend([0u8; 4]);
                (1, out)
            }
            2 => {
                let length = usize::try_from(varint(bytes, at)?)
                    .map_err(|_| WireError::Traversal("unknown byte length overflow"))?;
                (2, take(bytes, at, length)?.to_vec())
            }
            3 => (3, unknowns(bytes, at, Some(number), depth - 1)?),
            _ => return Err(WireError::Traversal("unknown wire kind")),
        };
        out.push(kind);
        put(&mut out, value.len() as u64);
        out.extend(value);
    }
    if end.is_some() {
        return Err(WireError::Traversal("unterminated unknown group"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    fn field(number: u32, bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        put(&mut out, (u64::from(number) << 3) | 2);
        put(&mut out, bytes.len() as u64);
        out.extend(bytes);
        out
    }
    fn request_id(number: u32) -> Vec<u8> {
        let mut out = vec![8];
        put(&mut out, u64::from(number));
        out
    }
    fn same_tree(actual: &PreservedMessage, expected: &PreservedMessage, children: &[&str]) {
        assert_eq!(
            actual.unknown_wire(),
            expected.unknown_wire(),
            "source unknown value bytes"
        );
        for name in children {
            match (actual.nested(name), expected.nested(name)) {
                (Some(a), Some(b)) => same_tree(&a, &b, &["blob", "key", "schema_id", "encoding"]),
                (None, None) => (),
                _ => panic!("nested presence mismatch {name}"),
            }
        }
    }
    #[test]
    fn unknown_text_identity_keeps_order_but_unifies_fixed_width_and_overlong_values() {
        assert_eq!(
            unknown_identity(&[0xa0, 6, 1]).unwrap(),
            unknown_identity(&[0xa0, 6, 0x81, 0]).unwrap()
        );
        assert_eq!(
            unknown_identity(&[0xa5, 6, 1, 0, 0, 0]).unwrap(),
            unknown_identity(&[0xa1, 6, 1, 0, 0, 0, 0, 0, 0, 0]).unwrap()
        );
        assert_ne!(
            unknown_identity(&[0xa0, 6, 1]).unwrap(),
            unknown_identity(&[0xa5, 6, 1, 0, 0, 0]).unwrap()
        );
        assert_ne!(
            unknown_identity(&[0xa0, 6, 1, 0xa8, 6, 2]).unwrap(),
            unknown_identity(&[0xa8, 6, 2, 0xa0, 6, 1]).unwrap()
        );
    }
    #[test]
    #[ignore = "requires independently executed fixed-Go participant blob oracle"]
    fn actual_go_blob_key_identity_and_unknown_response_provenance_match_all_26_cases() {
        let cases =
            std::fs::read(std::env::var("BLOB_GO_CASES").expect("actual cases path")).unwrap();
        let cases: Vec<serde_json::Value> = serde_json::from_slice(&cases).unwrap();
        let results = std::fs::read_to_string(
            std::env::var("BLOB_GO_RESULTS").expect("actual fixed-Go JSONL path"),
        )
        .unwrap();
        let results: Vec<serde_json::Value> = results
            .lines()
            .filter(|s| !s.trim().is_empty())
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(cases.len(), 26);
        assert_eq!(results.len(), 26);
        let mut ids = std::collections::BTreeSet::new();
        for (case, result) in cases.iter().zip(&results) {
            assert_eq!(case["id"], result["id"]);
            assert!(ids.insert(case["id"].as_str().unwrap()));
            let left = STANDARD.decode(case["left"].as_str().unwrap()).unwrap();
            let right = STANDARD.decode(case["right"].as_str().unwrap()).unwrap();
            let a = key_from_wire(PreservedMessage::decode("livekit.DataBlobKey", &left).unwrap())
                .unwrap();
            let b = key_from_wire(PreservedMessage::decode("livekit.DataBlobKey", &right).unwrap())
                .unwrap();
            assert_eq!(
                a == b,
                result["equal"].as_bool().unwrap(),
                "actual Go String equality: {}",
                case["id"]
            );
            assert_eq!(
                a == b,
                result["get"].as_bool().unwrap(),
                "actual Go participant store lookup: {}",
                case["id"]
            );
            let event = SignalEvent::DataBlobStored {
                request_id: 71,
                key: a,
            };
            let actual = response_wire(&event).unwrap();
            let expected = STANDARD
                .decode(result["store_response"].as_str().unwrap())
                .unwrap();
            let a = PreservedMessage::decode("livekit.SignalResponse", &actual).unwrap();
            let b = PreservedMessage::decode("livekit.SignalResponse", &expected).unwrap();
            assert_eq!(
                a.project::<pb::SignalResponse>(),
                b.project::<pb::SignalResponse>(),
                "stored known response: {}",
                case["id"]
            );
            same_tree(&a, &b, &["store_data_blob_response"]);
            let mut blob = field(1, &left);
            blob.extend(field(2, &[0, 255, 1]));
            blob.extend(
                STANDARD
                    .decode(case["blob_unknown"].as_str().unwrap())
                    .unwrap(),
            );
            let mut store = request_id(71);
            store.extend(field(2, &blob));
            let raw = field(22, &store);
            let Inbound::Forward(SessionRequest::StoreDataBlob {
                request_id: 71,
                blob: Some(blob),
            }) = decode_request(&raw).unwrap()
            else {
                panic!("store provenance must survive real signal decoder")
            };
            if result["get"].as_bool().unwrap() {
                let actual = response_wire(&SignalEvent::DataBlobRetrieved {
                    request_id: 72,
                    blob,
                })
                .unwrap();
                let expected = STANDARD
                    .decode(result["get_response"].as_str().unwrap())
                    .unwrap();
                let a = PreservedMessage::decode("livekit.SignalResponse", &actual).unwrap();
                let b = PreservedMessage::decode("livekit.SignalResponse", &expected).unwrap();
                assert_eq!(
                    a.project::<pb::SignalResponse>(),
                    b.project::<pb::SignalResponse>(),
                    "get known response: {}",
                    case["id"]
                );
                same_tree(&a, &b, &["get_data_blob_response"]);
            }
            // Authentic raw Get decoder uses the same key identity path.
            let mut get = request_id(72);
            get.extend(field(2, b"publisher"));
            get.extend(field(3, &right));
            let Inbound::Forward(SessionRequest::GetDataBlob {
                key: Some(decoded), ..
            }) = decode_request(&field(23, &get)).unwrap()
            else {
                panic!("get decoder")
            };
            assert_eq!(decoded, b_key(&right));
        }
    }
    fn b_key(raw: &[u8]) -> DataBlobKey {
        key_from_wire(PreservedMessage::decode("livekit.DataBlobKey", raw).unwrap()).unwrap()
    }
}
