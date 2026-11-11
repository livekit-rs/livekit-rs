//! Twirp request/response encoding (impl-notes §2.2).
//!
//! Requests: JSON is read as the first JSON value of the body (trailing bytes are
//! ignored like `json.Decoder`), then decoded with the pbjson deserializer of the
//! primary type set, which accepts both camelCase and snake_case and ignores
//! unknown fields. Protobuf bodies use `prost::Message::decode`.
//!
//! Responses: JSON via the `twirp_json` twin (snake_case, every field emitted);
//! protobuf as binary.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::error::TwirpError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Json,
    Protobuf,
}

pub const JSON_DECODE_ERROR: &str = "the json request could not be decoded";
pub const PROTOBUF_DECODE_ERROR: &str = "the protobuf request could not be decoded";

impl Codec {
    /// Media type before `;`, trimmed and lowercased.
    pub fn from_content_type(value: &str) -> Option<Codec> {
        let media = value
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match media.as_str() {
            "application/json" => Some(Codec::Json),
            "application/protobuf" => Some(Codec::Protobuf),
            _ => None,
        }
    }

    pub fn body_read_error(self) -> TwirpError {
        match self {
            Codec::Json => TwirpError::malformed(JSON_DECODE_ERROR),
            Codec::Protobuf => TwirpError::malformed("failed to read request body"),
        }
    }
}

pub fn decode<T>(codec: Codec, body: &[u8]) -> Result<T, TwirpError>
where
    T: prost::Message + prost::Name + Default,
{
    match codec {
        Codec::Json => {
            lk_protocol::json::decode(body).map_err(|_| TwirpError::malformed(JSON_DECODE_ERROR))
        }
        Codec::Protobuf => lk_protocol::wire::decode::<T>(body)
            .map_err(|_| TwirpError::malformed(PROTOBUF_DECODE_ERROR)),
    }
}

/// Encode `msg` (primary type `A`) as the response; `B` is its `twirp_json` twin.
pub fn encode<A, B>(codec: Codec, msg: &A) -> Response
where
    A: prost::Message + prost::Name,
    B: prost::Message + Default + Serialize,
{
    match codec {
        Codec::Json => match lk_protocol::to_twirp_json::<A, B>(msg) {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                bytes,
            )
                .into_response(),
            Err(e) => TwirpError::internal(format!("failed to marshal json response: {e}"))
                .into_response(),
        },
        Codec::Protobuf => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/protobuf")],
            msg.encode_to_vec(),
        )
            .into_response(),
    }
}

/// Render a request-local wire-preserving response. Unknown protobuf fields
/// remain in binary output and are omitted from JSON by the real descriptor.
pub fn encode_preserved(
    codec: Codec,
    wire: &prost_reflect::DynamicMessage,
    raw_bytes: Vec<u8>,
) -> Response {
    let (content_type, result) = match codec {
        Codec::Json => (
            "application/json",
            lk_protocol::json::encode_twirp_dynamic(wire),
        ),
        Codec::Protobuf => ("application/protobuf", Ok(raw_bytes)),
    };
    match result {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, content_type)],
            bytes,
        )
            .into_response(),
        Err(error) => TwirpError::internal(format!("failed to marshal json response: {error}"))
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lk_protocol::livekit as pb;
    use lk_protocol::twirp_json::livekit as tj;

    #[test]
    fn content_types() {
        assert_eq!(
            Codec::from_content_type("application/json;charset=UTF-8"),
            Some(Codec::Json)
        );
        assert_eq!(
            Codec::from_content_type(" Application/Protobuf "),
            Some(Codec::Protobuf)
        );
        assert_eq!(Codec::from_content_type("text/plain"), None);
        assert_eq!(Codec::from_content_type(""), None);
    }

    #[test]
    fn json_requests_accept_both_name_styles() {
        let a: pb::CreateRoomRequest = decode(
            Codec::Json,
            br#"{"name":"r","emptyTimeout":5,"max_participants":2,"bogus":1} trailing"#,
        )
        .unwrap();
        assert_eq!(
            (a.name.as_str(), a.empty_timeout, a.max_participants),
            ("r", 5, 2)
        );
        assert!(decode::<pb::CreateRoomRequest>(Codec::Json, b"").is_err());
        assert!(decode::<pb::CreateRoomRequest>(Codec::Json, b"[1]").is_err());
        let e = decode::<pb::CreateRoomRequest>(Codec::Protobuf, &[0xff]).unwrap_err();
        assert_eq!(e.msg, PROTOBUF_DECODE_ERROR);
    }

    #[test]
    fn typed_json_preserves_known_duplicate_and_alias_tokens() {
        for body in [
            &br#"{"room":"a","room":"b"}"#[..],
            &br#"{"identity":"a","identity":"b"}"#[..],
        ] {
            let error = decode::<pb::RoomParticipantIdentity>(Codec::Json, body).unwrap_err();
            assert_eq!(error.code, super::super::error::Code::Malformed);
            assert_eq!(error.msg, JSON_DECODE_ERROR);
        }
        for body in [
            &br#"{"emptyTimeout":1,"empty_timeout":2}"#[..],
            &br#"{"empty_timeout":1,"emptyTimeout":2}"#[..],
            &br#"{"name":"a","name":"b"} trailing"#[..],
        ] {
            assert!(decode::<pb::CreateRoomRequest>(Codec::Json, body).is_err());
        }
        assert!(
            decode::<pb::ListRoomsRequest>(Codec::Json, br#"{"names":["a"],"names":["b"]}"#)
                .is_err()
        );
        assert!(
            decode::<pb::CreateSipInboundTrunkRequest>(
                Codec::Json,
                br#"{"trunk":{"name":"a","name":"b"}}"#
            )
            .is_err()
        );
    }

    #[test]
    fn typed_json_keeps_first_value_unknown_fields_and_valid_aliases() {
        let request: pb::CreateRoomRequest = decode(
            Codec::Json,
            br#"{"name":"r","empty_timeout":7,"future":1,"future":{"x":true}} invalid trailing {"#,
        )
        .unwrap();
        assert_eq!(request.name, "r");
        assert_eq!(request.empty_timeout, 7);
        let request: pb::CreateRoomRequest = decode(
            Codec::Json,
            br#"{"name":"r","emptyTimeout":8} {"name":"ignored"}"#,
        )
        .unwrap();
        assert_eq!(request.empty_timeout, 8);
        assert_eq!(request.name, "r");
        for body in [&b""[..], &b"null"[..], &b"[]"[..], &b"{\"name\":"[..]] {
            assert!(decode::<pb::CreateRoomRequest>(Codec::Json, body).is_err());
        }
        // A known numeric enum remains accepted. Future numeric/symbolic enum
        // equivalence requires a separate protojson completeness correction.
        let request: pb::CreateIngressRequest = decode(Codec::Json, br#"{"inputType":1}"#).unwrap();
        assert_eq!(request.input_type, 1);
    }

    #[test]
    fn json_response_is_snake_case_with_defaults() {
        let room = pb::Room {
            name: "r".into(),
            creation_time: 1_700_000_000,
            ..Default::default()
        };
        let bytes = lk_protocol::to_twirp_json::<pb::Room, tj::Room>(&room).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["name"], "r");
        assert_eq!(v["creation_time"], "1700000000");
        assert_eq!(v["empty_timeout"], 0);
        assert_eq!(v["turn_password"], "");
    }
}
