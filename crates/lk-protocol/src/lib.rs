//! LiveKit wire types. Only `lk-compat` and the `livecloud` assembly may depend on
//! this crate (ARCH-01); the RTC engine crates must not.

#![allow(clippy::all, clippy::pedantic, missing_docs)]

/// Primary type set: protobuf + default protojson (camelCase, defaults omitted).
pub mod livekit {
    include!(concat!(env!("OUT_DIR"), "/livekit.rs"));
    include!(concat!(env!("OUT_DIR"), "/livekit.serde.rs"));
}

pub mod logger {
    include!(concat!(env!("OUT_DIR"), "/logger.rs"));
}

/// Fixed internal worker RPC messages, sharing the public LiveKit wire types.
pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/rpc.rs"));
}

/// Twirp v8 JSON dialect (snake_case names, all fields emitted). Same wire format
/// as [`livekit`]; convert with [`to_twirp_json`].
pub mod twirp_json {
    pub mod livekit {
        include!(concat!(env!("OUT_DIR"), "/twirp_json/livekit.rs"));
        include!(concat!(env!("OUT_DIR"), "/twirp_json/livekit.serde.rs"));
    }
    pub mod logger {
        include!(concat!(env!("OUT_DIR"), "/twirp_json/logger.rs"));
    }
}

pub use pbjson_types as well_known;

/// Protocol version implemented by the pinned server (v1.13.10 `CurrentProtocol`).
pub const CURRENT_PROTOCOL: i32 = 17;
/// Fixed pkg/agent/worker.go CurrentProtocol, advertised by both RTC and worker services.
pub const CURRENT_AGENT_PROTOCOL: i32 = 1;

/// Re-encode a message of the primary set as its Twirp-dialect twin and render JSON.
pub fn to_twirp_json<A, B>(msg: &A) -> Result<Vec<u8>, serde_json::Error>
where
    A: prost::Message + prost::Name,
    B: prost::Message + Default + serde::Serialize,
{
    let bytes = msg.encode_to_vec();
    // Matching schema twins share the wire encoding. Surface an accidental
    // mismatched generic type instead of silently rendering an empty response.
    let twin =
        B::decode(bytes.as_slice()).map_err(<serde_json::Error as serde::ser::Error>::custom)?;
    let mut value = match serde_json::to_value(&twin) {
        Ok(value) => value,
        // Generated enum serializers require a known Rust variant. Real
        // protobuf enums also permit unknown i32 values; reflect those actual
        // wire values without defaulting or changing valid enum spellings.
        Err(_) => json::response::reflect_value(&A::full_name(), &bytes)?,
    };
    json::response::complete(&A::full_name(), &mut value)?;
    serde_json::to_vec(&value)
}

/// Pinned psrpc bus envelopes, isolated from public LiveKit protocol types.
pub mod psrpc {
    include!(concat!(env!("OUT_DIR"), "/psrpc.rs"));
    pub mod internal {
        include!(concat!(env!("OUT_DIR"), "/psrpc/internal.rs"));
    }
}

/// Descriptor-aware request JSON decoding, independent of response dialects.
pub mod json;

/// Fixed-Go compatible typed binary request projection.
pub mod wire;
