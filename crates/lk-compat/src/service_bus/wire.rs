//! Bounded psrpc envelope codec. No subscriptions or request lifecycle live here.

use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use lk_protocol::psrpc::internal as pb;
use prost::Message;
use std::io::{Read, Write};

/// Explicit local policy, supplied on every codec call. Zero permits zero bytes;
/// unlike upstream's optional decompression cap it never means unlimited here.
#[derive(Clone, Copy, Debug)]
pub struct WireLimits {
    pub encoded_bytes: usize,
    pub decompressed_bytes: usize,
}

/// Publishing follows upstream quality/threshold/smaller-than-input behavior.
/// Nonpositive quality disables compression; positive quality clamps to nine.
#[derive(Clone, Copy, Debug, Default)]
pub struct CompressionPolicy {
    pub quality: i32,
    pub threshold: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("psrpc envelope exceeds {0} bytes")]
    EncodedLimit(usize),
    #[error("psrpc decoded message exceeds {0} bytes")]
    DecodedLimit(usize),
    #[error("psrpc byte limit cannot be represented")]
    InvalidLimit,
    #[error("psrpc malformed envelope: {0}")]
    Envelope(prost::DecodeError),
    #[error("psrpc malformed payload: {0}")]
    Payload(prost::DecodeError),
    #[error("psrpc unrecognized message compression {0}")]
    Compression(i32),
    #[error("psrpc gzip error: {0}")]
    Gzip(std::io::Error),
    #[error("psrpc envelope type does not match expected {expected}")]
    TypeMismatch { expected: &'static str },
}

/// Names are generated schema names, not Rust module paths.
pub trait BusMessage: Message + Default {
    const FULL_NAME: &'static str;
}
macro_rules! bus_types {
    ($($name:ident),* $(,)?) => {$(
        impl BusMessage for pb::$name { const FULL_NAME: &'static str = concat!("internal.", stringify!($name)); }
    )*};
}
bus_types!(Request, Response, ClaimRequest, ClaimResponse, Stream);

/// A published business message. `pkg/bus/bus.go:79` serializes the message
/// itself — `serialize(msg, channel.Local, …)` — so its `type_url` is the schema
/// name, unlike the `internal.*` envelopes an RPC request/response uses.
impl BusMessage for lk_protocol::rpc::KeepalivePing {
    const FULL_NAME: &'static str = "rpc.KeepalivePing";
}

/// WorkerRegistered publishes the actual business Empty, not an RPC response.
impl BusMessage for pbjson_types::Empty {
    const FULL_NAME: &'static str = "google.protobuf.Empty";
}

/// Envelope routing metadata survives decode independently of the typed body.
#[derive(Debug)]
pub struct Decoded<T> {
    pub message: T,
    pub local_channel: String,
    pub type_url: String,
    pub compression: i32,
}

pub fn encode<T: BusMessage>(
    message: &T,
    local_channel: &str,
    policy: CompressionPolicy,
    limits: WireLimits,
) -> Result<Vec<u8>, WireError> {
    if message.encoded_len() > limits.decompressed_bytes {
        return Err(WireError::DecodedLimit(limits.decompressed_bytes));
    }
    encode_raw::<T>(&message.encode_to_vec(), local_channel, policy, limits)
}

/// Encode an already validated original message body without projecting away
/// unknown child fields. All original envelope and decompressed limits apply.
pub fn encode_raw<T: BusMessage>(
    payload: &[u8],
    local_channel: &str,
    policy: CompressionPolicy,
    limits: WireLimits,
) -> Result<Vec<u8>, WireError> {
    if payload.len() > limits.decompressed_bytes {
        return Err(WireError::DecodedLimit(limits.decompressed_bytes));
    }
    // Metadata alone is also bounded before copying caller strings.
    if local_channel.len().saturating_add(T::FULL_NAME.len()) > limits.encoded_bytes {
        return Err(WireError::EncodedLimit(limits.encoded_bytes));
    }
    let mut value = payload.to_vec();
    let mut compression = 0;
    if policy.quality > 0 && value.len() >= policy.threshold {
        let mut gzip = GzEncoder::new(Vec::new(), Compression::new(policy.quality.min(9) as u32));
        gzip.write_all(&value).map_err(WireError::Gzip)?;
        let encoded = gzip.finish().map_err(WireError::Gzip)?;
        if encoded.len() < value.len() {
            value = encoded;
            compression = 1;
        }
    }
    let envelope = pb::Msg {
        type_url: format!("type.googleapis.com/{}", T::FULL_NAME),
        value,
        channel: local_channel.into(),
        compression,
    };
    if envelope.encoded_len() > limits.encoded_bytes {
        return Err(WireError::EncodedLimit(limits.encoded_bytes));
    }
    Ok(envelope.encode_to_vec())
}

/// Choose the actual schema pool for a bus payload. WorkerRegistered is a
/// business Empty from the pinned public AgentInternal schema, not an internal
/// PSRPC envelope. Its descriptor is intentionally absent from internal.proto.
pub(crate) fn filter_bus_known(
    name: &str,
    payload: &[u8],
) -> Result<Vec<u8>, lk_protocol::wire::WireError> {
    if name == "google.protobuf.Empty" {
        lk_protocol::wire::filter_known(name, payload)
    } else {
        lk_protocol::wire::filter_known_psrpc(name, payload)
    }
}

/// Local typed consumer projection uses the same fixed schema rule as the
/// forwarder. Original bytes stay owned by the subscription/response sidecar.
pub(crate) fn decode_psrpc<T: BusMessage>(
    bytes: &[u8],
    limits: WireLimits,
) -> Result<Decoded<T>, WireError> {
    let raw = decode_raw::<T>(bytes, limits)?;
    let known = filter_bus_known(T::FULL_NAME, &raw.payload)
        .map_err(|error| WireError::Payload(prost::DecodeError::new(error.to_string())))?;
    let message = T::decode(known.as_slice()).map_err(WireError::Payload)?;
    Ok(Decoded {
        message,
        local_channel: raw.local_channel,
        type_url: raw.type_url,
        compression: raw.compression,
    })
}

/// Original bounded, once-decompressed body and its actual routing metadata.
#[derive(Debug)]
pub struct RawDecoded {
    pub payload: Vec<u8>,
    pub local_channel: String,
    pub type_url: String,
    pub compression: i32,
}
pub fn decode<T: BusMessage>(bytes: &[u8], limits: WireLimits) -> Result<Decoded<T>, WireError> {
    let raw = decode_raw::<T>(bytes, limits)?;
    let message = T::decode(raw.payload.as_slice()).map_err(WireError::Payload)?;
    Ok(Decoded {
        message,
        local_channel: raw.local_channel,
        type_url: raw.type_url,
        compression: raw.compression,
    })
}
/// Raw seam for request-owned provenance. It performs the same gzip/type/size
/// validation as decode, never an unbounded decompression or second parse policy.
pub fn decode_raw<T: BusMessage>(
    bytes: &[u8],
    limits: WireLimits,
) -> Result<RawDecoded, WireError> {
    decode_raw_with_gzip_limit::<T>(bytes, limits, limits.decompressed_bytes)
}
/// Source localbus maxSize applies only to expanded GZIP, never NONE or publish.
/// Plain/practical resource limits remain independent and are not reduced.
pub(crate) fn decode_raw_with_gzip_limit<T: BusMessage>(
    bytes: &[u8],
    limits: WireLimits,
    gzip_limit: usize,
) -> Result<RawDecoded, WireError> {
    if bytes.len() > limits.encoded_bytes {
        return Err(WireError::EncodedLimit(limits.encoded_bytes));
    }
    let envelope = pb::Msg::decode(bytes).map_err(WireError::Envelope)?;
    // Go Any resolution uses the full message name after the last slash; its
    // resolver does not require the canonical type.googleapis.com prefix.
    if envelope.type_url.rsplit('/').next() != Some(T::FULL_NAME) {
        return Err(WireError::TypeMismatch {
            expected: T::FULL_NAME,
        });
    }
    let value = match envelope.compression {
        0 => {
            if envelope.value.len() > limits.decompressed_bytes {
                return Err(WireError::DecodedLimit(limits.decompressed_bytes));
            }
            envelope.value
        }
        1 => {
            let gzip_limit = gzip_limit.min(limits.decompressed_bytes);
            let max = gzip_limit.checked_add(1).ok_or(WireError::InvalidLimit)?;
            let mut reader = MultiGzDecoder::new(envelope.value.as_slice()).take(max as u64);
            let mut value = Vec::new();
            reader.read_to_end(&mut value).map_err(WireError::Gzip)?;
            if value.len() > gzip_limit {
                return Err(WireError::DecodedLimit(gzip_limit));
            }
            value
        }
        other => return Err(WireError::Compression(other)),
    };
    Ok(RawDecoded {
        payload: value,
        local_channel: envelope.channel,
        type_url: envelope.type_url,
        compression: envelope.compression,
    })
}

/// Current psrpc handler execution decodes raw_request, not the legacy Any.
/// Keep both schema fields intact, but never invent an Any-as-fallback policy.
pub fn request_payload<T: Message + Default>(
    request: &pb::Request,
    max_bytes: usize,
) -> Result<T, WireError> {
    if request.raw_request.len() > max_bytes {
        return Err(WireError::DecodedLimit(max_bytes));
    }
    T::decode(request.raw_request.as_slice()).map_err(WireError::Payload)
}

/// Response error/code/details remain in the envelope; callers must process an
/// error before decoding the raw response, matching the upstream client.
pub fn response_payload<T: Message + Default>(
    response: &pb::Response,
    max_bytes: usize,
) -> Result<T, WireError> {
    if response.raw_response.len() > max_bytes {
        return Err(WireError::DecodedLimit(max_bytes));
    }
    T::decode(response.raw_response.as_slice()).map_err(WireError::Payload)
}

#[cfg(test)]
mod tests;
