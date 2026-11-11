//! Shared worker-boundary codec with request-owned protobuf provenance.
//! Feature gateways depend on this adapter, never on another feature gateway.
use super::{
    codec::{Codec, decode, encode_preserved},
    error::TwirpError,
};
use crate::{sip::wire::PreservedMessage, workers::preserved::WireValue};
use axum::response::Response;
use prost::{Message, Name};
pub(crate) fn input<T: Message + Name + Default>(
    codec: Codec,
    bytes: &[u8],
) -> Result<WireValue<T>, TwirpError> {
    let message: T = decode(codec, bytes)?;
    let wire = match codec {
        Codec::Json => PreservedMessage::from_message(&message),
        Codec::Protobuf => PreservedMessage::decode(&T::full_name(), bytes)
            .map_err(|_| TwirpError::malformed(super::codec::PROTOBUF_DECODE_ERROR))?,
    };
    Ok(WireValue { message, wire })
}
pub(crate) fn output<T: Message + Name + Default>(codec: Codec, value: WireValue<T>) -> Response {
    let wire = value.wire.overlay(&value.message);
    encode_preserved(codec, wire.as_dynamic(), wire.encode_to_vec())
}
