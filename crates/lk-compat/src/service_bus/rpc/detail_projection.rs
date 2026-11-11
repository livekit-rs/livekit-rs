//! RpcError helper module; typed error metadata may be updated locally
//! without resurrecting stale known fields from a preserved Any wrapper.
use crate::sip::wire::PreservedMessage;
use prost::Message;
#[derive(Clone, PartialEq, Message)]
struct AnyProjection {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}
impl prost::Name for AnyProjection {
    const NAME: &'static str = "Any";
    const PACKAGE: &'static str = "google.protobuf";
}
/// `wire` is validated at received-response construction. Never decode opaque
/// Any.value as the wrapper, transplant Response roots or consult a global map.
pub(super) fn overlay_any(
    wire: &[u8],
    current: &pbjson_types::Any,
) -> Result<Vec<u8>, crate::sip::wire::WireError> {
    let source = PreservedMessage::decode("google.protobuf.Any", wire)?;
    Ok(source
        .overlay(&AnyProjection {
            type_url: current.type_url.clone(),
            value: current.value.to_vec(),
        })
        .encode_to_vec())
}
