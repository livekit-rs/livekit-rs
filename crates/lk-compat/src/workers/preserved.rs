//! Request/response-owned protobuf provenance, never a shared unknown-field cache.
use super::error::IoError;
use crate::sip::wire::PreservedMessage;
use prost::{Message, Name};
#[derive(Clone, Debug)]
pub struct WireValue<T> {
    pub message: T,
    pub wire: PreservedMessage,
}
impl<T: Message + Name + Default> WireValue<T> {
    pub fn decode(bytes: &[u8]) -> Result<Self, IoError> {
        let wire = PreservedMessage::decode(&T::full_name(), bytes).map_err(|_| {
            use lk_protocol::json::wire_error::{WireDecodeFailure, classify};
            if classify(&T::full_name(), bytes) == WireDecodeFailure::InvalidUtf8 {
                IoError::ordinary("string field contains invalid UTF-8")
            } else {
                IoError::ordinary("proto: cannot parse invalid wire-format data")
            }
        })?;
        let message = wire.project();
        Ok(Self { message, wire })
    }
    pub fn from_message(message: T) -> Self {
        let wire = PreservedMessage::from_message(&message);
        Self { message, wire }
    }
    pub fn with_message(mut self, message: T) -> Self {
        self.wire = self.wire.overlay(&message);
        self.message = message;
        self
    }
    pub fn encode(&self) -> Vec<u8> {
        self.wire.overlay(&self.message).encode_to_vec()
    }
}
