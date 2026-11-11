//! Data-channel payload policy. Payload encoding is protocol specific, so the room
//! asks an adapter-provided [`DataHandler`] what to do with each inbound message.

use crate::ids::*;
use crate::media_api::DataKind;
use crate::model::ParticipantKind;

/// Sender of an inbound data message.
#[derive(Clone, Debug)]
pub struct DataSender<'a> {
    pub sid: &'a ParticipantSid,
    pub identity: &'a Identity,
    pub hidden: bool,
    pub kind: ParticipantKind,
    pub is_agent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataVerdict {
    Drop,
    /// Forward the (possibly rewritten) payload on the same kind of channel.
    Forward {
        payload: Vec<u8>,
        /// Empty = everyone except the sender.
        destination_identities: Vec<Identity>,
        /// Legacy `destination_sids`; a recipient matching either list receives it.
        destination_sids: Vec<ParticipantSid>,
        /// Reliable sequence number (0 = unsequenced).
        sequence: u32,
        /// `UserPacket.nonce` for 2 s duplicate suppression.
        nonce: Option<Vec<u8>>,
    },
    /// The adapter consumed the message (e.g. an RPC response to a server call).
    Consumed,
}

pub trait DataHandler: Send + Sync + 'static {
    /// Extract the reliable envelope sequence before payload policy or RPC side
    /// effects. Zero means unsequenced or invalid. Sequenced adapters override it.
    fn reliable_sequence(&self, _label: &str, _raw: &[u8]) -> u32 {
        0
    }

    /// Decide what happens with a message from `sender` on a `_reliable`/`_lossy`
    /// channel (`label` given for other channels).
    fn inbound(
        &self,
        sender: &DataSender<'_>,
        kind: DataKind,
        label: &str,
        raw: &[u8],
    ) -> DataVerdict;
}
