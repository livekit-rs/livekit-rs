//! Wire-independent admission boundary for participant update chunking.
use super::{ParticipantInfo, Room};
use std::sync::Arc;

/// Supplied by the active protocol adapter using its actual encoded message.
/// The engine must never estimate a schema-specific byte size itself.
pub trait ParticipantUpdateSizer: Send + Sync + 'static {
    /// Size of the participant message, excluding its enclosing update wrapper.
    fn encoded_len(&self, participant: &ParticipantInfo) -> usize;
}

impl Room {
    /// Bind the protocol's actual size provider before this room starts serving.
    pub fn set_participant_update_sizer(&mut self, sizer: Arc<dyn ParticipantUpdateSizer>) {
        self.participant_update_sizer = Some(sizer);
    }

    pub(super) fn chunk_participant_updates(
        &self,
        infos: Vec<ParticipantInfo>,
    ) -> Vec<Vec<ParticipantInfo>> {
        if infos.is_empty() {
            return vec![];
        }
        let Some(sizer) = &self.participant_update_sizer else {
            // A protocol-neutral engine without an attached adapter has no
            // schema to measure. Keep its logical batch intact, never invent
            // an approximate protobuf size or silently discard participants.
            return vec![infos];
        };
        let target = i128::from(self.policy.update_batch_target_size);
        let mut batches = Vec::new();
        let mut batch = Vec::new();
        let mut size = 0i128;
        for info in infos {
            let next = sizer.encoded_len(&info) as i128;
            // Fixed rtc/utils.go ChunkProtoBatch cuts only when > target.
            // A single oversize item survives intact; zero/negative targets
            // are meaningful signed values, not "disable batching" sentinels.
            if size + next > target {
                if !batch.is_empty() {
                    batches.push(std::mem::take(&mut batch));
                }
                size = 0;
            }
            size += next;
            batch.push(info);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
        batches
    }
}
