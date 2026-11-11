//! Participant-local data blobs, port of pinned participant_data_blob_handler.go.
//! Apache-2.0 LiveKit source attribution is recorded in the F27 source lock.
use super::*;
use crate::session::RequestOutcome;

impl Room {
    pub(super) fn store_data_blob(
        &mut self,
        sid: &ParticipantSid,
        request_id: u32,
        blob: Option<DataBlob>,
    ) {
        let invalid = |room: &mut Self, outcome, message| {
            room.request_response(sid, 0, request_id, outcome, message)
        };
        if !self.policy.enable_participant_data_blob {
            invalid(self, RequestOutcome::NotAllowed, "data blob not enabled");
            return;
        }
        let Some(blob) = blob else {
            invalid(self, RequestOutcome::InvalidRequest, "data blob is invalid");
            return;
        };
        let Some(key) = blob.key.as_ref() else {
            invalid(self, RequestOutcome::InvalidRequest, "data blob is invalid");
            return;
        };
        let key_len = key.identifying_len();
        if key_len == 0
            || (self.policy.max_data_blob_key_length != 0
                && !i64::try_from(key_len)
                    .is_ok_and(|len| len <= self.policy.max_data_blob_key_length))
        {
            invalid(self, RequestOutcome::InvalidRequest, "data blob is invalid");
            return;
        }
        if let DataBlobKeyValue::Schema(schema) = &key.value
            && let Some(DataTrackEncoding::Custom(value)) = &schema.encoding
            && (value.is_empty()
                || (self.policy.max_data_track_custom_encoding_length != 0
                    && !i64::try_from(value.len())
                        .is_ok_and(|len| len <= self.policy.max_data_track_custom_encoding_length)))
        {
            invalid(
                self,
                RequestOutcome::InvalidRequest,
                "encoding identifier is empty or exceeds the maximum length",
            );
            return;
        }
        if blob.contents.is_empty() {
            invalid(self, RequestOutcome::InvalidRequest, "data blob is empty");
            return;
        }
        let Some(p) = self.participants.get_mut(sid) else {
            return;
        };
        // Source intentionally includes the OLD entry when checking replacement.
        // Go casts accumulated int to uint32 before comparing this uint32 limit.
        let total = p.data_blobs.iter().fold(0usize, |sum, old| {
            sum.wrapping_add(old.key.as_ref().map_or(0, DataBlobKey::identifying_len))
                .wrapping_add(old.contents.len())
        });
        let total = total
            .wrapping_add(key_len)
            .wrapping_add(blob.contents.len()) as u32;
        if self.policy.max_data_blob_size != 0 && total > self.policy.max_data_blob_size {
            invalid(
                self,
                RequestOutcome::LimitExceeded,
                "async attribute definition exceeds limit",
            );
            return;
        }
        let key = key.clone();
        if let Some(old) = p
            .data_blobs
            .iter_mut()
            .find(|old| old.key.as_ref() == Some(&key))
        {
            *old = blob;
        } else {
            p.data_blobs.push(blob);
        }
        // No dirty/version/participant update: pinned Room.OnStoreDataBlob is empty.
        self.signal(sid, SignalEvent::DataBlobStored { request_id, key });
    }

    pub(super) fn get_data_blob(
        &mut self,
        sid: &ParticipantSid,
        request_id: u32,
        publisher: Identity,
        key: Option<DataBlobKey>,
    ) {
        let Some(key) = key else {
            self.request_response(
                sid,
                0,
                request_id,
                RequestOutcome::InvalidRequest,
                "data blob key is required",
            );
            return;
        };
        // Get is deliberately NOT gated by enable_participant_data_blob, publish,
        // subscribe or data-publish grants. The room session already authorized it.
        let Some(p) = self
            .participants
            .values()
            .find(|p| p.info.identity == publisher)
        else {
            self.request_response(
                sid,
                0,
                request_id,
                RequestOutcome::NotFound,
                "participant not found",
            );
            return;
        };
        let Some(blob) = p
            .data_blobs
            .iter()
            .find(|blob| blob.key.as_ref() == Some(&key))
            .cloned()
        else {
            self.request_response(
                sid,
                0,
                request_id,
                RequestOutcome::NotFound,
                "data blob not found",
            );
            return;
        };
        self.signal(sid, SignalEvent::DataBlobRetrieved { request_id, blob });
    }
}
