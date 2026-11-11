//! One actor-turn debug projection. Pending tracks come from the real pending
//! publication queue, rather than the API's published-participant cache.
use super::Room;
use crate::model::{ParticipantInfo, RoomInfo, TrackInfo};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct DebugRoomSnapshot {
    pub room: RoomInfo,
    pub participants: Vec<DebugParticipantSnapshot>,
    pub media: Option<crate::media_debug::MediaDebugSnapshot>,
}
#[derive(Clone, Debug)]
pub struct DebugParticipantSnapshot {
    pub info: ParticipantInfo,
    pub pending_tracks: BTreeMap<String, DebugPendingTracks>,
    /// Actual owner state; monotonic clock is not serialized into Go debug JSON.
    pub signaling_last_seen: std::time::Instant,
    pub signaling_rtt_ms: u32,
}
#[derive(Clone, Debug)]
pub struct DebugPendingTracks {
    pub track_infos: Vec<TrackInfo>,
    /// Only a real migration consumer may supply this. Ordinary pending state
    /// does not prove either a completed migration or its absence.
    pub migrated: Option<bool>,
}
impl Room {
    pub fn debug_snapshot(&mut self) -> DebugRoomSnapshot {
        let room = self.info();
        let participants = self
            .participants
            .values_mut()
            .map(|participant| {
                let info = participant.snapshot();
                let mut pending_tracks = BTreeMap::<String, DebugPendingTracks>::new();
                for pending in &participant.pending {
                    pending_tracks
                        .entry(pending.request.cid.clone())
                        .or_insert_with(|| DebugPendingTracks {
                            track_infos: Vec::new(),
                            migrated: None,
                        })
                        .track_infos
                        .push(pending.info.clone());
                }
                DebugParticipantSnapshot {
                    info,
                    pending_tracks,
                    signaling_last_seen: participant.fallback.last_signal,
                    signaling_rtt_ms: participant.signal_rtt_ms,
                }
            })
            .collect();
        DebugRoomSnapshot {
            room,
            participants,
            media: None,
        }
    }
}
