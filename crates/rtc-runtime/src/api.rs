//! Public types of the runtime used by adapters (signaling and server API).

use std::collections::BTreeMap;

use rtc_core::ids::*;
use rtc_core::model::{ParticipantInfo, RoomInfo};
use rtc_core::permission::{ParticipantPermission, RoomConfig};
use rtc_core::session::{SessionOutput, SessionRequest};
use tokio::sync::{mpsc, watch};

use crate::room_task::RoomInput;

/// Server API failure, mapped by the adapter to its error envelope.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    /// Ordinary signalling owner error, not frontend typed InvalidArgument.
    #[error("{0}")]
    MetadataLimit(String),
    #[error("{0}")]
    NotFound(String),
    /// Target room/participant has no owner (upstream: psrpc "no response from servers").
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    FailedPrecondition(String),
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    Internal(String),
}

/// Captured owner-side termination target. The actor wakes cleanup on actual
/// signal closure, participant leave, or replacement of the captured generation.
pub struct AgentParticipantScope {
    pub participant_sid: ParticipantSid,
    pub signal_generation: SessionGeneration,
    pub signal_retired: bool,
    pub session_retirement: Option<watch::Receiver<()>>,
}

pub(crate) enum AgentParticipantLookup {
    Present(AgentParticipantScope),
    AwaitAdmission(watch::Receiver<()>),
}

/// `CreateRoom` parameters after the adapter resolved presets and token config.
#[derive(Clone, Debug, Default)]
pub struct CreateRoomOptions {
    pub config: RoomConfig,
}

/// A dispatch retained by one room task. No worker job is implied by creation.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentDispatchRecord {
    /// Only jobs returned by a real worker assignment are retained.
    pub jobs: Vec<AgentJobRecord>,
    pub id: String,
    pub agent_name: String,
    pub room: String,
    pub metadata: String,
    pub restart_policy: i32,
    pub deployment: String,
    pub attributes: BTreeMap<String, String>,
    /// Upstream AgentDispatchState uses Unix nanoseconds, unlike room timestamps.
    pub created_at_ns: i64,
}

/// Protocol-neutral worker assignment result; no protobuf types enter runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentJobState {
    pub status: i32,
    pub error: String,
    pub started_at_ns: i64,
    pub ended_at_ns: i64,
    pub updated_at_ns: i64,
    pub participant_identity: String,
    pub worker_id: String,
    pub agent_id: String,
}
#[derive(Clone, Debug, PartialEq)]
pub struct AgentJobRecord {
    pub id: String,
    pub dispatch_id: String,
    pub job_type: i32,
    pub room: RoomInfo,
    pub participant: Option<ParticipantInfo>,
    pub namespace: String,
    pub metadata: String,
    pub agent_name: String,
    pub deployment: String,
    pub attributes: BTreeMap<String, String>,
    pub enable_recording: bool,
    pub enable_redaction: bool,
    pub state: AgentJobState,
}
/// A bounded adapter bridge consumes effects. No launch effect is a successful job.
#[derive(Clone, Debug)]
pub struct AgentLaunch {
    pub key: RoomKey,
    pub room: RoomInfo,
    pub dispatch: AgentDispatchRecord,
    pub participant: Option<ParticipantInfo>,
    pub job_type: i32,
}

#[derive(Clone, Debug)]
pub enum AgentWorkEvent {
    Launch(Box<AgentLaunch>),
    DeleteDispatch {
        key: RoomKey,
        room_sid: RoomSid,
        dispatch_id: String,
    },
    ParticipantLeft {
        key: RoomKey,
        room_sid: RoomSid,
        identity: Identity,
    },
    RoomClosed {
        key: RoomKey,
        room_sid: RoomSid,
    },
}
pub(crate) type AgentEventSender =
    std::sync::Arc<std::sync::Mutex<Option<mpsc::Sender<AgentWorkEvent>>>>;

/// Validated adapter input. Dispatch storage is scoped to the room generation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentDispatchOptions {
    pub id: String,
    pub agent_name: String,
    pub metadata: String,
    pub restart_policy: i32,
    pub deployment: String,
    pub attributes: BTreeMap<String, String>,
}

/// `UpdateParticipant`: only non-empty name/metadata are applied; attributes merge
/// and an empty value deletes the key; `permission` replaces when different.
#[derive(Clone, Debug, Default)]
pub struct ParticipantUpdate {
    pub name: String,
    pub metadata: String,
    pub attributes: BTreeMap<String, String>,
    pub permission: Option<ParticipantPermission>,
}

/// Node-level counters for health, `list-nodes` and metrics.
#[derive(Clone, Debug, Default)]
pub struct NodeStats {
    pub forward_stats: rtc_core::forward_stats::ForwardStatsSnapshot,
    pub lifecycle: rtc_core::room::LifecycleCounters,
    pub rooms: u32,
    pub participants: u32,
    pub tracks_in: u32,
    pub tracks_out: u32,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub packets_in: u64,
    pub packets_out: u64,
    pub nacks: u64,
    pub retransmit_bytes_out: u64,
    pub retransmit_packets_out: u64,
    pub estimated_capacity_bps: u64,
    pub allocated_bps: u64,
    pub allocation_overshoot_bps: u64,
    pub paused_video_tracks: u64,
    pub probe_attempts: u64,
    pub probe_successes: u64,
    pub probe_failures: u64,
    pub congestion_events: u64,
    pub started_at_ms: i64,
}

/// One signal connection's link to its participant. Dropping it tells the room that
/// the signal source closed (the participant may still resume).
pub struct SessionHandle {
    pub participant_sid: ParticipantSid,
    pub identity: Identity,
    pub generation: SessionGeneration,
    pub(crate) room: mpsc::Sender<RoomInput>,
    /// Room → session messages in order; `None` once the room dropped the session.
    pub output: mpsc::Receiver<SessionOutput>,
}

impl SessionHandle {
    pub async fn named_subscriptions_prepared(
        &self,
        targets: Vec<rtc_core::session::NamedSubscription>,
    ) -> bool {
        let (reply, receipt) = tokio::sync::oneshot::channel();
        if self
            .room
            .send(RoomInput::NamedSubscriptionsPrepared {
                sid: self.participant_sid.clone(),
                generation: self.generation,
                targets,
                reply,
            })
            .await
            .is_err()
        {
            return false;
        }
        receipt.await.unwrap_or(false)
    }

    /// Apply an adapter request and acknowledge only after actual room effects settle.
    /// The room task checks both participant SID and session generation.
    pub async fn apply(&self, request: SessionRequest) -> bool {
        let (reply, receipt) = tokio::sync::oneshot::channel();
        if self
            .room
            .send(RoomInput::ApplySessionRequest {
                sid: self.participant_sid.clone(),
                generation: self.generation,
                request,
                reply,
            })
            .await
            .is_err()
        {
            return false;
        }
        receipt.await.unwrap_or(false)
    }

    /// Forward a client request. Returns `false` when the room is gone.
    /// Applies backpressure: a client flooding requests waits here.
    pub async fn send(&self, request: SessionRequest) -> bool {
        self.room
            .send(RoomInput::Request {
                sid: self.participant_sid.clone(),
                generation: self.generation,
                request,
            })
            .await
            .is_ok()
    }
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        let closed = RoomInput::SignalClosed {
            sid: self.participant_sid.clone(),
            generation: self.generation,
        };
        if let Err(mpsc::error::TrySendError::Full(closed)) = self.room.try_send(closed) {
            // Drop cannot await, but a busy room must still learn that its
            // signal connection disappeared once the control backlog drains.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let room = self.room.clone();
                runtime.spawn(async move {
                    let _ = room.send(closed).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::events::RoomEndReason;
    use std::time::Duration;

    #[tokio::test]
    async fn dropping_session_delivers_close_after_full_inbox_drains() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(RoomInput::Close(RoomEndReason::Unknown))
            .unwrap();
        let (_, output) = mpsc::channel(1);
        let handle = SessionHandle {
            participant_sid: "PA_test".into(),
            identity: "alice".into(),
            generation: SessionGeneration(3),
            room: tx,
            output,
        };
        drop(handle);
        rx.recv().await.unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(closed, RoomInput::SignalClosed { generation: SessionGeneration(3), sid } if sid.as_str() == "PA_test")
        );
    }
}
