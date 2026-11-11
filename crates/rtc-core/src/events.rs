//! Lifecycle events for webhooks and telemetry (impl-notes/api-auth-webhook.md §4.2).

use crate::model::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoomEndReason {
    Unknown,
    ApiDelete,
    IdleTimeout,
    ServerShutdown,
    Superseded,
    OpenFailed,
}

#[derive(Clone, Debug)]
pub enum EngineEvent {
    RoomStarted {
        room: RoomInfo,
    },
    RoomFinished {
        room: RoomInfo,
        reason: RoomEndReason,
    },
    /// Participant became ACTIVE (media connected).
    ParticipantJoined {
        room: RoomInfo,
        participant: ParticipantInfo,
    },
    /// Participant left after having been ACTIVE.
    ParticipantLeft {
        room: RoomInfo,
        participant: ParticipantInfo,
    },
    /// Participant closed without ever becoming ACTIVE.
    ParticipantConnectionAborted {
        room: RoomInfo,
        participant: ParticipantInfo,
    },
    TrackPublished {
        room: RoomInfo,
        participant: ParticipantInfo,
        track: TrackInfo,
    },
    TrackUnpublished {
        room: RoomInfo,
        participant: ParticipantInfo,
        track: TrackInfo,
    },
}

/// Complete owner-local directory view, not a wire protocol or a Redis command.
/// The participant rows are the cached store view, excluding transient live
/// dimension reads. Terminal projections remain owned until acknowledged.
#[derive(Clone, Debug, PartialEq)]
pub struct RoomProjection {
    /// Source room generation and current cached directory fields.
    pub room: RoomInfo,
    /// Complete owner-local stored participant inventory for this generation.
    pub participants: Vec<ParticipantInfo>,
    /// Actual owner close state; retained until external cleanup is acknowledged.
    pub terminal: bool,
}

pub trait EventSink: Send + Sync + 'static {
    /// Must not block; implementations queue.
    fn emit(&self, event: EngineEvent);
    fn reserve_room(&self, _room: &RoomInfo) -> Result<(), String> {
        Ok(())
    }
    fn cancel_room_reservation(&self, _room: &RoomInfo) {}
    fn wants_room_projection(&self) -> bool {
        false
    }
    fn project_room(&self, _projection: RoomProjection) {}
}

/// Sink that drops everything (no webhooks configured).
pub struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _event: EngineEvent) {}
}
