//! Runtime: one tokio task per room owning the room state machine, every media
//! transport of that room and its timers; the room manager; media socket I/O.
//! Protocol agnostic — must not depend on any `lk-*` crate (ARCH-01).

pub mod api;
pub mod automatic_recording;
mod idle_hold;
mod manager;
pub mod net;
mod process_stats;
mod room_task;

pub use api::{ApiError, CreateRoomOptions, NodeStats, ParticipantUpdate, SessionHandle};
pub use manager::{
    RoomCreationLease, RoomManager, RoomOwnerHandle, RoomOwnerLifecycle, RoomOwnerOperation,
    RuntimeDeps,
};

/// Validate a one-shot full offer using the same actual native RTC parser.
pub use rtc_media::validate_native_offer;

mod forward_stats;
