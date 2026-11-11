//! SFU media engine of the LiveKit-compatible server, built on str0m 0.24.1 using the
//! frame-level `MediaData` API (not `rtp_mode`).
//!
//! One [`MediaEngine`] owns every str0m `Rtc` of one room and forwards media
//! synchronously inside the room task: no locks, no threads, bounded buffers. The room
//! talks to it with [`rtc_core::media_api::MediaCommand`]s and drains
//! [`rtc_core::media_api::MediaEvent`]s after every step (M-02).
//!
//! Behaviour references: `livekit-rs-docs/impl-notes/sfu-media.md` and
//! `livekit-rs-docs/impl-notes/signaling.md` §3 and §7.
//!
//! This crate must not depend on any `lk-*` crate (ARCH-01).

mod allocator;
mod audio_level;
mod av1;
mod av1_packet_forward;
mod codecs;
mod data_fanout;
mod data_fanout_cpu;
mod data_track_writer;
mod debug;
mod downtrack;
mod dynacast;
mod engine;
mod error;
mod feedback;
mod forward;
mod forward_timing;
mod ice_fallback;
mod ingress_metrics;
mod layers;
mod net;
mod one_dd_forward;
mod ordinary_data_writer;
mod probe_controller;
mod quality;
/// Deterministic receiver-estimate policy, consumed by the Remote feedback owner.
pub mod remote_bwe;
mod sdp_munge;
mod signaling;
mod slab;
mod stream_tracker;
mod subscriber_rtt;
mod svc;
mod transport;
mod uptrack;
mod video_dimensions;

pub use engine::{EngineStats, MediaEngine, MediaEngineConfig};
pub use error::MediaError;
pub use net::{Datagram, IceCredentials, LocalCandidate, PacketOut, Proto};

#[cfg(test)]
mod regression;
#[cfg(test)]
mod warp;

/// Native RTC semantic parser; HTTP preflight separately uses the pinned Pion grammar.
pub fn validate_native_offer(offer: &str) -> Result<(), String> {
    str0m::change::SdpOffer::from_sdp_string(offer)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub mod pion_gcc;

mod codec_receivers;

mod receiver_traffic;

mod temporal_packet;
mod temporal_rate;

mod dd_fallback;
