//! Native media diagnostics, copied synchronously by the room's media owner.
//! Optional values mean that their actual transport/packet has not existed yet;
//! no packet counters, addresses or munger state are synthesized by the adapter.
use crate::{
    ids::{ParticipantSid, TrackSid},
    model::VideoLayerMode,
};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Default)]
pub struct MediaDebugSnapshot {
    pub published: BTreeMap<TrackSid, PublishedDebug>,
}
#[derive(Clone, Debug)]
pub struct PublishedDebug {
    pub publisher: ParticipantSid,
    pub receivers: Vec<ReceiverDebug>,
    pub down_tracks: Vec<DownTrackDebug>,
}
#[derive(Clone, Debug)]
pub struct ReceiverDebug {
    pub mime: String,
    pub video_layer_mode: VideoLayerMode,
    pub up_tracks: Vec<UpTrackDebug>,
}
#[derive(Clone, Debug)]
pub struct UpTrackDebug {
    pub layer: i32,
    pub ssrc: u32,
    pub msid: String,
    pub rid: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindDebug {
    Unbound,
    WaitForReceiverReady,
    Bound,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RtpPacketDebug {
    pub sequence: u64,
    pub timestamp: u64,
    pub marker: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SenderReportDebug {
    pub ntp_time: u64,
    pub rtp_time: u32,
    pub packet_count: u32,
}
#[derive(Clone, Debug)]
pub struct DownTrackDebug {
    pub subscriber: ParticipantSid,
    pub track: TrackSid,
    pub stream: String,
    pub ssrc: Option<u32>,
    pub mime: String,
    pub bind_state: BindDebug,
    pub muted: bool,
    pub publisher_muted: bool,
    pub current_spatial_layer: i32,
    pub last_pli_unix_ns: i64,
    pub last_packet: Option<RtpPacketDebug>,
    pub second_last_packet: Option<RtpPacketDebug>,
    pub sender_report: Option<SenderReportDebug>,
    /// The native frame time-continuity offset, expressed with Go's subtract
    /// convention. Sequence-offset state is not claimed: this engine repacketizes.
    pub timestamp_offset: u64,
}
