//! Same-owner debug view of forwarding state and native packet/SR producers.
use crate::engine::MediaEngine;
use rtc_core::{media_debug::*, model::VideoLayerMode};
use std::time::{Instant, UNIX_EPOCH};
use str0m::media::Rid;
fn packet(value: Option<(u64, u64, bool)>) -> Option<RtpPacketDebug> {
    value.map(|(sequence, timestamp, marker)| RtpPacketDebug {
        sequence,
        timestamp,
        marker,
    })
}
impl MediaEngine {
    pub fn debug_snapshot(&mut self, now: Instant) -> MediaDebugSnapshot {
        let mut snapshot = MediaDebugSnapshot::default();
        for (_, up) in self.ups.iter() {
            let Some(transport) = self.transports.get_mut(&up.transport) else {
                continue;
            };
            let msid = transport
                .rtc
                .media(up.mid)
                .map(|media| format!("{} {}", media.stream_id(), media.track_id()))
                .unwrap_or_default();
            let mut rids = vec![None];
            rids.extend(up.sdp_rids.iter().map(|rid| Some(Rid::from(rid.as_str()))));
            rids.extend(
                up.stats
                    .iter()
                    .filter(|stat| stat.last_seen.is_some())
                    .map(|stat| stat.rid),
            );
            let mut tracks = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for rid in rids {
                let mut direct = transport.rtc.direct_api();
                if let Some(stream) = direct.stream_rx_by_mid(up.mid, rid)
                    && stream.last_time().is_some()
                {
                    let ssrc = *stream.ssrc();
                    if !seen.insert(ssrc) {
                        continue;
                    }
                    let rid = stream.rid().map(|rid| rid.to_string()).unwrap_or_default();
                    let layer = up
                        .layers
                        .iter()
                        .find(|layer| layer.ssrc == ssrc || (!rid.is_empty() && layer.rid == rid))
                        .map(|layer| layer.spatial_layer)
                        .unwrap_or(0);
                    tracks.push(UpTrackDebug {
                        layer,
                        ssrc,
                        msid: msid.clone(),
                        rid,
                    });
                }
            }
            let mode = up
                .spec
                .codecs
                .iter()
                .find(|codec| codec.mime_type.eq_ignore_ascii_case(&up.mime))
                .map(|codec| codec.layer_mode)
                .unwrap_or(VideoLayerMode::Unused);
            let receiver = ReceiverDebug {
                mime: up.mime.clone(),
                video_layer_mode: mode,
                up_tracks: tracks,
            };
            snapshot
                .published
                .entry(up.sid.clone())
                .or_insert_with(|| PublishedDebug {
                    publisher: up.publisher.clone(),
                    receivers: Vec::new(),
                    down_tracks: Vec::new(),
                })
                .receivers
                .push(receiver);
        }
        for (_, down) in self.downs.iter() {
            let Some(up) = self.ups.get(down.up) else {
                continue;
            };
            let mut ssrc = None;
            let mut last_packet = None;
            let mut second_last_packet = None;
            let mut sender_report = None;
            if let (Some(id), Some(mid)) = (down.transport, down.mid)
                && let Some(transport) = self.transports.get_mut(&id)
            {
                let mut direct = transport.rtc.direct_api();
                if let Some(stream) = direct.stream_tx_by_mid(mid, None) {
                    ssrc = Some(*stream.ssrc());
                    let (last, previous) = stream.debug_primary_packets();
                    last_packet = packet(last);
                    second_last_packet = packet(previous);
                    sender_report = stream.debug_sender_report(now).map(|report| {
                        let wall = report
                            .ntp_time
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default();
                        let seconds = wall.as_secs().wrapping_add(2_208_988_800);
                        let fraction = ((wall.subsec_nanos() as u64) << 32) / 1_000_000_000;
                        SenderReportDebug {
                            ntp_time: (seconds << 32) | fraction,
                            rtp_time: report.rtp_time.numer() as u32,
                            packet_count: report.sender_packet_count,
                        }
                    });
                }
            }
            let bind_state = if !down.negotiated {
                BindDebug::Unbound
            } else if !down.bound_emitted {
                BindDebug::WaitForReceiverReady
            } else {
                BindDebug::Bound
            };
            let debug = DownTrackDebug {
                subscriber: down.spec.subscriber.clone(),
                track: down.spec.track_sid.clone(),
                stream: down.spec.stream_id.clone(),
                ssrc,
                mime: up.mime.clone(),
                bind_state,
                muted: down.sub_muted,
                publisher_muted: down.pub_muted,
                current_spatial_layer: down.current,
                last_pli_unix_ns: down.debug_last_pli_unix_ns,
                last_packet,
                second_last_packet,
                sender_report,
                timestamp_offset: down.debug_timestamp_offset(),
            };
            snapshot
                .published
                .entry(up.sid.clone())
                .or_insert_with(|| PublishedDebug {
                    publisher: up.publisher.clone(),
                    receivers: Vec::new(),
                    down_tracks: Vec::new(),
                })
                .down_tracks
                .push(debug);
        }
        snapshot
    }
}
