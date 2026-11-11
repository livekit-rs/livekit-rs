//! Fixed Go per-receiver GetDeltaStats -> OnStatsUpdate gate. A rejected window
//! is consumed, not a lifetime counter that can be retroactively re-enabled.
use crate::engine::MediaEngine;
use rtc_core::ids::TransportId;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use str0m::{MediaTraffic, ReceiveTraffic};
const INTERVAL: Duration = Duration::from_secs(5);
#[derive(Default)]
pub(crate) struct ReceiverTraffic {
    sources: HashMap<(TransportId, u64), Source>,
    windows: HashMap<usize, Window>,
    pub accepted: MediaTraffic,
}
#[cfg(test)]
impl ReceiverTraffic {
    pub(crate) fn test_bound_sources(&self) -> (usize, Option<u64>) {
        let mut sources = self.sources.values().filter(|s| s.owner.is_some());
        let first = sources.next().map(|s| s.snapshot.source_id);
        (usize::from(first.is_some()) + sources.count(), first)
    }
}
struct Source {
    snapshot: ReceiveTraffic,
    owner: Option<usize>,
}
struct Window {
    next: Instant,
    primary: bool,
    pending: HashMap<u64, SourceDelta>,
}
#[derive(Default)]
struct SourceDelta {
    bytes: u64,
    expected: u64,
    duplicates: u64,
    nacks: u64,
}
impl MediaEngine {
    pub(crate) fn poll_receiver_traffic(&mut self, now: Instant) {
        let mut pending = self
            .receiver_traffic
            .sources
            .iter()
            .filter(|(_, source)| source.owner.is_none())
            .map(|(key, source)| (*key, source.snapshot.clone()))
            .collect::<HashMap<_, _>>();
        for (tid, transport) in self.transports.iter_mut() {
            for snapshot in transport.rtc.take_receive_traffic() {
                pending.insert((*tid, snapshot.source_id), snapshot);
            }
        }
        for ((tid, _), snapshot) in pending {
            let key = (tid, snapshot.source_id);
            if snapshot.discarded_on_restart {
                self.receiver_traffic.sources.remove(&key);
                for window in self.receiver_traffic.windows.values_mut() {
                    window.pending.remove(&snapshot.source_id);
                }
                continue;
            }
            let previous = self.receiver_traffic.sources.remove(&key);
            let owner = previous
                .as_ref()
                .and_then(|source| source.owner)
                .filter(|ui| self.ups.get(*ui).is_some())
                .or_else(|| {
                    (!snapshot.retired)
                        .then(|| self.up_by_mid.get(&(tid, snapshot.mid)).copied())
                        .flatten()
                });
            let Some(ui) = owner else {
                if !snapshot.retired {
                    self.receiver_traffic.sources.insert(
                        key,
                        Source {
                            snapshot,
                            owner: None,
                        },
                    );
                }
                continue;
            };
            if let Some(old) = snapshot.restarted_from {
                for window in self.receiver_traffic.windows.values_mut() {
                    window.pending.remove(&old);
                }
                self.receiver_traffic.sources.remove(&(tid, old));
                if previous
                    .as_ref()
                    .is_none_or(|source| source.owner != Some(ui))
                {
                    let subscribers = {
                        let up = self.ups.get_mut(ui).unwrap();
                        up.detected_source_restart(&self.audio_params, now);
                        up.subscribers.clone()
                    };
                    for di in subscribers {
                        if let Some(d) = self.downs.get_mut(di) {
                            d.stop_forwarding();
                            d.av1 = Default::default();
                            d.av1_packet_selector.restart();
                            if let (Some(target), Some(mid)) = (d.transport, d.mid) {
                                if let Some(t) = self.transports.get_mut(&target) {
                                    let mut direct = t.rtc.direct_api();
                                    if let Some(stream) = direct.stream_tx_by_mid(mid, None) {
                                        stream.flush_receiver_restart_history();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let Some(first) = snapshot.first_at else {
                continue;
            };
            let Some(codec) = snapshot.codec else {
                continue;
            };
            let mime = if snapshot.red {
                "audio/red"
            } else {
                crate::codecs::mime_of(codec)
            };
            let up = self.ups.get(ui).unwrap();
            let priority = up
                .spec
                .codecs
                .iter()
                .position(|declared| declared.mime_type.eq_ignore_ascii_case(mime));
            let primary = match priority {
                Some(priority) => priority == 0,
                None if up.spec.codecs.is_empty()
                    || (up.spec.codecs.len() == 1 && up.spec.codecs[0].mime_type.is_empty()) =>
                {
                    true
                }
                None => continue,
            };
            let before = previous
                .as_ref()
                .filter(|source| source.owner == Some(ui))
                .map(|source| &source.snapshot);
            let delta = SourceDelta {
                bytes: snapshot
                    .bytes
                    .saturating_sub(before.map_or(0, |previous| previous.bytes)),
                expected: snapshot
                    .expected_packets
                    .saturating_sub(before.map_or(0, |previous| previous.expected_packets)),
                duplicates: snapshot
                    .duplicates
                    .saturating_sub(before.map_or(0, |previous| previous.duplicates)),
                nacks: snapshot
                    .nacks
                    .saturating_sub(before.map_or(0, |previous| previous.nacks)),
            };
            let window = self
                .receiver_traffic
                .windows
                .entry(ui)
                .or_insert_with(|| Window {
                    next: first + INTERVAL,
                    primary,
                    pending: Default::default(),
                });
            let pending = window.pending.entry(snapshot.source_id).or_default();
            pending.bytes = pending.bytes.saturating_add(delta.bytes);
            pending.expected = pending.expected.saturating_add(delta.expected);
            pending.duplicates = pending.duplicates.saturating_add(delta.duplicates);
            pending.nacks = pending.nacks.saturating_add(delta.nacks);
            self.codec_receiver_started(ui, mime, now);
            if snapshot.codec_changed {
                self.invalidate_codec_receiver(ui, now);
            }
            if !snapshot.retired {
                self.receiver_traffic.sources.insert(
                    key,
                    Source {
                        snapshot,
                        owner: Some(ui),
                    },
                );
            }
        }
        let due = self
            .receiver_traffic
            .windows
            .iter()
            .filter(|(_, window)| window.next <= now)
            .map(|(ui, _)| *ui)
            .collect::<Vec<_>>();
        for ui in due {
            self.consume_receiver_window(ui, now);
        }
    }
    fn consume_receiver_window(&mut self, ui: usize, now: Instant) {
        let Some(window) = self.receiver_traffic.windows.get_mut(&ui) else {
            return;
        };
        let target_received = self
            .ups
            .get(ui)
            .and_then(|up| self.codec_receivers.get(&up.sid))
            .is_some_and(|group| group.target_received);
        let sources = std::mem::take(&mut window.pending);
        // Original RTPStats.deltaInfo consumes even zero-progress/too-large
        // windows, returning zero (or nil) rather than exposing late-only bytes.
        // Analytics PrimaryPackets uses expected SN span, not received count.
        if window.primary || target_received {
            for delta in sources
                .into_values()
                .filter(|delta| delta.expected > 0 && delta.expected <= 65536)
            {
                self.receiver_traffic.accepted += MediaTraffic {
                    bytes_in: delta.bytes,
                    packets_in: delta.expected.saturating_add(delta.duplicates),
                    nacks_sent: delta.nacks,
                    ..Default::default()
                };
            }
        }
        window.next = now + INTERVAL;
    }
    pub(crate) fn retire_receiver_traffic(&mut self, ui: usize, now: Instant) {
        self.poll_receiver_traffic(now);
        self.consume_receiver_window(ui, now);
        self.receiver_traffic.windows.remove(&ui);
        self.receiver_traffic
            .sources
            .retain(|_, source| source.owner != Some(ui));
    }
    pub(crate) fn retire_transport_traffic(&mut self, tid: TransportId, now: Instant) {
        self.poll_receiver_traffic(now);
        let receivers = self
            .ups
            .iter()
            .filter(|(_, up)| up.transport == tid)
            .map(|(ui, _)| ui)
            .collect::<Vec<_>>();
        for ui in receivers {
            self.consume_receiver_window(ui, now);
            self.receiver_traffic.windows.remove(&ui);
        }
        self.receiver_traffic
            .sources
            .retain(|(source_tid, _), _| *source_tid != tid);
    }
    pub(crate) fn receiver_traffic_deadline(&self) -> Option<Instant> {
        self.receiver_traffic
            .windows
            .values()
            .map(|window| window.next)
            .min()
    }
}

#[cfg(test)]
impl MediaEngine {
    pub(crate) fn receiver_traffic_is_empty(&self) -> bool {
        self.receiver_traffic.sources.is_empty() && self.receiver_traffic.windows.is_empty()
    }
}

#[cfg(test)]
mod original_go_window_controls {
    #[test]
    #[ignore = "requires independently executed original Go receiver/buffer window controls"]
    fn original_go_window_invalid_and_generated_nack_receipts() {
        let text = std::fs::read_to_string(
            std::env::var("GO_CODEC_WINDOW_RESULTS").expect("original Go source-control receipt"),
        )
        .unwrap();
        let rows = text
            .lines()
            .filter_map(|line| line.strip_prefix("SOURCE_CODEC_WINDOW "))
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            5,
            "original discarded/current/close/Invalid/generated-NACK consumers must all execute"
        );
        let phase = |name: &str| rows.iter().find(|row| row["phase"] == name).unwrap();
        let first = phase("discarded_window");
        assert_eq!(first["packets"]["video/VP9"], 1);
        assert_eq!(first["bytes"]["video/VP9"], 20);
        assert!(first["packets"]["video/H264"].is_null());
        assert_eq!(first["pre_start_ignored"], true);
        assert_eq!(first["zero_progress_discarded"], true);
        assert_eq!(first["osn_only_duplicate_counted"], true);
        assert_eq!(first["short_repair_rejected"], true);
        let current = phase("current_window_after_target");
        for (codec, packets, bytes) in [
            ("video/VP9", 2, 40),
            ("video/VP8", 1, 23),
            ("video/H264", 1, 16),
        ] {
            assert_eq!(current["packets"][codec], packets);
            assert_eq!(current["bytes"][codec], bytes);
        }
        let terminal = phase("terminal_flush");
        assert_eq!(terminal["packets"]["video/VP9"], 3);
        assert_eq!(terminal["packets"]["video/H264"], 2);
        let invalid = phase("in_order_codec_invalid");
        assert_eq!(invalid["invalid"], true);
        assert_eq!(invalid["routed"], true);
        assert_eq!(invalid["target_received"], false);
        let generated = phase("generated_untransmitted_nack");
        assert!(generated["nacks"].as_u64().unwrap() > 0);
        assert_eq!(generated["socket_emissions"], 0);
        assert_eq!(generated["packets"]["video/VP9"], 4);
        assert_eq!(generated["bytes"]["video/VP9"], 66);
    }
}
