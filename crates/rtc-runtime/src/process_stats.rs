//! Process-lifetime media and external signaling counters, independent of room directory membership.
//! A room reports deltas while alive, including its terminal sample. Removing or
//! replacing that room cannot erase traffic already handled by this process.
use crate::{api::NodeStats, room_task::RoomSnapshot};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(crate) struct ProcessStats(Arc<Mutex<NodeStats>>);
impl ProcessStats {
    pub(crate) fn record_rtc_init(&self) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lifecycle
            .participant_rtc_init += 1;
    }
    pub(crate) fn record_rtc_canceled(&self, count: u64) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lifecycle
            .participant_rtc_canceled += count;
    }
    pub(crate) fn record_signal_connected(&self) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lifecycle
            .participant_signal_connected += 1;
    }
    pub(crate) fn snapshot(&self) -> NodeStats {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
    pub(crate) fn record_signaling(&self, bytes: usize, outgoing: bool) {
        let mut total = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if outgoing {
            total.bytes_out = total.bytes_out.saturating_add(bytes as u64);
            total.packets_out = total.packets_out.saturating_add(1);
        } else {
            total.bytes_in = total.bytes_in.saturating_add(bytes as u64);
            total.packets_in = total.packets_in.saturating_add(1);
        }
    }
    pub(crate) fn reporter(&self) -> Reporter {
        Reporter {
            total: self.clone(),
            previous: NodeStats::default(),
        }
    }
}
pub(crate) struct Reporter {
    total: ProcessStats,
    previous: NodeStats,
}
impl Reporter {
    pub(crate) fn sample(
        &mut self,
        room: &RoomSnapshot,
        lifecycle: rtc_core::room::LifecycleCounters,
    ) {
        let mut total = self.total.0.lock().unwrap_or_else(|p| p.into_inner());
        total
            .lifecycle
            .accumulate_delta(lifecycle, &mut self.previous.lifecycle);
        macro_rules! delta {
            ($($field:ident),+ $(,)?) => {$(
                total.$field = total.$field.saturating_add(room.$field.saturating_sub(self.previous.$field));
                self.previous.$field = room.$field;
            )+};
        }
        delta!(
            bytes_in,
            bytes_out,
            packets_in,
            packets_out,
            nacks,
            retransmit_bytes_out,
            retransmit_packets_out,
            probe_attempts,
            probe_successes,
            probe_failures,
            congestion_events
        );
    }
}
