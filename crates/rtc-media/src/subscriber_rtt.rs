//! Immediate authenticated downlink RR -> currently bound subscriber observation.
use crate::engine::MediaEngine;
use rtc_core::media_api::MediaEvent;
impl MediaEngine {
    pub(crate) fn bind_receiver_report_source(&mut self, di: usize) {
        let Some(d) = self.downs.get_mut(di) else {
            return;
        };
        if !d.bound_emitted || d.rr_source_id.is_some() {
            return;
        }
        let (Some(tid), Some(mid)) = (d.transport, d.mid) else {
            return;
        };
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        let mut direct = t.rtc.direct_api();
        let Some(stream) = direct.stream_tx_by_mid(mid, None) else {
            return;
        };
        d.rr_source_id = Some(stream.reset_receiver_report_source());
        d.rr_report_sequence = 0;
    }
    pub(crate) fn retire_receiver_report_source(&mut self, di: usize) {
        let Some(d) = self.downs.get_mut(di) else {
            return;
        };
        if let (Some(tid), Some(mid)) = (d.transport, d.mid) {
            if let Some(t) = self.transports.get_mut(&tid) {
                let mut direct = t.rtc.direct_api();
                if let Some(stream) = direct.stream_tx_by_mid(mid, None) {
                    stream.reset_receiver_report_source();
                }
            }
        }
        d.rr_source_id = None;
        d.rr_report_sequence = 0;
    }
    pub(crate) fn poll_subscriber_rtt(&mut self) {
        let pending = self
            .downs
            .iter()
            .filter(|(_, d)| d.bound_emitted && d.rr_source_id.is_none())
            .map(|(di, _)| di)
            .collect::<Vec<_>>();
        for di in pending {
            self.bind_receiver_report_source(di);
        }
        let mut received = Vec::new();
        let mut losses = Vec::new();
        for (tid, t) in self.transports.iter_mut() {
            if t.recovery_pending.is_some() {
                // Do not replay old-credential RR feedback into a new recovery.
                let _ = t.rtc.take_receiver_report_loss();
                let _ = t.rtc.take_receiver_report_rtt();
                continue;
            }
            for sample in t.rtc.take_receiver_report_loss() {
                let valid = t
                    .rtc
                    .direct_api()
                    .stream_tx(&sample.ssrc.into())
                    .is_some_and(|stream| {
                        stream.mid() == sample.mid
                            && stream.receiver_report_source_id() == sample.source_id
                    });
                if valid {
                    losses.push((*tid, sample));
                }
            }
            for sample in t.rtc.take_receiver_report_rtt() {
                // Resolve against the *current* native lifetime before touching
                // any logical subscription; queued old-SSRC/rebind samples die.
                let valid = t
                    .rtc
                    .direct_api()
                    .stream_tx(&sample.ssrc.into())
                    .is_some_and(|stream| {
                        stream.mid() == sample.mid
                            && stream.receiver_report_source_id() == sample.source_id
                    });
                if valid {
                    received.push((*tid, sample));
                }
            }
        }
        for (tid, sample) in losses {
            let Some(di) = self.down_by_mid.get(&(tid, sample.mid)).copied() else {
                continue;
            };
            let Some(d) = self.downs.get_mut(di) else {
                continue;
            };
            if !d.bound_emitted || d.transport != Some(tid) || d.mid != Some(sample.mid) {
                continue;
            }
            if d.rr_source_id != Some(sample.source_id) {
                d.rr_source_id = Some(sample.source_id);
                d.rr_report_sequence = 0;
            }
            self.events.push(MediaEvent::ReceiverLoss {
                recovery_epoch: self.transports.get(&tid).map_or(0, |t| t.recovery_epoch),
                transport: tid,
                track: d.spec.track_sid.clone(),
                fraction_lost: sample.fraction_lost,
                received_at: sample.received_at,
            });
        }
        for (tid, sample) in received {
            let Some(di) = self.down_by_mid.get(&(tid, sample.mid)).copied() else {
                continue;
            };
            let Some(d) = self.downs.get_mut(di) else {
                continue;
            };
            if !d.bound_emitted || d.transport != Some(tid) || d.mid != Some(sample.mid) {
                continue;
            }
            if d.rr_source_id != Some(sample.source_id) {
                // A real native SSRC reset changed its lifetime while the same
                // subscription remained bound; old queued samples were fenced.
                d.rr_source_id = Some(sample.source_id);
                d.rr_report_sequence = 0;
            }
            if sample.sequence <= d.rr_report_sequence {
                continue;
            }
            d.rr_report_sequence = sample.sequence;
            self.events.push(MediaEvent::SubscriberRtt {
                transport: tid,
                track: d.spec.track_sid.clone(),
                rtt_ms: sample.rtt_ms,
                received_at: sample.received_at,
            });
        }
    }
}
