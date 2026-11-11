//! Actual downlink receiver-report RTT, separate from TWCC and ICE estimates.
use crate::{
    media::Mid,
    rtp_::{ReceptionReport, Ssrc},
    util::SystemTimeExt,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime},
};

/// One changed, nonzero RTT from an authenticated report for a real sender.
#[derive(Clone, Copy, Debug)]
pub struct ReceiverReportRtt {
    /// Native sender lifetime; changes on SSRC reset or logical rebinding.
    pub source_id: u64,
    /// Strictly increasing report observation sequence within this lifetime.
    pub sequence: u64,
    /// Actual RTCP datagram receipt time, never a periodic stats sampling time.
    pub received_at: Instant,
    /// Negotiated media section of the sender.
    pub mid: Mid,
    /// Primary sender SSRC; repair reports cannot update participant RTT.
    pub ssrc: u32,
    /// Fixed Go GetRttMs result, rounded upwards, without the NACK 70ms fallback.
    pub rtt_ms: u32,
}
/// Fraction loss from an actual authenticated RR block for a live primary
/// sender. Unlike RTT, this is observed even when RTT is unchanged/no LSR.
#[derive(Clone, Copy, Debug)]
pub struct ReceiverReportLoss {
    /// Current native sender lifetime, fenced by the application on rebind.
    pub source_id: u64,
    /// Authenticated RTCP datagram receipt timestamp.
    pub received_at: Instant,
    /// Current negotiated native sender MID.
    pub mid: Mid,
    /// Primary sender SSRC (never an RTX report).
    pub ssrc: u32,
    /// Actual eight-bit fraction loss from this report.
    pub fraction_lost: u8,
}
static NEXT_SOURCE: AtomicU64 = AtomicU64::new(1);
#[derive(Debug)]
pub(crate) struct Tracker {
    pub source_id: u64,
    first: Option<u64>,
    highest: u64,
    marker: Option<(u64, Instant)>,
    last_rtt_ms: u32,
    sequence: u64,
    pub pending: Option<ReceiverReportRtt>,
}
impl Default for Tracker {
    fn default() -> Self {
        Self {
            source_id: NEXT_SOURCE.fetch_add(1, Ordering::Relaxed),
            first: None,
            highest: 0,
            marker: None,
            last_rtt_ms: 0,
            sequence: 0,
            pending: None,
        }
    }
}
impl Tracker {
    pub fn primary(&mut self, seq: u64) {
        self.first.get_or_insert(seq);
        self.highest = self.highest.max(seq);
    }
    pub fn sender_report(&mut self, ntp: SystemTime, now: Instant) {
        if self.first.is_some() {
            self.marker = Some((ntp.as_ntp_64(), now));
        }
    }
    pub fn report(&mut self, mid: Mid, ssrc: Ssrc, rr: ReceptionReport, now: Instant) {
        if rr.ssrc != ssrc {
            return;
        }
        let Some(first) = self.first else {
            return;
        };
        // The report sequence number must refer to emitted primary RTP, not
        // dummy pre-start, future, or repair packets. Extend the 32-bit RR SN
        // around the actual emitted sender sequence.
        let report_seq = crate::rtp_::extend_u32(Some(self.highest), rr.max_seq);
        if report_seq < first || report_seq > self.highest {
            return;
        }
        let Some((ntp, sent_at)) = self.marker else {
            return;
        };
        let Some(rtt_ms) = matched_rtt_ms(ntp, sent_at, rr.last_sr_time, rr.last_sr_delay, now)
        else {
            return;
        };
        if rtt_ms == self.last_rtt_ms {
            return;
        }
        self.last_rtt_ms = rtt_ms;
        self.pending = None;
        if rtt_ms == 0 {
            return;
        }
        self.sequence = self.sequence.checked_add(1).expect("RR sequence exhausted");
        self.pending = Some(ReceiverReportRtt {
            source_id: self.source_id,
            sequence: self.sequence,
            received_at: now,
            mid,
            ssrc: *ssrc,
            rtt_ms,
        });
    }
}
// Original mediatransportutil.GetRttMs uses the most recent actual SR NTP and
// its monotonic emission marker. DLSR=0 is valid; LSR=0/mismatched/stale and
// anachronous (>2^31 tick subtraction) reports are rejected.
fn matched_rtt_ms(ntp: u64, sent_at: Instant, lsr: u32, dlsr: u32, now: Instant) -> Option<u32> {
    if lsr == 0 || lsr != (ntp >> 16) as u32 {
        return None;
    }
    let elapsed = now.checked_duration_since(sent_at)?;
    // NtpTime.Time() truncates fractional nanoseconds before adding elapsed.
    let nanos = (u128::from(ntp >> 32) * 1_000_000_000)
        + ((u128::from(ntp as u32) * 1_000_000_000) >> 32)
        + elapsed.as_nanos();
    let seconds = nanos / 1_000_000_000;
    let fraction = ((nanos % 1_000_000_000) << 32) / 1_000_000_000;
    let current = ((seconds as u32) << 16) | ((fraction as u32) >> 16);
    let diff = current.wrapping_sub(lsr).wrapping_sub(dlsr);
    (diff <= 1 << 31).then(|| ((u64::from(diff) * 1000).div_ceil(65536)) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_marker_wrap_zero_dlsr_and_round_up_use_no_native_rtt_fallback() {
        let start = Instant::now();
        // Deliberately cross the middle-NTP32 wrap at65536 seconds.
        let marker = (65535u64 << 32) | (3u64 << 30);
        let lsr = (marker >> 16) as u32;
        assert_eq!(
            matched_rtt_ms(
                marker,
                start,
                lsr,
                0,
                start + std::time::Duration::from_millis(500)
            ),
            Some(500)
        );
        assert_eq!(
            matched_rtt_ms(
                marker,
                start,
                lsr,
                8192,
                start + std::time::Duration::from_millis(250)
            ),
            Some(125)
        );
        assert_eq!(matched_rtt_ms(marker, start, lsr, 0, start), Some(0));
        assert_eq!(matched_rtt_ms(marker, start, 0, 0, start), None);
        assert_eq!(
            matched_rtt_ms(marker, start, lsr.wrapping_add(1), 0, start),
            None
        );
        assert_eq!(
            matched_rtt_ms(
                marker,
                start,
                lsr,
                u32::MAX / 2,
                start + std::time::Duration::from_millis(500)
            ),
            None
        );
    }
    #[test]
    #[ignore = "requires actual original RTPStatsSender/SR/RR observer rows"]
    fn original_go_sender_rr_consumer_rows_match_native_marker_clock_brackets() {
        let text = std::fs::read_to_string(
            std::env::var("GO_RPC_MEDIA_RTT_RESULTS").expect("original Go receipt"),
        )
        .unwrap();
        let rows = text
            .lines()
            .filter_map(|l| l.strip_prefix("SOURCE_RPC_RR "))
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            7,
            "all actual sender/SR/valid/invalid producer branches must execute"
        );
        let start = Instant::now();
        let mut names = std::collections::BTreeSet::new();
        for row in rows {
            let name = row["case"].as_str().unwrap();
            assert!(names.insert(name.to_owned()));
            let valid = name.starts_with("valid_");
            assert_eq!(row["changed"].as_bool().unwrap(), valid);
            let got = row["rtt_ms"].as_u64().unwrap() as u32;
            if !valid {
                assert_eq!(got, 0);
                continue;
            }
            let ntp = row["marker_ntp"].as_u64().unwrap();
            let lsr = row["lsr"].as_u64().unwrap() as u32;
            let dlsr = row["dlsr"].as_u64().unwrap() as u32;
            let before = std::time::Duration::from_nanos(row["before_ns"].as_u64().unwrap());
            let after = std::time::Duration::from_nanos(row["after_ns"].as_u64().unwrap());
            let minimum = matched_rtt_ms(ntp, start, lsr, dlsr, start + before).unwrap();
            let maximum = matched_rtt_ms(ntp, start, lsr, dlsr, start + after).unwrap();
            assert!(
                (minimum..=maximum).contains(&got),
                "actual Go method executes between its measured monotonic clock brackets: {name}: {got} not {minimum}..{maximum}"
            );
            assert!(got > 0);
        }
    }
}
