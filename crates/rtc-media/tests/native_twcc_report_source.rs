//! Real TWCC encoder/decoder source; only surrounding type/trait glue is local.
//! The 16-bit extension glue supplies the surrounding RTP-header dependency;
//! no TWCC report, timing, loss-status or serialization algorithm is copied.
#![allow(dead_code, unused_imports)]
#[macro_use]
extern crate tracing;
#[allow(unexpected_cfgs)]
#[path = "../../../vendor/str0m/src/rtp/id.rs"]
mod native_ids;
mod rtp_ {
    pub(crate) use crate::native_ids::{TwccClusterId, TwccSeq};
}
use native_ids::Ssrc;
#[path = "../../../vendor/str0m/src/rtp/rtcp/fmt.rs"]
mod native_fmt;
use native_fmt::{FeedbackMessageType, PayloadType, TransportType};
#[path = "../../../vendor/str0m/src/rtp/rtcp/header.rs"]
mod native_header;
use native_header::{RtcpHeader, RtcpType};
trait RtcpPacket {
    fn header(&self) -> RtcpHeader;
    fn length_words(&self) -> usize;
    fn write_to(&self, buf: &mut [u8]) -> usize;
}
fn extend_u16(previous: Option<u64>, seq: u16) -> u64 {
    let Some(previous) = previous else {
        return u64::from(seq);
    };
    let candidate = (previous & !65535) | u64::from(seq);
    if candidate.saturating_add(32768) < previous {
        candidate + 65536
    } else if candidate > previous.saturating_add(32768) {
        candidate.saturating_sub(65536)
    } else {
        candidate
    }
}
#[path = "../../../vendor/str0m/src/rtp/rtcp/twcc_observation.rs"]
mod twcc_observation;
#[path = "../../../vendor/str0m/src/rtp/rtcp/twcc_time.rs"]
mod twcc_time;
use twcc_observation::BweIncomingTwccObservation;
// Preserve upstream source spelling; only these style lints apply to the imported module.
#[allow(clippy::unusual_byte_groupings, clippy::manual_is_multiple_of)]
#[path = "../../../vendor/str0m/src/rtp/rtcp/twcc.rs"]
mod native_twcc;

#[test]
fn actual_wire_reports_are_identical_opt_in_off_on_and_preserve_gap_missing_and_duplicates() {
    use std::time::{Duration, Instant};
    let start = Instant::now();
    let inputs: Vec<_> = (0..200u64)
        .filter(|seq| *seq != 70)
        .map(|seq| {
            let at = start + Duration::from_micros(seq * 100 + if seq >= 100 { 193000 } else { 0 });
            (seq, at)
        })
        .collect();
    let encode = |enabled| {
        let mut reg = native_twcc::TwccRecvRegister::new(100);
        reg.set_observations_enabled(enabled);
        for &(seq, at) in &inputs {
            reg.update_seq(seq.into(), at);
            if seq == 80 {
                reg.update_seq(seq.into(), at);
            }
        }
        let report = reg.build_report(1400).unwrap();
        let mut bytes = vec![0; report.length_words() * 4];
        report.write_to(&mut bytes);
        (bytes, report, reg.incoming_twcc_snapshot())
    };
    let off = encode(false);
    assert!(
        off.2.is_none(),
        "default off adds no diagnostic report allocation"
    );
    let on = encode(true);
    assert_eq!(
        off.0, on.0,
        "explicit diagnostics cannot change a single wire byte"
    );
    let o = on.2.unwrap();
    assert_eq!(o.received_packets, 199);
    assert_eq!(o.missing_packets, 1);
    assert_eq!(o.status_count, 200);
    assert!(o.max_encoding_error <= Duration::from_micros(125));
    assert!(o.max_receiver_gap >= Duration::from_millis(193));
    let decoded: Vec<_> = on.1.into_iter(start, 0.into()).collect();
    assert_eq!(decoded.iter().filter(|r| r.2.is_some()).count(), 199);
    assert!(
        decoded[70].2.is_none(),
        "only actual missing sequence70 is lost"
    );
    for (seq, _, received) in decoded {
        if let Some(received) = received {
            let actual = inputs.iter().find(|r| r.0 == *seq).unwrap().1;
            assert!(
                actual.max(received).duration_since(actual.min(received))
                    <= Duration::from_micros(125)
            );
        }
    }
}

#[test]
fn explicit_observation_policy_is_default_off_idempotent_and_clears_only_diagnostics() {
    use std::time::{Duration, Instant};
    let at = Instant::now();
    let mut reg = native_twcc::TwccRecvRegister::new(100);
    reg.update_seq(1.into(), at);
    assert!(reg.build_report(1400).is_some());
    assert!(reg.incoming_twcc_snapshot().is_none());
    reg.set_observations_enabled(true);
    reg.update_seq(2.into(), at + Duration::from_millis(1));
    assert!(reg.build_report(1400).is_some());
    let before = reg.incoming_twcc_snapshot().unwrap();
    reg.set_observations_enabled(true);
    assert_eq!(
        reg.incoming_twcc_snapshot().unwrap().feedback_count,
        before.feedback_count
    );
    reg.set_observations_enabled(false);
    assert!(reg.incoming_twcc_snapshot().is_none());
    reg.update_seq(3.into(), at + Duration::from_millis(2));
    let report = reg.build_report(1400).unwrap();
    assert_eq!(
        report.base_seq, 3,
        "disabling diagnostics must retain actual receive/report progress"
    );
    assert!(reg.incoming_twcc_snapshot().is_none());
    reg.set_observations_enabled(true);
    reg.update_seq(4.into(), at + Duration::from_millis(3));
    assert!(reg.build_report(1400).is_some());
    assert_eq!(reg.incoming_twcc_snapshot().unwrap().base_seq, 4);
}
