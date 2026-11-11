//! Application-owned actual Pion feedback provider. Native matched records and
//! native GCC are deliberately absent. One owner serializes explicit inputs.
use crate::pion_gcc::{
    Ack, Adapter, CompoundFeedback, Estimator, FeedbackError, FeedbackTime, Timestamp,
    parse_compound, rfc8888_report_time,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use str0m::{SendSideFeedback, SendSidePacket};

#[derive(Clone, Debug)]
pub(crate) enum InputError {
    Feedback(FeedbackError),
    MissingTwcc,
    Discontinuity,
    /// Full datagram was admitted. Earlier packet callbacks are irreversible,
    /// exactly like source WriteRTCP's ordered packet loop before a later error.
    PostParse {
        at: Instant,
        changes: Vec<u64>,
        cause: FeedbackError,
    },
}
impl std::fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Feedback(error) | Self::PostParse { cause: error, .. } => write!(f, "{error:?}"),
            Self::MissingTwcc => f.write_str("missing transport layer cc header extension"),
            Self::Discontinuity => f.write_str("send-side input observation discontinuity"),
        }
    }
}
impl InputError {
    pub fn take_prefix(&mut self) -> Option<(Instant, Vec<u64>)> {
        match self {
            Self::PostParse { at, changes, .. } => Some((*at, std::mem::take(changes))),
            _ => None,
        }
    }
}
impl From<FeedbackError> for InputError {
    fn from(value: FeedbackError) -> Self {
        Self::Feedback(value)
    }
}
/// Read-only actual-input diagnostics; no quantity here grants capacity.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InputStats {
    pub sent_packets: u64,
    pub feedback_packets: u64,
    pub matched_received: u64,
    pub callbacks: u64,
}
pub(crate) struct Provider {
    adapter: Adapter,
    estimator: Estimator,
    local_origin: Instant,
    source_origin: Timestamp,
    stats: InputStats,
}
impl Provider {
    pub fn new(now: Instant) -> Self {
        let wall = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as i128,
            Err(e) => -(e.duration().as_nanos() as i128),
        };
        Self::new_at(now, 62_135_596_800i128 * 1_000_000_000 + wall)
    }
    fn new_at(now: Instant, source_origin: Timestamp) -> Self {
        Self {
            adapter: Adapter::new(),
            estimator: Estimator::new(source_origin),
            local_origin: now,
            source_origin,
            stats: InputStats::default(),
        }
    }
    fn source_time(&self, now: Instant) -> Timestamp {
        if let Some(d) = now.checked_duration_since(self.local_origin) {
            self.source_origin + d.as_nanos() as i128
        } else {
            self.source_origin - self.local_origin.duration_since(now).as_nanos() as i128
        }
    }
    pub fn close(&mut self) {
        self.estimator.close()
    }
    pub fn target(&self) -> u64 {
        self.estimator.target().max(0) as u64
    }
    pub fn stats(&self) -> InputStats {
        self.stats
    }
    fn sent(&mut self, s: SendSidePacket) -> Result<(), InputError> {
        let (ssrc, sequence, size) = if s.twcc_negotiated {
            (
                0,
                s.transport_sequence.ok_or(InputError::MissingTwcc)?,
                s.header_bytes + s.payload_bytes,
            )
        } else {
            (s.ssrc, s.sequence, s.payload_bytes)
        };
        self.adapter.sent(Ack {
            sequence,
            ssrc,
            size: size as i64,
            departure: Some(self.source_time(s.at)),
            ..Ack::default()
        });
        self.stats.sent_packets = self.stats.sent_packets.wrapping_add(1);
        Ok(())
    }
    pub fn handle(&mut self, event: SendSideFeedback) -> Result<(Instant, Vec<u64>), InputError> {
        match event {
            SendSideFeedback::Sent(s) => {
                self.sent(s)?;
                Ok((s.at, vec![]))
            }
            SendSideFeedback::Report { at, packet } => {
                // Source cc.Interceptor parses the entire RTCP datagram first.
                // Do not leak valid feedback from a malformed compound prefix.
                let report = parse_compound(&packet)?;
                let receipt = self.source_time(at);
                let mut changes = Vec::new();
                // Keep actual prefix emissions even if a later admitted packet
                // fails history/delta processing. Never roll estimator state back.
                for feedback in report.feedback {
                    self.stats.feedback_packets = self.stats.feedback_packets.wrapping_add(1);
                    let (acks, timing) = match feedback {
                        CompoundFeedback::Twcc(fb) => (
                            self.adapter
                                .twcc(fb.base, fb.reference, &fb.chunks, &fb.deltas_us)
                                .map_err(|cause| InputError::PostParse {
                                    at,
                                    changes: changes.clone(),
                                    cause,
                                })?,
                            FeedbackTime::Twcc,
                        ),
                        CompoundFeedback::Rfc8888(fb) => {
                            let report_time = rfc8888_report_time(fb.timestamp);
                            (
                                self.adapter.rfc8888(report_time, &fb.blocks),
                                FeedbackTime::Rfc8888 { report_time },
                            )
                        }
                    };
                    self.stats.matched_received = self
                        .stats
                        .matched_received
                        .wrapping_add(acks.iter().filter(|a| a.arrival.is_some()).count() as u64);
                    let emitted = self
                        .estimator
                        .feedback_timed(&acks, timing, receipt)
                        .map_err(|cause| InputError::PostParse {
                            at,
                            changes: changes.clone(),
                            cause,
                        })?;
                    self.stats.callbacks = self.stats.callbacks.wrapping_add(emitted.len() as u64);
                    changes.extend(emitted.into_iter().map(|v| v.max(0) as u64));
                }
                Ok((at, changes))
            }
            SendSideFeedback::Discontinuity => {
                self.close();
                Err(InputError::Discontinuity)
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_send_history_distinguishes_twcc_header_payload_from_rfc_payload_and_padding() {
        let now = Instant::now();
        let mut p = Provider::new_at(now, 1_000_000_000);
        let sent = SendSidePacket {
            at: now,
            sequence: 9,
            ssrc: 44,
            header_bytes: 24,
            payload_bytes: 0,
            twcc_negotiated: true,
            transport_sequence: Some(7),
        };
        p.handle(SendSideFeedback::Sent(sent)).unwrap();
        let acks = p
            .adapter
            .twcc(
                7,
                1,
                &[crate::pion_gcc::Chunk::Run {
                    symbol: 1,
                    length: 1,
                }],
                &[250],
            )
            .unwrap();
        assert_eq!(acks[0].size, 24);
        assert_eq!(acks[0].ssrc, 0);
        p.handle(SendSideFeedback::Sent(SendSidePacket {
            twcc_negotiated: false,
            transport_sequence: None,
            payload_bytes: 123,
            ..sent
        }))
        .unwrap();
        let acks = p
            .adapter
            .rfc8888(1_000_000_000, &[(44, 9, vec![(true, 0, 0)])]);
        assert_eq!(acks[0].size, 123);
        assert_eq!(acks[0].ssrc, 44);
        assert!(matches!(
            p.handle(SendSideFeedback::Sent(SendSidePacket {
                transport_sequence: None,
                ..sent
            })),
            Err(InputError::MissingTwcc)
        ));
        assert_eq!(p.stats().sent_packets, 2);
        assert_eq!(p.target(), 1_000_000);
    }
    #[test]
    fn malformed_later_packet_cannot_mutate_provider_history_timing_or_callbacks() {
        let now = Instant::now();
        let mut p = Provider::new_at(now, 1_000_000_000);
        p.sent(SendSidePacket {
            at: now,
            sequence: 9,
            ssrc: 44,
            header_bytes: 20,
            payload_bytes: 100,
            twcc_negotiated: true,
            transport_sequence: Some(1),
        })
        .unwrap();
        let valid = vec![
            0x8f, 205, 0, 5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 1, 0, 1, 0, 0, 1, 0, 0x20, 1, 0, 0,
        ];
        let mut malformed = valid.clone();
        malformed.push(0);
        assert!(
            p.handle(SendSideFeedback::Report {
                at: now,
                packet: malformed
            })
            .is_err()
        );
        assert_eq!(p.stats().feedback_packets, 0);
        assert_eq!(p.stats().matched_received, 0);
        assert_eq!(p.stats().callbacks, 0);
        assert_eq!(p.target(), 1_000_000);
        p.handle(SendSideFeedback::Report {
            at: now,
            packet: valid,
        })
        .unwrap();
        assert_eq!(p.stats().feedback_packets, 1);
        assert_eq!(p.stats().matched_received, 1);
    }
    #[test]
    fn source_discontinuity_retires_real_estimator_instead_of_replaying_an_initial_callback() {
        let now = Instant::now();
        let mut p = Provider::new_at(now, 1_000_000_000);
        assert!(matches!(
            p.handle(SendSideFeedback::Discontinuity),
            Err(InputError::Discontinuity)
        ));
        assert_eq!(
            p.estimator.feedback(&[], 1_000_000_000),
            Err(FeedbackError::Closed)
        );
        assert_eq!(p.stats().callbacks, 0);
    }
}

#[cfg(test)]
mod sent_source_oracle {
    use super::*;
    use serde_json::Value;
    #[test]
    #[ignore = "requires actual fixed Go OnSent eight-case output"]
    fn actual_source_sent_header_payload_padding_conventions() {
        let inputs = std::fs::read_to_string(
            std::env::var("PION_SENT_SIZE_INPUT").expect("frozen input required"),
        )
        .unwrap();
        let results = std::fs::read_to_string(
            std::env::var("PION_SENT_SIZE_RESULTS").expect("actual Go output required"),
        )
        .unwrap();
        let source: Vec<Value> = results
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let mut count = 0;
        for (line, row) in inputs.lines().zip(&source) {
            let case: Value = serde_json::from_str(line).unwrap();
            assert_eq!(case["name"], row["case"]);
            let now = Instant::now();
            let origin = (62_135_596_800i128 + 1000) * 1_000_000_000;
            let mut provider = Provider::new_at(now, origin);
            let packet = SendSidePacket {
                at: now,
                sequence: case["sequence"].as_u64().unwrap() as u16,
                ssrc: case["ssrc"].as_u64().unwrap() as u32,
                header_bytes: row["header_bytes"].as_u64().unwrap() as usize,
                payload_bytes: case["payload_bytes"].as_u64().unwrap() as usize,
                twcc_negotiated: case["twcc_negotiated"].as_bool().unwrap(),
                transport_sequence: case["extension_present"]
                    .as_bool()
                    .unwrap()
                    .then(|| case["transport_sequence"].as_u64().unwrap() as u16),
            };
            let actual = provider.sent(packet);
            if let Some(error) = row["error"].as_str() {
                assert_eq!(actual.unwrap_err().to_string(), error);
                assert_eq!(provider.stats().sent_packets, 0);
            } else {
                actual.unwrap();
                let source = &row["history"][0];
                let acks = if packet.twcc_negotiated {
                    provider
                        .adapter
                        .twcc(
                            packet.transport_sequence.unwrap(),
                            1,
                            &[crate::pion_gcc::Chunk::Run {
                                symbol: 1,
                                length: 1,
                            }],
                            &[250],
                        )
                        .unwrap()
                } else {
                    provider.adapter.rfc8888(
                        1_000_000_000,
                        &[(packet.ssrc, packet.sequence, vec![(true, 0, 0)])],
                    )
                };
                assert_eq!(acks.len(), 1);
                let ack = acks[0];
                assert_eq!(
                    u64::from(ack.sequence),
                    source["sequence"].as_u64().unwrap()
                );
                assert_eq!(u64::from(ack.ssrc), source["ssrc"].as_u64().unwrap());
                assert_eq!(ack.size, source["size"].as_i64().unwrap());
                assert_eq!(ack.departure, Some(origin));
                assert_eq!(row["payload_bytes"], case["payload_bytes"]);
                assert_eq!(
                    row["wire_bytes"].as_u64().unwrap(),
                    row["header_bytes"].as_u64().unwrap()
                        + case["payload_bytes"].as_u64().unwrap()
                        + case["padding_bytes"].as_u64().unwrap()
                );
            }
            count += 1;
        }
        assert_eq!(count, 8);
        assert_eq!(count, source.len());
    }
}

#[cfg(test)]
mod prefix_wire_source_oracle {
    use super::*;
    use serde_json::Value;
    use std::time::Duration;

    #[test]
    #[ignore = "requires genuine fixed Go stateful raw-wire/history callback output"]
    fn actual_source_post_parse_error_preserves_prior_callbacks_and_state() {
        let input = std::fs::read_to_string(std::env::var("PION_PREFIX_INPUT").unwrap()).unwrap();
        let output = std::fs::read_to_string(std::env::var("PION_PREFIX_OUTPUT").unwrap()).unwrap();
        let rows: Vec<Value> = output
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let now = Instant::now();
        let origin = 62_135_597_800_i128 * 1_000_000_000;
        let mut row_index = 0;
        let mut cases = 0;
        let mut witnessed_prefix = 0;
        let mut witnessed_atomic_parse_failure = 0;
        for line in input.lines() {
            let case: Value = serde_json::from_str(line).unwrap();
            let mut p = Provider::new_at(now, origin);
            let mut total_callbacks = 0;
            let challenge = case["challenge_index"].as_u64().unwrap() as usize;
            for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
                let row = &rows[row_index];
                row_index += 1;
                assert_eq!(row["case"], case["name"]);
                assert_eq!(row["index"].as_u64().unwrap(), index as u64);
                assert_eq!(row["kind"], step["kind"]);
                let at = now + Duration::from_nanos(step["at_ns"].as_u64().unwrap());
                let before_target = p.target();
                let before_feedback = p.stats.feedback_packets;
                let event = if step["kind"] == "sent" {
                    SendSideFeedback::Sent(SendSidePacket {
                        at,
                        sequence: step["sequence"].as_u64().unwrap() as u16,
                        ssrc: step["ssrc"].as_u64().unwrap() as u32,
                        header_bytes: step["header_bytes"].as_u64().unwrap() as usize,
                        payload_bytes: step["payload_bytes"].as_u64().unwrap() as usize,
                        twcc_negotiated: true,
                        transport_sequence: Some(
                            step["transport_sequence"].as_u64().unwrap() as u16
                        ),
                    })
                } else {
                    let hex = step["wire_hex"].as_str().unwrap().as_bytes();
                    let packet = hex
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .map(|pair| {
                            u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
                        })
                        .collect();
                    SendSideFeedback::Report { at, packet }
                };
                let (changes, parse_failed, process_failed) = match p.handle(event) {
                    Ok((receipt, changes)) => {
                        assert_eq!(receipt, at);
                        (changes, false, false)
                    }
                    Err(mut error) => {
                        let prefix = error.take_prefix();
                        match prefix {
                            Some((receipt, changes)) => {
                                assert_eq!(receipt, at);
                                assert!(matches!(
                                    error,
                                    InputError::PostParse {
                                        cause: FeedbackError::InvalidFeedback,
                                        ..
                                    }
                                ));
                                // The owner drains actual prefix only once.
                                assert!(error.take_prefix().unwrap().1.is_empty());
                                (changes, false, true)
                            }
                            None => {
                                assert!(matches!(
                                    error,
                                    InputError::Feedback(FeedbackError::Wire(_))
                                ));
                                (Vec::new(), true, false)
                            }
                        }
                    }
                };
                assert_eq!(
                    parse_failed,
                    !row["parse_error"].as_str().unwrap().is_empty(),
                    "{case} step {index}"
                );
                assert_eq!(
                    process_failed,
                    !row["process_error"].as_str().unwrap().is_empty(),
                    "{case} step {index}"
                );
                if process_failed {
                    assert_eq!(row["process_error"], "invalid feedback");
                }
                let expected: Vec<u64> = row["changes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap())
                    .collect();
                assert_eq!(changes, expected, "{case} step {index}");
                assert_eq!(
                    p.target(),
                    row["target"].as_u64().unwrap(),
                    "{case} step {index}"
                );
                assert_eq!(
                    p.estimator.latest_rtt(),
                    row["rtt_ns"].as_i64().unwrap(),
                    "{case} step {index}"
                );
                total_callbacks += changes.len() as u64;
                assert_eq!(p.stats.callbacks, total_callbacks);
                if index == challenge && case["name"] == "good-bad-good" {
                    assert!(process_failed);
                    assert!(
                        !changes.is_empty(),
                        "source warmup must produce an actual prefix callback"
                    );
                    witnessed_prefix += 1;
                }
                if parse_failed {
                    assert!(changes.is_empty());
                    assert_eq!(p.target(), before_target);
                    assert_eq!(p.stats.feedback_packets, before_feedback);
                    witnessed_atomic_parse_failure += 1;
                }
            }
            cases += 1;
        }
        assert_eq!(cases, 4);
        assert_eq!(row_index, rows.len());
        assert_eq!(witnessed_prefix, 1);
        assert_eq!(witnessed_atomic_parse_failure, 1);
    }
}
