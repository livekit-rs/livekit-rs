//! One-shot-only RTP accounting from actual authenticated native raw receive events.
//! Fixed rtpstats_receiver.go8192 history; no allocation estimate or declared layer rate.
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Instant,
};
#[derive(Default)]
pub(crate) struct IngressMetrics {
    wire: HashMap<(u32, u16), (usize, Instant)>,
    order: VecDeque<(u32, u16)>,
    streams: HashMap<(String, u32), Stream>,
}
/// Metadata from a native authenticated receive event, paired with its original datagram.
pub(crate) struct AuthenticatedRtp {
    pub mid: String,
    pub outer_ssrc: u32,
    pub outer_seq: u16,
    pub main_ssrc: u32,
    pub sequence: u16,
    pub header_len: usize,
    pub payload_len: usize,
    pub auth_tag_len: usize,
    pub repair: bool,
}
struct Receipt {
    mid: String,
    ssrc: u32,
    seq: u16,
    wire_len: usize,
    header_len: usize,
    payload_len: usize,
    repair: bool,
    at: Instant,
}
struct Stream {
    first: Instant,
    bytes: u64,
    highest: u64,
    seen: HashSet<u64>,
    order: VecDeque<u64>,
}
impl IngressMetrics {
    pub fn wire(&mut self, packet: &[u8], at: Instant) {
        if packet.len() < 12 || packet[0] & 0xc0 != 0x80 || (192..=223).contains(&packet[1]) {
            return;
        }
        let key = (
            u32::from_be_bytes(packet[8..12].try_into().unwrap()),
            u16::from_be_bytes(packet[2..4].try_into().unwrap()),
        );
        if !self.wire.contains_key(&key) {
            self.order.push_back(key);
        }
        self.wire.insert(key, (packet.len(), at));
        while self.order.len() > 8192 {
            if let Some(old) = self.order.pop_front() {
                self.wire.remove(&old);
            }
        }
    }
    pub fn received(&mut self, packet: AuthenticatedRtp) {
        let AuthenticatedRtp {
            mid,
            outer_ssrc,
            outer_seq,
            main_ssrc,
            sequence,
            header_len,
            payload_len,
            auth_tag_len,
            repair,
        } = packet;
        let Some((wire_len, at)) = self.wire.remove(&(outer_ssrc, outer_seq)) else {
            return;
        };
        let receipt = Receipt {
            mid,
            ssrc: main_ssrc,
            seq: sequence,
            wire_len,
            header_len,
            payload_len,
            repair,
            at,
        };
        self.account(receipt, auth_tag_len);
    }
    fn account(&mut self, r: Receipt, tag: usize) {
        let Some(mut bytes) = r.wire_len.checked_sub(tag) else {
            return;
        };
        if r.repair {
            if r.payload_len < 2 {
                return;
            }
            bytes = bytes.saturating_sub(2);
        }
        let payload = r.payload_len.saturating_sub(if r.repair { 2 } else { 0 });
        if payload == 0 && !self.streams.contains_key(&(r.mid.clone(), r.ssrc)) {
            return;
        }
        let stream = self
            .streams
            .entry((r.mid, r.ssrc))
            .or_insert_with(|| Stream {
                first: r.at,
                bytes: 0,
                highest: u64::from(r.seq),
                seen: HashSet::new(),
                order: VecDeque::new(),
            });
        if r.at < stream.first {
            stream.first = r.at;
        }
        let low = stream.highest as u16;
        let delta = r.seq.wrapping_sub(low) as i16;
        let extended = if delta >= 0 {
            stream.highest.saturating_add(delta as u64)
        } else {
            stream
                .highest
                .saturating_sub(u64::from(delta.unsigned_abs()))
        };
        if extended.saturating_add(8192) <= stream.highest || !stream.seen.insert(extended) {
            return;
        }
        stream.highest = stream.highest.max(extended);
        stream.order.push_back(extended);
        while stream.order.len() > 8192 {
            if let Some(old) = stream.order.pop_front() {
                stream.seen.remove(&old);
            }
        }
        // Source excludes padding-only packets but counts RTP header+payload+padding for real media.
        if payload > 0 && bytes >= r.header_len {
            stream.bytes = stream.bytes.wrapping_add(bytes as u64);
        }
    }
    pub fn bitrate(&self, mid: &str, now: Instant) -> u32 {
        let Some(start) = self
            .streams
            .iter()
            .filter(|((m, _), _)| m == mid)
            .map(|(_, s)| s.first)
            .min()
        else {
            return 0;
        };
        let elapsed = now.saturating_duration_since(start).as_secs_f64();
        if elapsed == 0.0 {
            return 0;
        }
        let bytes = self
            .streams
            .iter()
            .filter(|((m, _), _)| m == mid)
            .map(|(_, s)| s)
            .fold(0u64, |sum, s| sum.wrapping_add(s.bytes));
        let value = bytes as f64 * 8.0 / elapsed;
        (value as u64) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn receive(
        m: &mut IngressMetrics,
        seq: u16,
        payload: usize,
        padding: usize,
        header: usize,
        at: Instant,
    ) {
        let mut p = vec![0; header + payload + padding + 16];
        p[0] = 0x80;
        p[1] = 111;
        p[2..4].copy_from_slice(&seq.to_be_bytes());
        p[8..12].copy_from_slice(&1u32.to_be_bytes());
        m.wire(&p, at);
        m.received(AuthenticatedRtp {
            mid: "0".into(),
            outer_ssrc: 1,
            outer_seq: seq,
            main_ssrc: 1,
            sequence: seq,
            header_len: header,
            payload_len: payload,
            auth_tag_len: 16,
            repair: false,
        });
    }
    #[test]
    fn excludes_padding_and_duplicate_but_preserves_real_header_and_padding() {
        let at = Instant::now();
        let mut m = IngressMetrics::default();
        receive(&mut m, 0, 0, 32, 12, at - Duration::from_secs(1));
        receive(&mut m, 1, 100, 0, 12, at);
        receive(&mut m, 1, 100, 0, 12, at);
        receive(&mut m, 2, 100, 12, 20, at);
        receive(&mut m, 3, 0, 32, 12, at);
        assert_eq!(m.bitrate("0", at + Duration::from_secs(1)), (112 + 132) * 8);
    }
    #[test]
    fn all_padded_media_counts_without_an_unpadded_bootstrap() {
        let at = Instant::now();
        let mut m = IngressMetrics::default();
        receive(&mut m, 1, 100, 20, 12, at);
        assert_eq!(m.bitrate("0", at + Duration::from_secs(1)), 1056);
    }

    #[test]
    fn exact_per_receipt_tags_rtx_and_duplicate_history() {
        let at = Instant::now();
        for tag in [10, 16] {
            let mut m = IngressMetrics::default();
            for (outer, main, repair, payload, padding) in [
                (1u16, 1u16, false, 100usize, 20usize),
                (2, 2, true, 102, 20),
                (3, 2, true, 102, 20),
            ] {
                let mut p = vec![0; 12 + payload + padding + tag];
                p[0] = 0x80;
                p[1] = 111;
                p[2..4].copy_from_slice(&outer.to_be_bytes());
                p[8..12].copy_from_slice(&1u32.to_be_bytes());
                m.wire(&p, at);
                m.received(AuthenticatedRtp {
                    mid: "0".into(),
                    outer_ssrc: 1,
                    outer_seq: outer,
                    main_ssrc: 1,
                    sequence: main,
                    header_len: 12,
                    payload_len: payload,
                    auth_tag_len: tag,
                    repair,
                });
            }
            assert_eq!(m.bitrate("0", at + Duration::from_secs(1)), 2 * 132 * 8);
        }
        let mut m = IngressMetrics::default();
        for seq in 0..9000u16 {
            receive(&mut m, seq, 1, 0, 12, at);
        }
        assert_eq!(m.streams.values().next().unwrap().seen.len(), 8192);
        assert_eq!(m.wire.len(), 0);
        assert_eq!(m.order.len(), 8192);
        // A packet older than the dedup history is not newly counted.
        receive(&mut m, 0, 1, 0, 12, at);
        assert_eq!(m.bitrate("0", at + Duration::from_secs(1)), 9000 * 13 * 8);
    }

    #[test]
    #[ignore = "requires coordinator-run fixed RTPStatsReceiver byte oracle"]
    fn actual_go_receiver_bytes() {
        let cases =
            std::fs::read_to_string(std::env::var("WHIP_METRICS_GO_CASES").expect("TSV cases"))
                .unwrap();
        let results = std::fs::read_to_string(
            std::env::var("WHIP_METRICS_GO_RESULTS").expect("actual Go TSV"),
        )
        .unwrap();
        let cases: Vec<_> = cases.lines().collect();
        let rows: Vec<_> = results.lines().collect();
        assert_eq!(cases.len(), 6);
        assert_eq!(rows.len(), 6);
        let at = Instant::now();
        for (case, row) in cases.iter().zip(rows.iter()) {
            let (id, packets) = case.split_once('\t').unwrap();
            let (actual_id, expected) = row.split_once('\t').unwrap();
            assert_eq!(id, actual_id);
            let mut m = IngressMetrics::default();
            for packet in packets.split(';') {
                let p: Vec<u64> = packet.split(',').map(|s| s.parse().unwrap()).collect();
                assert_eq!(p.len(), 5);
                receive(
                    &mut m,
                    p[0] as u16,
                    p[3] as usize,
                    p[4] as usize,
                    p[2] as usize,
                    at,
                );
            }
            assert_eq!(
                u64::from(m.bitrate("0", at + Duration::from_secs(1))),
                expected.parse::<u64>().unwrap() * 8,
                "{id}"
            );
        }
    }
}
