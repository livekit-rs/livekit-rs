// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
//! Fixed internal/cc/feedback_adapter.go. Keeps original full-chunk, default-ACK
//! and matched-only delta advancement semantics; native TWCC matches are not used.
use super::{Ack, MS, Timestamp};
use std::collections::{HashMap, VecDeque};
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Chunk {
    Run { symbol: u8, length: u16 },
    Vector(Vec<u8>),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedbackError {
    InvalidFeedback,
    Closed,
    Wire(&'static str),
}
/// Source report block: SSRC, base sequence, (received, offset, ECN) metrics.
pub type Rfc8888Block = (u32, u16, Vec<(bool, u16, u8)>);
/// Exact source 250-key sent history (update moves existing key to MRU).
pub struct Adapter {
    history: HashMap<(u32, u16), Ack>,
    order: VecDeque<(u32, u16)>,
}
impl Default for Adapter {
    fn default() -> Self {
        Self::new()
    }
}
impl Adapter {
    pub fn new() -> Self {
        Self {
            history: HashMap::new(),
            order: VecDeque::new(),
        }
    }
    pub fn sent(&mut self, ack: Ack) {
        let key = (ack.ssrc, ack.sequence);
        if self.history.insert(key, ack).is_some() {
            self.order.retain(|v| *v != key)
        }
        self.order.push_front(key);
        if self.order.len() > 250 {
            self.history.remove(&self.order.pop_back().unwrap());
        }
    }
    /// Ref is the exact 24-bit TWCC value; deltas are actual signed microseconds.
    /// The source adapter consumes full chunks, even their unused trailing symbols.
    pub fn twcc(
        &self,
        base: u16,
        reference: u32,
        chunks: &[Chunk],
        deltas_us: &[i64],
    ) -> Result<Vec<Ack>, FeedbackError> {
        let mut index = base;
        let mut time = i128::from(reference) * 64 * i128::from(MS);
        let mut delta = 0;
        let mut out = Vec::new();
        for chunk in chunks {
            let (length, uniform) = match chunk {
                Chunk::Run { symbol, length } => (*length as usize, Some(*symbol)),
                Chunk::Vector(v) => (v.len(), None),
            };
            for i in 0..length {
                let symbol = uniform.unwrap_or_else(|| match chunk {
                    Chunk::Vector(v) => v[i],
                    _ => unreachable!(),
                });
                let mut ack = self.history.get(&(0, index)).copied().unwrap_or_default();
                if self.history.contains_key(&(0, index)) && symbol != 0 {
                    time +=
                        i128::from(*deltas_us.get(delta).ok_or(FeedbackError::InvalidFeedback)?)
                            * 1000;
                    delta += 1;
                    ack.arrival = (time != 0).then_some(time);
                }
                out.push(ack);
                index = index.wrapping_add(1);
            }
        }
        Ok(out)
    }
    /// RFC8888 arrival offset units are 1/1024s, truncated like source conversion.
    /// Reference timestamp has already been converted from actual compact NTP.
    pub fn rfc8888(&self, reference_ns: Timestamp, blocks: &[Rfc8888Block]) -> Vec<Ack> {
        let mut out = Vec::new();
        for (ssrc, base, metrics) in blocks {
            for (i, (received, offset, ecn)) in metrics.iter().enumerate() {
                if let Some(mut ack) = self
                    .history
                    .get(&(*ssrc, base.wrapping_add(i as u16)))
                    .copied()
                {
                    if *received {
                        let delta =
                            super::go_int(f64::from(*offset) / 1024.0 * super::SECOND as f64);
                        ack.arrival = Some(reference_ns - i128::from(delta));
                        ack.ecn = *ecn
                    }
                    out.push(ack)
                }
            }
        }
        out
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_full_vector_unmatched_defaults_and_matched_delta_index_are_retained() {
        let mut a = Adapter::new();
        a.sent(Ack {
            sequence: 2,
            departure: Some(1),
            size: 1200,
            ..Ack::default()
        });
        let out = a
            .twcc(1, 2, &[Chunk::Vector(vec![1, 1, 0])], &[250, 500])
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], Ack::default());
        assert_eq!(out[1].arrival, Some(i128::from(128 * MS + 250_000)));
        assert_eq!(out[2], Ack::default());
    }
    #[test]
    fn history_wrap_replacement_eviction_and_bad_delta_are_real_source_paths() {
        let mut a = Adapter::new();
        for i in 0..251 {
            a.sent(Ack {
                sequence: i,
                departure: Some(i128::from(i)),
                ..Ack::default()
            })
        }
        assert_eq!(a.history.len(), 250);
        assert!(!a.history.contains_key(&(0, 0)));
        assert_eq!(
            a.twcc(
                250,
                0,
                &[Chunk::Run {
                    symbol: 1,
                    length: 1
                }],
                &[]
            ),
            Err(FeedbackError::InvalidFeedback)
        );
    }
}
