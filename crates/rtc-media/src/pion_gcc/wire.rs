// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
//! Pinned Pion RTCP TransportLayerCC.Unmarshal wire boundary; keeps full vector
//! symbols, min(status remaining,run), and the original strict chunk-end test.
use super::{Chunk, FeedbackError};
#[derive(Clone, Debug)]
pub struct TwccReport {
    pub base: u16,
    pub reference: u32,
    pub chunks: Vec<Chunk>,
    pub deltas_us: Vec<i64>,
}
fn short() -> FeedbackError {
    FeedbackError::Wire("rtcp: packet too short")
}
pub fn parse_twcc(raw: &[u8]) -> Result<TwccReport, FeedbackError> {
    if raw.len() < 8 {
        return Err(short());
    }
    if raw[0] >> 6 != 2 {
        return Err(FeedbackError::Wire("rtcp: invalid packet version"));
    }
    let total = 4 * (usize::from(u16::from_be_bytes([raw[2], raw[3]])) + 1);
    if total < 20 || raw.len() < total {
        return Err(short());
    }
    if raw[1] != 205 || raw[0] & 31 != 15 {
        return Err(FeedbackError::Wire("rtcp: wrong packet type"));
    }
    let base = u16::from_be_bytes([raw[12], raw[13]]);
    let count = u16::from_be_bytes([raw[14], raw[15]]);
    let reference = u32::from_be_bytes([0, raw[16], raw[17], raw[18]]);
    let mut at = 20;
    let mut processed = 0u16;
    let mut chunks = vec![];
    let mut delta_types = vec![];
    while processed < count {
        // This >= predicate is deliberately retained from Pion, not changed
        // into the more permissive native-str0m parser's endpoint behavior.
        if at + 2 >= total {
            return Err(short());
        }
        let chunk = u16::from_be_bytes([raw[at], raw[at + 1]]);
        at += 2;
        if chunk & 0x8000 == 0 {
            let symbol = ((chunk >> 13) & 3) as u8;
            let length = chunk & 0x1fff;
            let consumed = length.min(count - processed);
            if matches!(symbol, 1 | 2) {
                delta_types.extend(std::iter::repeat_n(symbol, consumed as usize));
            }
            processed = processed.wrapping_add(consumed);
            chunks.push(Chunk::Run { symbol, length });
        } else {
            let bits = if chunk & 0x4000 == 0 { 1 } else { 2 };
            let n = 14 / bits;
            let symbols: Vec<_> = (0..n)
                .map(|i| ((chunk >> (14 - bits * (i + 1))) & ((1 << bits) - 1)) as u8)
                .collect();
            for &symbol in &symbols {
                if matches!(symbol, 1 | 2) {
                    delta_types.push(symbol)
                }
            }
            processed = processed.wrapping_add(n as u16);
            chunks.push(Chunk::Vector(symbols));
        }
    }
    let mut deltas_us = Vec::with_capacity(delta_types.len());
    for typ in delta_types {
        let delta = if typ == 1 {
            if at + 1 > total {
                return Err(short());
            }
            let value = i64::from(raw[at]) * 250;
            at += 1;
            value
        } else {
            if at + 2 > total {
                return Err(short());
            }
            let value = i64::from(i16::from_be_bytes([raw[at], raw[at + 1]])) * 250;
            at += 2;
            value
        };
        deltas_us.push(delta);
    }
    Ok(TwccReport {
        base,
        reference,
        chunks,
        deltas_us,
    })
}

/// Actual RFC8888 feedback fields; the original report timestamp is retained.
#[derive(Clone, Debug)]
pub struct Rfc8888Report {
    pub timestamp: u32,
    pub blocks: Vec<super::Rfc8888Block>,
}
/// Fixed RTCP CCFeedbackReport.Unmarshal semantics, including its full-packet
/// block bound. The compound dispatcher must select PT205/FMT11 beforehand.
pub fn parse_rfc8888(raw: &[u8]) -> Result<Rfc8888Report, FeedbackError> {
    if raw.len() < 12 {
        return Err(short());
    }
    if raw[0] >> 6 != 2 {
        return Err(FeedbackError::Wire("rtcp: invalid packet version"));
    }
    if raw[1] != 205 {
        return Err(FeedbackError::Wire("rtcp: wrong packet type"));
    }
    let end = raw.len() - 4;
    let timestamp = u32::from_be_bytes(raw[end..].try_into().unwrap());
    let mut at = 8;
    let mut blocks = Vec::new();
    while at < end {
        if raw.len() - at < 8 {
            return Err(FeedbackError::Wire(
                "feedback report blocks must be at least 8 bytes",
            ));
        }
        let ssrc = u32::from_be_bytes(raw[at..at + 4].try_into().unwrap());
        let base = u16::from_be_bytes(raw[at + 4..at + 6].try_into().unwrap());
        let count = usize::from(u16::from_be_bytes(raw[at + 6..at + 8].try_into().unwrap()));
        if raw.len() - at < 8 + count * 2 {
            return Err(FeedbackError::Wire(
                "feedback report block contains less reports than num_reports",
            ));
        }
        let mut metrics = Vec::with_capacity(count);
        for i in 0..count {
            let pos = at + 8 + i * 2;
            let value = u16::from_be_bytes([raw[pos], raw[pos + 1]]);
            metrics.push(if value & 0x8000 != 0 {
                (true, value & 0x1fff, ((value >> 13) & 3) as u8)
            } else {
                (false, 0, 0)
            });
        }
        blocks.push((ssrc, base, metrics));
        at += 8 + (count + count % 2) * 2;
    }
    Ok(Rfc8888Report { timestamp, blocks })
}
/// Actual interceptor internal/ntp.ToTime(uint64(reportTimestamp)<<16).
/// This deliberately retains source compact-NTP era behavior, not ToTime32.
pub fn rfc8888_report_time(timestamp: u32) -> super::Timestamp {
    let seconds = u64::from(timestamp >> 16);
    let fraction = f64::from((timestamp & 0xffff) << 16) / f64::from(u32::MAX);
    let since_1900 = i128::from(seconds) * i128::from(super::SECOND)
        + i128::from(super::go_int(fraction * super::SECOND as f64));
    (62_135_596_800i128 - 2_208_988_800i128) * i128::from(super::SECOND) + since_1900
}
