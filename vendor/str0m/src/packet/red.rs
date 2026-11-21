//! RFC 2198 "RTP Payload for Redundant Audio Data" (RED) encode/decode.
//!
//! The RED structure is part of the RTP payload (so it is fully covered by SRTP): N redundant
//! block headers (4 bytes each, F=1: 7-bit PT, 14-bit timestamp offset, 10-bit length), then the
//! primary block header (1 byte, F=0: 7-bit PT), then all block payloads in the same order.

use std::collections::VecDeque;

use crate::rtp_::{Pt, SeqNo};

use super::PacketError;

const F_BIT: u8 = 0x80;

/// Largest representable timestamp offset (14-bit field).
const MAX_TS_OFFSET: u32 = 0x3fff;

/// Largest representable redundant block length (10-bit field).
const MAX_BLOCK_LEN: usize = 0x3ff;

/// Maximum redundant blocks accepted in one RED payload. RFC 2198 sets no limit, but real
/// Opus-RED senders use a single level of redundancy (str0m sends one; libwebrtc defaults to one
/// and its receiver caps at 32). A 4-byte redundant header can otherwise pack hundreds of blocks
/// into one datagram, so reject more than this to bound the parse work a malformed or hostile
/// peer can trigger.
const MAX_REDUNDANT_BLOCKS: usize = 32;

/// How many packets back we recover from on receive, and the deepest distance a sender may be
/// configured to send (see `CodecConfig::set_red_distances`). This covers realistic multi-level
/// patterns such as `[1, 3, 5]` while bounding the per-packet recovery work (decode + depayload +
/// seq lookup) a hostile peer can trigger, independent of how many blocks the payload carries.
pub(crate) const MAX_RED_RECOVERY_DEPTH: u64 = 8;

/// A redundant block to prepend to the primary payload (older media).
#[derive(Debug, Clone)]
pub struct RedundantBlock {
    /// Block payload type (7-bit).
    pub pt: u8,
    /// `primary_rtp_timestamp - block_timestamp`, 14-bit (max 16383).
    pub timestamp_offset: u32,
    /// Block payload bytes, 10-bit length (max 1023).
    pub payload: Vec<u8>,
}

impl RedundantBlock {
    /// Whether this block fits RFC 2198's 14-bit offset and 10-bit length fields.
    pub fn fits(&self) -> bool {
        self.timestamp_offset <= MAX_TS_OFFSET && self.payload.len() <= MAX_BLOCK_LEN
    }
}

/// A block parsed out of a RED payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedBlock<'a> {
    /// Block payload type (7-bit).
    pub pt: u8,
    /// Timestamp offset from the primary (0 for the primary block).
    pub timestamp_offset: u32,
    /// Block payload bytes (no header).
    pub payload: &'a [u8],
    /// Whether this is the primary (final) block.
    pub is_primary: bool,
}

/// Encoder for RFC 2198 RED payloads.
pub struct RedEncoder;

impl RedEncoder {
    /// Encode a RED payload: redundant blocks (oldest first) followed by the primary block.
    ///
    /// Redundant blocks whose offset or length do not fit RFC 2198's 14-bit / 10-bit fields are
    /// skipped (see [`RedundantBlock::fits`]) so the output is always a valid RED payload, never
    /// silently truncated.
    pub fn encode(primary_pt: u8, primary: &[u8], redundant: &[RedundantBlock]) -> Vec<u8> {
        // Cap at the decoder's bound so encoder output always round-trips: `RedDecoder::decode`
        // rejects more than `MAX_REDUNDANT_BLOCKS` redundant blocks as a DoS guard. The internal
        // sender never approaches this (at most `MAX_RED_RECOVERY_DEPTH` levels), so this only
        // clamps public-API misuse.
        let blocks: Vec<&RedundantBlock> = redundant
            .iter()
            .filter(|b| b.fits())
            .take(MAX_REDUNDANT_BLOCKS)
            .collect();

        let mut out = Vec::with_capacity(primary.len() + blocks.len() * 4 + 1);

        for b in &blocks {
            // `fits()` guarantees ts <= 0x3fff and len <= 0x3ff, so no masking is needed.
            let ts = b.timestamp_offset;
            let len = b.payload.len() as u32;
            out.push(F_BIT | (b.pt & 0x7f));
            out.push((ts >> 6) as u8);
            out.push((((ts & 0x3f) << 2) | (len >> 8)) as u8);
            out.push((len & 0xff) as u8);
        }

        // Primary header, F=0.
        out.push(primary_pt & 0x7f);

        for b in &blocks {
            out.extend_from_slice(&b.payload);
        }
        out.extend_from_slice(primary);

        out
    }
}

/// Decoder for RFC 2198 RED payloads.
pub struct RedDecoder;

impl RedDecoder {
    /// Parse a RED payload into its blocks (redundant first, primary last).
    ///
    /// Never panics; returns `Err` on any malformed input. RED payloads come from untrusted
    /// remote peers, so this strictly validates header chaining and block lengths.
    pub fn decode(data: &[u8]) -> Result<Vec<RedBlock<'_>>, PacketError> {
        struct Hdr {
            pt: u8,
            ts_offset: u32,
            len: Option<usize>,
        }

        // Phase 1: parse headers up to and including the primary (F=0) header.
        let mut hdrs: Vec<Hdr> = Vec::new();
        let mut i = 0;
        loop {
            let first = *data.get(i).ok_or(PacketError::ErrRedCorruptedPacket)?;
            if first & F_BIT == 0 {
                hdrs.push(Hdr {
                    pt: first & 0x7f,
                    ts_offset: 0,
                    len: None,
                });
                i += 1;
                break;
            }
            // Bound the redundant blocks (untrusted input) before parsing/allocating more. At this
            // point `hdrs` holds only redundant headers, since the primary (F=0) breaks the loop.
            if hdrs.len() >= MAX_REDUNDANT_BLOCKS {
                return Err(PacketError::ErrRedCorruptedPacket);
            }
            let h = data
                .get(i..i + 4)
                .ok_or(PacketError::ErrRedCorruptedPacket)?;
            let ts_offset = ((h[1] as u32) << 6) | ((h[2] as u32) >> 2);
            let len = (((h[2] as usize) & 0x03) << 8) | (h[3] as usize);
            hdrs.push(Hdr {
                pt: h[0] & 0x7f,
                ts_offset,
                len: Some(len),
            });
            i += 4;
        }

        // Phase 2: slice the payloads in the same order as the headers.
        let mut out = Vec::with_capacity(hdrs.len());
        for h in &hdrs {
            match h.len {
                Some(len) => {
                    let payload = data
                        .get(i..i + len)
                        .ok_or(PacketError::ErrRedCorruptedPacket)?;
                    out.push(RedBlock {
                        pt: h.pt,
                        timestamp_offset: h.ts_offset,
                        payload,
                        is_primary: false,
                    });
                    i += len;
                }
                None => {
                    // Primary block takes the remaining bytes (may be empty).
                    out.push(RedBlock {
                        pt: h.pt,
                        timestamp_offset: 0,
                        payload: &data[i..],
                        is_primary: true,
                    });
                }
            }
        }

        Ok(out)
    }
}

/// The redundant blocks that carry `primary_pt` and sit strictly before the primary.
///
/// RFC 2198 allows a redundant block to use a different PT than the primary; str0m only
/// recovers the negotiated codec, so blocks of another PT are skipped. Which sequence number a
/// block belongs to is decided by the receiving stream (it needs the surrounding received
/// packets, see `StreamRx::red_locate_seq`), not from the offsets alone.
pub(crate) fn red_same_pt_blocks<'a>(
    redundant: &'a [RedBlock<'a>],
    primary_pt: u8,
) -> impl Iterator<Item = &'a RedBlock<'a>> {
    redundant
        .iter()
        .filter(move |b| b.pt == primary_pt && b.timestamp_offset > 0)
}

/// Go `maxRedPayload` in livekit-server `pkg/sfu/redreceiver.go`: a primary frame of at least
/// this size cannot be stored as a redundant block (the RFC 2198 block length field is 10 bits),
/// so Go broadcasts the original packet and never records the frame for redundancy.
const MAX_RED_PAYLOAD: usize = 1 << 10;

/// Go's timestamp test for a redundant block: an offset of `1 << 14` RTP ticks or more is a
/// different talk-spurt and is not repeated.
const MAX_RED_TS_GAP: u32 = 1 << 14;

/// RFC 2198 redundant block header size in bytes (4), as used by Go's `encodeRedForPrimary`
/// budget accounting.
const REDUNDANT_HEADER_LEN: usize = 4;

/// Half the 16-bit sequence space, i.e. RTP serial-number arithmetic: `a - b < 1 << 15` means `a`
/// is more recent than `b` (Go's slot-selection test in `encodeRedForPrimary`).
const SEQ_IS_MORE_RECENT: u16 = 1 << 15;

/// One `pktBuff` entry of Go's `RedReceiver`: the parts of a primary frame kept for redundancy.
#[derive(Debug, Clone)]
struct HistPacket {
    /// Wire sequence number, compared with 16-bit modular arithmetic like Go.
    seq: u16,
    rtp_time: u32,
    payload: Vec<u8>,
}

/// Send-side RFC 2198 RED state, ported from livekit-server `pkg/sfu/redreceiver.go`.
///
/// `distances` lists how many packets back each redundant level carries (`[1, 2]` is LiveKit's
/// pattern and the equivalent of Go's fixed `maxRedCount = 2` window over the two previous
/// frames; `[1, 3, 5]` = three levels for higher loss), always sorted, distinct and >= 1.
/// `history` is Go's `pktBuff`: a window of `max(distances)` frames whose front slots stay unfilled
/// while a talk-spurt starts up.
///
/// This is the send-side companion to [`RedDecoder`]: [`Payloader`][crate::packet::Payloader]
/// stays codec- and RED-agnostic; the `Media` layer owns the `RedSender` and applies it through a
/// [`RedSink`][crate::packet::RedSink].
#[derive(Debug)]
pub(crate) struct RedSender {
    red_pt: Pt,
    primary_pt: Pt,
    distances: Box<[u32]>,
    /// Go `pktBuff`: oldest at index 0, `None` = slot not filled yet. Its length is the deepest
    /// configured distance, which for LiveKit's `[1, 2]` equals Go's `maxRedCount`.
    history: Vec<Option<HistPacket>>,
}

/// [`RedSender::wrap`] output: the payload bytes plus the payload type they must go out on.
///
/// Go's `RedReceiver.ForwardRTP` either broadcasts the wrapped packet on `opusRedPT` or, for a
/// frame too large to be a redundant block, the *original* packet on the primary PT. Labelling
/// plain Opus bytes with the RED payload type would make the peer parse them as a RED payload, so
/// the choice travels with the payload instead of being decided by the caller.
pub(crate) enum RedWrapped {
    /// An RFC 2198 payload: the primary block plus redundant blocks when history allowed them.
    Redundant(Vec<u8>),
    /// The primary frame unwrapped — Go's fallback, sent on the primary payload type.
    Primary(Vec<u8>),
}

impl RedWrapped {
    /// The payload type this packet must be written with.
    pub(crate) fn pt(&self, red_pt: Pt, primary_pt: Pt) -> Pt {
        match self {
            RedWrapped::Redundant(_) => red_pt,
            RedWrapped::Primary(_) => primary_pt,
        }
    }

    /// The payload bytes.
    pub(crate) fn into_payload(self) -> Vec<u8> {
        match self {
            RedWrapped::Redundant(payload) | RedWrapped::Primary(payload) => payload,
        }
    }
}

impl RedSender {
    /// Create a RED sender for `primary_pt`, wrapping into `red_pt`, with the given distance
    /// pattern (must be sorted, distinct and >= 1).
    pub(crate) fn new(red_pt: Pt, primary_pt: Pt, distances: &[u32]) -> Self {
        let mut sender = RedSender {
            red_pt,
            primary_pt,
            distances: distances.into(),
            history: Vec::new(),
        };
        sender.resize_history();
        sender
    }

    /// The RED payload type a wrapped packet is sent on.
    pub(crate) fn red_pt(&self) -> Pt {
        self.red_pt
    }

    /// The primary payload type, used for the frames that bypass RED wrapping.
    pub(crate) fn primary_pt(&self) -> Pt {
        self.primary_pt
    }

    /// Keep the sender current with the negotiated RED PT, primary PT and distance pattern without
    /// discarding a live talk-spurt's redundancy history. If the pattern changed, adopt it and
    /// resize the window to the new deepest distance. `distances` must be sorted, distinct and >= 1.
    pub(crate) fn sync(&mut self, red_pt: Pt, primary_pt: Pt, distances: &[u32]) {
        self.red_pt = red_pt;
        self.primary_pt = primary_pt;
        if *self.distances != *distances {
            self.distances = distances.into();
            self.resize_history();
        }
    }

    /// Wrap the primary frame `payload` (sequence number `seq`, timestamp `rtp_time`) into a RED
    /// payload, following Go's `RedReceiver.encodeRedForPrimary` step for step:
    ///
    /// 1. A frame of [`MAX_RED_PAYLOAD`] or more, or one that leaves no room for the mandatory
    ///    1-byte primary block header, is [`RedWrapped::Primary`] and records nothing — Go
    ///    broadcasts the original packet in that case, before touching the history.
    /// 2. The redundant blocks are the recorded history slots at the configured recency ranks, and
    ///    Go's own filters apply to each candidate: the same sequence number (a repeat), a sequence
    ///    gap larger than the window (an unordered, i.e. older, frame) or a timestamp gap of
    ///    `1 << 14` ticks (another talk-spurt) never become redundancy.
    /// 3. If the primary plus every accepted block does not fit `budget`, ALL redundancy is
    ///    dropped (Go `redPkts = redPkts[:0]`) instead of shedding only the deepest level, so a
    ///    packet is either fully protected or plain.
    ///
    /// Finally the primary frame is recorded exactly like Go (see [`Self::insert_history`]) and the
    /// blocks are emitted oldest-first as RFC 2198 requires.
    pub(crate) fn wrap(
        &mut self,
        payload: Vec<u8>,
        seq: SeqNo,
        rtp_time: u32,
        budget: usize,
    ) -> RedWrapped {
        // Go: `if len(pkt.Packet.Payload) >= maxRedPayload { broadcast the original }`.
        if payload.len() >= MAX_RED_PAYLOAD {
            return RedWrapped::Primary(payload);
        }
        // The primary block header is mandatory, so a frame with no room left for it cannot be
        // wrapped. Go's fixed 1500-byte RED buffer makes this unreachable; a small configured send
        // MTU does not, and sending the frame unwrapped on the primary PT is the same fallback Go
        // uses for an oversized frame instead of pushing the packet past the MTU.
        if payload.len() + 1 > budget {
            return RedWrapped::Primary(payload);
        }

        let primary_pt = *self.primary_pt;
        let seq16 = seq.as_u16();
        let depth = self.history.len();
        // Go's `redLength`: the window size, which is what the sequence-gap filter compares against.
        let window = depth as u16;
        // Go's `lastNilPkt`: the highest still-empty slot (-1 once the window is full), so only
        // frames that were actually recorded are candidates.
        let start = match (0..depth).rev().find(|&i| self.history[i].is_none()) {
            Some(nil) => nil + 1,
            None => 0,
        };

        let mut blocks: Vec<RedundantBlock> = Vec::new();
        for i in (start..depth).rev() {
            // Recency rank 1 is the newest slot, i.e. a distance of 1.
            let distance = (depth - i) as u32;
            if !self.distances.contains(&distance) {
                continue;
            }
            let Some(prev) = self.history[i].as_ref() else {
                continue;
            };
            if prev.seq == seq16 {
                continue; // the very same frame again (Go: repeat)
            }
            if seq16.wrapping_sub(prev.seq) > window {
                // Outside the window — including a frame that is *older* than this slot, whose
                // subtraction wraps past the 16-bit range (Go: unordered frame).
                continue;
            }
            let timestamp_offset = rtp_time.wrapping_sub(prev.rtp_time);
            if timestamp_offset >= MAX_RED_TS_GAP {
                // A different talk-spurt (Go: `>= 1<<14`), or a backwards timestamp.
                continue;
            }
            blocks.push(RedundantBlock {
                pt: primary_pt,
                timestamp_offset,
                payload: prev.payload.clone(),
            });
        }
        // Collected newest-first; Go's `redPkts` is history order, which is oldest-first.
        blocks.reverse();

        // Go `encodeRedForPrimary` budget: primary payload + 1 header byte + (4 header bytes +
        // payload) per block; over the buffer means no redundancy at all.
        let needed = payload.len()
            + 1
            + blocks
                .iter()
                .map(|b| b.payload.len() + REDUNDANT_HEADER_LEN)
                .sum::<usize>();
        if needed > budget {
            blocks.clear();
        }

        let bytes = RedEncoder::encode(primary_pt, &payload, &blocks);

        self.insert_history(seq16, rtp_time, payload);
        RedWrapped::Redundant(bytes)
    }

    /// Record the primary frame in the window exactly like Go's insert loop: scan from the newest
    /// slot backwards and use the first slot that is still empty or holds a frame older than this
    /// one (`seq - slot_seq < 1<<15`, RTP serial arithmetic), aging the frames below it out of the
    /// way. A frame older than every recorded frame is not recorded at all, as in Go.
    fn insert_history(&mut self, seq: u16, rtp_time: u32, payload: Vec<u8>) {
        let depth = self.history.len();
        for i in (0..depth).rev() {
            let more_recent = match self.history[i].as_ref() {
                None => true,
                Some(slot) => seq.wrapping_sub(slot.seq) < SEQ_IS_MORE_RECENT,
            };
            if !more_recent {
                continue;
            }
            // Go ages the older frames out (`for j := 0; j < i; j++ { pktBuff[j] = pktBuff[j+1] }`)
            // and then writes the new frame into slot `i`. Go's `freeHistSlot`/`histPayloads` only
            // recycle those buffers; owning each payload here makes the equivalent move.
            for j in 0..i {
                self.history[j] = self.history[j + 1].take();
            }
            self.history[i] = Some(HistPacket {
                seq,
                rtp_time,
                payload,
            });
            return;
        }
    }

    /// Size the window to the deepest configured distance (Go's `maxRedCount`), keeping the newest
    /// frames when the pattern changes and leaving the front slots empty for a fresh talk-spurt.
    fn resize_history(&mut self) {
        let depth = self.distances.last().copied().unwrap_or(1) as usize;
        let mut kept: Vec<_> = std::mem::take(&mut self.history)
            .into_iter()
            .flatten()
            .collect();
        let skip = kept.len().saturating_sub(depth);
        let mut history: Vec<Option<HistPacket>> = kept.drain(skip..).map(Some).collect();
        while history.len() < depth {
            history.insert(0, None);
        }
        self.history = history;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn red_sender(distances: &[u32]) -> RedSender {
        RedSender::new(Pt::new_with_value(63), Pt::new_with_value(111), distances)
    }

    // ---- Go-faithful vectors (livekit-server pkg/sfu/redreceiver.go) ------------------------
    //
    // `redreceiver.go` fixes `maxRedCount = 2`, `mtuSize = 1500`, `maxRedPayload = 1 << 10`,
    // `opusPT = 111` and `opusRedPT = 63`, so these tests mirror Go's constants instead of reading
    // them from configuration. Every vector is the frozen fixture of Go's `TestRedReceiver`
    // subtests: 48 kHz frames 10 ms apart starting at sequence number 65534 (so the sequence number
    // wraps through 0 inside the vector) and RTP timestamp `(1 << 31) - 2*tsStep`, with the
    // marshalled RTP header as the payload so each frame's redundant bytes are distinguishable.

    /// Go `tsStep = uint32(48000 / 1000 * 10)`.
    const TS_STEP: u32 = 48_000 / 1000 * 10;

    /// Go `rtp.Header{SequenceNumber: 65534, ...}`.
    const BASE_SEQ: u16 = 65534;

    /// Go `Timestamp: (uint32(1) << 31) - 2*tsStep`.
    const BASE_TS: u32 = (1 << 31) - 2 * TS_STEP;

    /// Go `mtuSize`: the length of `redPayloadBuf`, i.e. the budget `ForwardRTP` gives the encoder.
    const GO_BUDGET: usize = 1500;

    /// A budget small enough to reach the fit check without involving Go's constants.
    const WRAP_BUDGET: usize = 1200;

    /// One input frame: `(wire sequence number, RTP timestamp, primary payload)`.
    type Frame = (u16, u32, Vec<u8>);

    /// Go's `header.Marshal()` for the fixture header (V=2, PT=111, no extension, SSRC 0).
    fn go_payload(seq: u16, rtp_time: u32) -> Vec<u8> {
        let mut payload = vec![0x80, 111, (seq >> 8) as u8, seq as u8];
        payload.extend_from_slice(&rtp_time.to_be_bytes());
        payload.extend_from_slice(&[0, 0, 0, 0]);
        payload
    }

    /// Go `generatePkts`: `count` frames `step` ticks apart, sequence numbers wrapping on the wire.
    fn go_frames(seq: u16, rtp_time: u32, count: usize, step: u32) -> Vec<Frame> {
        (0..count)
            .map(|i| {
                let seq = seq.wrapping_add(i as u16);
                let rtp_time = rtp_time.wrapping_add(i as u32 * step);
                (seq, rtp_time, go_payload(seq, rtp_time))
            })
            .collect()
    }

    /// A wire sequence number as str0m's `SeqNo`; RED only uses the 16-bit wire value, so the
    /// rollover counter is irrelevant here.
    fn seq_no(seq: u16) -> SeqNo {
        u64::from(seq).into()
    }

    /// Wrap each frame the way `RedReceiver.ForwardRTP` does for one down track, reporting whether
    /// the packet went out on the RED payload type (`true`) or on the primary payload type.
    fn forward_all(red: &mut RedSender, frames: &[Frame], budget: usize) -> Vec<(bool, Vec<u8>)> {
        frames
            .iter()
            .map(|(seq, rtp_time, payload)| {
                let wrapped = red.wrap(payload.clone(), seq_no(*seq), *rtp_time, budget);
                match wrapped {
                    RedWrapped::Redundant(bytes) => (true, bytes),
                    RedWrapped::Primary(bytes) => (false, bytes),
                }
            })
            .collect()
    }

    /// The RFC 2198 header chain of an encoded payload as `(pt, timestamp_offset, payload)`,
    /// oldest block first with the primary block last.
    fn red_blocks(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
        RedDecoder::decode(bytes)
            .expect("a RED payload must decode")
            .iter()
            .map(|b| (b.pt, b.timestamp_offset, b.payload.to_vec()))
            .collect()
    }

    /// Check every wrapped output against `expect[index]`, the redundant blocks as
    /// `(timestamp_offset, source frame index)` in wire order; the primary block is the frame
    /// itself. `bypass` lists the frames Go broadcasts unwrapped on the primary payload type.
    fn assert_outputs(
        outputs: &[(bool, Vec<u8>)],
        frames: &[Frame],
        expect: &[Vec<(u32, usize)>],
        bypass: &[usize],
    ) {
        assert_eq!(outputs.len(), frames.len());
        assert_eq!(expect.len(), frames.len(), "one expectation per frame");
        for (index, ((on_red_pt, bytes), blocks)) in outputs.iter().zip(expect).enumerate() {
            let is_bypass = bypass.contains(&index);
            assert_eq!(
                *on_red_pt, !is_bypass,
                "frame {index} payload type (bypassed frames: {bypass:?})"
            );
            if is_bypass {
                assert_eq!(
                    bytes, &frames[index].2,
                    "frame {index} must go out as the original payload"
                );
                continue;
            }
            let decoded = red_blocks(bytes);
            assert_eq!(
                decoded.len(),
                blocks.len() + 1,
                "frame {index} redundant block count"
            );
            for (block_index, (offset, source)) in blocks.iter().enumerate() {
                let (pt, actual_offset, payload) = &decoded[block_index];
                assert_eq!(*pt, 111, "frame {index} block {block_index} payload type");
                assert_eq!(
                    *actual_offset, *offset,
                    "frame {index} block {block_index} timestamp offset"
                );
                assert_eq!(
                    payload, &frames[*source].2,
                    "frame {index} block {block_index} must repeat frame {source}"
                );
            }
            let (pt, offset, payload) = decoded.last().unwrap();
            assert_eq!(*pt, 111, "frame {index} primary payload type");
            assert_eq!(*offset, 0, "frame {index} primary block has no timestamp offset");
            assert_eq!(payload, &frames[index].2, "frame {index} primary payload");
        }
    }

    #[test]
    fn red_sender_matches_go_normal_consecutive_frames() {
        let mut red = red_sender(&[1, 2]);
        let frames = go_frames(BASE_SEQ, BASE_TS, 10, TS_STEP);
        let outputs = forward_all(&mut red, &frames, GO_BUDGET);

        // Go's window is two frames deep: the second frame repeats the first, every later frame
        // repeats both, oldest first.
        let expect: Vec<Vec<(u32, usize)>> = (0..10)
            .map(|i| match i {
                0 => vec![],
                1 => vec![(TS_STEP, 0)],
                _ => vec![(2 * TS_STEP, i - 2), (TS_STEP, i - 1)],
            })
            .collect();
        assert_outputs(&outputs, &frames, &expect, &[]);

        // Wire check of the third frame: redundant headers for offsets 960 and 480 with 12-byte
        // blocks, the primary header byte, then the three payloads.
        let (_, bytes) = &outputs[2];
        assert_eq!(
            bytes.len(),
            45,
            "two redundant headers, one primary header and three payloads"
        );
        assert_eq!(
            &bytes[..9],
            &[0xEF, 0x0F, 0x00, 0x0C, 0xEF, 0x07, 0x80, 0x0C, 111],
            "F|PT, timestamp offset and block length fields of both redundant blocks"
        );
    }

    #[test]
    fn red_sender_matches_go_packet_loss_and_sequence_jump() {
        let mut red = red_sender(&[1, 2]);
        // Go "packet lost and jump": every other frame is dropped, then a ten-sequence jump.
        let mut frames: Vec<Frame> = go_frames(BASE_SEQ, BASE_TS, 10, TS_STEP)
            .into_iter()
            .filter(|(seq, _, _)| seq.wrapping_sub(BASE_SEQ) % 2 == 1)
            .collect();
        frames.extend(go_frames(
            BASE_SEQ.wrapping_add(20),
            BASE_TS.wrapping_add(20 * TS_STEP),
            3,
            TS_STEP,
        ));
        let outputs = forward_all(&mut red, &frames, GO_BUDGET);

        // A loss leaves a surviving frame a whole step behind the newest slot, so each survivor
        // repeats only its predecessor. After the jump the gap exceeds the window, so the first
        // frame of the new run is plain and redundancy rebuilds as the window fills again.
        let expect: Vec<Vec<(u32, usize)>> = vec![
            vec![],
            vec![(2 * TS_STEP, 0)],
            vec![(2 * TS_STEP, 1)],
            vec![(2 * TS_STEP, 2)],
            vec![(2 * TS_STEP, 3)],
            vec![],
            vec![(TS_STEP, 5)],
            vec![(2 * TS_STEP, 5), (TS_STEP, 6)],
        ];
        assert_eq!(expect.len(), frames.len(), "5 survivors plus 3 frames after the jump");
        assert_outputs(&outputs, &frames, &expect, &[]);

        // The jump is what removes the redundancy, not an encoder failure: the frame is still
        // wrapped, just primary-only.
        assert_eq!(outputs[5].1.len(), 13, "post-jump frame carries no redundant block");
    }

    #[test]
    fn red_sender_matches_go_unordered_and_repeated_frames() {
        let mut red = red_sender(&[1, 2]);
        let mut frames = go_frames(BASE_SEQ, BASE_TS, 10, TS_STEP);
        // Go "unorder and repeat": an old frame arrives late, then the newest frame arrives again.
        let unordered = frames[7].clone();
        let repeated = frames[9].clone();
        frames.push(unordered);
        frames.push(repeated);
        let outputs = forward_all(&mut red, &frames, GO_BUDGET);

        let mut expect: Vec<Vec<(u32, usize)>> = (0..10)
            .map(|i| match i {
                0 => vec![],
                1 => vec![(TS_STEP, 0)],
                _ => vec![(2 * TS_STEP, i - 2), (TS_STEP, i - 1)],
            })
            .collect();
        // The late frame is older than both slots, so Go's sequence-gap test (which wraps through the
        // 16-bit range) rejects it as redundancy and it is not recorded either.
        expect.push(vec![]);
        // The repeat is skipped as a repeat, but its older neighbour is still redundant for it.
        expect.push(vec![(TS_STEP, 8)]);
        assert_outputs(&outputs, &frames, &expect, &[]);

        // Repeating the newest frame writes it into both slots of the window, so the next frame
        // carries the same block twice. That is Go's behaviour, not a Rust-side artefact: a
        // receiver deduplicates the equal payloads by sequence number.
        let seq = BASE_SEQ.wrapping_add(10);
        let rtp_time = BASE_TS.wrapping_add(10 * TS_STEP);
        let after_repeat = red.wrap(go_payload(seq, rtp_time), seq_no(seq), rtp_time, GO_BUDGET);
        let RedWrapped::Redundant(bytes) = after_repeat else {
            panic!("a consecutive frame must be wrapped as RED");
        };
        let blocks = red_blocks(&bytes);
        assert_eq!(blocks.len(), 3, "the duplicated history yields two redundant blocks");
        assert_eq!(blocks[0].1, blocks[1].1, "both blocks have the same offset");
        assert_eq!(blocks[0].2, blocks[1].2, "both blocks carry the repeated frame");
    }

    #[test]
    fn red_sender_matches_go_large_timestamp_gap_drops_redundancy() {
        let mut red = red_sender(&[1, 2]);
        // Go "large timestamp gap": normal frames, then frames 40*tsStep apart.
        let mut frames = go_frames(BASE_SEQ, BASE_TS, 4, TS_STEP);
        frames.extend(go_frames(
            BASE_SEQ.wrapping_add(4),
            BASE_TS.wrapping_add(4 * TS_STEP),
            4,
            40 * TS_STEP,
        ));
        let outputs = forward_all(&mut red, &frames, GO_BUDGET);

        let mut expect = vec![
            vec![],
            vec![(TS_STEP, 0)],
            vec![(2 * TS_STEP, 0), (TS_STEP, 1)],
            vec![(2 * TS_STEP, 1), (TS_STEP, 2)],
            // The first jumped frame is still one step behind its predecessor, so the ramp holds.
            vec![(2 * TS_STEP, 2), (TS_STEP, 3)],
        ];
        expect.extend((0..3).map(|_| vec![]));
        assert_outputs(&outputs, &frames, &expect, &[]);

        // 40*480 = 19200 >= 1<<14, so the timestamp test (not the window) is what removes the
        // redundancy, and the frame is still wrapped on the RED payload type.
        assert!(40 * TS_STEP >= MAX_RED_TS_GAP, "the vector must cross the 1<<14 test");
        assert_eq!(frames[6].1.wrapping_sub(frames[5].1), 40 * TS_STEP);
        assert_eq!(outputs[5].1.len(), 13, "primary-only RED payload after the gap");
    }

    #[test]
    fn red_sender_matches_go_drop_all_redundancy_when_blocks_do_not_fit() {
        // Go's budget check is all-or-nothing (`redPkts = redPkts[:0]`), not "shed the deepest level".
        let frames: Vec<Frame> = (0..4)
            .map(|i| (100u16 + i as u16, i as u32 * TS_STEP, vec![i as u8 + 1; 500]))
            .collect();

        let mut tight = red_sender(&[1, 2]);
        let outputs = forward_all(&mut tight, &frames, WRAP_BUDGET);
        assert_outputs(
            &outputs,
            &frames,
            &[vec![], vec![(TS_STEP, 0)], vec![], vec![]],
            &[],
        );
        for (index, (_, bytes)) in outputs.iter().enumerate() {
            assert!(
                bytes.len() <= WRAP_BUDGET,
                "frame {index} must fit the budget, got {}",
                bytes.len()
            );
        }
        assert_eq!(outputs[2].1.len(), 501, "both blocks do not fit, so neither is used");

        // Control: the same frames with room for both blocks keep both, which proves it is the
        // budget that dropped them and not the selection rules.
        let mut ample = red_sender(&[1, 2]);
        let outputs = forward_all(&mut ample, &frames, 1600);
        assert_outputs(
            &outputs,
            &frames,
            &[
                vec![],
                vec![(TS_STEP, 0)],
                vec![(2 * TS_STEP, 0), (TS_STEP, 1)],
                vec![(2 * TS_STEP, 1), (TS_STEP, 2)],
            ],
            &[],
        );
        assert_eq!(outputs[2].1.len(), 1509, "both redundant blocks fit in 1600 bytes");
    }

    #[test]
    fn red_sender_matches_go_oversized_frame_is_broadcast_unwrapped() {
        let mut red = red_sender(&[1, 2]);
        // A frame of at least `maxRedPayload` cannot be expressed as a 10-bit block length, so Go
        // broadcasts the original packet on the primary payload type and records nothing.
        let frames = vec![
            (100u16, 0u32, vec![0xAAu8; MAX_RED_PAYLOAD]),
            (101, TS_STEP, vec![0xBBu8; MAX_RED_PAYLOAD - 1]),
            (102, 2 * TS_STEP, vec![0xCCu8; 20]),
            (103, 3 * TS_STEP, vec![0xDDu8; 20]),
        ];
        let outputs = forward_all(&mut red, &frames, GO_BUDGET);

        assert_outputs(
            &outputs,
            &frames,
            &[
                vec![],
                vec![],
                // Only the 1023-byte frame is in history: the 1024-byte frame was never recorded.
                vec![(TS_STEP, 1)],
                vec![(2 * TS_STEP, 1), (TS_STEP, 2)],
            ],
            &[0],
        );
        assert_eq!(outputs[0].1.len(), MAX_RED_PAYLOAD, "byte-for-byte the original payload");
        assert_eq!(outputs[2].1.len(), 4 + 1 + 1023 + 20, "one 1023-byte redundant block");
        assert_eq!(&outputs[2].1[..4], &[0xEF, 0x07, 0x83, 0xFF], "offset 480, length 1023");
        assert_eq!(outputs[3].1.len(), 8 + 1 + 1023 + 20 + 20, "both blocks fit the budget");
    }

    #[test]
    fn red_sender_frame_without_room_for_primary_header_stays_on_primary_pt() {
        let mut red = red_sender(&[1, 2]);
        // The primary block header is mandatory, so a frame that leaves no room for it is not
        // wrapped — and, like Go's oversized branch, it is not recorded either.
        let budget = 64;
        let frames = vec![
            (100u16, 0u32, vec![0x11u8; budget]),
            (101, TS_STEP, vec![0x22u8; 8]),
            (102, 2 * TS_STEP, vec![0x33u8; 8]),
        ];
        let outputs = forward_all(&mut red, &frames, budget);
        assert_outputs(&outputs, &frames, &[vec![], vec![], vec![(TS_STEP, 1)]], &[0]);

        // Control: the same frames with room for the header are wrapped, so the room test — not the
        // frame size — is what bypassed the first one.
        let mut red = red_sender(&[1, 2]);
        let outputs = forward_all(&mut red, &frames, 200);
        assert_outputs(
            &outputs,
            &frames,
            &[vec![], vec![(TS_STEP, 0)], vec![(2 * TS_STEP, 0), (TS_STEP, 1)]],
            &[],
        );
    }

    #[test]
    fn red_sender_multi_distance_pattern_emits_levels_oldest_first() {
        // LiveKit and Go use `[1, 2]`; a deeper `distances` pattern is str0m's extension for higher
        // loss. The window grows with the deepest distance and only the configured ranks repeat,
        // always oldest first, with Go's recency and timestamp filters unchanged.
        let frames: Vec<Frame> = (0..6)
            .map(|i| (100u16 + i as u16, i as u32 * 960, vec![i as u8 + 1; 8]))
            .collect();

        let mut red = red_sender(&[1, 3]);
        let outputs = forward_all(&mut red, &frames, WRAP_BUDGET);
        assert_outputs(
            &outputs,
            &frames,
            &[
                vec![],
                vec![(960, 0)],
                vec![(960, 1)],
                vec![(3 * 960, 0), (960, 2)],
                vec![(3 * 960, 1), (960, 3)],
                vec![(3 * 960, 2), (960, 4)],
            ],
            &[],
        );
        assert_eq!(&outputs[5].1[..4], &[0xEF, 0x2D, 0x00, 0x08], "distance 3 block first");
        assert_eq!(&outputs[5].1[4..8], &[0xEF, 0x0F, 0x00, 0x08], "then the distance 1 block");

        // A rank that is not configured is never repeated even when it sits in the window: frame 5
        // of `[1, 3]` skips the distance 2 frame. With `[1, 2, 3]` all three levels appear.
        let mut red = red_sender(&[1, 2, 3]);
        let outputs = forward_all(&mut red, &frames, WRAP_BUDGET);
        assert_outputs(
            &outputs,
            &frames,
            &[
                vec![],
                vec![(960, 0)],
                vec![(2 * 960, 0), (960, 1)],
                vec![(3 * 960, 0), (2 * 960, 1), (960, 2)],
                vec![(3 * 960, 1), (2 * 960, 2), (960, 3)],
                vec![(3 * 960, 2), (2 * 960, 3), (960, 4)],
            ],
            &[],
        );
    }

    #[test]
    fn red_sender_sync_resizes_window_on_pattern_change() {
        let frames: Vec<Frame> = (0..5)
            .map(|i| (100u16 + i as u16, i as u32 * 960, vec![i as u8 + 1; 8]))
            .collect();

        // Shrink the pattern: the window becomes one frame deep and keeps the newest frame, so the
        // next packet has a single distance 1 block instead of the two a `[1, 3]` window yields.
        let mut red = red_sender(&[1, 3]);
        let before = forward_all(&mut red, &frames[..4], WRAP_BUDGET);
        assert_eq!(red_blocks(&before[3].1).len(), 3, "[1, 3] yields two redundant blocks");
        red.sync(Pt::new_with_value(63), Pt::new_with_value(111), &[1]);
        let (seq, rtp_time, payload) = &frames[4];
        let after = red.wrap(payload.clone(), seq_no(*seq), *rtp_time, WRAP_BUDGET);
        let RedWrapped::Redundant(bytes) = after else {
            panic!("a consecutive frame must be wrapped as RED");
        };
        let blocks = red_blocks(&bytes);
        assert_eq!(blocks.len(), 2, "only distance 1 survives the shrink");
        assert_eq!(blocks[0].1, 960);
        assert_eq!(blocks[0].2, frames[3].2, "the newest frame is kept");

        // Grow the pattern: the window deepens with empty front slots, so the ramp continues exactly
        // like a sender that was configured with the deeper pattern from the start.
        let mut grown = red_sender(&[1]);
        let mut fresh = red_sender(&[1, 2]);
        for (index, (seq, rtp_time, payload)) in frames.iter().take(4).enumerate() {
            if index == 1 {
                grown.sync(Pt::new_with_value(63), Pt::new_with_value(111), &[1, 2]);
            }
            let g = grown.wrap(payload.clone(), seq_no(*seq), *rtp_time, WRAP_BUDGET);
            let f = fresh.wrap(payload.clone(), seq_no(*seq), *rtp_time, WRAP_BUDGET);
            assert!(
                matches!((&g, &f), (RedWrapped::Redundant(_), RedWrapped::Redundant(_))),
                "frame {index}: both senders must wrap on the RED payload type"
            );
            assert_eq!(
                g.into_payload(),
                f.into_payload(),
                "a deepened window must ramp like a fresh sender at frame {index}"
            );
        }
    }

    #[test]
    fn red_sender_labels_only_wrapped_bytes_with_the_red_payload_type() {
        // The defect this fixes: primary bytes that bypass RED wrapping must not be labelled with
        // the RED payload type, or the peer parses plain Opus as an RFC 2198 payload.
        let red_pt = Pt::new_with_value(63);
        let primary_pt = Pt::new_with_value(111);
        assert_eq!(*red_sender(&[1, 2]).red_pt(), 63);
        assert_eq!(*red_sender(&[1, 2]).primary_pt(), 111);

        let mut red = red_sender(&[1, 2]);
        let oversized = red.wrap(vec![0x55u8; MAX_RED_PAYLOAD], seq_no(100), 0, GO_BUDGET);
        assert_eq!(*oversized.pt(red_pt, primary_pt), *primary_pt);
        assert_eq!(oversized.into_payload(), vec![0x55u8; MAX_RED_PAYLOAD]);

        let wrapped = red.wrap(vec![0x77u8; 40], seq_no(101), TS_STEP, GO_BUDGET);
        assert_eq!(*wrapped.pt(red_pt, primary_pt), *red_pt);
        let bytes = wrapped.into_payload();
        assert_ne!(bytes, vec![0x77u8; 40], "a wrapped frame is never the raw payload");
        assert_eq!(bytes.len(), 1 + 40, "the oversized frame was not recorded, so no block");
        assert_eq!(bytes[0], 111, "primary block header with F = 0");
        assert_eq!(&bytes[1..], &[0x77u8; 40]);

        // A frame with a redundant block sets the continuation bit on the first header byte only.
        let third = red.wrap(vec![0x88u8; 40], seq_no(102), 2 * TS_STEP, GO_BUDGET);
        assert_eq!(*third.pt(red_pt, primary_pt), *red_pt);
        let bytes = third.into_payload();
        assert_eq!(bytes[0] & 0x80, 0x80, "first block is followed by another header");
        assert_eq!(bytes[4] & 0x80, 0, "the primary header ends the chain");
        assert_eq!(&bytes[5..], &[0x88u8; 40], "the primary payload is last");
    }

    #[test]
    #[ignore = "requires actual fixed-Go SOURCE_RED_ENCODE rows"]
    fn original_go_red_encoder_vectors_match_red_sender_byte_for_byte() {
        // Fixed Go: livekit-stable `pkg/sfu/redreceiver.go` through `tests/oracles/red-encode`;
        // see that directory's README for the run command. Every row is one
        // `RedReceiver.ForwardRTP` call with a single down track, so the comparison covers the
        // payload type choice and the complete encoded payload rather than a summary of it.
        let path = std::env::var("GO_RED_ENCODE_RESULTS").expect("original observer log");
        let log = std::fs::read_to_string(path).unwrap();

        // case -> rows of `(index, seq, ts, input payload, Go payload type, Go payload)`
        let mut order: Vec<String> = Vec::new();
        let mut cases: std::collections::HashMap<String, Vec<(usize, u16, u32, Vec<u8>, u8, Vec<u8>)>> =
            std::collections::HashMap::new();
        let mut rows = 0;
        for line in log.lines() {
            let Some((_, row)) = line.split_once("SOURCE_RED_ENCODE ") else {
                continue;
            };
            let v: serde_json::Value = serde_json::from_str(row).unwrap();
            let case = v["case"].as_str().unwrap().to_owned();
            let entry = cases.entry(case.clone()).or_default();
            if entry.is_empty() {
                order.push(case);
            }
            entry.push((
                v["index"].as_u64().unwrap() as usize,
                v["seq"].as_u64().unwrap() as u16,
                v["ts"].as_u64().unwrap() as u32,
                hex_bytes(v["payload_hex"].as_str().unwrap()),
                v["out_pt"].as_u64().unwrap() as u8,
                hex_bytes(v["out_payload_hex"].as_str().unwrap()),
            ));
            rows += 1;
        }

        // Anti-vacuity: the six frozen vectors must all be present, with both the wrapped and the
        // bypassed branch and plenty of redundancy, otherwise "every row is equal" would prove
        // nothing. normal 10 + loss/jump 8 + unorder/repeat 12 + budget 5 + timestamp gap 8 +
        // oversized 4.
        assert_eq!(rows, 47, "the frozen Go vectors must contribute all 47 rows");
        assert_eq!(order.len(), 6, "every frozen vector must be present, got {order:?}");
        let wrapped_rows = cases.values().flatten().filter(|r| r.4 == 63).count();
        let bypassed_rows = rows - wrapped_rows;
        assert!(wrapped_rows >= 40, "most rows must exercise RED wrapping, got {wrapped_rows}");
        assert_eq!(
            bypassed_rows, 1,
            "only the frame of 1 KiB or more may bypass RED wrapping, got {bypassed_rows}"
        );

        for case in &order {
            let mut vector = cases.get(case).unwrap().clone();
            vector.sort_by_key(|row| row.0);
            assert_eq!(
                vector.iter().map(|row| row.0).collect::<Vec<_>>(),
                (0..vector.len()).collect::<Vec<_>>(),
                "{case}: row indexes must be contiguous, a truncated vector would pass silently"
            );
            // Go builds a fresh `RedReceiver` per subtest, so each vector starts with an empty window.
            let mut red = RedSender::new(Pt::new_with_value(63), Pt::new_with_value(111), &[1, 2]);
            for (index, seq, rtp_time, payload, go_pt, go_bytes) in &vector {
                let wrapped = red.wrap(payload.clone(), seq_no(*seq), *rtp_time, GO_BUDGET);
                let pt = wrapped.pt(Pt::new_with_value(63), Pt::new_with_value(111));
                assert_eq!(*pt, *go_pt, "{case} frame {index} payload type");
                assert_eq!(
                    wrapped.into_payload(),
                    *go_bytes,
                    "{case} frame {index} encoded RED payload bytes"
                );
            }
        }
    }

    /// Decode a hex string emitted by the fixed-Go observer.
    fn hex_bytes(text: &str) -> Vec<u8> {
        (0..text.len() / 2)
            .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("hex byte"))
            .collect()
    }

    #[test]
    fn primary_only_roundtrip() {
        let bytes = RedEncoder::encode(96, &[1, 2, 3], &[]);
        assert_eq!(bytes, vec![96, 1, 2, 3]);

        let blocks = RedDecoder::decode(&bytes).unwrap();
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_primary);
        assert_eq!(blocks[0].pt, 96);
        assert_eq!(blocks[0].timestamp_offset, 0);
        assert_eq!(blocks[0].payload, &[1, 2, 3]);
    }

    #[test]
    fn one_redundant_block_roundtrip() {
        let red = [RedundantBlock {
            pt: 96,
            timestamp_offset: 960,
            payload: vec![9, 9],
        }];
        let bytes = RedEncoder::encode(96, &[1, 2, 3], &red);

        // Redundant header for ts_offset=960, len=2: F|pt=0xE0; 960>>6=15, low-6 ts bits and the
        // 2 high len bits are 0, len low byte = 2.
        assert_eq!(&bytes[..4], &[0xE0, 15, 0, 2]);
        assert_eq!(bytes[4], 96); // primary header, F=0
        assert_eq!(&bytes[5..], &[9, 9, 1, 2, 3]); // redundant payload then primary

        let blocks = RedDecoder::decode(&bytes).unwrap();
        assert_eq!(blocks.len(), 2);
        assert!(!blocks[0].is_primary);
        assert_eq!(blocks[0].timestamp_offset, 960);
        assert_eq!(blocks[0].payload, &[9, 9]);
        assert!(blocks[1].is_primary);
        assert_eq!(blocks[1].payload, &[1, 2, 3]);
    }

    #[test]
    fn encode_skips_oversized_blocks() {
        // A block whose offset (>14 bits) or length (>10 bits) does not fit is dropped, so the
        // wire payload is never silently corrupted — even in release builds.
        let bad_offset = RedundantBlock {
            pt: 96,
            timestamp_offset: 0x4000, // > 0x3fff
            payload: vec![1, 2],
        };
        let bad_len = RedundantBlock {
            pt: 96,
            timestamp_offset: 100,
            payload: vec![0u8; 2000], // > 0x3ff
        };
        let bytes = RedEncoder::encode(96, &[7, 7], &[bad_offset, bad_len]);

        let blocks = RedDecoder::decode(&bytes).unwrap();
        assert_eq!(blocks.len(), 1, "oversized blocks must be skipped");
        assert!(blocks[0].is_primary);
        assert_eq!(blocks[0].payload, &[7, 7]);
    }

    #[test]
    fn truncated_header_is_error_not_panic() {
        assert!(RedDecoder::decode(&[0x80, 0x00]).is_err()); // F=1 but < 4 header bytes
        assert!(RedDecoder::decode(&[]).is_err()); // empty
    }

    #[test]
    fn block_length_past_end_is_error() {
        // redundant header claims len=200 (len hi byte 0, lo byte 200) but no payload follows
        let b = [0x80 | 96, 0, 0, 200, 96];
        assert!(RedDecoder::decode(&b).is_err());
    }

    #[test]
    fn never_panics_on_arbitrary_input() {
        // Exhaustively poke short inputs; the decoder must only ever return Ok/Err.
        for a in 0u16..=255 {
            for b in 0u16..=255 {
                let _ = RedDecoder::decode(&[a as u8, b as u8]);
            }
        }
    }

    #[test]
    fn decode_rejects_too_many_redundant_blocks() {
        // One 4-byte redundant header per block lets a single datagram pack hundreds of blocks.
        // The decoder must bound this so a hostile peer can't expand one packet into an unbounded
        // number of recovered packets. `n` zero-length redundant blocks, then an empty primary.
        fn red_with_redundant_blocks(n: usize) -> Vec<u8> {
            let mut data = Vec::with_capacity(n * 4 + 1);
            for _ in 0..n {
                data.extend_from_slice(&[F_BIT | 96, 0, 0, 0]); // F=1, pt=96, ts=0, len=0
            }
            data.push(96); // primary header, F=0
            data
        }

        // At the limit: MAX redundant + 1 primary still decodes.
        let ok = red_with_redundant_blocks(MAX_REDUNDANT_BLOCKS);
        let blocks = RedDecoder::decode(&ok).expect("max redundant blocks is allowed");
        assert_eq!(blocks.len(), MAX_REDUNDANT_BLOCKS + 1);

        // One over the limit, and a pathological count, are rejected (not decoded).
        assert!(RedDecoder::decode(&red_with_redundant_blocks(MAX_REDUNDANT_BLOCKS + 1)).is_err());
        assert!(RedDecoder::decode(&red_with_redundant_blocks(360)).is_err());
    }

    #[test]
    fn same_pt_blocks_skip_foreign_pt_and_primary_offset() {
        let block = |pt, ts| RedBlock {
            pt,
            timestamp_offset: ts,
            payload: &[0u8],
            is_primary: false,
        };
        let redundant = vec![
            block(111, 1920),
            block(96, 960),
            block(111, 0),
            block(111, 960),
        ];
        let offsets: Vec<u32> = red_same_pt_blocks(&redundant, 111)
            .map(|b| b.timestamp_offset)
            .collect();
        assert_eq!(offsets, vec![1920, 960]);
    }

    #[test]
    fn public_encoder_output_is_accepted_by_public_decoder() {
        let redundant: Vec<_> = (0..33)
            .map(|i| RedundantBlock {
                pt: 111,
                timestamp_offset: i + 1,
                payload: vec![i as u8],
            })
            .collect();

        let encoded = RedEncoder::encode(111, &[0xAA], &redundant);
        assert!(
            RedDecoder::decode(&encoded).is_ok(),
            "the public decoder must accept packets produced by the public encoder"
        );
    }
}
