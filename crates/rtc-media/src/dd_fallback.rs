// Copyright 2026 livekit-rs contributors. Apache-2.0.
// Keyframe semantics follow pinned mediatransportutil pkg/codec/keyframe.go;
// its AV1 routine credits Juliusz Chroboczek and the Galene SFU.
//! Fixed Go Buffer parser lifetime and per-DownTrack DD selector retirement.
//! Missing descriptors and malformed present descriptors are different inputs.
use crate::{engine::MediaEngine, forward_timing::ForwardingAdmissions};
use rtc_core::{ids::TransportId, media_api::MediaEvent, model::StreamState};
use std::time::Instant;
use str0m::{format::Codec, media::MediaPacket, rtp::RtpWrite};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Input {
    Padding,
    Descriptor,
    Missing,
}
#[derive(Default)]
pub(crate) struct Source {
    identity: Option<u64>,
    engaged: bool,
    vp9_disabled: bool,
    sequences: PaddingSequences,
}
// Fixed Buffer.snRangeMap excludes only in-order padding source slots. Its
// 100 closed ranges plus one open range retain late unknown holes exactly.
#[derive(Default)]
struct PaddingSequences {
    ranges: std::collections::VecDeque<PaddingRange>,
}
struct PaddingRange {
    start: u64,
    end: Option<u64>,
    offset: u64,
}
impl PaddingSequences {
    fn exclude(&mut self, sequence: u64) -> Option<()> {
        if self.ranges.is_empty() {
            self.ranges.push_back(PaddingRange {
                start: 0,
                end: None,
                offset: 0,
            });
        }
        let open = self.ranges.back_mut()?;
        if open.start > sequence {
            return None;
        }
        let new_offset = open.offset.wrapping_add(1);
        if open.start == sequence {
            open.start = sequence.wrapping_add(1);
            open.offset = new_offset;
            return Some(());
        }
        open.end = Some(sequence.wrapping_sub(1));
        self.ranges.push_back(PaddingRange {
            start: sequence.wrapping_add(1),
            end: None,
            offset: new_offset,
        });
        while self.ranges.len() > 101 {
            self.ranges.pop_front();
        }
        Some(())
    }
    fn translate(&self, sequence: u64) -> Option<u64> {
        if self.ranges.is_empty() {
            return Some(sequence);
        }
        let open = self.ranges.back()?;
        if sequence >= open.start {
            return Some(sequence.wrapping_sub(open.offset));
        }
        self.ranges
            .iter()
            .rev()
            .find(|range| sequence >= range.start && range.end.is_some_and(|end| sequence <= end))
            .map(|range| sequence.wrapping_sub(range.offset))
    }
}
impl Source {
    pub(crate) fn normalize_sequence(
        &mut self,
        source: u64,
        sequence: u64,
        padding: bool,
        out_of_order: bool,
    ) -> Option<u64> {
        if self.identity != Some(source) {
            *self = Self {
                identity: Some(source),
                ..Default::default()
            };
        }
        if padding && !out_of_order {
            self.sequences.exclude(sequence)?;
            return None;
        }
        self.sequences.translate(sequence)
    }
    pub(crate) fn engaged(&self) -> bool {
        self.engaged
    }
    pub(crate) fn uses_vp9_payload(&self) -> bool {
        self.vp9_disabled
    }
    pub(crate) fn observe(
        &mut self,
        source: u64,
        codec: Codec,
        configured: bool,
        present: bool,
        nonempty: bool,
    ) -> Input {
        if self.identity != Some(source) {
            *self = Self {
                identity: Some(source),
                ..Default::default()
            };
        }
        if !nonempty {
            return Input::Padding;
        }
        self.engaged |= configured || codec == Codec::Av1;
        if codec == Codec::Vp9 && self.vp9_disabled {
            return Input::Missing;
        }
        if configured && present {
            return Input::Descriptor;
        }
        if configured && codec == Codec::Vp9 {
            self.vp9_disabled = true;
        }
        Input::Missing
    }
}
#[derive(Default)]
pub(crate) struct Selector {
    identity: Option<(u64, Codec)>,
    pub(crate) disabled: bool,
}
impl Selector {
    pub(crate) fn sync(&mut self, source: u64, codec: Codec, configured: bool) {
        if self.identity != Some((source, codec)) {
            self.identity = Some((source, codec));
            self.disabled = !configured;
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Payload {
    spatial: i32,
    temporal: u8,
    key: bool,
    beginning: bool,
    ending: bool,
    switching_up: bool,
}
fn skip(data: &[u8], index: &mut usize, n: usize) -> Option<()> {
    *index = index.checked_add(n)?;
    (*index <= data.len()).then_some(())
}
fn byte(data: &[u8], index: &mut usize) -> Option<u8> {
    let v = *data.get(*index)?;
    *index += 1;
    Some(v)
}
// RFC 7741 descriptor length validation; selector decisions remain in the
// existing fixed-Go spatial-first VP9 consumer. Source bytes are never changed.
fn vp9(data: &[u8]) -> Option<Payload> {
    let flags = *data.first()?;
    let mut i = 1;
    if flags & 0x80 != 0 {
        let pid = byte(data, &mut i)?;
        if pid & 0x80 != 0 {
            skip(data, &mut i, 1)?;
        }
    }
    let mut spatial = 0;
    let mut temporal = 0;
    let mut switching_up = false;
    if flags & 0x20 != 0 {
        let layer = byte(data, &mut i)?;
        spatial = i32::from((layer >> 1) & 7);
        temporal = layer >> 5;
        switching_up = layer & 0x10 != 0;
        if flags & 0x10 == 0 {
            skip(data, &mut i, 1)?;
        }
    }
    if flags & 0x50 == 0x50 {
        let mut done = false;
        for _ in 0..3 {
            if byte(data, &mut i)? & 1 == 0 {
                done = true;
                break;
            }
        }
        if !done {
            return None;
        }
    }
    if flags & 2 != 0 {
        let ss = byte(data, &mut i)?;
        if ss & 0x10 != 0 {
            skip(data, &mut i, 4 * (usize::from(ss >> 5) + 1))?;
        }
        if ss & 8 != 0 {
            let n = byte(data, &mut i)?;
            for _ in 0..n {
                let group = byte(data, &mut i)?;
                skip(data, &mut i, usize::from((group >> 2) & 3))?;
            }
        }
    }
    let body = data.get(i..)?;
    let beginning = flags & 8 != 0;
    let key = beginning
        && body.first().is_some_and(|b| {
            b & 0xc0 == 0x80
                && if (b >> 4) & 3 != 3 {
                    b & 0x0c == 0
                } else {
                    b & 6 == 0
                }
        });
    Some(Payload {
        spatial,
        temporal,
        key,
        beginning,
        ending: flags & 4 != 0,
        switching_up,
    })
}
pub(crate) fn vp9_key(data: &[u8]) -> bool {
    vp9(data).is_some_and(|payload| payload.key)
}

pub(crate) fn vp9_valid(data: &[u8]) -> bool {
    vp9(data).is_some()
}

// Fixed mediatransportutil codec.IsAV1KeyFrame: N alone is insufficient. The
// packet must contain the sequence header and a KEY_FRAME frame/header OBU.
pub(crate) fn av1_key(data: &[u8]) -> bool {
    if data.len() < 2 || data[0] & 0x88 != 8 {
        return false;
    }
    let w = usize::from((data[0] >> 4) & 3);
    let mut offset = 1;
    let mut index = 0;
    loop {
        let Some(tail) = data.get(offset..) else {
            return false;
        };
        let (obu, len, truncated) = if w == index + 1 {
            (tail, tail.len(), false)
        } else {
            let mut pos = 0;
            let mut length = 0usize;
            let mut complete = false;
            while pos < 4 {
                let Some(&v) = tail.get(pos) else {
                    return false;
                };
                length |= usize::from(v & 127) << (pos * 7);
                pos += 1;
                if v & 128 == 0 {
                    complete = true;
                    break;
                }
            }
            if !complete {
                return false;
            }
            if let Some(end) = pos.checked_add(length).filter(|end| *end <= tail.len()) {
                (&tail[pos..end], end, false)
            } else {
                (&tail[pos..], tail.len(), true)
            }
        };
        let Some(&head) = obu.first() else {
            return false;
        };
        let kind = (head >> 3) & 7;
        if index == 0 {
            if kind != 1 {
                return false;
            }
        } else if kind == 3 || kind == 6 {
            return obu.get(1).is_some_and(|b| b & 0xe0 == 0);
        }
        if truncated || index >= w {
            return false;
        }
        offset += len;
        index += 1;
    }
}
impl MediaEngine {
    pub(crate) fn on_dd_payload_packet(
        &mut self,
        tid: TransportId,
        data: &MediaPacket,
        now: Instant,
        configured: bool,
        missing: bool,
        dd_layer: Option<(i32, u8)>,
        codec: Codec,
    ) {
        let dequeued = Instant::now();
        let Some(&ui) = self.up_by_mid.get(&(tid, data.mid)) else {
            return;
        };
        let packet = &data.packet;
        let blank = packet.payload.is_empty();
        // With DD still parsed by its Buffer, original VP9.Select receives a
        // nil VP9 payload header. A retired selector cannot invent that header
        // just because the raw wire payload is available to this engine.
        if codec == Codec::Vp9 && !missing {
            return;
        }
        let mut info = if blank {
            Payload {
                spatial: 0,
                temporal: 0,
                key: false,
                beginning: false,
                ending: packet.header.marker,
                switching_up: false,
            }
        } else {
            match codec {
                Codec::Vp9 => {
                    let Some(v) = vp9(&packet.payload) else {
                        return;
                    };
                    v
                }
                Codec::Av1 => Payload {
                    spatial: 0,
                    temporal: 0,
                    key: av1_key(&packet.payload),
                    beginning: true,
                    ending: packet.header.marker,
                    switching_up: false,
                },
                _ => return,
            }
        };
        let observation = packet.forwarding_observation().map(|mut v| {
            let layer = dd_layer.unwrap_or((info.spatial, info.temporal));
            v.spatial = Some(layer.0 as u8);
            v.temporal = Some(layer.1);
            v
        });
        let mut admissions = ForwardingAdmissions::new(observation.as_slice());
        let mut changed = false;
        let mut want_pli = false;
        {
            let Self {
                ups,
                downs,
                transports,
                events,
                dirty,
                cfg,
                io,
                ..
            } = self;
            let Some(up) = ups.get_mut(ui) else {
                return;
            };
            if missing && !blank {
                let ssrc_spatial = up.spatial_for_rid(data.rid);
                let ssrc_changed = up.observe_layer_ssrc(ssrc_spatial, *packet.header.ssrc, 0);
                let first_start = !up.started;
                if first_start {
                    up.started = true;
                }
                if first_start || ssrc_changed {
                    events.push(MediaEvent::TrackMediaStarted {
                        track_sid: up.sid.clone(),
                        mime_type: up.mime.clone(),
                        layers: up.layers.clone(),
                    });
                }
                if codec == Codec::Vp9 {
                    up.svc = true;
                    up.on_frame(info.spatial, None, data.logical_rtp_bytes, now);
                } else {
                    up.on_frame(info.spatial, None, packet.payload.len(), now);
                }
            }
            if up.muted {
                return;
            }
            let available = up.available(now);
            let configured_layers = up.configured_layers();
            let max_layer = up.spatial_for_rid(data.rid);
            if codec == Codec::Av1 {
                info.spatial = max_layer;
            }
            for &di in &up.subscribers {
                let Some(d) = downs.get_mut(di) else {
                    continue;
                };
                if !d.negotiated || d.codec_failed {
                    continue;
                }
                d.dd_fallback.sync(data.source_id, codec, configured);
                if !missing && !d.dd_fallback.disabled {
                    continue;
                }
                if blank && codec == Codec::Vp9 && d.dd_fallback.disabled {
                    continue;
                }

                let (Some(target), Some(mid)) = (d.transport, d.mid) else {
                    continue;
                };
                d.refresh_target(available, configured_layers);
                if d.target < 0 {
                    continue;
                }
                let Some(t) = transports.get_mut(&target) else {
                    continue;
                };
                if !t.usable() || !t.can_send(mid) {
                    continue;
                }
                let out_pt = {
                    let Some(w) = t.rtc.writer(mid) else {
                        continue;
                    };
                    if blank {
                        d.last_pt
                            .map(|(_, pt)| pt)
                            .filter(|pt| {
                                w.payload_params().any(|params| {
                                    params.pt() == *pt && params.spec().codec == codec
                                })
                            })
                            .or_else(|| {
                                w.payload_params()
                                    .find(|params| params.spec().codec == codec)
                                    .map(|params| params.pt())
                            })
                    } else {
                        w.match_params(data.params)
                            .filter(|pt| w.payload_params().any(|params| params.pt() == *pt))
                    }
                };
                let Some(out_pt) = out_pt else {
                    continue;
                };
                let trigger = missing && !d.dd_fallback.disabled;
                if trigger {
                    d.dd_fallback.disabled = true;
                }
                let before = (d.current, d.forwarded_temporal_cap());
                let was_active = d.current >= 0;
                let max_temporal = d.subscriber_temporal_limit(up, d.max_spatial);
                let selected = if trigger {
                    false
                } else {
                    match codec {
                        Codec::Vp9 => {
                            matches!(
                                d.on_vp9_frame(
                                    info.spatial,
                                    info.temporal,
                                    info.key,
                                    max_temporal,
                                    info.switching_up,
                                    info.beginning,
                                    info.ending
                                ),
                                crate::downtrack::Forward::Send { .. }
                            ) && info.spatial <= d.vp9_forwarded_spatial_cap(info.temporal)
                        }
                        Codec::Av1 => matches!(
                            d.on_frame(info.spatial, info.key, now),
                            crate::downtrack::Forward::Send { .. }
                        ),
                        _ => false,
                    }
                };
                changed |= before != (d.current, d.forwarded_temporal_cap());
                want_pli |= !selected && d.wants_keyframe();
                // An inactive DD selector's initial rejection is irrelevant to
                // RTPMunger. An already live selector marks this genuine drop.
                if trigger && !was_active {
                    continue;
                }
                let mut direct = t.rtc.direct_api();
                let Some(stream) = direct.stream_tx_by_mid(mid, None) else {
                    continue;
                };
                if !d.av1_packet_sequences.is_initialized() || (selected && !was_active) {
                    d.av1_packet_sequences
                        .reset(*packet.seq_no, *stream.next_seq_no());
                }
                let late = d.av1_packet_sequences.is_late(*packet.seq_no);
                let Some(slot) = d.av1_packet_sequences.translate(*packet.seq_no, selected) else {
                    continue;
                };
                let time = d.out_original_packet_time(packet.time, packet.timestamp, late);
                match slot {
                    crate::av1::PacketSlot::Media(sequence) if selected => {
                        let mut extensions = packet.header.ext_vals.clone();
                        extensions.mid = None;
                        extensions.rid = None;
                        extensions.rid_repair = None;
                        extensions.transport_cc = None;
                        extensions.abs_send_time = None;
                        extensions.user_values.remove::<crate::av1::RawDescriptor>();
                        extensions
                            .user_values
                            .remove::<crate::av1::OutPacketDescriptor>();
                        extensions.play_delay_min = None;
                        extensions.play_delay_max = None;
                        if let Some((min, max)) = t.spec.effective_playout_delay(cfg.playout_delay)
                        {
                            extensions.play_delay_min =
                                Some(str0m::media::MediaTime::from_millis(u64::from(min)));
                            extensions.play_delay_max =
                                Some(str0m::media::MediaTime::from_millis(u64::from(max)));
                        }
                        let marker = packet.header.marker
                            || (codec == Codec::Vp9 && info.ending && d.current == info.spatial);
                        if !blank {
                            d.last_pt = Some((data.params.pt(), out_pt));
                        }
                        stream.write_rtp(
                            RtpWrite::new(
                                out_pt,
                                sequence.into(),
                                time.numer() as u32,
                                packet.timestamp,
                                packet.payload.clone(),
                            )
                            .marker(marker)
                            .ext_vals(extensions)
                            .csrc(&packet.header.csrc[..packet.header.csrc_count])
                            .nackable(true),
                        );
                        if info.key {
                            // Go DownTrack.WriteRTP: a forwarded keyframe packet ends
                            // the FlagStopRTXOnPLI throttle.
                            if stream.nack_throttled() {
                                tracing::debug!(
                                    path = "dd_fallback.rs",
                                    "keyframe packet forwarded: NACK throttle cleared"
                                );
                            }
                            stream.set_nack_throttled(false);
                        }
                        let layer = dd_layer.unwrap_or((info.spatial, info.temporal));
                        admissions.accepted(layer.0, layer.1, layer.0);
                        if !was_active {
                            events.push(MediaEvent::StreamState {
                                subscriber: d.spec.subscriber.clone(),
                                publisher: d.spec.publisher.clone(),
                                track_sid: d.spec.track_sid.clone(),
                                state: StreamState::Active,
                            });
                        }
                    }
                    crate::av1::PacketSlot::Padding(sequence) => {
                        stream.write_rtp(RtpWrite::padding_only(
                            out_pt,
                            sequence.into(),
                            time.numer() as u32,
                            packet.timestamp,
                        ))
                    }
                    _ => {}
                }
                if !dirty.contains(&target) {
                    dirty.push(target);
                }
            }
            if want_pli {
                crate::forward::request_pli(
                    transports,
                    up,
                    0,
                    &cfg.rtc.pli_throttle,
                    io,
                    dirty,
                    now,
                );
            }
        }
        self.record_forwarding_cohort(&admissions, Instant::now(), dequeued);
        if changed {
            self.update_dynacast(ui, now);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn buffer_and_selector_latches_preserve_padding_malformed_and_real_source_lifetimes() {
        let mut source = Source::default();
        assert!(matches!(
            source.observe(8, Codec::Vp9, true, true, true),
            Input::Descriptor
        ));
        assert!(matches!(
            source.observe(8, Codec::Vp9, true, false, false),
            Input::Padding
        ));
        assert!(matches!(
            source.observe(8, Codec::Vp9, true, true, true),
            Input::Descriptor
        ));
        assert!(matches!(
            source.observe(8, Codec::Vp9, true, false, true),
            Input::Missing
        ));
        assert!(matches!(
            source.observe(8, Codec::Vp9, true, true, true),
            Input::Missing
        ));
        assert!(
            matches!(
                source.observe(2, Codec::Vp9, true, true, true),
                Input::Descriptor
            ),
            "numerically older pre-created source is a distinct actual receiver"
        );
        let mut mode = Selector::default();
        mode.sync(8, Codec::Av1, true);
        mode.disabled = true;
        mode.sync(8, Codec::Av1, true);
        assert!(mode.disabled);
        mode.sync(2, Codec::Av1, true);
        assert!(!mode.disabled);
    }
    #[test]
    fn buffer_source_sequence_ranges_exclude_padding_but_preserve_unknown_late_holes() {
        let mut source = Source::default();
        assert_eq!(source.normalize_sequence(9, 1000, false, false), Some(1000));
        assert_eq!(source.normalize_sequence(9, 1002, true, false), None);
        assert_eq!(source.normalize_sequence(9, 1003, false, false), Some(1002));
        assert_eq!(
            source.normalize_sequence(9, 1001, false, true),
            Some(1001),
            "unknown preceding gap retains its earlier source offset"
        );
        assert_eq!(
            source.normalize_sequence(9, 1002, false, true),
            None,
            "a removed padding source slot cannot become media later"
        );
        assert_eq!(source.normalize_sequence(9, 1004, true, false), None);
        assert_eq!(source.normalize_sequence(9, 1005, false, false), Some(1003));
        assert_eq!(
            source.normalize_sequence(3, 1000, false, false),
            Some(1000),
            "actual new source, even a smaller pre-created ID, resets offsets"
        );
        for sequence in (1002..1500).step_by(2) {
            assert_eq!(source.normalize_sequence(3, sequence, true, false), None);
        }
        assert_eq!(
            source.sequences.ranges.len(),
            101,
            "exact100 closed plus1 open bounded range retention"
        );
        assert_eq!(
            source.normalize_sequence(3, 1000, false, true),
            None,
            "discarded old adjustment ranges cannot alias current source offsets"
        );
    }

    #[test]
    fn codec_keyframe_requires_actual_body_and_av1_sequence_and_frame_obus() {
        assert!(vp9(&[0xbc, 0x81, 1, 0, 0x80, 0]).unwrap().key);
        assert!(!vp9(&[0xbc, 0x81, 1, 0, 0x34, 0]).unwrap().key);
        assert!(vp9(&[0xbc, 0x81]).is_none());
        assert!(av1_key(&[0x28, 1, 8, 0x18, 0]));
        assert!(!av1_key(&[0x18, 0x34, 0]));
        assert!(!av1_key(&[0x28, 1, 8, 0x18, 0x80]));
    }
}
