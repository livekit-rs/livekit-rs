//! Actual authenticated RTP -> fixed Go BufferBase frame-rate consumer. Each
//! independent encoding has its own parser/calculator lifetime; SVC retains all
//! spatial calculators inside its single receiving buffer.
use crate::{
    av1,
    engine::MediaEngine,
    temporal_rate::{Dd, Vpx},
};
use rtc_core::ids::TransportId;
use str0m::{format::Codec, media::MediaPacket};
pub(crate) fn vp9_spatial(payload: &[u8]) -> Option<usize> {
    let flags = *payload.first()?;
    if flags & 0x20 == 0 {
        return Some(0);
    }
    let mut index = 1;
    if flags & 0x80 != 0 {
        index += if *payload.get(index)? & 0x80 != 0 {
            2
        } else {
            1
        };
    }
    payload
        .get(index)
        .map(|layer| usize::from((layer >> 1) & 7))
}

fn vp9_header(payload: &[u8]) -> Option<(u16, usize, usize)> {
    let flags = *payload.first()?;
    if flags & 0x80 == 0 {
        return None;
    }
    let byte = *payload.get(1)?;
    let (picture, index) = if byte & 0x80 != 0 {
        (
            (u16::from(byte & 0x7f) << 8) | u16::from(*payload.get(2)?),
            3,
        )
    } else {
        (u16::from(byte), 2)
    };
    let (spatial, temporal) = if flags & 0x20 != 0 {
        let layer = *payload.get(index)?;
        (usize::from((layer >> 1) & 7), usize::from(layer >> 5))
    } else {
        (0, 0)
    };
    Some((picture, spatial, temporal))
}
struct Encoding {
    source: Option<u64>,
    dd_allowed: bool,
    using_dd: bool,
    calculated: bool,
    vp9: [Vpx; 3],
    dd: Dd,
    parser: av1::RateParser,
}
impl Default for Encoding {
    fn default() -> Self {
        Self {
            source: None,
            dd_allowed: true,
            using_dd: true,
            calculated: false,
            vp9: Default::default(),
            dd: Default::default(),
            parser: Default::default(),
        }
    }
}
#[derive(Default)]
pub(crate) struct Receivers {
    encodings: [Encoding; 3],
    per_encoding: bool,
}
impl Receivers {
    pub(crate) fn per_encoding(&self) -> bool {
        self.per_encoding
    }
    pub(crate) fn rates(&self, spatial: usize) -> [f32; 4] {
        let (slot, inside) = if self.per_encoding {
            (spatial, 0)
        } else {
            (0, spatial)
        };
        let Some(encoding) = self.encodings.get(slot) else {
            return [0.0; 4];
        };
        if encoding.using_dd {
            encoding.dd.rates(inside)
        } else {
            encoding.vp9.get(inside).map_or([0.0; 4], Vpx::rates)
        }
    }
    pub(crate) fn observe(
        &mut self,
        source: u64,
        slot: usize,
        per_encoding: bool,
        codec: Codec,
        sequence: u64,
        timestamp: u32,
        payload: &[u8],
        values: &str0m::rtp::ExtensionValues,
    ) {
        if payload.is_empty() {
            return;
        } // RTP padding never disables an active DD parser.
        if slot >= 3 {
            return;
        }
        self.per_encoding = per_encoding;
        let encoding = &mut self.encodings[if per_encoding { slot } else { 0 }];
        // This identity denotes the actual authenticated native receiver lifetime,
        // not a sortable generation: SDP may pre-create a receiver before another
        // slot first becomes active. Native retirement removes its pending event.
        if encoding.source != Some(source) {
            *encoding = Encoding {
                source: Some(source),
                ..Default::default()
            };
        }
        if encoding.dd_allowed {
            if values.user_values.get::<av1::RawDescriptor>().is_some() {
                encoding.using_dd = true;
                let Some(packet) = encoding.parser.parse(values, sequence) else {
                    return;
                };
                encoding.dd.max_layer(packet.maximum.0, packet.maximum.1);
                if !encoding.calculated {
                    encoding.dd.observe(
                        packet.frame,
                        packet.spatial,
                        packet.temporal,
                        timestamp,
                        &packet.diffs,
                        90000,
                    );
                    encoding.calculated = encoding.dd.completed();
                }
                return;
            }
            if codec == Codec::Vp9 {
                // Fixed processVideoPacket disables DD only on extension-not-found. A
                // malformed present descriptor returns an error and leaves this parser live.
                encoding.dd_allowed = false;
                encoding.using_dd = false;
                encoding.vp9 = Default::default();
                // Do not clear BufferBase.frameRateCalculated: original replacement after
                // completion leaves the new payload calculators cold until source restart.
            } else {
                return;
            }
        }
        if codec == Codec::Vp9 && !encoding.calculated {
            if let Some((picture, spatial, temporal)) = vp9_header(payload) {
                if let Some(rate) = encoding.vp9.get_mut(spatial) {
                    rate.observe(picture, temporal, timestamp, 90000)
                }
                encoding.calculated = encoding.vp9.iter().all(Vpx::completed);
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn test_source_ids(&self) -> [Option<u64>; 3] {
        self.encodings.each_ref().map(|encoding| encoding.source)
    }
    #[cfg(test)]
    pub(crate) fn test_using_dd(&self, slot: usize) -> bool {
        self.encodings[slot].using_dd
    }
    #[cfg(test)]
    pub(crate) fn observe_vp9_test_source(
        &mut self,
        spatial: usize,
        frame: u16,
        temporal: usize,
        timestamp: u32,
    ) {
        let e = &mut self.encodings[0];
        e.using_dd = false;
        e.dd_allowed = false;
        e.vp9[spatial].observe(frame, temporal, timestamp, 90000);
    }
}
impl MediaEngine {
    pub(crate) fn observe_temporal_packet(&mut self, tid: TransportId, data: &MediaPacket) {
        // Padding carries no codec observation. Original Buffer drops it before
        // codec-change and FPS parser consumers, even if its wire PT differs.
        if data.packet.payload.is_empty() {
            return;
        }

        let Some(&ui) = self.up_by_mid.get(&(tid, data.mid)) else {
            return;
        };
        let Some(up) = self.ups.get_mut(ui) else {
            return;
        };
        let mime = crate::codecs::mime_of(data.params.spec().codec);
        if !up.mime.eq_ignore_ascii_case(mime) {
            up.set_mime(mime, data.packet.timestamp)
        }
        let per_encoding = up.rate_is_per_encoding(data.rid);
        let slot = if per_encoding {
            up.spatial_for_rid(data.rid).clamp(0, 2) as usize
        } else {
            0
        };
        up.source_rates.observe(
            data.source_id,
            slot,
            per_encoding,
            data.params.spec().codec,
            *data.packet.seq_no,
            data.packet.time.numer() as u32,
            &data.packet.payload,
            &data.packet.header.ext_vals,
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_vp9_header_uses_wire_picture_and_actual_temporal_spatial() {
        assert_eq!(vp9_header(&[0xa0, 0x81, 0x23, 0x64]), Some((0x123, 2, 3)));
        assert_eq!(vp9_header(&[0x80, 7]), Some((7, 0, 0)));
        assert_eq!(vp9_header(&[0xa0, 0x81]), None);
        assert_eq!(vp9_header(&[0x20, 0]), None);
    }
}

#[cfg(test)]
mod receiver_domain_tests {
    use super::*;
    fn dd(frame: u16, attached: bool) -> str0m::rtp::ExtensionValues {
        let mut bits = Vec::new();
        let mut write = |value: u32, width: usize| {
            for shift in (0..width).rev() {
                bits.push(((value >> shift) & 1) as u8)
            }
        };
        write(3, 2);
        write(u32::from(!attached), 6);
        write(u32::from(frame), 16);
        if attached {
            write(1, 1);
            write(0, 4);
            write(0, 6);
            write(0, 5);
            write(0, 2);
            write(3, 2);
            write(2, 2);
            write(3, 2);
            write(0, 1);
            write(1, 1);
            write(0, 4);
            write(0, 1);
            write(0, 1);
            write(0, 1);
        }
        let mut raw = vec![0; (bits.len() + 7) / 8];
        for (i, bit) in bits.into_iter().enumerate() {
            raw[i / 8] |= bit << (7 - i % 8)
        }
        let mut values = str0m::rtp::ExtensionValues::default();
        values.user_values.set(av1::RawDescriptor(raw.into()));
        values
    }
    fn payload(frame: u8) -> [u8; 5] {
        [0xac, frame, 0, frame, 0x80]
    }
    #[test]
    fn valid_vp9_dd_precedes_missing_picture_id_and_malformed_present_does_not_disable_it() {
        let mut receivers = Receivers::default();
        let mut malformed = str0m::rtp::ExtensionValues::default();
        assert!(!str0m::rtp::ExtensionSerializer::parse_value(
            &av1::Serializer,
            &[0x80, 0],
            &mut malformed
        ));
        receivers.observe(10, 0, false, Codec::Vp9, 999, 3000, &payload(0), &malformed);
        assert!(receivers.encodings[0].dd_allowed);
        for frame in 0..12u16 {
            receivers.observe(
                10,
                0,
                false,
                Codec::Vp9,
                1000 + u64::from(frame),
                3000 + u32::from(frame) * 6000,
                &[0x40],
                &dd(100 + frame, frame == 0),
            )
        }
        assert!(receivers.encodings[0].using_dd);
        assert!(
            receivers.rates(0)[0] > 0.0,
            "real DD metadata measures frames even though payload has no VP9 picture identifier"
        );
        assert_eq!(
            receivers.encodings[0].vp9[0].rates(),
            [0.0; 4],
            "payload calculator was never substituted"
        );
        let measured = receivers.rates(0);
        receivers.observe(
            10,
            0,
            false,
            Codec::Vp9,
            2000,
            100000,
            &[],
            &Default::default(),
        );
        assert_eq!(receivers.rates(0), measured, "padding cannot turn DD off");
    }
    #[test]
    fn three_independent_rid_domains_keep_rates_when_a_precreated_receiver_replaces_one_slot() {
        let mut receivers = Receivers::default();
        for frame in 0..64u16 {
            for (slot, step) in [18000, 6000, 3000].into_iter().enumerate() {
                receivers.observe(
                    100 + slot as u64,
                    slot,
                    true,
                    Codec::Vp9,
                    1000 + u64::from(frame),
                    3000 + u32::from(frame) * step,
                    &payload(frame as u8),
                    &Default::default(),
                )
            }
        }
        assert_eq!(receivers.rates(0), [5.0, 0.0, 0.0, 0.0]);
        assert_eq!(receivers.rates(1), [15.0, 0.0, 0.0, 0.0]);
        assert_eq!(receivers.rates(2), [30.0, 0.0, 0.0, 0.0]);
        for frame in 0..64u16 {
            receivers.observe(
                50,
                0,
                true,
                Codec::Vp9,
                2000 + u64::from(frame),
                3000 + u32::from(frame) * 1500,
                &payload(frame as u8),
                &Default::default(),
            )
        }
        assert_eq!(
            receivers.rates(0),
            [60.0, 0.0, 0.0, 0.0],
            "a fresh actual receiver may have a numerically older pre-created identity"
        );
        assert_eq!(receivers.rates(1), [15.0, 0.0, 0.0, 0.0]);
        assert_eq!(receivers.rates(2), [30.0, 0.0, 0.0, 0.0]);
    }
    #[test]
    fn completed_dd_to_payload_replacement_retains_original_buffer_completion_gate() {
        let mut receivers = Receivers::default();
        for frame in 0..12u16 {
            receivers.observe(
                10,
                0,
                false,
                Codec::Vp9,
                1000 + u64::from(frame),
                3000 + u32::from(frame) * 6000,
                &[0x40],
                &dd(100 + frame, frame == 0),
            )
        }
        assert!(receivers.encodings[0].calculated);
        for frame in 12..76u16 {
            receivers.observe(
                10,
                0,
                false,
                Codec::Vp9,
                1000 + u64::from(frame),
                3000 + u32::from(frame) * 6000,
                &payload(frame as u8),
                &Default::default(),
            )
        }
        assert!(!receivers.encodings[0].using_dd);
        assert_eq!(
            receivers.rates(0),
            [0.0; 4],
            "fixed BufferBase swaps calculators but does not reset frameRateCalculated on DD disappearance"
        );
        receivers.observe(
            10,
            0,
            false,
            Codec::Vp9,
            3000,
            500000,
            &[0x40],
            &dd(200, true),
        );
        assert!(
            !receivers.encodings[0].dd_allowed,
            "later extension does not reenable a disabled parser before source restart"
        );
    }
}
