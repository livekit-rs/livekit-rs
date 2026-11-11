//! VP9 frame-mode SVC bridge. str0m exposes spatial boundaries after depayloading
//! but its frame writer emits a single RTP spatial layer. A VP9 superframe index
//! preserves the component boundaries for the decoder after repacketization.
//! Selection keeps every lower spatial dependency. Upgrades and temporal changes
//! are keyframe-gated; unknown or malformed metadata never reaches the decoder.

use std::sync::Arc;
use str0m::format::{CodecExtra, Vp9CodecExtra};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Vp9Layers {
    pub ends: [Option<usize>; 3],
    pub temporal: u8,
}

impl Vp9Layers {
    #[cfg(test)]
    pub(crate) fn parse(extra: CodecExtra, len: usize) -> Option<Self> {
        Self::parse_with_dependencies(extra, len, None)
    }
    pub(crate) fn parse_with_dependencies(
        extra: CodecExtra,
        len: usize,
        dependencies: Option<str0m::media::Vp9FrameDependencies>,
    ) -> Option<Self> {
        let CodecExtra::Vp9(Vp9CodecExtra {
            layers_scheme,
            tid: Some(temporal),
            ..
        }) = extra
        else {
            return None;
        };
        let mut previous = 0;
        for end in layers_scheme.into_iter().flatten() {
            if end <= previous || end > len {
                return None;
            }
            previous = end;
        }
        if previous != len || temporal > 3 {
            return None;
        }
        for spatial in 1..3 {
            if layers_scheme[spatial].is_some()
                && layers_scheme[spatial - 1].is_none()
                && dependencies
                    .and_then(|value| value.0[spatial])
                    .unwrap_or(true)
            {
                return None;
            }
        }
        Some(Self {
            ends: layers_scheme,
            temporal,
        })
    }

    pub(crate) fn highest(self) -> i32 {
        self.ends
            .iter()
            .rposition(Option::is_some)
            .map_or(-1, |i| i as i32)
    }

    /// Reconstruct a standards-defined VP9 superframe, never just concatenate
    /// elementary frames. Size fields are little endian and include no index.
    pub(crate) fn payload(self, data: &[u8], spatial: i32) -> Option<Arc<[u8]>> {
        let spatial = usize::try_from(spatial).ok().filter(|s| *s < 3)?;
        let mut sizes = Vec::with_capacity(spatial + 1);
        let mut previous = 0;
        for end in self.ends.iter().take(spatial + 1).flatten() {
            sizes.push(end.checked_sub(previous).filter(|size| *size > 0)?);
            previous = *end;
        }
        let end = previous;
        if sizes.is_empty() || end > data.len() {
            return None;
        }
        if sizes.len() == 1 {
            return Some(Arc::from(&data[..end]));
        }
        let frame_count = sizes.len();
        let largest = *sizes.iter().max()?;
        let magnitude = (1..=4).find(|bytes| (largest as u64) < (1u64 << (bytes * 8)))?;
        let marker = 0xc0 | ((magnitude as u8 - 1) << 3) | (frame_count as u8 - 1);
        let mut out = Vec::with_capacity(end + 2 + sizes.len() * magnitude);
        out.extend_from_slice(&data[..end]);
        out.push(marker);
        for size in sizes {
            for byte in 0..magnitude {
                out.push((size >> (8 * byte)) as u8);
            }
        }
        out.push(marker);
        Some(out.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn extra(ends: [Option<usize>; 3], tid: u8) -> CodecExtra {
        CodecExtra::Vp9(Vp9CodecExtra {
            layers_scheme: ends,
            tid: Some(tid),
            ..Default::default()
        })
    }
    #[test]
    fn vp9_spatial_prefix_has_superframe_boundaries_and_lower_dependencies() {
        let layers = Vp9Layers::parse(extra([Some(2), Some(5), Some(9)], 1), 9).unwrap();
        let data = [0x80, 1, 0x81, 2, 3, 0x82, 4, 5, 6];
        assert_eq!(layers.payload(&data, 0).unwrap().as_ref(), &data[..2]);
        assert_eq!(
            layers.payload(&data, 1).unwrap().as_ref(),
            &[0x80, 1, 0x81, 2, 3, 0xc1, 2, 3, 0xc1]
        );
        assert_eq!(
            layers.payload(&data, 2).unwrap().as_ref(),
            &[0x80, 1, 0x81, 2, 3, 0x82, 4, 5, 6, 0xc2, 2, 3, 4, 0xc2]
        );
        assert_eq!(layers.temporal, 1);
        assert_eq!(layers.highest(), 2);
    }
    #[test]
    fn independent_sparse_spatial_frame_keeps_exact_bytes_and_dependency_gates() {
        let raw = extra([None, None, Some(2)], 0);
        assert!(
            Vp9Layers::parse(raw, 2).is_none(),
            "without actual D metadata missing dependencies remain rejected"
        );
        let high = Vp9Layers::parse_with_dependencies(
            raw,
            2,
            Some(str0m::media::Vp9FrameDependencies([
                None,
                None,
                Some(false),
            ])),
        )
        .unwrap();
        assert_eq!(high.highest(), 2);
        assert_eq!(high.payload(&[0x82, 0], 2).unwrap().as_ref(), &[0x82, 0]);
        assert!(high.payload(&[0x82, 0], 0).is_none());
        assert!(
            Vp9Layers::parse_with_dependencies(
                raw,
                2,
                Some(str0m::media::Vp9FrameDependencies([None, None, Some(true)]))
            )
            .is_none(),
            "real D=1 cannot manufacture missing lower data"
        );
        let holes = Vp9Layers::parse_with_dependencies(
            extra([Some(2), None, Some(4)], 0),
            4,
            Some(str0m::media::Vp9FrameDependencies([
                Some(false),
                None,
                Some(false),
            ])),
        )
        .unwrap();
        assert_eq!(
            holes.payload(&[0x80, 0, 0x82, 0], 2).unwrap().as_ref(),
            &[0x80, 0, 0x82, 0, 0xc1, 2, 2, 0xc1]
        );
        assert_eq!(
            holes.payload(&[0x80, 0, 0x82, 0], 1).unwrap().as_ref(),
            &[0x80, 0]
        );
    }
    #[test]
    fn sizes_use_little_endian_and_minimum_magnitude() {
        let layers = Vp9Layers::parse(extra([Some(256), Some(513), None], 0), 513).unwrap();
        let out = layers.payload(&vec![0; 513], 1).unwrap();
        assert_eq!(&out[513..], &[0xc9, 0, 1, 1, 1, 0xc9]);
    }
    #[test]
    fn malformed_metadata_fails_closed() {
        for ends in [
            [Some(0), None, None],
            [Some(3), Some(2), None],
            [Some(3), None, Some(6)],
            [Some(7), None, None],
            [Some(3), None, None],
        ] {
            assert!(Vp9Layers::parse(extra(ends, 0), 6).is_none());
        }
        assert_eq!(
            Vp9Layers::parse(extra([Some(6), None, None], 3), 6)
                .unwrap()
                .temporal,
            3,
            "fixed Go admits the fourth temporal layer"
        );
        assert!(Vp9Layers::parse(extra([Some(6), None, None], 4), 6).is_none());
        assert!(Vp9Layers::parse(CodecExtra::None, 6).is_none());
    }
}

/// Source T0 cadence, independent of PLI-induced keyframes and network arrival
/// jitter. A keyframe can insert/reset the temporal pattern, so neither its
/// incoming interval nor the interval immediately after it is a cadence sample.
#[derive(Default)]
pub(crate) struct TemporalRate {
    frequency: Option<str0m::media::Frequency>,
    last: Option<str0m::media::MediaTime>,
    intervals: [u64; 9],
    samples: usize,
    next: usize,
}

impl TemporalRate {
    pub(crate) fn observe(&mut self, temporal: u8, keyframe: bool, time: str0m::media::MediaTime) {
        if temporal != 0 {
            return;
        }
        if self.frequency != Some(time.frequency()) {
            *self = Self::default();
            self.frequency = Some(time.frequency());
        }
        if keyframe {
            self.last = None;
            return;
        }
        if let Some(previous) = self.last {
            if previous.frequency() != time.frequency() {
                *self = Self::default();
            } else if time.numer() <= previous.numer() {
                return;
            } else {
                let ticks = time.numer() - previous.numer();
                let frequency = time.frequency().get() as u64;
                if ticks >= frequency.div_ceil(120) && ticks <= frequency {
                    self.intervals[self.next] = ticks;
                    self.next = (self.next + 1) % self.intervals.len();
                    self.samples = (self.samples + 1).min(self.intervals.len());
                } else {
                    // A discontinuity / resumed stream needs fresh cadence.
                    self.samples = 0;
                    self.next = 0;
                }
            }
        }
        self.last = Some(time);
    }

    pub(crate) fn fps(&self) -> Option<f64> {
        if self.samples < 3 {
            return None;
        }
        let mut sorted = self.intervals;
        sorted[..self.samples].sort_unstable();
        Some(self.frequency?.get() as f64 / sorted[self.samples / 2] as f64)
    }
}

#[cfg(test)]
mod temporal_rate_tests {
    use super::*;
    use str0m::media::{Frequency, MediaTime};
    fn time(ticks: u64) -> MediaTime {
        MediaTime::new(ticks, Frequency::NINETY_KHZ)
    }
    #[test]
    fn consecutive_pli_keyframes_and_adjacent_intervals_do_not_inflate_cadence() {
        let mut rate = TemporalRate::default();
        let mut ticks = 0;
        for _ in 0..12 {
            rate.observe(0, false, time(ticks));
            ticks += 12_000;
        }
        assert_eq!(rate.fps(), Some(7.5));
        for _ in 0..12 {
            rate.observe(0, true, time(ticks));
            assert_eq!(
                rate.fps(),
                Some(7.5),
                "keyframes preserve the stable estimate"
            );
            ticks += 3_000;
        }
        // First regular T0 after the keyframe is only a new anchor.
        rate.observe(0, false, time(ticks));
        assert_eq!(rate.fps(), Some(7.5));
        for _ in 0..6 {
            ticks += 12_000;
            rate.observe(0, false, time(ticks));
        }
        assert_eq!(rate.fps(), Some(7.5));
        rate.observe(0, false, time(ticks - 1));
        rate.observe(0, false, time(ticks));
        assert_eq!(
            rate.fps(),
            Some(7.5),
            "reordered and duplicate timestamps have no weight"
        );
    }
    #[test]
    fn median_tracks_real_source_rate_changes_in_both_directions() {
        let mut rate = TemporalRate::default();
        let mut ticks = 0;
        for _ in 0..12 {
            rate.observe(0, false, time(ticks));
            ticks += 12_000;
        }
        assert_eq!(rate.fps(), Some(7.5)); // L3T3 source 30fps
        for _ in 0..12 {
            rate.observe(0, false, time(ticks));
            ticks += 24_000;
        }
        assert_eq!(rate.fps(), Some(3.75)); // L3T3 source 15fps
        for _ in 0..12 {
            rate.observe(0, false, time(ticks));
            ticks += 12_000;
        }
        assert_eq!(rate.fps(), Some(7.5));
    }
    #[test]
    fn cold_start_and_resumed_stream_need_multiple_clean_samples() {
        let mut rate = TemporalRate::default();
        for ticks in [0, 12_000, 24_000] {
            rate.observe(0, false, time(ticks));
        }
        assert_eq!(rate.fps(), None);
        rate.observe(0, false, time(36_000));
        assert_eq!(rate.fps(), Some(7.5));
        rate.observe(0, false, time(900_000));
        assert_eq!(rate.fps(), None);
        for ticks in [912_000, 924_000, 936_000] {
            rate.observe(0, false, time(ticks));
        }
        assert_eq!(rate.fps(), Some(7.5));
    }
}
