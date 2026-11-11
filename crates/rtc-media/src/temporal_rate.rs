//! Fixed LiveKit buffer/fps.go calculators, Copyright 2023 LiveKit, Inc.
//! Apache-2.0 (rtc-media/THIRD_PARTY.md). Actual source identifiers and RTP
//! timestamps are the inputs; no assumed powers-of-two or wall-clock cadence.
#[derive(Clone, Copy, Default)]
struct Frame {
    ts: u32,
    number: u16,
    spatial: usize,
    temporal: usize,
}
const MINIMUM: [usize; 4] = [8, 15, 40, 60];
pub(crate) struct Vpx {
    rates: [f32; 4],
    first: [Option<Frame>; 4],
    second: [Option<Frame>; 4],
    received: [Option<Frame>; 64],
    base: Option<Frame>,
    completed: bool,
}
impl Default for Vpx {
    fn default() -> Self {
        Self {
            rates: [0.0; 4],
            first: [None; 4],
            second: [None; 4],
            received: [None; 64],
            base: None,
            completed: false,
        }
    }
}
impl Vpx {
    fn reset(&mut self) {
        self.first = [None; 4];
        self.second = [None; 4];
        self.received = [None; 64];
        self.base = None;
    }
    pub(crate) fn completed(&self) -> bool {
        self.completed
    }
    pub(crate) fn rates(&self) -> [f32; 4] {
        self.rates
    }
    pub(crate) fn observe(&mut self, number: u16, temporal: usize, ts: u32, clock: u32) {
        if self.completed || temporal > 3 {
            return;
        }
        let f = Frame {
            ts,
            number,
            temporal,
            spatial: 0,
        };
        let Some(base) = self.base else {
            self.base = Some(f);
            self.received[0] = Some(f);
            self.first[temporal] = Some(f);
            return;
        };
        let diff = number.wrapping_sub(base.number) as usize;
        if diff == 0 || diff > 0x4000 {
            return;
        }
        if diff >= 64 {
            self.reset();
            return;
        }
        if self.received[diff].is_some() {
            return;
        }
        self.received[diff] = Some(f);
        match self.first[temporal] {
            None => self.first[temporal] = Some(f),
            Some(first) => {
                if self.second[temporal].is_none_or(|s| s.number < number)
                    && number != first.number
                    && number.wrapping_sub(first.number) < 0x4000
                {
                    self.second[temporal] = Some(f)
                }
            }
        }
        let mut count = 0;
        for layer in 0..4 {
            if self.rates[layer] > 0.0 {
                count += 1;
                continue;
            }
            let first = self.first[layer];
            if count > 0 && first.is_none() {
                count += 1;
                continue;
            }
            let (Some(first), Some(second)) = (first, self.second[layer]) else {
                continue;
            };
            let mut frames = 0;
            let mut last = first.ts;
            for index in first.number.wrapping_sub(base.number) as usize + 1
                ..=second.number.wrapping_sub(base.number) as usize
            {
                let Some(fi) = self.received[index] else {
                    break;
                };
                if fi.temporal <= layer {
                    frames += 1;
                    last = fi.ts
                }
            }
            if frames >= MINIMUM[layer] {
                self.rates[layer] =
                    clock as f32 / last.wrapping_sub(first.ts) as f32 * frames as f32;
                count += 1;
            }
        }
        if count == 4 {
            self.completed = true;
            if self.rates[2] > 0.0 && self.rates[2] > self.rates[1] * 3.0 {
                self.rates[1] = self.rates[2] / 2.0
            }
            self.reset();
        }
    }
}
pub(crate) struct Dd {
    rates: [[f32; 4]; 3],
    first: [[Option<Frame>; 4]; 3],
    second: [[Option<Frame>; 4]; 3],
    received: [Option<Frame>; 256],
    base: Option<Frame>,
    chains: [[Vec<u16>; 4]; 3],
    completed: bool,
    max_spatial: usize,
    max_temporal: usize,
}
impl Default for Dd {
    fn default() -> Self {
        Self {
            rates: [[0.0; 4]; 3],
            first: [[None; 4]; 3],
            second: [[None; 4]; 3],
            received: [None; 256],
            base: None,
            chains: Default::default(),
            completed: false,
            max_spatial: 2,
            max_temporal: 3,
        }
    }
}
impl Dd {
    pub(crate) fn completed(&self) -> bool {
        self.completed
    }
    pub(crate) fn rates(&self, spatial: usize) -> [f32; 4] {
        self.rates.get(spatial).copied().unwrap_or([0.0; 4])
    }
    pub(crate) fn max_layer(&mut self, spatial: usize, temporal: usize) {
        if spatial <= 2 {
            self.max_spatial = spatial
        }
        if temporal <= 3 {
            self.max_temporal = temporal
        }
    }
    fn reset(&mut self) {
        self.first = [[None; 4]; 3];
        self.second = [[None; 4]; 3];
        self.received = [None; 256];
        self.base = None;
        for row in &mut self.chains {
            for chain in row {
                chain.clear()
            }
        }
    }
    pub(crate) fn observe(
        &mut self,
        number: u16,
        spatial: usize,
        temporal: usize,
        ts: u32,
        diffs: &[u16],
        clock: u32,
    ) {
        if self.completed || spatial > 2 || temporal > 3 {
            return;
        }
        let fi = Frame {
            number,
            spatial,
            temporal,
            ts,
        };
        let Some(base) = self.base else {
            let base = Frame {
                ts,
                number,
                spatial: 0,
                temporal: 0,
            };
            self.base = Some(base);
            self.received[0] = Some(base);
            self.first[spatial][temporal] = Some(base);
            self.second[spatial][temporal] = Some(base);
            return;
        };
        let difference = number.wrapping_sub(base.number) as usize;
        if difference == 0 || difference > 0x8000 {
            return;
        }
        if difference >= 256 {
            self.reset();
            return;
        }
        if self.received[difference].is_some() {
            return;
        }
        self.received[difference] = Some(fi);
        if self.first[spatial][temporal].is_none() {
            self.first[spatial][temporal] = Some(fi);
            self.second[spatial][temporal] = Some(fi);
            return;
        }
        let chain = &mut self.chains[spatial][temporal];
        if chain.is_empty() {
            chain.push(number)
        }
        for difference in diffs {
            let dependency = number.wrapping_sub(*difference);
            if dependency.wrapping_sub(self.second[spatial][temporal].unwrap().number) > 0x8000 {
                continue;
            }
            // Source walks backwards, inserting after last older item and not replacing
            // an already-known item. Lists are bounded by the 256 input frame window.
            if chain.contains(&dependency) {
                continue;
            }
            let position = chain
                .iter()
                .rposition(|n| {
                    let d = dependency.wrapping_sub(*n);
                    d != 0 && d < 0x8000
                })
                .map_or(0, |p| p + 1);
            chain.insert(position, dependency);
        }
        let mut total = 0;
        for s in 0..=self.max_spatial {
            let mut spatial_count = 0;
            for t in 0..=self.max_temporal {
                if self.rates[s][t] > 0.0 {
                    total += 1;
                    spatial_count += 1;
                    continue;
                }
                let mut first = self.first[s][t];
                if spatial_count > 0 && first.is_none() {
                    spatial_count += 1;
                    total += 1;
                    continue;
                }
                let mut last = None;
                for number in &self.chains[s][t] {
                    let index = number.wrapping_sub(base.number) as usize;
                    if index >= 256 {
                        continue;
                    }
                    let Some(f) = self.received[index] else { break };
                    last = Some(f);
                    if first.is_none() && f.spatial == s && f.temporal == t {
                        first = Some(f)
                    }
                }
                let Some(last) = last else { continue };
                let Some(previous) = self.second[s][t] else {
                    continue;
                };
                if last.number <= previous.number {
                    continue;
                }
                self.second[s][t] = Some(last);
                let Some(first) = first else { continue };
                let mut frames = 0;
                for index in first.number.wrapping_sub(base.number) as usize
                    ..=last.number.wrapping_sub(base.number) as usize
                {
                    if let Some(f) = self.received[index] {
                        if f.spatial == s && f.temporal <= t {
                            frames += 1
                        }
                    }
                }
                if frames >= MINIMUM[t] && last.ts > first.ts {
                    self.rates[s][t] = clock as f32 / (last.ts - first.ts) as f32 * frames as f32;
                    total += 1;
                }
            }
        }
        if total == (self.max_spatial + 1) * (self.max_temporal + 1) {
            self.completed = true;
            self.reset()
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_vpx_all_layers_complete_and_freeze_actual_non_power_of_two_rates() {
        let mut rate = Vpx::default();
        for number in 0..64u16 {
            let temporal = if number % 4 == 0 {
                0
            } else if number % 2 == 0 {
                1
            } else {
                2
            };
            rate.observe(number, temporal, 3000 + u32::from(number) * 6000, 90000)
        }
        assert_eq!(rate.rates(), [3.75, 7.5, 15.0, 0.0]);
        for number in 64..128 {
            rate.observe(number, 0, 3000 + u32::from(number) * 3000, 90000)
        }
        assert_eq!(
            rate.rates(),
            [3.75, 7.5, 15.0, 0.0],
            "fixed source completed calculators do not re-estimate"
        );
    }
    #[test]
    fn fourth_temporal_column_uses_real_sixty_frame_threshold() {
        let mut rate = Vpx::default();
        for frame in 0..61u16 {
            let temporal = [0, 2, 1, 3][frame as usize % 4];
            rate.observe(frame, temporal, u32::from(frame) * 6000, 90000);
            assert_eq!(
                rate.rates()[3],
                0.0,
                "fourth temporal needs sixty cumulative frame intervals, not forty"
            );
        }
        for frame in 61..64u16 {
            let temporal = [0, 2, 1, 3][frame as usize % 4];
            rate.observe(frame, temporal, u32::from(frame) * 6000, 90000);
        }
        assert_eq!(
            rate.rates()[3],
            15.0,
            "TID3 must have its own measured column"
        );
    }

    #[test]
    fn original_vpx_duplicate_and_gap_reset_do_not_invent_cadence() {
        let mut r = Vpx::default();
        r.observe(10, 0, 0, 90000);
        r.observe(10, 2, 90000, 90000);
        assert_eq!(r.rates(), [0.0; 4]);
        r.observe(100, 0, 3000, 90000);
        assert!(r.base.is_none());
        assert_eq!(r.rates(), [0.0; 4]);
    }
}

#[cfg(test)]
mod original_packet_oracle {
    use super::*;
    #[test]
    #[ignore = "requires fixed original Go packet trace, never helper-generated expected output"]
    fn original_go_vpx_dd_calculators_match_every_actual_packet_window() {
        let path =
            std::env::var("GO_TEMPORAL_RATE_RESULTS").expect("actual original Go observer log");
        let input = std::fs::read_to_string(path).unwrap();
        let mut name = String::new();
        let mut vp9: [Vpx; 3] = Default::default();
        let mut dd = Dd::default();
        let mut rows = 0;
        let mut cases = std::collections::HashSet::new();
        let mut tid3 = 0;
        for line in input.lines() {
            let Some(json) = line.strip_prefix("TEMPORAL_PACKET ") else {
                continue;
            };
            let row: serde_json::Value = serde_json::from_str(json).unwrap();
            if row["case"].as_str().unwrap() != name {
                name = row["case"].as_str().unwrap().to_owned();
                vp9 = Default::default();
                dd = Default::default();
            }
            let spatial = row["spatial"].as_u64().unwrap() as usize;
            let temporal = row["temporal"].as_u64().unwrap() as usize;
            cases.insert(row["case"].as_str().unwrap().to_owned());
            tid3 += usize::from(temporal == 3);
            let picture = row["picture"].as_u64().unwrap() as u16;
            let frame = row["frame"].as_u64().unwrap() as u16;
            let ts = row["timestamp"].as_u64().unwrap() as u32;
            let diffs = row["diffs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_u64().unwrap() as u16)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            vp9[spatial].observe(picture, temporal, ts, 90000);
            dd.observe(frame, spatial, temporal, ts, &diffs, 90000);
            let expected = |key: &str| -> [f32; 4] {
                let array = row[key].as_array().unwrap();
                assert_eq!(
                    array.len(),
                    4,
                    "original source has four temporal columns; never truncate"
                );
                std::array::from_fn(|i| array[i].as_f64().unwrap() as f32)
            };
            assert_eq!(
                vp9[spatial].rates(),
                expected("vp9"),
                "actual VP9 source row {rows} {row}"
            );
            assert_eq!(
                dd.rates(spatial),
                expected("dd"),
                "actual DD source row {rows} {row}"
            );
            rows += 1;
        }
        assert_eq!(
            cases.len(),
            6,
            "all original clean/loss/reorder/wrap and TID3 profiles required"
        );
        assert!(
            tid3 > 100,
            "actual fourth-layer source packets must exercise every rate column"
        );
        assert!(
            rows > 2000,
            "all actual clean/loss/reorder/duplicate/wrap source traces must run, got{rows}"
        );
    }
}
