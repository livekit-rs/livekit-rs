// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `pkg/sfu/bwe/sendsidebwe/traffic_stats.go`.
//!
//! Upstream splits the aggregate counters from the weighted-loss configuration:
//! `packetGroup.Traffic()` returns a `trafficStats` whose params are the zero
//! config and is only ever consumed by `Merge` into a configured instance. This
//! port encodes that split in the types, so an unconfigured aggregate can never
//! answer a rate or loss question.

use rtc_core::config::{PolicyDuration, WeightedLossConfig};

/// The counters `packetGroup.Traffic()` produces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TrafficData {
    pub(crate) min_send_time: i64,
    pub(crate) max_send_time: i64,
    pub(crate) send_delta: i64,
    pub(crate) recv_delta: i64,
    pub(crate) acked_packets: i64,
    pub(crate) acked_bytes: i64,
    pub(crate) lost_packets: i64,
    pub(crate) lost_bytes: i64,
}

/// The zero-valued `WeightedLossConfig` that Go's parameterless
/// `&trafficStats{…}` literal carries. It is deliberately *not*
/// `WeightedLossConfig::default()`, which holds the frozen upstream defaults.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
pub(crate) fn zero_weighted_loss() -> WeightedLossConfig {
    WeightedLossConfig {
        min_duration_for_loss_validity: PolicyDuration::ZERO,
        base_duration: PolicyDuration::ZERO,
        base_pps: 0,
        loss_penalty_factor: 0.0,
    }
}

/// The transcendental pair of `WeightedLoss` is reproduced from the reference
/// rather than delegated to the platform libm. macOS libm lands one ulp away on
/// about a quarter of `log10` inputs and a tenth of `exp` inputs, and an ulp of
/// weighted loss is an ulp of loss penalty, of captured traffic ratio and of
/// every capacity path that reads them.
///
/// What "the reference" is here is pinned per function by the toolchain build
/// tags, and the three differ:
///
/// * `math.Log10` is `log10(x) = Log(x) * (1 / Ln10)` (`src/math/log10.go`),
///   with no assembly variant outside s390x.
/// * `math.Log` dispatches to `archLog` only under `//go:build amd64 || s390x`
///   (`log_asm.go`); on arm64 `haveArchLog` is false, so it runs the fdlibm
///   transcription in `src/math/log.go`. That source, compiled by the pinned
///   toolchain, contracts every single-use `mul` that feeds an `add`/`sub` into
///   one hardware instruction (`go tool objdump -s '^math\.log$'` shows three
///   `FMADDD` on the `t1` Horner levels, two on `t2`, none on the `s2` multiply,
///   `FMADDD` for `R = t1 + t2`, …). Reproducing the *source* order with plain
///   operators therefore lands one ulp away even though it is correctly rounded;
///   the fused sites below are the disassembly's, each written with
///   `mul_add` so a Rust build on any target emits the same single rounding.
/// * `math.Exp` is *not* `src/math/exp.go` on this host: `exp_asm.go` sets
///   `haveArchExp` for `amd64 || arm64 || loong64 || s390x`, and arm64 runs the
///   hand-written `src/math/exp_arm64.s` (the amd64 one is a different Shibata
///   algorithm entirely). `go_exp` ports that assembly, whose fusion is
///   explicit in the source text rather than a compiler decision.
///
/// The constants are the hex words the Go sources name as the intended ones.
const LN2_HI: f64 = f64::from_bits(0x3fe6_2e42_fee0_0000);
const LN2_LO: f64 = f64::from_bits(0x3dea_39ef_3579_3c76);
/// `1 / Ln10`: Go evaluates that as an exact constant expression and rounds it
/// once, which is one ulp above dividing the rounded `Ln10` at run time.
const RECIPROCAL_LN10: f64 = f64::from_bits(0x3fdb_cb7b_1526_e50e);
const SQRT2: f64 = f64::from_bits(0x3ff6_a09e_667f_3bcd);
const EXP_LOG2_E: f64 = f64::from_bits(0x3ff7_1547_652b_82fe);
const EXP_OVERFLOW: f64 = f64::from_bits(0x4086_2e42_fefa_39ef);
const EXP_UNDERFLOW: f64 = f64::from_bits(0xc087_4910_d52d_3051);
/// `1.0 / (1 << 28)`, the span inside which upstream answers `1 + x`.
const EXP_NEAR_ZERO: f64 = f64::from_bits(0x3e30_0000_0000_0000);

const EXP_BIAS: i32 = 1023;
const EXP_MASK: u64 = 0x7ff;
const EXP_SHIFT: u64 = 52;

/// `normalize` from `src/math/bits.go`: pull a subnormal into the normal range.
fn go_normalize(x: f64) -> (f64, i32) {
    const SMALLEST_NORMAL: f64 = f64::from_bits(0x0010_0000_0000_0000);
    if x.abs() < SMALLEST_NORMAL {
        (x * (1u64 << 52) as f64, -52)
    } else {
        (x, 0)
    }
}

/// `math.Frexp` as upstream computes it, so the exponent split is the same
/// bit surgery on every host instead of a libm call.
fn go_frexp(x: f64) -> (f64, i32) {
    if x == 0.0 || !x.is_finite() {
        return (x, 0);
    }
    let (x, mut exponent) = go_normalize(x);
    let word = x.to_bits();
    exponent += ((word >> EXP_SHIFT) & EXP_MASK) as i32 - EXP_BIAS + 1;
    let frac =
        f64::from_bits(word & !(EXP_MASK << EXP_SHIFT) | ((EXP_BIAS - 1) as u64) << EXP_SHIFT);
    (frac, exponent)
}

/// The word Go's `math.NaN()` hands back. Its payload is 1, not the quiet-NaN
/// zero that `f64::NAN` carries, so the negative-input branch returns this.
const GO_NAN: f64 = f64::from_bits(0x7ff8_0000_0000_0001);

/// `math.Log` as upstream computes it.
fn go_log(x: f64) -> f64 {
    const L1: f64 = f64::from_bits(0x3fe5_5555_5555_5593);
    const L2: f64 = f64::from_bits(0x3fd9_9999_9997_fa04);
    const L3: f64 = f64::from_bits(0x3fd2_4924_9422_9359);
    const L4: f64 = f64::from_bits(0x3fcc_71c5_1d8e_78af);
    const L5: f64 = f64::from_bits(0x3fc7_4664_96cb_03de);
    const L6: f64 = f64::from_bits(0x3fc3_9a09_d078_c69f);
    const L7: f64 = f64::from_bits(0x3fc2_f112_df3e_5244);

    if x.is_nan() || x == f64::INFINITY {
        return x;
    }
    if x < 0.0 {
        return GO_NAN;
    }
    if x == 0.0 {
        return f64::NEG_INFINITY;
    }

    let (unscaled, exponent) = go_frexp(x);
    let (unscaled, exponent) = if unscaled < SQRT2 / 2.0 {
        (unscaled * 2.0, exponent - 1)
    } else {
        (unscaled, exponent)
    };
    let f = unscaled - 1.0;
    let k = exponent as f64;

    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    // The `t1` Horner levels and the `s2` multiply that produces `t1` are three
    // and one contractions: `s2*(…)` never rounds on its own, it is the fused
    // product inside `R = t1 + t2`. `t2 = s4*(…)` is the multiply the register
    // allocator left unfused, so `R` fuses `t1`'s and keeps `t2` rounded.
    let p1 = s4.mul_add(L7, L5);
    let p1 = s4.mul_add(p1, L3);
    let p1 = s4.mul_add(p1, L1);
    let p2 = s4.mul_add(L6, L4);
    let p2 = s4.mul_add(p2, L2);
    let t2 = p2 * s4;
    let r = s2.mul_add(p1, t2);
    // `0.5 * f` is exact and lives in one register, and its product with `f` is
    // never rounded separately either: it is fused into `hfsq + R` and, a second
    // time, into `hfsq - C`.
    let half_f = 0.5 * f;
    let hr = half_f.mul_add(f, r);
    let c = s.mul_add(hr, k * LN2_LO);
    let d = half_f.mul_add(f, -c);
    let e = d - f;
    LN2_HI.mul_add(k, -e)
}

/// `math.Log10` as upstream computes it.
fn go_log10(x: f64) -> f64 {
    go_log(x) * RECIPROCAL_LN10
}

/// `math.Exp` as the pinned toolchain actually computes it on arm64, which is
/// the hand-written `src/math/exp_arm64.s` (`archExp`), not `src/math/exp.go`.
/// The fused sites are written in the assembly text itself rather than chosen
/// by a compiler, and the scaling step is an inlined `Ldexp` that skips the
/// `Inf`/`NaN`/`Zero` checks `math.Ldexp` makes.
fn go_exp(x: f64) -> f64 {
    const P1: f64 = f64::from_bits(0x3fc5_5555_5555_5555);
    const P2: f64 = f64::from_bits(0xbf66_c16c_16be_bd93);
    const P3: f64 = f64::from_bits(0x3f11_566a_af25_de2c);
    const P4: f64 = f64::from_bits(0xbebb_bd41_c5d2_6bf1);
    const P5: f64 = f64::from_bits(0x3e66_3769_72be_a4d0);
    /// `FracMask` and `C1 = 2**-52` from the same source file.
    const FRAC_MASK: u64 = 0x000f_ffff_ffff_ffff;
    const TWO_POW_52: f64 = f64::from_bits(0x3cb0_0000_0000_0000);

    if x.is_nan() {
        return x;
    }
    if x > EXP_OVERFLOW {
        return f64::INFINITY;
    }
    if x < EXP_UNDERFLOW {
        return 0.0;
    }
    if x.abs() < EXP_NEAR_ZERO {
        return 1.0 + x;
    }

    // Argument reduction. Both candidates are formed with the product fused
    // against the half, and the sign test picks one before truncating.
    let k = if x < 0.0 {
        EXP_LOG2_E.mul_add(x, -0.5)
    } else {
        EXP_LOG2_E.mul_add(x, 0.5)
    };
    let k = k as i64;
    let kd = k as f64;
    let hi = (-kd).mul_add(LN2_HI, x);
    let lo = kd * LN2_LO;

    let r = hi - lo;
    let t = r * r;
    let p = t.mul_add(P5, P4);
    let p = t.mul_add(p, P3);
    let p = t.mul_add(p, P2);
    let p = t.mul_add(p, P1);
    let c = (-t).mul_add(p, r);
    let q = (r * c) / (2.0 - c);
    let y = 1.0 - ((lo - q) - hi);

    // The scaling is `Ldexp(y, k)` inlined, so the exponent of `y` is added to
    // `k` as raw bits and only the subnormal case reaches for a second factor.
    let word = y.to_bits();
    let frac = word & FRAC_MASK;
    let mut exponent = (word >> EXP_SHIFT) as i64 + k;
    let mut scale = 1.0;
    if exponent < 1 {
        exponent += 52;
        scale = TWO_POW_52;
    }
    scale * f64::from_bits(frac | ((exponent as u64) << EXP_SHIFT))
}

/// `trafficStats`: configured aggregate of one or more `TrafficData`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TrafficStats {
    config: WeightedLossConfig,
    data: TrafficData,
}

impl TrafficStats {
    pub(crate) fn new(config: WeightedLossConfig) -> Self {
        Self {
            config,
            data: TrafficData::default(),
        }
    }

    /// `Merge`: minimum send time only advances past an unset zero, and only the
    /// counters combine.
    pub(crate) fn merge(&mut self, rhs: &TrafficData) {
        if self.data.min_send_time == 0 || rhs.min_send_time < self.data.min_send_time {
            self.data.min_send_time = rhs.min_send_time;
        }
        if rhs.max_send_time > self.data.max_send_time {
            self.data.max_send_time = rhs.max_send_time;
        }
        self.data.send_delta += rhs.send_delta;
        self.data.recv_delta += rhs.recv_delta;
        self.data.acked_packets += rhs.acked_packets;
        self.data.acked_bytes += rhs.acked_bytes;
        self.data.lost_packets += rhs.lost_packets;
        self.data.lost_bytes += rhs.lost_bytes;
    }

    /// `NumBytes`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn num_bytes(&self) -> i64 {
        self.data.acked_bytes + self.data.lost_bytes
    }

    /// `Duration()` in microseconds.
    pub(crate) fn duration_us(&self) -> i64 {
        self.data.max_send_time - self.data.min_send_time
    }

    /// `AcknowledgedBitrate()`: bytes per second scaled by the captured ratio,
    /// truncated like Go's `int64` conversion.
    pub(crate) fn acknowledged_bitrate(&self) -> i64 {
        let duration = self.duration_us();
        if duration == 0 {
            return 0;
        }
        let acked_bitrate = self.data.acked_bytes as f64 * 8.0 * 1e6 / self.duration_us() as f64;
        super::time::float_i64(acked_bitrate * self.captured_traffic_ratio())
    }

    /// `CapturedTrafficRatio()`.
    pub(crate) fn captured_traffic_ratio(&self) -> f64 {
        if self.data.recv_delta == 0 {
            return 0.0;
        }
        (self.data.send_delta as f64 / (self.data.recv_delta + self.loss_penalty()) as f64).min(1.0)
    }

    /// `WeightedLoss()`.
    pub(crate) fn weighted_loss(&self) -> f64 {
        let duration_micro = self.duration_us();
        if duration_micro * 1_000 < self.config.min_duration_for_loss_validity.as_nanos_i64() {
            return 0.0;
        }

        let total_packets = (self.data.lost_packets + self.data.acked_packets) as f64;
        let pps = total_packets * 1e6 / duration_micro as f64;

        let mut delta_duration = duration_micro * 1_000 - self.config.base_duration.as_nanos_i64();
        if delta_duration < 0 {
            delta_duration = 0;
        }
        let threshold = go_exp(-delta_duration as f64 / 1e9) * self.config.base_pps as f64;
        if pps < threshold {
            return 0.0;
        }

        let loss_ratio = if total_packets != 0.0 {
            self.data.lost_packets as f64 / total_packets
        } else {
            0.0
        };
        loss_ratio * go_log10(pps)
    }

    /// `lossPenalty()`: simulated queuing contribution of dropped packets.
    pub(crate) fn loss_penalty(&self) -> i64 {
        super::time::float_i64(
            self.data.recv_delta as f64 * self.weighted_loss() * self.config.loss_penalty_factor,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn data(&self) -> TrafficData {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::{CongestionDetectorConfig, PolicyDuration};

    fn configured() -> TrafficStats {
        TrafficStats::new(CongestionDetectorConfig::default().weighted_loss)
    }

    fn data(
        min_send: i64,
        max_send: i64,
        send_delta: i64,
        recv_delta: i64,
        acked: i64,
        bytes: i64,
        lost: i64,
    ) -> TrafficData {
        TrafficData {
            min_send_time: min_send,
            max_send_time: max_send,
            send_delta,
            recv_delta,
            acked_packets: acked,
            acked_bytes: bytes,
            lost_packets: lost,
            lost_bytes: lost * 100,
        }
    }

    /// Every answer must be the pinned toolchain's word rather than the host
    /// libm's, because the two differ by an ulp on 28% of these `log10` inputs
    /// and 11% of these `exp` inputs (see `math_census.txt`). Rebuild the table
    /// with the command in `math_probe.go`; swapping the port back to
    /// `f64::log10`/`f64::exp` fails it on 201 of the 1568 rows.
    #[test]
    fn the_ported_transcendentals_reproduce_the_pinned_go_table() {
        let mut checked = 0usize;
        for line in include_str!("math_probe.txt").lines() {
            let mut fields = line.split(' ');
            let kind = fields.next().expect("kind");
            let in_bits = u64::from_str_radix(fields.next().expect("input word"), 16)
                .expect("input word is hex");
            let want = u64::from_str_radix(fields.next().expect("output word"), 16)
                .expect("output word is hex");
            let x = f64::from_bits(in_bits);
            let got = match kind {
                "log" => go_log(x),
                "log10" => go_log10(x),
                "exp" => go_exp(x),
                other => panic!("unexpected probe kind {other}"),
            };
            let got = got.to_bits();
            assert_eq!(
                got, want,
                "{kind}({x:?}) gave 0x{got:016x}, the pinned Go gave 0x{want:016x}"
            );
            checked += 1;
        }
        assert_eq!(checked, 1_568, "the probe table is truncated");
    }

    /// The census table is a sweep of the interval the detector actually
    /// produces; a port tuned only to it could still be wrong outside it. This
    /// second table is anchored on the domain instead: the specials the branches
    /// key on (`Log10(1) == 0` and its sign change, `Exp`'s near-zero/overflow/
    /// underflow cut-offs and both Nextafter sides of each), every `pps` the
    /// `total*1e6/duration` expression can produce from the campaign's packet
    /// counts and bucket durations, and both sides of the
    /// `pps < exp(-deltaDuration)*basePPS` gate for the configurations the
    /// detector uses. Rebuild with the command in `math_domain.go`.
    ///
    /// `C73_MATH_TABLE` swaps the committed table for a generated one and
    /// `C73_MATH_ROWS` states how many rows it must hold. That is how the
    /// seeded `-sweep` block is checked without committing 120 000 further
    /// rows: a generated table is only as trustworthy as the row count it is
    /// required to have, so the count is supplied by the caller and a silently
    /// truncated file still fails the last assertion.
    #[test]
    fn the_ported_transcendentals_hold_over_the_whole_operating_domain() {
        let (table, rows) = match std::env::var("C73_MATH_TABLE") {
            Err(_) => (include_str!("math_domain.txt").to_owned(), 4_832),
            Ok(path) => {
                let want = std::env::var("C73_MATH_ROWS")
                    .unwrap_or_else(|_| panic!("{path} needs C73_MATH_ROWS"))
                    .parse::<usize>()
                    .expect("C73_MATH_ROWS is a row count");
                (
                    std::fs::read_to_string(&path).expect("the generated table is readable"),
                    want,
                )
            }
        };
        let mut checked = 0usize;
        for line in table.lines() {
            let mut fields = line.split(' ');
            let kind = fields.next().expect("kind");
            let in_bits = u64::from_str_radix(fields.next().expect("input word"), 16)
                .expect("input word is hex");
            let want = u64::from_str_radix(fields.next().expect("output word"), 16)
                .expect("output word is hex");
            let x = f64::from_bits(in_bits);
            let got = match kind {
                "log" => go_log(x),
                "log10" => go_log10(x),
                "exp" => go_exp(x),
                other => panic!("unexpected domain kind {other}"),
            };
            let got = got.to_bits();
            assert_eq!(
                got, want,
                "{kind}({x:?}) gave 0x{got:016x}, the pinned Go gave 0x{want:016x}"
            );
            checked += 1;
        }
        assert_eq!(checked, rows, "the domain table is truncated");
    }

    #[test]
    fn zero_recv_delta_forces_the_captured_ratio_and_bitrate_to_zero() {
        let mut stats = configured();
        stats.merge(&data(0, 100_000, 50_000, 0, 100, 100_000, 0));
        assert_eq!(stats.captured_traffic_ratio(), 0.0);
        assert_eq!(stats.acknowledged_bitrate(), 0);
        assert_eq!(stats.duration_us(), 100_000);
        assert_eq!(stats.num_bytes(), 100_000);
    }

    #[test]
    fn ratio_caps_at_one_when_send_span_exceeds_recv_span() {
        let mut stats = configured();
        stats.merge(&data(0, 100_000, 200_000, 10_000, 100, 100_000, 0));
        assert_eq!(stats.captured_traffic_ratio(), 1.0);
        assert_eq!(stats.acknowledged_bitrate(), 8_000_000);
    }

    #[test]
    fn weighted_loss_requires_the_minimum_duration_and_minimum_packet_rate() {
        let config = CongestionDetectorConfig::default().weighted_loss;
        // 50ms of span is below the 100ms validity floor.
        let mut short = TrafficStats::new(config);
        short.merge(&data(0, 50_000, 50_000, 50_000, 100, 100_000, 10));
        assert_eq!(short.weighted_loss(), 0.0);

        // 200ms span at 550 packets per second exceeds the decayed threshold.
        let mut long = TrafficStats::new(config);
        long.merge(&data(0, 200_000, 200_000, 200_000, 100, 100_000, 10));
        assert_eq!(long.weighted_loss(), 0.249_123_880_863_113_07);
        assert_eq!(long.loss_penalty(), 12_456);
        assert_eq!(long.acknowledged_bitrate(), 3_765_485);

        // Oracle tr_sparse: a 5s span at 3 packets per second still clears the
        // heavily decayed threshold (exp(-4.5s) * 30pps), so the loss counts.
        let mut sparse = TrafficStats::new(config);
        sparse.merge(&data(0, 5_000_000, 5_000_000, 5_000_000, 10, 10_000, 5));
        assert_eq!(sparse.weighted_loss(), 0.159_040_418_239_887_46);
        assert_eq!(sparse.loss_penalty(), 198_800);

        // Oracle tr_lowpps: only a rate below that threshold is ignored. One
        // packet over 100ms is 10pps against a 30pps threshold.
        let mut low = TrafficStats::new(config);
        low.merge(&data(0, 100_000, 100_000, 100_000, 0, 0, 1));
        assert_eq!(low.weighted_loss(), 0.0);
    }

    #[test]
    fn loss_penalty_grows_with_the_weighted_loss_and_factor() {
        // Oracle tr_pen and tr_pen0: the same group, only the factor moved.
        let mut config = CongestionDetectorConfig::default().weighted_loss;
        let mut with_loss = TrafficStats::new(config);
        with_loss.merge(&data(0, 200_000, 200_000, 200_000, 100, 100_000, 10));
        assert_eq!(with_loss.loss_penalty(), 12_456);
        assert_eq!(with_loss.captured_traffic_ratio(), 0.941_371_389_840_72);

        config.loss_penalty_factor = 0.0;
        let mut none = TrafficStats::new(config);
        none.merge(&data(0, 200_000, 200_000, 200_000, 100, 100_000, 10));
        assert_eq!(none.loss_penalty(), 0);
        assert_eq!(none.captured_traffic_ratio(), 1.0);
        assert_eq!(none.acknowledged_bitrate(), 4_000_000);
    }

    #[test]
    fn merge_takes_the_earliest_non_zero_send_and_accumulates_the_rest() {
        let mut stats = configured();
        stats.merge(&data(1_000, 2_000, 100, 200, 3, 300, 1));
        stats.merge(&data(500, 4_000, 50, 60, 2, 200, 2));
        assert_eq!(stats.data().min_send_time, 500);
        assert_eq!(stats.data().max_send_time, 4_000);
        assert_eq!(stats.duration_us(), 3_500);
        assert_eq!(stats.data().send_delta, 150);
        assert_eq!(stats.data().recv_delta, 260);
        assert_eq!(stats.data().acked_packets, 5);
        assert_eq!(stats.data().acked_bytes, 500);
        assert_eq!(stats.data().lost_packets, 3);
        assert_eq!(stats.data().lost_bytes, 300);
    }

    #[test]
    fn validity_floor_of_zero_admits_the_shortest_span() {
        // Oracle tr_zero: a 1us span at 2 packets per second clears the
        // threshold `exp(0) * 1pps`, and half the packets lost is a weighted
        // loss of `0.5 * log10(2e6)`.
        let mut config = CongestionDetectorConfig::default().weighted_loss;
        config.min_duration_for_loss_validity = PolicyDuration::ZERO;
        config.base_pps = 1;
        config.base_duration = PolicyDuration::ZERO;
        let mut stats = TrafficStats::new(config);
        stats.merge(&data(0, 1, 1, 1, 1, 100, 1));
        assert_eq!(stats.weighted_loss(), 3.150_514_997_831_990_6);
        assert_eq!(stats.loss_penalty(), 0);
        assert_eq!(stats.captured_traffic_ratio(), 1.0);
        assert_eq!(stats.acknowledged_bitrate(), 800_000_000);
    }
}
