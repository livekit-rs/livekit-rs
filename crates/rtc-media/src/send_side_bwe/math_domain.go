// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Domain-anchored vectors for the ported transcendental functions.
//
// `math_probe.txt` is a census: it measures how often the platform libm and the
// pinned reference disagree. This program is the other half of the argument, a
// table built from the *definition domain* of the only Go caller this port has,
// `pkg/sfu/bwe/sendsidebwe/traffic_stats.go`, so that a port which merely fits
// the census cannot pass:
//
//   - the specials of each function (0, -0, subnormal, +/-Inf, NaN, and each
//     branch threshold with its next-after neighbour, which is where the branch
//     structure itself is decided),
//   - the packet-rate grid `pps = totalPackets*1e6/durationMicro` that
//     `WeightedLoss` hands to Log10, plus both sides of the gate
//     `pps < exp(-deltaDuration.Seconds())*basePPS`, because that comparison is
//     the one place where an ulp of Exp changes a control decision rather than
//     a mantissa,
//   - the arguments where Log10 changes sign (just under and just over 1),
//     which is what turns a loss penalty negative,
//   - the argument-reduction breakpoints of Exp, `(n -/+ 0.5)/Log2e`, where the
//     integer `k` and so the inlined `Ldexp` exponent move.
//
// Run with the pinned toolchain, exactly as captured:
//
//	GOTOOLCHAIN=local GOFLAGS=-mod=mod GOPROXY=off \
//	  /Users/zoujiaqing/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go \
//	  run math_domain.go > math_domain.txt
//
// `-sweep` appends a second, deterministic random block (fixed seed, so the
// rows are reproducible word for word) covering 120 000 rows spread over the
// same functions: log-uniform 2^-80..2^40 pairs for `log`/`log10`, a linear
// 0..1e7 sweep of the reachable packet rates, `exp` across the whole finite
// argument range, `exp` near zero, and the `-deltaDuration/1e9` arguments the
// rate gate actually sees. The table is deliberately not committed at that
// size; verify the port against it with the pinned Go and the same Rust port:
//
//	GOTOOLCHAIN=local GOFLAGS=-mod=mod GOPROXY=off <go> run math_domain.go -sweep > /tmp/math_sweep.txt
//	C73_MATH_TABLE=/tmp/math_sweep.txt C73_MATH_ROWS=124832 \
//	  cargo test -p rtc-media --lib send_side_bwe::traffic::tests::the_ported_transcendentals_hold_over_the_whole_operating_domain
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
)

// sweepSeed is fixed, not wall-clock, because a random sweep whose rows move
// between runs cannot be bound to an evidence identity.
const sweepSeed = 20261121

func emit(w *bufio.Writer, kind string, x float64) {
	var y float64
	switch kind {
	case "log":
		y = math.Log(x)
	case "log10":
		y = math.Log10(x)
	case "exp":
		y = math.Exp(x)
	default:
		panic("unknown kind " + kind)
	}
	fmt.Fprintf(w, "%s %016x %016x\n", kind, math.Float64bits(x), math.Float64bits(y))
}

func main() {
	sweep := flag.Bool("sweep", false, "append the deterministic random sweep")
	flag.Parse()

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	// ---- specials of the positive-domain pair ----
	for _, x := range []float64{
		0.0, math.Copysign(0, -1), math.SmallestNonzeroFloat64,
		-math.SmallestNonzeroFloat64, -1.0, 0.5, 1.0, 2.0, 10.0, 100.0, 1e6, 1e308,
		math.Nextafter(1.0, 0.0), math.Nextafter(1.0, 2.0),
		math.Sqrt2, math.Sqrt2 / 2,
		math.Nextafter(math.Sqrt2/2, 0.0), math.Nextafter(math.Sqrt2/2, 2.0),
		math.Inf(1), math.Inf(-1), math.NaN(),
	} {
		emit(w, "log", x)
		emit(w, "log10", x)
	}

	// ---- the packet-rate grid WeightedLoss evaluates, per configuration ----
	durations := []int64{1, 50_000, 100_000, 150_000, 200_000, 500_000, 1_000_000, 5_000_000, 20_000_000}
	for _, total := range []int64{0, 1, 2, 3, 5, 10, 100, 500, 1000, 3000, 100000} {
		for _, dur := range durations {
			emit(w, "log10", float64(total)*1e6/float64(dur))
		}
	}

	// ---- both sides of the rate gate, for the frozen defaults and the
	// degenerate zero configuration ----
	type lossCfg struct {
		basePPS   int64
		baseDurNs int64
	}
	for _, c := range []lossCfg{{30, 500_000_000}, {30, 0}, {1, 0}, {0, 0}, {30, 2_000_000_000}} {
		for _, dur := range durations {
			delta := dur*1_000 - c.baseDurNs
			if delta < 0 {
				delta = 0
			}
			arg := -float64(delta) / 1e9
			emit(w, "exp", arg)
			threshold := math.Exp(arg) * float64(c.basePPS)
			emit(w, "log10", threshold)
			emit(w, "log10", math.Nextafter(threshold, math.Inf(1)))
			emit(w, "log10", math.Nextafter(threshold, math.Inf(-1)))
		}
	}

	// ---- the Log10 sign change, which is a negative loss penalty ----
	for _, x := range []float64{0.9, 0.99, 0.999, 0.999999999999, 1.000000000001, 1.001, 1.01, 1.1} {
		emit(w, "log", x)
		emit(w, "log10", x)
	}

	// ---- dense sweep over the reachable packet rates, 1 to 1e7 ----
	for i := 0; i < 1024; i++ {
		x := math.Exp(float64(i) / 1024.0 * 16.118096)
		emit(w, "log10", x)
		emit(w, "log", x)
	}

	// ---- Exp specials, including every branch threshold and its neighbours ----
	nearZero := 1.0 / (1 << 28)
	overflow := 7.09782712893383973096e+02
	underflow := -7.45133219101941108420e+02
	for _, x := range []float64{
		0.0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
		nearZero, -nearZero,
		math.Nextafter(nearZero, 0.0), math.Nextafter(nearZero, 1.0),
		math.Nextafter(-nearZero, 0.0), math.Nextafter(-nearZero, -1.0),
		overflow, math.Nextafter(overflow, 0.0), math.Nextafter(overflow, 1e6),
		underflow, math.Nextafter(underflow, 0.0), math.Nextafter(underflow, -1e6),
		math.Inf(1), math.Inf(-1), math.NaN(),
	} {
		emit(w, "exp", x)
	}

	// ---- argument-reduction breakpoints ----
	const log2e = 1.44269504088896338700e+00
	for n := -64; n <= 0; n++ {
		kd := float64(n)
		for _, x := range []float64{
			(kd - 0.5) / log2e, (kd + 0.5) / log2e,
			math.Nextafter((kd-0.5)/log2e, 1e6), math.Nextafter((kd+0.5)/log2e, -1e6),
		} {
			emit(w, "exp", x)
		}
	}
	for n := -1075; n < -64; n += 17 {
		kd := float64(n)
		emit(w, "exp", (kd-0.5)/log2e)
		emit(w, "exp", (kd+0.5)/log2e)
	}

	// ---- dense sweep of the decay band, and the whole normal range below it ----
	for i := 0; i < 1024; i++ {
		emit(w, "exp", -float64(i)*1e-3)
	}
	for i := 0; i < 512; i++ {
		emit(w, "exp", -float64(i)/1e9)
	}
	for i := 0; i < 512; i++ {
		emit(w, "exp", -float64(i*i+3))
	}

	if *sweep {
		randomSweep(w)
	}
}

// randomSweep is the breadth half of the argument: the domain blocks above are
// chosen, so a port could in principle be tuned to them. These rows come from a
// seeded stream instead, so they are not chosen and cannot be fitted, and the
// seed is fixed so the row count and the words stay reproducible for an
// evidence identity.
func randomSweep(w *bufio.Writer) {
	r := rand.New(rand.NewSource(sweepSeed))

	// log/log10 over the packet-rate range the detector can produce, spread
	// log-uniformly because `pps` is a ratio of counts, then linearly in the
	// low band where the loss ratio multiplies the result.
	for i := 0; i < 30_000; i++ {
		x := math.Pow(2, float64(r.Intn(120)-80)) * (1 + r.Float64())
		emit(w, "log", x)
		emit(w, "log10", x)
	}
	for i := 0; i < 10_000; i++ {
		x := r.Float64() * 1e7
		emit(w, "log", x)
		emit(w, "log10", x)
	}

	// exp over the whole finite argument range, the near-zero band the gate
	// degenerates to, and the `-deltaDuration/1e9` arguments it really sees.
	for i := 0; i < 20_000; i++ {
		emit(w, "exp", r.Float64()*1454-745)
	}
	for i := 0; i < 10_000; i++ {
		emit(w, "exp", (r.Float64()-1)*0.02)
	}
	for i := 0; i < 10_000; i++ {
		emit(w, "exp", -float64(r.Intn(5_000_001))/1e9*400)
	}
}
