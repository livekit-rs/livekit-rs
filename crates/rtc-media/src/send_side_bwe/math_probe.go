// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Emit the bit-exact answers of the pinned Go toolchain's math.Log, math.Log10
// and math.Exp for a fixed input table. The Rust port in traffic.rs is verified
// against this table, and math_census.py reports how far the host libm sits from
// it. Inputs and outputs are hex float64 words so no consumer has to agree on
// decimal parsing.
//
// Run with the pinned toolchain, no network and no module cache writes:
//
//	GOTOOLCHAIN=local GOFLAGS=-mod=mod GOPROXY=off \
//	  /Users/zoujiaqing/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run math_probe.go > math_probe.txt
package main

import (
	"fmt"
	"math"
)

func emit(kind string, x float64) {
	var y float64
	switch kind {
	case "log":
		y = math.Log(x)
	case "log10":
		y = math.Log10(x)
	case "exp":
		y = math.Exp(x)
	default:
		panic(kind)
	}
	fmt.Printf("%s %016x %016x\n", kind, math.Float64bits(x), math.Float64bits(y))
}

func main() {
	// The rates the pinned campaign actually feeds through WeightedLoss, plus
	// the upstream comment's own examples and the decay spans of the threshold.
	for _, x := range []float64{
		math.Inf(1), 1e6, 300000.0, 1000.0, 100.0, 20.0,
		6e6 / 175000.0, 3.0, 1e6 / 100000.0, 1.0, 0.5, 0.0, -1.0,
	} {
		emit("log", x)
		emit("log10", x)
	}
	for i := 0; i < 512; i++ {
		x := float64(i*i + 3)
		emit("log", x)
		emit("log10", x)
	}
	for _, x := range []float64{0.0, -0.45, -1e-9, 1e-9, -4.5, -20.0} {
		emit("exp", x)
	}
	for i := 0; i < 512; i++ {
		emit("exp", -float64(i)/128.0)
	}
}
