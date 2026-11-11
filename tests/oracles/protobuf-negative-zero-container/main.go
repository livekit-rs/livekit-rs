// Actual Google protobuf wire oracle for the Rust FloatContainers encoder.
// Marshals real protoc-gen-go generated code (float_containers.proto) with the
// pinned google.golang.org/protobuf v1.36.12; no wire byte is hand-authored.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"

	"google.golang.org/protobuf/proto"
)

type vector struct {
	Name       string `json:"name"`
	SingleBits string `json:"single_bits"`
	DoubleBits string `json:"double_bits"`
	Optional   bool   `json:"optional"`
	Repeated   bool   `json:"repeated"`
	Map        bool   `json:"map"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: negzero-container-oracle CASES.json")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var vectors []vector
	if err := json.Unmarshal(raw, &vectors); err != nil {
		panic(err)
	}
	out := json.NewEncoder(os.Stdout)
	for _, v := range vectors {
		singleBits, err := strconv.ParseUint(v.SingleBits, 16, 32)
		if err != nil {
			panic(err)
		}
		doubleBits, err := strconv.ParseUint(v.DoubleBits, 16, 64)
		if err != nil {
			panic(err)
		}
		single := math.Float32frombits(uint32(singleBits))
		double := math.Float64frombits(doubleBits)

		m := &FloatContainers{Single: single, Double: double}
		if v.Optional {
			m.Optional = proto.Float64(double)
		}
		if v.Repeated {
			m.Repeated = []float32{single, 0}
		}
		if v.Map {
			m.Map = map[string]float64{"negative": double, "positive": 0}
			m.SingleMap = map[string]float32{"negative": single, "positive": 0}
		}

		// The Rust consumer stores maps in a BTreeMap, i.e. sorted by key, so the
		// comparable Go operand is the deterministic marshal. Go's default marshal
		// leaves map iteration order unspecified; that is probed below and recorded
		// rather than assumed away.
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		if err != nil {
			panic(err)
		}
		stable := true
		for i := 0; i < 8; i++ {
			other, err := proto.Marshal(m)
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(other, wire) {
				stable = false
			}
		}
		row := map[string]any{
			"name":                        v.Name,
			"single_bits":                 v.SingleBits,
			"double_bits":                 v.DoubleBits,
			"optional":                    v.Optional,
			"repeated":                    v.Repeated,
			"map":                         v.Map,
			"wire_base64":                 base64.StdEncoding.EncodeToString(wire),
			"wire_len":                    len(wire),
			"default_marshal_order_stable": stable,
		}
		if err := out.Encode(row); err != nil {
			panic(err)
		}
	}
}