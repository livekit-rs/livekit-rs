// Copyright 2024 LiveKit, Inc. SPDX-License-Identifier: Apache-2.0
// Calls the actual exported fixed upstream rtc.ChunkProtoBatch.
// Run this external file from references/livekit-stable; do not modify it.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	officialrtc "github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"os"
	"strconv"
	"strings"
)

const expectedSourceSHA = "b8bd3cf807a7202b969072a9da7b12c46824260a5db6b9d2537ed6d372992353"

func main() {
	sourcePath := flag.String("source", "", "required pinned pkg/rtc/utils.go path")
	flag.Parse()
	if strconv.IntSize != 64 {
		panic("fixed source oracle requires 64-bit Go int")
	}
	source, err := os.ReadFile(*sourcePath)
	if err != nil {
		panic(err)
	}
	text := string(source)
	start := strings.Index(text, "func ChunkProtoBatch[")
	if start < 0 {
		panic("fixed source function missing")
	}
	end := strings.Index(text[start:], "\n}\n")
	if end < 0 {
		panic("fixed source function end missing")
	}
	sum := sha256.Sum256([]byte(text[start : start+end+3]))
	sourceSHA := hex.EncodeToString(sum[:])
	if sourceSHA != expectedSourceSHA {
		panic("source oracle function differs from fixed input")
	}
	var input struct {
		Participants []json.RawMessage `json:"participants"`
		Targets      []int64           `json:"targets"`
	}
	if err = json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		panic(err)
	}
	participants := make([]*livekit.ParticipantInfo, 0, len(input.Participants))
	sizes := make([]int, 0, len(input.Participants))
	for _, raw := range input.Participants {
		p := &livekit.ParticipantInfo{}
		if err = protojson.Unmarshal(raw, p); err != nil {
			panic(err)
		}
		participants = append(participants, p)
		sizes = append(sizes, proto.Size(p))
	}
	type row struct {
		Target int64      `json:"target"`
		Chunks [][]string `json:"chunks"`
	}
	rows := make([]row, 0, len(input.Targets))
	for _, target := range input.Targets {
		r := row{Target: target}
		for _, chunk := range officialrtc.ChunkProtoBatch(participants, int(target)) {
			ids := make([]string, 0, len(chunk))
			for _, p := range chunk {
				ids = append(ids, p.Identity)
			}
			r.Chunks = append(r.Chunks, ids)
		}
		rows = append(rows, r)
	}
	result := map[string]any{"source_function_sha256": sourceSHA, "observer": "actual_fixed_upstream_rtc.ChunkProtoBatch", "participant_proto_sizes": sizes, "cases": rows}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(fmt.Sprintf("output: %v", err))
	}
}
