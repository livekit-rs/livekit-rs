// SPDX-License-Identifier: Apache-2.0
// Supplemental typed observer over the original fixed generated Node decoder.
// Emits the same admission/known_json projection as tests/oracles/list-nodes-wire
// plus the two typed failure flags cli_nodes.rs:967-977 additionally requires.
package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// The pinned protobuf v1.36.12 exposes the invalid-UTF-8 family as the unexported
// errInvalidUTF8, which carries an exported InvalidUTF8() bool method
// (internal/impl/codec_field.go:17-21). The family is therefore probed by
// interface, not by error text. proto.Error is the exported sentinel re-exported
// at proto/proto.go:32-35 from internal/errors.Error.
type invalidUTF8 interface{ InvalidUTF8() bool }

func main() {
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, 4096), 1<<20)
	out := json.NewEncoder(os.Stdout)
	for s.Scan() {
		var c struct {
			ID  string `json:"id"`
			Hex string `json:"hex"`
		}
		if e := json.Unmarshal(s.Bytes(), &c); e != nil {
			panic(e)
		}
		raw, e := hex.DecodeString(c.Hex)
		if e != nil {
			panic(e)
		}
		var node livekit.Node
		row := map[string]any{"id": c.ID}
		if e = proto.Unmarshal(raw, &node); e != nil {
			var typed invalidUTF8
			row["accepted"] = false
			row["error"] = e.Error()
			row["invalid_utf8"] = errors.As(e, &typed) && typed.InvalidUTF8()
			row["error_is_proto"] = errors.Is(e, proto.Error)
		} else {
			row["accepted"] = true
			b, e := protojson.Marshal(&node)
			if e != nil {
				panic(e)
			}
			row["known_json"] = json.RawMessage(b)
		}
		if e := out.Encode(row); e != nil {
			panic(e)
		}
	}
	if e := s.Err(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}