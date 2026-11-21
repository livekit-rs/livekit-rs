package main

// Offline fixed-Pion observer for the RTP padding admission boundary.
//
// Scope: the parser decision that the Go publish path makes before any track,
// TWCC registration or publication exists. pion/webrtc warp.6
// rtptransceiver.go handleUnknownRTPPacket() runs rtp.Packet.Unmarshal() on the
// first packet of an unknown SSRC and returns its error, so a rejection there
// means no receiver, no RID/mid metadata and no media. livekit-server
// pkg/sfu/buffer/buffer.go Buffer.Write() re-runs the same full Unmarshal on
// every subsequent packet of a started stream.
//
// Both senders refuse to serialise malformed padding (pion/rtp Packet.MarshalTo
// returns errInvalidRTPPadding for a set padding bit with a zero pad count and
// always writes a terminal byte equal to its padding size; str0m
// RtpHeader::create_padding_packet refuses pad_len 0), so these vectors are
// crafted bytes, not packets produced by either side.
//
// Each input line is {"case":...,"bytes_hex":...}. Each output line is
// SOURCE_PION_RTP_PADDING {json} with the actual Pion result. Nothing is
// hardcoded here: the frozen expectations live in cases.jsonl and are compared
// by run.py, so a Pion or vector change fails loudly.

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pion/rtp"
)

type vector struct {
	Case     string `json:"case"`
	BytesHex string `json:"bytes_hex"`
}

type observation struct {
	Case        string `json:"case"`
	BytesHex    string `json:"bytes_hex"`
	Accepted    bool   `json:"accepted"`
	ErrorText   string `json:"error_text"`
	PaddingBit  bool   `json:"padding_bit"`
	PaddingSize int    `json:"padding_size"`
	PayloadHex  string `json:"payload_hex"`
	PaddingOnly bool   `json:"padding_only"`
}

func observe(v vector) observation {
	buf, err := hex.DecodeString(v.BytesHex)
	if err != nil {
		panic(fmt.Sprintf("vector %s is not hex: %v", v.Case, err))
	}
	packet := &rtp.Packet{}
	unmarshalErr := packet.Unmarshal(buf)
	out := observation{
		Case:        v.Case,
		BytesHex:    v.BytesHex,
		Accepted:    unmarshalErr == nil,
		PaddingBit:  packet.Padding,
		PaddingSize: int(packet.PaddingSize),
		PayloadHex:  hex.EncodeToString(packet.Payload),
	}
	if unmarshalErr != nil {
		out.ErrorText = unmarshalErr.Error()
	}
	// Actual upstream classification, warp.6 rtptransceiver.go:
	// isPaddingOnlyPacket := rp.Padding && len(rp.Payload) == 0
	// handleUnknownRTPPacket returns (..., false, err) before reaching it, so a
	// rejected buffer is never classified as padding-only however its partial
	// header fields read.
	out.PaddingOnly = out.Accepted && packet.Padding && len(packet.Payload) == 0
	return out
}

func main() {
	seen := map[string]bool{}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var v vector
		if err := json.Unmarshal(line, &v); err != nil {
			panic(fmt.Sprintf("bad input line %q: %v", string(line), err))
		}
		if v.Case == "" || seen[v.Case] {
			panic(fmt.Sprintf("case name empty or repeated: %q", v.Case))
		}
		seen[v.Case] = true
		encoded, err := json.Marshal(observe(v))
		if err != nil {
			panic(err)
		}
		fmt.Printf("SOURCE_PION_RTP_PADDING %s\n", encoded)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
