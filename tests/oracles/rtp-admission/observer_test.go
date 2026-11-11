// Copyright LiveKit, Apache-2.0. Observer calls unchanged locked production algorithms.
package rtpstats

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/protocol/logger"
	"testing"
)

type admissionInput struct {
	SN      uint16
	TS      uint32
	MS      int64
	Payload int
}

func TestOriginalRtpAdmissionSource(t *testing.T) {
	cases := map[string][]admissionInput{
		"timestamp_rejections":     {{100, 3000, 0, 3}, {100, 1, 10, 3}, {100, 3000, 20, 3}, {101, 9000, 33, 3}, {102, 6000, 40, 3}, {100, 12000, 50, 3}, {99, 9000, 60, 3}, {102, 9000, 70, 3}},
		"wrap":                     {{65534, 4294964295, 0, 3}, {65535, 4294966295, 20, 3}, {0, 1000, 40, 3}},
		"forced_sequence_rollover": {{30000, 100000, 0, 3}, {1000, 103000, 33, 3}, {1001, 106000, 66, 3}},
		"restart":                  {{1000, 90000, 0, 3}, {1001, 1000, 10, 3}, {1002, 2000, 20, 3}, {1003, 3000, 30, 3}, {1004, 4000, 40, 3}, {1005, 5000, 50, 3}, {1006, 8000, 60, 3}},
		"padding":                  {{100, 3000, 0, 0}, {100, 3000, 10, 3}, {101, 3000, 20, 0}},
		"long_timestamp_rollover":  {{100, 4000000000, 0, 3}, {101, 1000, 96000000, 3}},
	}
	for name, inputs := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewRTPStatsReceiver(RTPStatsParams{})
			r.SetClockRate(90000)
			r.SetLogger(logger.GetLogger())
			defer r.Stop()
			for i, p := range inputs {
				f := r.Update(1000000000+p.MS*1000000, p.SN, p.TS, true, 12, p.Payload, 0)
				row := map[string]any{"case": name, "index": i, "sn": p.SN, "ts": p.TS, "ms": p.MS, "payload": p.Payload, "reason": f.UnhandledReason.String(), "seq": f.ExtSequenceNumber, "timestamp": f.ExtTimestamp, "duplicate": f.IsDuplicate, "out_of_order": f.IsOutOfOrder, "highest": r.ExtendedHighestSequenceNumber()}
				raw, e := json.Marshal(row)
				if e != nil {
					t.Fatal(e)
				}
				fmt.Println("SOURCE_RTP_ADMISSION " + string(raw))
			}
		})
	}
}
