// Copyright LiveKit, Apache-2.0. Added observer; original algorithms unchanged.
package rtpstats

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/mediatransportutil"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/mono"
	"github.com/pion/rtcp"
	"testing"
	"time"
)

func TestOriginalActualSenderReportRpcRttSource(t *testing.T) {
	for _, name := range []string{"uninitialized", "no_sender_report", "zero_lsr", "mismatched_lsr", "anachronous", "valid_zero_dlsr", "valid_with_dlsr"} {
		t.Run(name, func(t *testing.T) {
			r := NewRTPStatsSender(RTPStatsParams{}, 128)
			r.SetClockRate(90000)
			r.SetLogger(logger.GetLogger())
			defer r.Stop()
			initialized := name != "uninitialized"
			if initialized {
				r.Update(mono.UnixNano(), 100, 3000, true, 12, 3, 0, false)
			}
			var sr *rtcp.SenderReport
			if name != "no_sender_report" && initialized {
				now := mono.UnixNano()
				sr = r.GetRtcpSenderReport(101, &livekit.RTCPSenderReportState{At: now, AtAdjusted: now, NtpTimestamp: uint64(mediatransportutil.ToNtpTime(time.Now())), RtpTimestampExt: 3000}, 0, true)
				if sr == nil {
					t.Fatal("original real sender-report producer did not start")
				}
			}
			rr := rtcp.ReceptionReport{SSRC: 101, LastSequenceNumber: 100}
			if sr != nil {
				rr.LastSenderReport = uint32(sr.NTPTime >> 16)
			}
			switch name {
			case "zero_lsr":
				rr.LastSenderReport = 0
			case "mismatched_lsr":
				rr.LastSenderReport++
			case "anachronous":
				rr.Delay = 0x7fffffff
			case "valid_zero_dlsr":
				time.Sleep(25 * time.Millisecond)
			case "valid_with_dlsr":
				time.Sleep(40 * time.Millisecond)
				rr.Delay = 655
			}
			before := time.Duration(0)
			if !r.rttMarker.sentAt.IsZero() {
				before = time.Since(r.rttMarker.sentAt)
			}
			value, changed := r.UpdateFromReceiverReport(rr)
			after := time.Duration(0)
			if !r.rttMarker.sentAt.IsZero() {
				after = time.Since(r.rttMarker.sentAt)
			}
			valid := name == "valid_zero_dlsr" || name == "valid_with_dlsr"
			if valid && (!changed || value == 0) {
				t.Fatalf("original valid source missing RTT: %d/%v", value, changed)
			}
			if !valid && (changed || value != 0) {
				t.Fatalf("original invalid source emitted RTT: %d/%v", value, changed)
			}
			row := map[string]any{"case": name, "initialized": initialized, "marker_ntp": uint64(r.rttMarker.ntpTime), "lsr": rr.LastSenderReport, "dlsr": rr.Delay, "before_ns": before.Nanoseconds(), "after_ns": after.Nanoseconds(), "rtt_ms": value, "changed": changed}
			b, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("SOURCE_RPC_RR %s\n", b)
		})
	}
}
