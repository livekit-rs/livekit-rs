package main

import (
	"fmt"
	"testing"

	"github.com/livekit/protocol/livekit"
)

func TestFixedGoMatrix(t *testing.T) {
	cases := scenarios()
	if len(cases) != 90 {
		t.Fatalf("case count: %d", len(cases))
	}
	// Expectations belong only in assertions, never the stdout observer.
	holds := [3][3]int32{{0, 0, 2}, {1, 1, 2}, {2, 2, 2}}
	stages := []string{"constructed", "before_first_packet", "first_packet", "set_max", "report", "resync", "resync_again", "muted", "unmuted", "pub_muted", "pub_unmuted", "matching_zero_sr", "matching_sr_resync", "restart", "restart_packet", "restart_resync", "before_fresh", "fresh", "fresh_packet", "fresh_resync"}
	for id, s := range cases {
		t.Run(fmt.Sprintf("%02d-%s-r%d-max%d-%s", id, s.CodecMode, s.Reference, s.MaxSpatial, s.Report), func(t *testing.T) {
			r, err := run(id, s)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Observations) != len(stages) {
				t.Fatalf("observations: %d", len(r.Observations))
			}
			maximumIndex := map[int32]int{-1: 0, 0: 1, 2: 2}[s.MaxSpatial]
			hold := holds[s.Reference][maximumIndex]
			afterReport := hold
			if s.Report == "matching-zero" || s.Report == "matching-valid" || s.Report == "premature-zero" {
				afterReport = s.MaxSpatial
			}
			wantMax := []int32{-1, 2, 2, hold, hold, afterReport, afterReport, -1, afterReport, afterReport, afterReport, s.MaxSpatial, s.MaxSpatial, s.MaxSpatial, hold, hold, s.MaxSpatial, s.MaxSpatial, hold, hold}
			for i, o := range r.Observations {
				if o.Stage != stages[i] {
					t.Fatalf("stage[%d]: %s", i, o.Stage)
				}
				if o.MaxSubscribedSpatial != wantMax[i] {
					t.Errorf("%s max subscribed = %d, want %d", o.Stage, o.MaxSubscribedSpatial, wantMax[i])
				}
				wantReference := s.Reference
				if i == 13 {
					wantReference = -1
				}
				wantStarted := i != 0 && i != 1 && i != 17
				if o.Started != wantStarted {
					t.Errorf("%s started = %v", o.Stage, o.Started)
				}
				if wantStarted {
					if o.ReferenceSpatial == nil || *o.ReferenceSpatial != wantReference {
						t.Errorf("%s reference = %v, want %d", o.Stage, o.ReferenceSpatial, wantReference)
					}
				} else if o.ReferenceSpatial != nil {
					t.Errorf("%s unexpected state", o.Stage)
				}
				wantCurrent, wantTarget := int32(-1), s.Reference
				if i == 2 || i == 3 || i == 4 || i == 14 || i == 18 {
					wantCurrent = s.Reference
				}
				if i == 0 || i == 1 || i == 13 || i == 17 {
					wantTarget = -1
				}
				if o.CurrentSpatial != wantCurrent || o.TargetSpatial != wantTarget {
					t.Errorf("%s current/target = %d/%d, want %d/%d", o.Stage, o.CurrentSpatial, o.TargetSpatial, wantCurrent, wantTarget)
				}
				wantMaximum := s.MaxSpatial
				if i == 0 {
					wantMaximum = -1
				}
				if i == 1 || i == 2 {
					wantMaximum = 2
				}
				if o.MaxSpatial != wantMaximum {
					t.Errorf("%s max = %d, want %d", o.Stage, o.MaxSpatial, wantMaximum)
				}
				if o.Muted != (i == 7) || o.PubMuted != (i == 9) {
					t.Errorf("%s mute flags = %v/%v", o.Stage, o.Muted, o.PubMuted)
				}
			}
		})
	}
}

func TestSVCReferenceIsNotSRBuffer(t *testing.T) {
	f := newForwarder("vp9-svc")
	f.SetMaxSpatialLayer(2)
	if err := packet(f, "vp9-svc", 2, 1); err != nil {
		t.Fatal(err)
	}
	sr := &livekit.RTCPSenderReportState{NtpTimestamp: 0xe900000000000000, RtpTimestamp: 93000, RtpTimestampExt: 93000}
	f.SetRefSenderReport(0, sr)
	spatial, isSVC, _, actualSR := f.GetSenderReportParams()
	if spatial != 2 || !isSVC || actualSR != sr {
		t.Fatalf("SVC SR buffer 0 not selected: spatial=%d svc=%v sr=%p", spatial, isSVC, actualSR)
	}
	f.SetMaxSpatialLayer(-1)
	f.Resync()
	if got := f.GetMaxSubscribedSpatial(); got != 2 {
		t.Fatalf("SR buffer 0 must not release reference 2: %d", got)
	}
	f.SetRefSenderReport(2, &livekit.RTCPSenderReportState{})
	if got := f.GetMaxSubscribedSpatial(); got != -1 {
		t.Fatalf("non-nil SR at reference 2 should release hold: %d", got)
	}
}
