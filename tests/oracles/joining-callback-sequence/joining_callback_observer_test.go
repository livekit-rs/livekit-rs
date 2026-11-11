// SPDX-License-Identifier: Apache-2.0
//
// Real original PCTransport connection-state bookkeeping observer.
//
// Seam: the original in-package constructor NewPCTransport is used exactly as
// the original pkg/rtc/transport_test.go uses it (Config: &WebRTCConfig{},
// Handler: &transportfakes.FakeHandler{} — the upstream counterfeiter fake,
// whose CallCount methods count real Handler invocations). No vnet/DTLS pair
// is needed because no wire claim is made.
//
// Every decision runs original upstream code:
//   - connected stages invoke the real registered PC callback
//     t.onPeerConnectionStateChange(webrtc.PeerConnectionStateConnected)
//     (registered at transport.go:652; body at :884), which calls
//     clearConnTimer, setPeerConnectionConnectedAt (:827, true only the first
//     time — resetShortConn does NOT clear peerConnectionFirstConnectedAt),
//     Handler.OnInitialConnected (:891) and maybeNotifyFullyEstablished (:893).
//   - reset uses the real ResetShortConnOnICERestart setter (:2072) and the
//     real consumption expression resetShortConnOnICERestart.CompareAndSwap
//     (true, false) -> resetShortConn() exactly as transport.go:2970/3060.
//   - channel-open stages set the same opened flags under t.lock that the
//     real onDataChannel OnOpen handler sets (transport.go:953 reliableDCOpened,
//     :960 lossyDCOpened; the same fields are handed out as dcReady pointers by
//     CreateDataChannel at :1296/:1301), then call the real
//     maybeNotifyFullyEstablished (:998). No writer is fabricated because no
//     real detached DataChannel exists without a wire; the flags are the exact
//     state the Rust consumer's opened_data_kinds mirrors.
//   - eligible is the real isFullyEstablished() predicate (:1011) with
//     UseOneShotSignallingMode=false and firstOfferNoDataChannel=false, i.e.
//     (reliableDCOpened && lossyDCOpened) && !peerConnectionLastconnectedAt.IsZero().
//
// first/last observe the real peerConnectionFirstConnectedAt /
// peerConnectionLastconnectedAt timestamps. Nothing here re-implements an
// upstream predicate, latch or return value; no expected values are authored.
package rtc

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/pion/webrtc/v4"

	"github.com/livekit/livekit-server/pkg/rtc/transport/transportfakes"
	"github.com/livekit/protocol/livekit"
)

type joiningCallbackStageTable struct {
	Sequences []struct {
		ID     string   `json:"id"`
		Stages []string `json:"stages"`
	} `json:"sequences"`
}

func TestJoiningCallbackSequenceSource(t *testing.T) {
	tableBytes, err := os.ReadFile(os.Getenv("JOINING_CALLBACK_STAGES"))
	if err != nil {
		t.Fatal(err)
	}
	var table joiningCallbackStageTable
	if err := json.Unmarshal(tableBytes, &table); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(os.Getenv("JOINING_CALLBACK_GO_RESULTS"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := json.NewEncoder(f)

	rows := 0
	for _, seq := range table.Sequences {
		handler := &transportfakes.FakeHandler{}
		tr, err := NewPCTransport(TransportParams{
			Config:    &WebRTCConfig{},
			Handler:   handler,
			Transport: livekit.SignalTarget_PUBLISHER,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, stage := range seq.Stages {
			switch stage {
			case "constructor":
				// no action, mirroring the Rust consumer
			case "first-connected":
				tr.lock.Lock()
				tr.reliableDCOpened = true
				tr.lossyDCOpened = seq.ID == "initial-ready"
				tr.lock.Unlock()
				tr.onPeerConnectionStateChange(webrtc.PeerConnectionStateConnected)
			case "reset":
				tr.ResetShortConnOnICERestart()
				if !tr.resetShortConnOnICERestart.CompareAndSwap(true, false) {
					t.Fatal("original reset flag was not consumed")
				}
				tr.resetShortConn()
			case "late-channel-open":
				tr.lock.Lock()
				tr.lossyDCOpened = true
				tr.lock.Unlock()
				tr.maybeNotifyFullyEstablished()
			case "next-channel-open":
				tr.maybeNotifyFullyEstablished()
			case "again-connected", "repeat-connected":
				tr.onPeerConnectionStateChange(webrtc.PeerConnectionStateConnected)
			default:
				t.Fatalf("unknown stage %q", stage)
			}

			tr.lock.RLock()
			row := map[string]any{
				"id":                seq.ID,
				"stage":             stage,
				"first":             !tr.peerConnectionFirstConnectedAt.IsZero(),
				"last":              !tr.peerConnectionLastconnectedAt.IsZero(),
				"reliable_opened":   tr.reliableDCOpened,
				"lossy_opened":      tr.lossyDCOpened,
				"initial_callbacks": handler.OnInitialConnectedCallCount(),
				"fully_callbacks":   handler.OnFullyEstablishedCallCount(),
			}
			tr.lock.RUnlock()
			row["eligible"] = tr.isFullyEstablished()
			if err := out.Encode(row); err != nil {
				t.Fatal(err)
			}
			rows++
		}
		tr.Close()
	}
	if rows != 18 {
		t.Fatalf("expected 18 ordered rows, got %d", rows)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}