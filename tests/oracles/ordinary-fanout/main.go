// SPDX-License-Identifier: Apache-2.0
//
// NEWLY AUTHORED Go oracle producer for ORDINARY_FANOUT_RESULTS.
//
// This drives the REAL exported upstream fanout entry point
// rtc.BroadcastDataPacketForRoom (pkg/rtc/room.go:2119), which is the exact
// function Room.SendDataPacket -> Room.onDataMessage reaches. It therefore
// exercises, unchanged:
//   - the single proto.Marshal(dp) whose buffer is shared by every recipient,
//   - the unexported dataForwardLoadBalanceThreshold = 4 constant,
//   - utils.ParallelExec (protocol-stable/utils/parallel.go:54) including its
//     serial path and its runtime.NumCPU() clamp,
//   - the real op.SendDataMessage(kind, dpData, ParticipantID(dp.GetParticipantSid()),
//     dp.GetSequence()) argument tuple.
//
// Only the recipients and the room inventory are fakes (pinned upstream
// typesfakes), which is what the Rust consumer's ignore reason scopes:
// "admission scheduling component, not wire proof".
//
// maximum_active is MEASURED, not computed: every recipient stub blocks on a
// release channel, so the observed peak of concurrently-inside-stub callers
// equals the number of workers ParallelExec actually spawned. The producer
// waits for that peak to stop growing, samples whether ParallelExec already
// returned (returned_while_blocked), then releases.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"google.golang.org/protobuf/proto"
)

type fanoutCall struct {
	Kind     int    `json:"kind"`
	Sender   string `json:"sender"`
	Sequence uint32 `json:"sequence"`
	WireHex  string `json:"wire_hex"`
}

type fanoutRow struct {
	Participants         int                   `json:"participants"`
	RuntimeCPUs          int                   `json:"runtime_cpus"`
	ReturnedWhileBlocked bool                  `json:"returned_while_blocked"`
	MaximumActive        int                   `json:"maximum_active"`
	Calls                map[string]fanoutCall `json:"calls"`

	// Extra transparency keys (the Rust consumer reads by key only).
	GoMaxProcs          int      `json:"gomaxprocs"`
	WireBytes           int      `json:"wire_bytes"`
	DistinctWireHex     int      `json:"distinct_wire_hex"`
	ArrivalOrder        []string `json:"arrival_order"`
	PeakStableMillis    int      `json:"peak_stable_millis"`
	AllWorkersReturned  bool     `json:"all_workers_returned"`
	RoundTripProtoEqual bool     `json:"round_trip_proto_equal"`
}

const (
	stableSamples   = 150 // 150 x 1ms with no peak growth => workers all parked
	stablePollDelay = time.Millisecond
	observeTimeout  = 30 * time.Second
)

func observe(n int, payload []byte) fanoutRow {
	room := &typesfakes.FakeRoom{}
	room.IsDataMessageUserPacketDuplicateReturns(false)

	var mu sync.Mutex
	active, peak := 0, 0
	calls := make(map[string]fanoutCall, n)
	wires := make([][]byte, 0, n)
	arrival := make([]string, 0, n)
	release := make(chan struct{})

	participants := make([]types.LocalParticipant, 0, n)
	fakes := make([]*typesfakes.FakeLocalParticipant, 0, n)
	for i := 0; i < n; i++ {
		sid := fmt.Sprintf("PA_fanout_n%d_%02d", n, i)
		f := &typesfakes.FakeLocalParticipant{}
		f.IDReturns(livekit.ParticipantID(sid))
		f.IdentityReturns(livekit.ParticipantIdentity("identity-" + sid))
		f.SendDataMessageStub = func(kind livekit.DataPacket_Kind, data []byte, sender livekit.ParticipantID, seq uint32) error {
			mu.Lock()
			if _, dup := calls[sid]; dup {
				mu.Unlock()
				panic("recipient " + sid + " called more than once")
			}
			calls[sid] = fanoutCall{
				Kind:     int(kind),
				Sender:   string(sender),
				Sequence: seq,
				WireHex:  hex.EncodeToString(data),
			}
			wires = append(wires, append([]byte(nil), data...))
			arrival = append(arrival, sid)
			active++
			if active > peak {
				peak = active
			}
			mu.Unlock()

			<-release

			mu.Lock()
			active--
			mu.Unlock()
			return nil
		}
		fakes = append(fakes, f)
		participants = append(participants, f)
	}
	room.GetLocalParticipantsReturns(participants)

	dp := &livekit.DataPacket{
		Kind: livekit.DataPacket_RELIABLE,
		Value: &livekit.DataPacket_User{
			User: &livekit.UserPacket{Payload: append([]byte(nil), payload...)},
		},
	}

	done := make(chan struct{})
	go func() {
		rtc.BroadcastDataPacketForRoom(room, nil, livekit.DataPacket_RELIABLE, dp, logger.GetLogger())
		close(done)
	}()

	// Wait until the concurrency peak stops growing: every spawned worker is
	// parked inside the stub, so the peak is the real worker count.
	stable, last := 0, -1
	deadline := time.Now().Add(observeTimeout)
	for {
		mu.Lock()
		cur := peak
		mu.Unlock()
		if cur == last && cur > 0 {
			stable++
		} else {
			stable, last = 0, cur
		}
		if stable >= stableSamples {
			break
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("n=%d: fanout never reached a stable peak (last=%d)", n, last))
		}
		time.Sleep(stablePollDelay)
	}

	mu.Lock()
	maximumActive := peak
	mu.Unlock()

	returnedWhileBlocked := false
	select {
	case <-done:
		returnedWhileBlocked = true
	default:
	}

	close(release)
	select {
	case <-done:
	case <-time.After(observeTimeout):
		panic(fmt.Sprintf("n=%d: ParallelExec did not finish after release", n))
	}

	mu.Lock()
	defer mu.Unlock()
	if active != 0 {
		panic(fmt.Sprintf("n=%d: %d workers still inside the stub", n, active))
	}
	if len(calls) != n {
		panic(fmt.Sprintf("n=%d: observed %d recipient calls", n, len(calls)))
	}
	totalCalls := 0
	for _, f := range fakes {
		totalCalls += f.SendDataMessageCallCount()
	}
	if totalCalls != n {
		panic(fmt.Sprintf("n=%d: fake call count %d", n, totalCalls))
	}

	// The upstream fanout marshals ONCE and shares the buffer: every recipient
	// must have received byte-identical wire data.
	distinct := map[string]struct{}{}
	for _, w := range wires {
		distinct[hex.EncodeToString(w)] = struct{}{}
	}
	if len(distinct) != 1 {
		panic(fmt.Sprintf("n=%d: recipients saw %d distinct wire buffers", n, len(distinct)))
	}
	roundTrip := true
	for _, w := range wires {
		got := &livekit.DataPacket{}
		if err := proto.Unmarshal(w, got); err != nil {
			panic(err)
		}
		if !proto.Equal(got, dp) {
			roundTrip = false
		}
	}

	return fanoutRow{
		Participants:         n,
		RuntimeCPUs:          runtime.NumCPU(),
		ReturnedWhileBlocked: returnedWhileBlocked,
		MaximumActive:        maximumActive,
		Calls:                calls,
		GoMaxProcs:           runtime.GOMAXPROCS(0),
		WireBytes:            len(wires[0]),
		DistinctWireHex:      len(distinct),
		ArrivalOrder:         arrival,
		PeakStableMillis:     stableSamples * int(stablePollDelay/time.Millisecond),
		AllWorkersReturned:   active == 0,
		RoundTripProtoEqual:  roundTrip,
	}
}

func main() {
	// Same real initialization the pinned pkg/rtc/room_test.go performs.
	if err := prometheus.Init("test", livekit.NodeType_SERVER); err != nil {
		panic(err)
	}
	loggingConf := config.LoggingConfig{PionLevel: config.DefaultConfig.Logging.PionLevel}
	config.InitLoggerFromConfig(&loggingConf)

	out := os.Getenv("ORDINARY_FANOUT_RESULTS")
	if out == "" {
		panic("ORDINARY_FANOUT_RESULTS must name a fresh output JSONL file")
	}
	f, err := os.OpenFile(out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	cpus := runtime.NumCPU()
	payload := []byte("ordinary-fanout-actual-go-payload")
	enc := json.NewEncoder(f)
	for _, n := range []int{3, 4, 2 * cpus} {
		row := observe(n, payload)
		if row.ReturnedWhileBlocked {
			panic(fmt.Sprintf("n=%d: ParallelExec returned while recipients were blocked", n))
		}
		if !row.RoundTripProtoEqual {
			panic(fmt.Sprintf("n=%d: wire bytes do not round-trip to the sent DataPacket", n))
		}
		if err := enc.Encode(row); err != nil {
			panic(err)
		}
		fmt.Fprintf(os.Stderr, "n=%d runtime_cpus=%d maximum_active=%d returned_while_blocked=%t wire_bytes=%d\n",
			row.Participants, row.RuntimeCPUs, row.MaximumActive, row.ReturnedWhileBlocked, row.WireBytes)
	}
}