package main

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/livekit"
)

// Independent model: integer state transitions, literal wire MIME identities,
// and explicit layer masks; does not call the observer's payload conversion.
func expectedEvents(n int) []event {
	names := []string{"video/VP8", "video/VP9", "video/H264"}[:n]
	state := []int{3, 3, 3}[:n]
	masks := map[int][3]bool{0: {true, false, false}, 1: {true, true, false}, 2: {true, true, true}, 3: {false, false, false}}
	var events []event
	for _, q := range []int{2, 1, 0, 3, 2} {
		for changed := range names {
			state[changed] = q
			e := event{Codecs: make([]codec, 0, n)}
			for i, name := range names {
				v := codec{Codec: name, Qualities: make([]quality, 3)}
				for layer, enabled := range masks[state[i]] {
					v.Qualities[layer] = quality{Quality: layer, Enabled: enabled}
				}
				e.Codecs = append(e.Codecs, v)
			}
			sort.Slice(e.Codecs, func(i, j int) bool { return e.Codecs[i].Codec < e.Codecs[j].Codec })
			events = append(events, e)
		}
	}
	return events
}

func TestIndependentEnqueueSnapshots(t *testing.T) {
	for n := 1; n <= 3; n++ {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("codecs=%d/batch=%v", n, batch), func(t *testing.T) {
				checks := 0
				r, err := runCase(n, batch, func(c *capture) error {
					checks++
					select {
					case <-c.entered:
					default:
						return fmt.Errorf("not entered")
					}
					select {
					case <-c.release:
						return fmt.Errorf("prematurely released")
					default:
					}
					if c.copies.Load() != int32(n) || len(c.events) != 0 {
						return fmt.Errorf("measured listener copied before release")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if (batch && checks != 1) || (!batch && checks != 0) {
					t.Fatalf("blocked Close-return checkpoint count=%d", checks)
				}
				want := expectedEvents(n)
				if len(r.Events) != 5*n || !reflect.DeepEqual(r.Events, want) {
					t.Fatalf("independent snapshots mismatch\ngot:  %+v\nwant: %+v", r.Events, want)
				}
				// A later snapshot must not alias an earlier event's nested slice.
				last := len(r.Events) - 1
				r.Events[last].Codecs[0].Qualities[0].Enabled = false
				if !reflect.DeepEqual(r.Events[0], want[0]) {
					t.Fatal("observer retained shared nested snapshot storage")
				}
			})
		}
	}
}

func TestListenerBlocksBeforeReadingPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), caseDeadline)
	defer cancel()
	c := newCapture(ctx)
	defer c.unblock()
	c.gate.Store(true)
	payload := []*livekit.SubscribedCodec{{Codec: "before", Qualities: []*livekit.SubscribedQuality{{Quality: livekit.VideoQuality_LOW, Enabled: false}}}}
	done := make(chan struct{})
	go func() {
		c.OnDynacastSubscribedMaxQualityChange(payload, nil)
		close(done)
	}()
	select {
	case <-c.entered:
	case <-ctx.Done():
		t.Fatal("listener did not enter")
	}
	if c.copies.Load() != 0 || len(c.events) != 0 {
		t.Fatal("payload read/copied before handshake release")
	}
	// Mutation is synchronized by entered/release. If the observer copied before
	// entering, it would return stale fields and this test would fail.
	payload[0].Codec = "after"
	payload[0].Qualities[0].Enabled = true
	c.unblock()
	d, err := c.next()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("listener did not finish")
	}
	if d.event.Codecs[0].Codec != "after" || !d.event.Codecs[0].Qualities[0].Enabled {
		t.Fatal("listener copied before blocking")
	}
	payload[0].Qualities[0].Enabled = false
	if !d.event.Codecs[0].Qualities[0].Enabled {
		t.Fatal("observer did not deep-copy callback payload")
	}
}

func TestOriginalQueueFlushAndNonWaitingStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), caseDeadline)
	defer cancel()
	queue := utils.NewOpsQueue(utils.OpsQueueParams{Name: "c55-control", MinSize: 64, FlushOnStop: true})
	entered, release := make(chan struct{}), make(chan struct{})
	var order []int // read only after original Stop's returned channel closes
	queue.Start()
	queue.Enqueue(func() {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		order = append(order, 1)
	})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("queue did not enter")
	}
	queue.Enqueue(func() { order = append(order, 2) })
	done := queue.Stop() // real API, not assumed to be exposed by the manager
	select {
	case <-done:
		t.Fatal("queue completed while callback blocked")
	default:
	}
	queue.Enqueue(func() { order = append(order, 99) }) // stopped enqueue drops
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("queue did not flush")
	}
	if !reflect.DeepEqual(order, []int{1, 2}) {
		t.Fatalf("original FIFO stop semantics changed: %v", order)
	}
}

func TestRejectInvalidCodecCounts(t *testing.T) {
	for _, n := range []int{0, 4} {
		if _, err := runCase(n, false, nil); err == nil {
			t.Fatalf("accepted %d codecs", n)
		}
	}
}
