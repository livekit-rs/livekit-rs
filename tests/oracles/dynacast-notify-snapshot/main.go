package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/livekit-server/pkg/rtc/dynacast"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
)

// This deadline is safety only, never an ordering/quiescence delay. It is below
// the original quality object's 10-second initial timer; every case closes first.
const caseDeadline = 3 * time.Second

var codecOrder = []mime.MimeType{mime.MimeTypeVP8, mime.MimeTypeVP9, mime.MimeTypeH264}
var rounds = []livekit.VideoQuality{livekit.VideoQuality_HIGH, livekit.VideoQuality_MEDIUM, livekit.VideoQuality_LOW, livekit.VideoQuality_OFF, livekit.VideoQuality_HIGH}

type quality struct {
	Quality int  `json:"quality"`
	Enabled bool `json:"enabled"`
}
type codec struct {
	Codec     string    `json:"codec"`
	Qualities []quality `json:"qualities"`
}
type event struct {
	Codecs []codec `json:"codecs"`
}
type row struct {
	CodecCount int     `json:"codec_count"`
	Batch      bool    `json:"batch"`
	Events     []event `json:"events"`
}
type delivery struct {
	event    event
	terminal bool
}

type capture struct {
	dynacast.DynacastManagerListenerNull
	ctx         context.Context
	gate        atomic.Bool
	copies      atomic.Int32
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	events      chan delivery
}

func newCapture(ctx context.Context) *capture {
	return &capture{ctx: ctx, entered: make(chan struct{}), release: make(chan struct{}), events: make(chan delivery, 64)}
}
func (c *capture) unblock() { c.releaseOnce.Do(func() { close(c.release) }) }
func (c *capture) OnDynacastSubscribedMaxQualityChange(payload []*livekit.SubscribedCodec, _ []types.SubscribedCodecQuality) {
	// Absolutely no payload inspection, iteration, or copy before this handshake.
	if c.gate.CompareAndSwap(true, false) {
		close(c.entered)
		select {
		case <-c.release:
		case <-c.ctx.Done():
			return
		}
	}
	d := delivery{event: event{Codecs: make([]codec, 0, len(payload))}}
	for _, p := range payload {
		v := codec{Codec: p.Codec, Qualities: make([]quality, 0, len(p.Qualities))}
		for _, q := range p.Qualities {
			v.Qualities = append(v.Qualities, quality{int(q.Quality), q.Enabled})
		}
		d.event.Codecs = append(d.event.Codecs, v)
		if p.Codec == mime.MimeTypeAV1.String() {
			d.terminal = true
		}
	}
	// Normalize map iteration order only; never rewrite codec strings or qualities.
	sort.Slice(d.event.Codecs, func(i, j int) bool { return d.event.Codecs[i].Codec < d.event.Codecs[j].Codec })
	c.copies.Add(1)
	c.events <- d
}
func (c *capture) next() (delivery, error) {
	select {
	case d := <-c.events:
		return d, nil
	case <-c.ctx.Done():
		return delivery{}, fmt.Errorf("callback deadline: %w", c.ctx.Err())
	}
}

func runCase(n int, batch bool, blockedCheck func(*capture) error) (result row, err error) {
	result = row{CodecCount: n, Batch: batch, Events: make([]event, 0, 5*n)}
	if n < 1 || n > len(codecOrder) {
		return result, fmt.Errorf("invalid codec count %d", n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), caseDeadline)
	defer cancel()
	c := newCapture(ctx)
	manager := dynacast.NewDynacastManagerVideo(dynacast.DynacastManagerVideoParams{DynacastPauseDelay: 0, Listener: c})
	defer c.unblock()
	defer manager.Close()
	notify := func(i int, q livekit.VideoQuality) {
		manager.NotifySubscriberMaxQuality(livekit.ParticipantID(fmt.Sprintf("s%d", i)), codecOrder[i], q)
	}
	// Wait for EVERY initial callback, not merely codec construction. Warmup is
	// deliberately excluded from measured output and from the blocked first call.
	for i := 0; i < n; i++ {
		notify(i, livekit.VideoQuality_OFF)
		d, e := c.next()
		if e != nil {
			return result, e
		}
		if d.terminal || len(d.event.Codecs) != i+1 {
			return result, fmt.Errorf("unexpected warmup %d: %+v", i, d)
		}
		for _, v := range d.event.Codecs {
			if len(v.Qualities) != 3 {
				return result, fmt.Errorf("warmup quality count")
			}
			for q, value := range v.Qualities {
				if value.Quality != q || value.Enabled {
					return result, fmt.Errorf("warmup not OFF")
				}
			}
		}
	}
	c.gate.Store(batch)
	for r, q := range rounds {
		for i := 0; i < n; i++ {
			notify(i, q)
			if batch {
				if r == 0 && i == 0 {
					select {
					case <-c.entered:
					case <-ctx.Done():
						return result, fmt.Errorf("first measured listener did not enter")
					}
				}
			} else {
				d, e := c.next()
				if e != nil {
					return result, e
				}
				if d.terminal {
					return result, fmt.Errorf("premature terminal callback")
				}
				result.Events = append(result.Events, d.event)
			}
		}
	}
	for i := 0; i < n; i++ {
		notify(i, livekit.VideoQuality_HIGH)
	}
	// A public-API FIFO barrier, identifiable without guessing an event count.
	// It follows all duplicate calls and is not part of the measured trace.
	// The queue has no exposed manager wait API. Its original FlushOnStop=true
	// guarantees delivery through this last operation after Close, which returns
	// without waiting. No producer calls follow it, and Close stops all timers.
	manager.NotifySubscriberMaxQuality("terminal-control", mime.MimeTypeAV1, livekit.VideoQuality_OFF)
	manager.Close()
	if ctx.Err() != nil {
		return result, fmt.Errorf("case exceeded deadline before stopping initial timers")
	}
	if batch {
		if c.copies.Load() != int32(n) || len(c.events) != 0 {
			return result, fmt.Errorf("payload copied while first listener blocked")
		}
		if blockedCheck != nil {
			if err := blockedCheck(c); err != nil {
				return result, err
			}
		}
		c.unblock()
	}
	for {
		d, e := c.next()
		if e != nil {
			return result, e
		}
		if d.terminal {
			if len(d.event.Codecs) != n+1 {
				return result, fmt.Errorf("bad terminal control")
			}
			break
		}
		result.Events = append(result.Events, d.event)
	}
	if len(result.Events) != 5*n || c.copies.Load() != int32(n+5*n+1) {
		return result, fmt.Errorf("extra/missing callbacks: measured=%d copies=%d", len(result.Events), c.copies.Load())
	}
	// Not a timed quiet-window assertion: the terminal FIFO barrier plus stopped
	// queue/producers rules out future callbacks in the pinned implementation.
	select {
	case <-c.events:
		return result, fmt.Errorf("callback after terminal control")
	default:
	}
	return result, nil
}

func main() {
	rows := make([]row, 0, 6)
	for n := 1; n <= 3; n++ {
		for _, batch := range []bool{false, true} {
			r, err := runCase(n, batch, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "codec_count=%d batch=%v: %v\n", n, batch, err)
				os.Exit(1)
			}
			rows = append(rows, r)
		}
	}
	for _, r := range rows {
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
