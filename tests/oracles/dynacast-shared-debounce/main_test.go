package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/livekit/livekit-server/pkg/rtc/dynacast"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
)

var codecOrder = []mime.MimeType{mime.MimeTypeVP8, mime.MimeTypeVP9, mime.MimeTypeH264}
var rawNames = []string{"video/VP8", "video/VP9", "video/H264"}

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
type observation struct {
	AtMS   int     `json:"at_ms"`
	Events []event `json:"events"`
}
type row struct {
	Scenario     string        `json:"scenario"`
	CodecCount   int           `json:"codec_count"`
	PauseMS      int           `json:"pause_ms"`
	Observations []observation `json:"observations"`
}
type delivery struct {
	at    time.Time
	event event
}
type capture struct {
	dynacast.DynacastManagerListenerNull
	events chan delivery
}

func (c *capture) OnDynacastSubscribedMaxQualityChange(payload []*livekit.SubscribedCodec, _ []types.SubscribedCodecQuality) {
	d := delivery{at: time.Now(), event: event{Codecs: make([]codec, 0, len(payload))}}
	// Copy original public listener payload in the callback, then transfer sole
	// ownership via the bubble-local channel. No reads of manager internals.
	for _, p := range payload {
		v := codec{Codec: p.Codec, Qualities: make([]quality, 0, len(p.Qualities))}
		for _, q := range p.Qualities {
			v.Qualities = append(v.Qualities, quality{int(q.Quality), q.Enabled})
		}
		d.event.Codecs = append(d.event.Codecs, v)
	}
	// Normalize map iteration order only. Preserve raw MIME and quality order.
	sort.Slice(d.event.Codecs, func(i, j int) bool { return d.event.Codecs[i].Codec < d.event.Codecs[j].Codec })
	c.events <- d
}

func (c *capture) drain(t *testing.T, at time.Time) []event {
	t.Helper()
	out := make([]event, 0)
	for {
		select {
		case d := <-c.events:
			if !d.at.Equal(at) {
				t.Fatalf("callback at %v, checkpoint %v", d.at, at)
			}
			out = append(out, d.event)
		default:
			return out
		}
	}
}

type scenario struct {
	name    string
	counts  []int
	pauseMS int
	times   []int
}

var scenarios = []scenario{
	{"shared-expiry", []int{2, 3}, 1000, []int{0, 200, 400, 999, 1000, 1200}},
	{"upgrade-flush", []int{2, 3}, 1000, []int{0, 200, 400, 999, 1000, 1399, 1400}},
	{"return-keeps-deadline", []int{1, 2}, 1000, []int{0, 200, 400, 999, 1000, 1400}},
	{"equal-expiry", []int{1, 2}, 1000, []int{0, 200, 999, 1000}},
	{"zero-delay", []int{2, 3}, 0, []int{0, 200, 400, 1000}},
}

// Explicit expected full aggregate states, not a reimplementation of the
// debounce algorithm. OFF is the public enum 3, not an ordered quality level.
func expectedStates(name string, n int) map[int][]int {
	switch name {
	case "shared-expiry":
		return map[int][]int{1000: []int{0, 3, 0}[:n]}
	case "upgrade-flush":
		if n == 2 {
			return map[int][]int{200: {0, 2}, 1400: {3, 2}}
		}
		return map[int][]int{200: {0, 2, 1}, 1400: {0, 2, 3}}
	case "return-keeps-deadline":
		return map[int][]int{1000: []int{3, 1}[:n]}
	case "equal-expiry":
		return map[int][]int{1000: []int{2, 1}[:n]}
	case "zero-delay":
		if n == 2 {
			return map[int][]int{0: {0, 1}, 200: {0, 3}, 400: {2, 3}}
		}
		return map[int][]int{0: {0, 1, 1}, 200: {0, 3, 1}, 400: {0, 3, 2}}
	default:
		panic("unknown scenario")
	}
}

func checkEvents(t *testing.T, got []event, state []int) {
	t.Helper()
	want := make([]event, 0)
	if state != nil {
		e := event{Codecs: make([]codec, 0, len(state))}
		for i, maximum := range state {
			v := codec{Codec: rawNames[i], Qualities: make([]quality, 0, 3)}
			for q := 0; q < 3; q++ {
				v.Qualities = append(v.Qualities, quality{q, maximum != 3 && q <= maximum})
			}
			e.Codecs = append(e.Codecs, v)
		}
		sort.Slice(e.Codecs, func(i, j int) bool { return e.Codecs[i].Codec < e.Codecs[j].Codec })
		want = append(want, e)
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("events=%s, want %s", gotJSON, wantJSON)
	}
}

func runCase(t *testing.T, s scenario, n int) row {
	t.Helper()
	result := row{Scenario: s.name, CodecCount: n, PauseMS: s.pauseMS, Observations: make([]observation, 0, len(s.times))}
	synctest.Test(t, func(t *testing.T) {
		// No context deadline/select sourced outside this bubble: it would prevent
		// durable blocking. The test binary's real timeout is the safety watchdog.
		c := &capture{events: make(chan delivery, 64)}
		manager := dynacast.NewDynacastManagerVideo(dynacast.DynacastManagerVideoParams{
			DynacastPauseDelay: time.Duration(s.pauseMS) * time.Millisecond, Listener: c,
		})
		defer func() {
			manager.Close() // original Close, no timer/source replacements
			synctest.Wait()
			checkEvents(t, c.drain(t, time.Now()), nil)
		}()
		notify := func(i int, q livekit.VideoQuality) {
			manager.NotifySubscriberMaxQuality(livekit.ParticipantID(fmt.Sprintf("s%d", i)), codecOrder[i], q)
			synctest.Wait()
		}
		created := time.Now()
		warmup := []int{2, 1, 1}
		for i := 0; i < n; i++ {
			notify(i, livekit.VideoQuality(warmup[i]))
			// Every warmup callback must arrive without advancing fake time.
			checkEvents(t, c.drain(t, created), warmup[:i+1])
		}
		start := time.Now() // relative zero AFTER all excluded warmup callbacks
		if !start.Equal(created) {
			t.Fatal("warmup unexpectedly advanced time")
		}
		states := expectedStates(s.name, n)
		previous := -1
		for _, at := range s.times {
			if at <= previous {
				t.Fatal("non-increasing checkpoint chronology")
			}
			previous = at
			deadline := start.Add(time.Duration(at) * time.Millisecond)
			if delay := deadline.Sub(time.Now()); delay > 0 {
				time.Sleep(delay) // synctest fake time only, never wall-clock ordering
			}
			synctest.Wait() // finish original AfterFunc and OpsQueue callbacks
			if !time.Now().Equal(deadline) || time.Since(created) >= 2*time.Second {
				t.Fatal("clock drift or initial-timer exclusion violated")
			}
			switch at {
			case 0:
				notify(0, livekit.VideoQuality_LOW)
			case 200:
				switch s.name {
				case "shared-expiry", "zero-delay":
					notify(1, livekit.VideoQuality_OFF)
				case "upgrade-flush":
					notify(1, livekit.VideoQuality_HIGH)
				default:
					notify(0, livekit.VideoQuality_HIGH)
				}
			case 400:
				switch s.name {
				case "shared-expiry":
					if n == 3 {
						notify(2, livekit.VideoQuality_LOW)
					}
				case "upgrade-flush":
					if n == 3 {
						notify(2, livekit.VideoQuality_OFF)
					} else {
						notify(0, livekit.VideoQuality_OFF)
					}
				case "return-keeps-deadline":
					notify(0, livekit.VideoQuality_OFF)
				case "zero-delay":
					if n == 3 {
						notify(2, livekit.VideoQuality_HIGH)
					} else {
						notify(0, livekit.VideoQuality_HIGH)
					}
				}
			}
			synctest.Wait()
			events := c.drain(t, deadline) // retain every empty checkpoint
			checkEvents(t, events, states[at])
			result.Observations = append(result.Observations, observation{AtMS: at, Events: events})
		}
	})
	return result
}

func collect(t *testing.T) []row {
	t.Helper()
	rows := make([]row, 0, 10)
	for _, s := range scenarios {
		for _, n := range s.counts {
			t.Run(fmt.Sprintf("%s/n%d", s.name, n), func(t *testing.T) {
				rows = append(rows, runCase(t, s, n))
			})
		}
	}
	if t.Failed() || len(rows) != 10 {
		t.Fatal("incomplete matrix")
	}
	return rows
}

func TestSharedDebounce(t *testing.T) { collect(t) }

// This test binary is also the observer. No file I/O occurs inside a bubble;
// race repetitions run TestSharedDebounce, and a separate single invocation
// runs TestObserve with a fresh task-owned C56_GO_JSONL destination.
func TestObserve(t *testing.T) {
	path := os.Getenv("C56_GO_JSONL")
	if path == "" {
		t.Skip("set C56_GO_JSONL for one observation run")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(tmp, parent)
	if err != nil || !filepath.IsAbs(path) || !strings.HasPrefix(rel, "c56-go-") {
		t.Fatal("C56_GO_JSONL must be under task-owned /tmp/c56-go-*")
	}
	rows := collect(t)
	out, err := os.OpenFile(filepath.Join(parent, filepath.Base(path)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := json.NewEncoder(out).Encode(r); err != nil {
			out.Close()
			t.Fatal(err)
		}
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}
