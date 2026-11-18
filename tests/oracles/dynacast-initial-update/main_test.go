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

var codecOrder = []mime.MimeType{mime.MimeTypeVP8, mime.MimeTypeVP9}
var rawNames = []string{"video/VP8", "video/VP9"}

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
	// Copy the original public listener payload at callback time. No internals.
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
}

var checkpoints = []int{0, 8000, 9000, 9500, 9999, 10000, 10001, 11499, 11500, 12000}
var scenarios = []scenario{
	{"settled-high", []int{1, 2}, 2000},
	{"settled-low", []int{1, 2}, 2000},
	{"pending-late", []int{1, 2}, 2000},
	{"pending-codue", []int{1, 2}, 2000},
	{"equal-codue", []int{1, 2}, 2000},
	{"zero-delay", []int{1, 2}, 0},
	{"no-demand", []int{1}, 2000},
	{"new-codec-initial", []int{2}, 2000},
}

// Literal agreed expectations, not a model of the manager. OFF is enum 3.
func expectedStates(name string, n int) map[int][]int {
	switch name {
	case "settled-high", "settled-low":
		return map[int][]int{}
	case "pending-late":
		return map[int][]int{11500: []int{0, 2}[:n]}
	case "pending-codue":
		return map[int][]int{10000: []int{0, 2}[:n]}
	case "equal-codue":
		return map[int][]int{10000: []int{2, 2}[:n]}
	case "zero-delay":
		return map[int][]int{9500: []int{0, 2}[:n]}
	case "no-demand":
		return map[int][]int{10000: {3}}
	case "new-codec-initial":
		return map[int][]int{10000: {0, 3}}
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
		// Do not abort: preserve all checkpoints and report every discrepancy.
		t.Errorf("events=%s, want %s", gotJSON, wantJSON)
	}
}

func runCase(t *testing.T, s scenario, n int, assertMeasured bool) row {
	t.Helper()
	result := row{Scenario: s.name, CodecCount: n, PauseMS: s.pauseMS, Observations: make([]observation, 0, len(checkpoints))}
	synctest.Test(t, func(t *testing.T) {
		// No external context/select inside this bubble. The test binary's real
		// timeout is the watchdog; Sleep only advances the original fake clock.
		c := &capture{events: make(chan delivery, 64)}
		created := time.Now()
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
		// All public AddCodec calls happen at creation time, with a Wait each.
		// AddCodec alone creates a quality object/timer, not a manager demand.
		for i := 0; i < n; i++ {
			manager.AddCodec(codecOrder[i])
			synctest.Wait()
			checkEvents(t, c.drain(t, created), nil)
		}
		warmup := []int{2, 2}
		if s.name == "settled-low" {
			warmup = []int{0, 0}
		}
		warmCount := n
		if s.name == "no-demand" {
			warmCount = 0
		} else if s.name == "new-codec-initial" {
			warmCount = 1
		}
		for i := 0; i < warmCount; i++ {
			notify(i, livekit.VideoQuality(warmup[i]))
			checkEvents(t, c.drain(t, created), warmup[:i+1])
		}
		if !time.Now().Equal(created) {
			t.Fatal("warmup unexpectedly advanced time")
		}
		states := expectedStates(s.name, n)
		previous := -1
		for _, at := range checkpoints {
			if at <= previous {
				t.Fatal("non-increasing checkpoint chronology")
			}
			previous = at
			deadline := created.Add(time.Duration(at) * time.Millisecond)
			if delay := deadline.Sub(time.Now()); delay > 0 {
				time.Sleep(delay)
			}
			synctest.Wait() // original AfterFunc and OpsQueue callbacks settle
			if !time.Now().Equal(deadline) {
				t.Fatal("clock drift")
			}
			switch at {
			case 8000:
				if s.name == "pending-codue" || s.name == "equal-codue" {
					notify(0, livekit.VideoQuality_LOW)
				}
			case 9000:
				if s.name == "equal-codue" {
					notify(0, livekit.VideoQuality_HIGH)
				}
			case 9500:
				if s.name == "pending-late" || s.name == "new-codec-initial" || s.name == "zero-delay" {
					notify(0, livekit.VideoQuality_LOW)
				}
			}
			synctest.Wait()
			events := c.drain(t, deadline)
			if assertMeasured {
				t.Logf("checkpoint=%dms", at)
				checkEvents(t, events, states[at])
			}
			result.Observations = append(result.Observations, observation{AtMS: at, Events: events})
		}
	})
	return result
}

func collect(t *testing.T, assertMeasured bool) []row {
	t.Helper()
	rows := make([]row, 0, 14)
	for _, s := range scenarios {
		for _, n := range s.counts {
			t.Run(fmt.Sprintf("%s/n%d", s.name, n), func(t *testing.T) {
				rows = append(rows, runCase(t, s, n, assertMeasured))
			})
		}
	}
	if len(rows) != 14 {
		t.Fatal("incomplete matrix")
	}
	return rows
}

func TestInitialUpdate(t *testing.T) { collect(t, true) }

// Separate count=1 observer: no expected-state filtering of measured output.
// Warmup, exact callback timestamps and Close checks still apply. File I/O is
// outside the bubble. The independent Python validator checks the raw matrix.
func TestObserve(t *testing.T) {
	path := os.Getenv("C57_GO_JSONL")
	if path == "" {
		t.Skip("set C57_GO_JSONL for one observation run")
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
	if err != nil || !filepath.IsAbs(path) || !strings.HasPrefix(rel, "c57-go-") {
		t.Fatal("C57_GO_JSONL must be under task-owned /tmp/c57-go-*")
	}
	rows := collect(t, false)
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
