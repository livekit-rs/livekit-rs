package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/webrtc/v4"
)

var codecOrder = []mime.MimeType{mime.MimeTypeVP8, mime.MimeTypeVP9}
var rawNames = []string{"video/VP8", "video/VP9"}
var checkpoints = []int{0, 100, 200, 300, 400, 1099, 1100, 1101, 9999, 10000, 10099, 10100, 10199, 10200, 10300}

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
type layer struct {
	Codec   string `json:"codec"`
	Spatial int32  `json:"spatial"`
}
type observation struct {
	AtMS           int     `json:"at_ms"`
	Events         []event `json:"events"`
	ExpectedLayers []layer `json:"expected_layers"`
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
type capture struct{ events chan delivery }

// This is the publisher callback attached to the original MediaTrack, not a
// replacement DynacastManagerListener. The original track is that listener.
func (c *capture) publisher(_ livekit.TrackID, _ *livekit.TrackInfo, payload []*livekit.SubscribedCodec, _ []types.SubscribedCodecQuality) error {
	d := delivery{at: time.Now(), event: event{Codecs: make([]codec, 0, len(payload))}}
	for _, p := range payload {
		v := codec{Codec: p.Codec, Qualities: make([]quality, 0, len(p.Qualities))}
		for _, q := range p.Qualities {
			v.Qualities = append(v.Qualities, quality{int(q.Quality), q.Enabled})
		}
		d.event.Codecs = append(d.event.Codecs, v)
	}
	// Only map-derived codec order is normalized; never sort/deduplicate events.
	sort.Slice(d.event.Codecs, func(i, j int) bool { return d.event.Codecs[i].Codec < d.event.Codecs[j].Codec })
	c.events <- d
	return nil
}

func (c *capture) drain(t *testing.T, at time.Time) []event {
	t.Helper()
	out := make([]event, 0)
	for {
		select {
		case d := <-c.events:
			if !d.at.Equal(at) {
				t.Errorf("raw publisher timestamp %v != checkpoint %v", d.at, at)
			}
			t.Logf("publisher timestamp=%s checkpoint=%s", d.at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano))
			out = append(out, d.event)
		default:
			return out
		}
	}
}

type layerCall struct {
	at      time.Time
	spatial int32
}

// Public receiver boundary spy only. A bare *rtc.DummyReceiver is unwrapped to
// nil by MediaTrackReceiver.Receiver. Embedding keeps its safe unused methods
// without that concrete-type match. No manager/MediaTrack implementation copied.
type receiverSpy struct {
	*rtc.DummyReceiver
	mu          sync.Mutex
	calls       []layerCall
	last        int32
	valid       bool
	infoUpdates int
	muted       bool
}

func (r *receiverSpy) SetMaxExpectedSpatialLayer(spatial int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last, r.valid = spatial, true
	r.calls = append(r.calls, layerCall{time.Now(), spatial})
}

func (r *receiverSpy) UpdateTrackInfo(ti *livekit.TrackInfo) {
	// No waits/yields here: SetMuted must finish synchronously before Wait.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.infoUpdates++
	r.muted = ti.Muted
}

func (r *receiverSpy) read(t *testing.T, at time.Time) int32 {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.valid {
		t.Fatal("missing real SetMaxExpectedSpatialLayer warmup; zero default forbidden")
	}
	for _, c := range r.calls {
		if !c.at.Equal(at) {
			t.Errorf("raw receiver timestamp %v != checkpoint %v", c.at, at)
		}
		t.Logf("receiver=%s spatial=%d timestamp=%s checkpoint=%s", r.Mime(), c.spatial, c.at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano))
	}
	r.calls = nil
	return r.last // last real call, not an expected-state calculation
}

type scenario struct {
	name    string
	counts  []int
	pauseMS int
}

var scenarios = []scenario{
	{"restart-settled", []int{1, 2}, 1000},
	{"restart-same", []int{1, 2}, 1000},
	{"restart-pending", []int{1, 2}, 1000},
	{"restart-repeat", []int{1, 2}, 1000},
	{"resend-high", []int{1, 2}, 1000},
	{"resend-low", []int{1, 2}, 1000},
	{"resend-pending", []int{1, 2}, 1000},
	{"resend-restart", []int{1, 2}, 1000},
	{"resend-muted", []int{1, 2}, 1000},
}

// Literal checkpoint vectors. These are assertion data, never used by capture.
func expectedLayers(name string, n int) [][]int32 {
	h, l, m := []int32{2, 2}[:n], []int32{0, 2}[:n], []int32{1, 2}[:n]
	switch name {
	case "restart-settled", "resend-restart":
		return [][]int32{l, h, h, h, h, h, h, h, h, h, h, l, l, l, l}
	case "restart-same":
		return [][]int32{l, h, l, l, l, l, l, l, l, l, l, l, l, l, l}
	case "restart-pending":
		return [][]int32{h, h, h, h, h, h, l, l, l, l, l, l, l, l, l}
	case "restart-repeat":
		return [][]int32{l, h, h, h, h, h, h, h, h, h, h, h, h, l, l}
	case "resend-high":
		return [][]int32{h, h, h, h, h, h, h, h, h, h, h, h, h, h, h}
	case "resend-low", "resend-muted":
		return [][]int32{l, l, l, l, l, l, l, l, l, l, l, l, l, l, l}
	case "resend-pending":
		return [][]int32{m, m, m, m, m, m, l, l, l, l, l, l, l, l, l}
	default:
		panic("unknown scenario")
	}
}

func expectedEvent(name string, at, n int) []int {
	l, m := []int{0, 2}[:n], []int{1, 2}[:n]
	switch name {
	case "restart-settled":
		if at == 10100 {
			return l
		}
	case "restart-same":
		if at == 200 {
			return l
		}
	case "restart-pending":
		if at == 1100 {
			return l
		}
	case "restart-repeat":
		if at == 10200 {
			return l
		}
	case "resend-high":
	case "resend-low":
		if at == 100 || at == 200 {
			return l
		}
	case "resend-pending":
		if at == 200 {
			return m
		}
		if at == 1100 {
			return l
		}
	case "resend-restart":
		if at == 10100 || at == 10200 {
			return l
		}
	case "resend-muted":
		if at == 300 || at == 400 {
			return l
		}
	default:
		panic("unknown scenario")
	}
	return nil
}

var literalQualities = map[int][]quality{
	0: {{0, true}, {1, false}, {2, false}},
	1: {{0, true}, {1, true}, {2, false}},
	2: {{0, true}, {1, true}, {2, true}},
	3: {{0, false}, {1, false}, {2, false}},
}

func checkEvents(t *testing.T, got []event, state []int) {
	t.Helper()
	want := make([]event, 0)
	if state != nil {
		e := event{Codecs: make([]codec, 0, len(state))}
		for i, q := range state {
			e.Codecs = append(e.Codecs, codec{rawNames[i], literalQualities[q]})
		}
		want = append(want, e)
	}
	checkJSON(t, got, want)
}

func checkJSON(t *testing.T, got, want any) {
	t.Helper()
	g, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	w, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(g) != string(w) {
		t.Errorf("got=%s want=%s", g, w)
	}
}

func runCase(t *testing.T, s scenario, n int, assertMeasured bool) row {
	t.Helper()
	result := row{Scenario: s.name, CodecCount: n, PauseMS: s.pauseMS, Observations: make([]observation, 0, len(checkpoints))}
	synctest.Test(t, func(t *testing.T) {
		created := time.Now()
		c := &capture{events: make(chan delivery, 64)}
		ti := &livekit.TrackInfo{Sid: "TR_LIFECYCLE", Type: livekit.TrackType_VIDEO, MimeType: rawNames[0], BackupCodecPolicy: livekit.BackupCodecPolicy_SIMULCAST}
		for i := 0; i < n; i++ {
			ti.Codecs = append(ti.Codecs, &livekit.SimulcastCodecInfo{MimeType: rawNames[i], Layers: []*livekit.VideoLayer{
				{Quality: livekit.VideoQuality_LOW, SpatialLayer: 0, Width: 320, Height: 180},
				{Quality: livekit.VideoQuality_MEDIUM, SpatialLayer: 1, Width: 640, Height: 360},
				{Quality: livekit.VideoQuality_HIGH, SpatialLayer: 2, Width: 1280, Height: 720},
			}})
		}
		track := rtc.NewMediaTrack(rtc.MediaTrackParams{
			ParticipantID: func() livekit.ParticipantID { return "PA_LIFECYCLE" },
			Logger:        logger.GetLogger(), VideoConfig: config.VideoConfig{DynacastPauseDelay: time.Duration(s.pauseMS) * time.Millisecond},
			OnSubscribedMaxQualityChange: c.publisher,
		}, ti)
		defer func() { track.Close(false); synctest.Wait(); checkEvents(t, c.drain(t, time.Now()), nil) }()
		spies := make([]*receiverSpy, n)
		for i := range spies {
			spies[i] = &receiverSpy{DummyReceiver: rtc.NewDummyReceiver(ti, "PA_LIFECYCLE", webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: rawNames[i], ClockRate: 90000}}, nil)}
			track.SetupReceiver(spies[i], i, "")
			synctest.Wait()
			if track.Receiver(codecOrder[i]) != spies[i] {
				t.Fatal("spy not reachable through original Receiver")
			}
			checkEvents(t, c.drain(t, created), nil)
		}
		notify := func(i int, q livekit.VideoQuality) {
			track.NotifySubscriberNodeMaxQuality("ND_LIFECYCLE", []types.SubscribedCodecQuality{{CodecMime: codecOrder[i], Quality: q}})
			synctest.Wait()
		}
		warm := []int{0, 2}
		switch s.name {
		case "restart-pending", "resend-high":
			warm[0] = 2
		case "resend-pending":
			warm[0] = 1
		}
		for i := range spies {
			notify(i, livekit.VideoQuality(warm[i]))
			checkEvents(t, c.drain(t, created), warm[:i+1])
			for j := 0; j <= i; j++ {
				checkJSON(t, spies[j].read(t, created), int32(warm[j]))
			}
		}
		if !time.Now().Equal(created) {
			t.Fatal("warmup advanced clock")
		}
		t.Logf("warmup nonvacuous codecs=%d; publisher payloads and real receiver layer values verified", n)
		muteCalls := 0
		muted := false
		setMuted := func(value bool) {
			track.SetMuted(value) // NO Wait within this original synchronous call
			muteCalls++
			muted = value
			if track.IsMuted() != value {
				t.Error("SetMuted did not complete synchronously")
			}
			synctest.Wait() // settle original ops queue only after SetMuted returns
		}
		for k, at := range checkpoints {
			deadline := created.Add(time.Duration(at) * time.Millisecond)
			if delay := deadline.Sub(time.Now()); delay > 0 {
				time.Sleep(delay)
			}
			synctest.Wait() // first timers/queue, then stimulus, then queue, then record
			if !time.Now().Equal(deadline) {
				t.Fatal("clock drift")
			}
			switch s.name {
			case "restart-settled":
				if at == 100 {
					track.Restart()
				}
			case "restart-same":
				if at == 100 {
					track.Restart()
				}
				if at == 200 {
					notify(0, livekit.VideoQuality_LOW)
				}
			case "restart-pending":
				if at == 100 {
					notify(0, livekit.VideoQuality_LOW)
				}
				if at == 200 {
					track.Restart()
				}
			case "restart-repeat":
				if at == 100 || at == 200 {
					track.Restart()
				}
			case "resend-high", "resend-low":
				if at == 100 || at == 200 {
					track.ResendSubscribedQuality()
				}
			case "resend-pending":
				if at == 100 {
					notify(0, livekit.VideoQuality_LOW)
				}
				if at == 200 {
					track.ResendSubscribedQuality()
				}
			case "resend-restart":
				if at == 100 {
					track.Restart()
				}
				if at == 200 || at == 10200 {
					track.ResendSubscribedQuality()
				}
			case "resend-muted":
				if at == 100 {
					setMuted(true)
				}
				if at == 200 || at == 400 {
					track.ResendSubscribedQuality()
				}
				if at == 300 {
					setMuted(false)
				}
			}
			synctest.Wait()
			t.Logf("checkpoint=%dms", at)
			o := observation{AtMS: at, Events: c.drain(t, deadline), ExpectedLayers: make([]layer, 0, n)}
			for _, spy := range spies {
				o.ExpectedLayers = append(o.ExpectedLayers, layer{spy.Mime().String(), spy.read(t, deadline)})
				spy.mu.Lock()
				if spy.infoUpdates != muteCalls || spy.muted != muted {
					t.Error("UpdateTrackInfo fanout mismatch")
				}
				spy.mu.Unlock()
			}
			if assertMeasured {
				checkEvents(t, o.Events, expectedEvent(s.name, at, n))
				want := make([]layer, 0, n)
				for i, spatial := range expectedLayers(s.name, n)[k] {
					want = append(want, layer{rawNames[i], spatial})
				}
				checkJSON(t, o.ExpectedLayers, want)
			}
			result.Observations = append(result.Observations, o)
		}
	})
	return result
}

func collect(t *testing.T, assertMeasured bool) []row {
	t.Helper()
	// Inherit C58's explicitly serial execution domain. synctest controls virtual
	// time and quiescence, not parallel equivalence. No waits within SetMuted,
	// Restart or ResendSubscribedQuality; drain only after the public call returns.
	if runtime.GOMAXPROCS(0) != 1 {
		t.Fatal("controlled LIFECYCLE sequence requires GOMAXPROCS=1")
	}
	rows := make([]row, 0, 18)
	for _, s := range scenarios {
		for _, n := range s.counts {
			t.Run(fmt.Sprintf("%s/n%d", s.name, n), func(t *testing.T) { rows = append(rows, runCase(t, s, n, assertMeasured)) })
		}
	}
	if len(rows) != 18 {
		t.Fatal("incomplete matrix")
	}
	return rows
}

func TestLifecycle(t *testing.T) { collect(t, true) }

// Independent single observation. No measured-state expectation controls output.
func TestObserve(t *testing.T) {
	path := os.Getenv("LIFECYCLE_GO_JSONL")
	if path == "" {
		t.Skip("set LIFECYCLE_GO_JSONL for one observation")
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
	if err != nil || !filepath.IsAbs(path) || !strings.HasPrefix(rel, "dynacast-lifecycle-go-") {
		t.Fatal("output must be task-owned /tmp/dynacast-lifecycle-go-*")
	}
	out, err := os.OpenFile(filepath.Join(parent, filepath.Base(path)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	for _, r := range collect(t, false) {
		if err := json.NewEncoder(out).Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}
