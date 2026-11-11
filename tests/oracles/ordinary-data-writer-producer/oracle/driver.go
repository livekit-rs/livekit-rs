package datachannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pion/datachannel"
)

// oracleNow is the single explicit clock that replaces mono.Now()/time.Now()
// in the staged upstream files (the only textual transforms applied). It is
// set to the previous completion instant before each public Write and advanced
// to the scripted completion instant inside the fake raw Write.
var oracleNow = time.Unix(1700000000, 0)

func sourceNow() time.Time { return oracleNow }

// lossyMinBufferedAmount mirrors the upstream construction constant
// lossyDataChannelMinBufferedAmount = 8 * 1024 at
// references/livekit-stable/pkg/rtc/transport.go:95 (used at :945/:959/:1335/:1337).
const lossyMinBufferedAmount uint64 = 8 * 1024

// Action is one scripted raw write completion from the committed corpus.
type Action struct {
	AtNS     uint64 `json:"at_ns"`
	N        uint64 `json:"n"`
	Buffered uint64 `json:"buffered"`
	Timeout  bool   `json:"timeout"`
	Closed   bool   `json:"closed"`
}

// Case keeps each call's actions as raw JSON so consumed actions are echoed
// verbatim (all five keys, original types) into the observation rows.
type Case struct {
	ID       string              `json:"id"`
	Reliable bool                `json:"reliable"`
	Slow     int                 `json:"slow"`
	TargetNS int64               `json:"target_ns"`
	Calls    [][]json.RawMessage `json:"calls"`
}

// Row is the exact 10-key observation consumed by the Rust differential test
// ordinary_data_writer::tests::actual_go_ordinary_writer_all_twenty_results.
// DeadlinesNS/Used marshal to JSON null when no raw write was consumed.
type Row struct {
	ID           string            `json:"id"`
	Call         int               `json:"call"`
	N            uint64            `json:"n"`
	Family       string            `json:"family"`
	RawCalls     int               `json:"raw_calls"`
	DeadlinesNS  []int64           `json:"deadlines_ns"`
	Used         []json.RawMessage `json:"used"`
	Buffered     uint64            `json:"buffered"`
	Bitrate      int               `json:"bitrate"`
	BitrateValid bool              `json:"bitrate_valid"`
}

// scriptedDC is the genuine writer IO seam: BufferedAmountGetter plus the raw
// datachannel.ReadWriteCloserDeadliner. Read/Close are never exercised by
// Write; the embedded nil interface supplies them. writeReliable may issue
// several raw Write+SetWriteDeadline attempts per public Write, so the fake
// walks a scripted action list instead of a single operation.
type scriptedDC struct {
	datachannel.ReadWriteCloserDeadliner
	base      time.Time
	actions   []Action
	raw       []json.RawMessage
	idx       int
	buffered  uint64
	deadlines []int64
	used      []json.RawMessage
}

// BufferedAmount returns the carried-in value at admission time and the
// scripted post-write value after each raw write.
func (d *scriptedDC) BufferedAmount() uint64 { return d.buffered }

func (d *scriptedDC) SetWriteDeadline(t time.Time) error {
	d.deadlines = append(d.deadlines, t.Sub(oracleNow).Nanoseconds())
	return nil
}

func (d *scriptedDC) Write(b []byte) (int, error) {
	if d.idx >= len(d.actions) {
		return 0, fmt.Errorf("script exhausted: raw write attempt %d exceeds %d scripted actions", d.idx+1, len(d.actions))
	}
	a := d.actions[d.idx]
	d.used = append(d.used, d.raw[d.idx])
	d.idx++
	// This raw attempt's deadline was set at the previous instant; the write
	// itself completes at the scripted instant.
	oracleNow = d.base.Add(time.Duration(a.AtNS))
	d.buffered = a.Buffered
	n := int(a.N)
	if a.Timeout {
		return n, context.DeadlineExceeded
	}
	if a.Closed {
		return n, io.ErrClosedPipe
	}
	return n, nil
}

func family(err error) (string, error) {
	switch {
	case err == nil:
		return "ok", nil
	case errors.Is(err, ErrDataDroppedByHighBufferedAmount):
		return "high", nil
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline", nil
	case errors.Is(err, ErrDataDroppedBySlowReader):
		return "slow", nil
	case errors.Is(err, io.ErrClosedPipe):
		return "closed", nil
	default:
		return "", fmt.Errorf("unmapped writer error: %v", err)
	}
}

func RunOracle(in io.Reader, out io.Writer) error {
	var cases []Case
	if err := json.NewDecoder(in).Decode(&cases); err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	for _, c := range cases {
		base := time.Unix(1700000000, 0)
		oracleNow = base
		d := &scriptedDC{base: base}
		// One writer instance per case, reused across all calls so the bitrate
		// calculator accumulates. The deprecated MaxBuffered cap lives outside
		// this wrapper (PCTransport) and is disabled (max=0) for this corpus.
		var w *DataChannelWriter[*scriptedDC]
		if c.Reliable {
			w = NewDataChannelWriterReliable(d, d, c.Slow)
		} else {
			w = NewDataChannelWriterUnreliable(d, d, time.Duration(c.TargetNS), lossyMinBufferedAmount)
		}
		var carriedAtNS uint64
		for i, call := range c.Calls {
			actions := make([]Action, len(call))
			for j, raw := range call {
				if err := json.Unmarshal(raw, &actions[j]); err != nil {
					return fmt.Errorf("case %s call %d action %d: %w", c.ID, i, j, err)
				}
			}
			d.actions, d.raw, d.idx = actions, call, 0
			d.deadlines, d.used = nil, nil
			// The first raw attempt's deadline is set at the previous call's
			// completion instant (the case start for call 0).
			oracleNow = base.Add(time.Duration(carriedAtNS))
			// The upstream writer never inspects len(p); the scripted n is the
			// raw write's reported count, and retries re-send the full payload.
			n, err := w.Write(make([]byte, 4))
			fam, ferr := family(err)
			if ferr != nil {
				return fmt.Errorf("case %s call %d: %w", c.ID, i, ferr)
			}
			if d.idx > 0 {
				carriedAtNS = actions[d.idx-1].AtNS
			}
			row := Row{
				ID: c.ID, Call: i, N: uint64(n), Family: fam,
				RawCalls: d.idx, DeadlinesNS: d.deadlines, Used: d.used,
				Buffered: d.buffered,
			}
			if w.rate != nil {
				row.Bitrate, row.BitrateValid = w.rate.Bitrate(oracleNow)
			}
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
	}
	return nil
}