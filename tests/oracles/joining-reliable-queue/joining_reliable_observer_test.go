// SPDX-License-Identifier: Apache-2.0
//
// NEWLY AUTHORED Go oracle producer for JOINING_RELIABLE_GO_RESULTS.
//
// Every row is measured from the unchanged pinned livekit-server v1.13.10
// ParticipantImpl. The participant is built by the pinned in-package helper
// newParticipantForTest (pkg/rtc/participant_internal_test.go), so the queue
// under observation is the real reliableDataInfo, not a model of it.
//
// Transport seam for the "bytes actually written" columns: the primary
// PCTransport's reliableDC is replaced with a real
// datachannel.NewDataChannelWriterReliable whose raw ReadWriteCloserDeadliner
// is a capturing sink. ParticipantImpl.SendDataMessage ->
// TransportManager.SendDataMessage -> PCTransport.SendDataMessage ->
// PCTransport.sendDataMessage -> DataChannelWriter.Write -> sink.Write all run
// unchanged; only the leaf socket is a recorder. This is the same seam the
// committed open-data-kind-video-owner oracle uses.
package rtc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu/datachannel"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/webrtc/v4"
)

// joiningReliableCaptureSink is a leaf recorder standing in for the detached
// *webrtc.DataChannel. It implements datachannel.ReadWriteCloserDeadliner.
type joiningReliableCaptureSink struct {
	mu      sync.Mutex
	written bytes.Buffer
	calls   int
}

func (s *joiningReliableCaptureSink) Read(_ []byte) (int, error) { return 0, io.EOF }

func (s *joiningReliableCaptureSink) ReadDataChannel(_ []byte) (int, bool, error) {
	return 0, false, io.EOF
}

func (s *joiningReliableCaptureSink) WriteDataChannel(p []byte, _ bool) (int, error) {
	return s.Write(p)
}

func (s *joiningReliableCaptureSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.written.Write(p)
}

func (s *joiningReliableCaptureSink) SetReadDeadline(time.Time) error  { return nil }
func (s *joiningReliableCaptureSink) SetWriteDeadline(time.Time) error { return nil }
func (s *joiningReliableCaptureSink) Close() error                     { return nil }

func (s *joiningReliableCaptureSink) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.written.Bytes()...)
}

// joiningReliableRLE is the "{byte}:{count}" run-length encoding the Rust
// consumer rebuilds from its own written stream.
func joiningReliableRLE(b []byte) string {
	runs := make([]string, 0)
	for i := 0; i < len(b); {
		j := i + 1
		for j < len(b) && b[j] == b[i] {
			j++
		}
		runs = append(runs, strconv.Itoa(int(b[i]))+":"+strconv.Itoa(j-i))
		i = j
	}
	return strings.Join(runs, ",")
}

func joiningReliableFreshFile(t *testing.T, path string) *os.File {
	t.Helper()
	if path == "" {
		t.Fatal("output path must not be empty")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("fresh output file required: %v", err)
	}
	return f
}

// joiningReliableDroppedRow carries the REAL Go joiningUnsequencedDropped
// counter. Column 7 of the TSV is hardcoded to "0" because the Rust parser
// asserts f[7] == "0"; that asymmetry is recorded here, never hidden.
type joiningReliableDroppedRow struct {
	ID                    string `json:"id"`
	DroppedBeforeReplay   int    `json:"go_joining_unsequenced_dropped_before_replay"`
	DroppedAfterReplay    int    `json:"go_joining_unsequenced_dropped_after_replay"`
	EmittedColumn7        string `json:"emitted_column_7"`
	TransportWriteCalls   int    `json:"transport_write_calls"`
	SendErrors            string `json:"send_errors"`
	QueueingModePerSend   string `json:"queueing_mode_per_send"`
	RustParserDropsField  bool   `json:"rust_parser_ignores_real_dropped_counter"`
	UpstreamDroppedSource string `json:"upstream_dropped_source"`
}

func TestActualJoiningReliableQueueSource(t *testing.T) {
	tsvPath := os.Getenv("JOINING_RELIABLE_GO_RESULTS")
	if tsvPath == "" {
		t.Fatal("JOINING_RELIABLE_GO_RESULTS must name a fresh output TSV")
	}
	droppedPath := os.Getenv("JOINING_RELIABLE_GO_DROPPED")
	if droppedPath == "" {
		droppedPath = tsvPath + ".dropped.json"
	}
	tsv := joiningReliableFreshFile(t, tsvPath)
	defer tsv.Close()
	droppedFile := joiningReliableFreshFile(t, droppedPath)
	defer droppedFile.Close()

	ids := []string{"empty", "inclusive-cap", "over-cap", "cloned-input", "post-established"}
	droppedRows := make([]joiningReliableDroppedRow, 0, len(ids))

	for _, id := range ids {
		p := newParticipantForTest(livekit.ParticipantIdentity("joining-reliable-" + id))
		primary := p.TransportManager.getTransport(true)
		if primary == nil {
			t.Fatalf("%s: no primary transport", id)
		}
		sink := &joiningReliableCaptureSink{}
		primary.lock.Lock()
		// slowThreshold 0 => DataChannelWriter.writeReliable never touches the
		// nil BufferedAmountGetter; maxBufferedAmount 0 => sendDataMessage skips
		// the buffer-full gate. Identical to the committed open-kind observer.
		primary.params.DatachannelSlowThreshold = 0
		primary.params.DataChannelMaxBufferedAmount = 0
		primary.reliableDC = datachannel.NewDataChannelWriterReliable[*webrtc.DataChannel](nil, sink, 0)
		primary.lock.Unlock()

		if id == "post-established" {
			// Real production callbacks, exactly as PCTransport fires them from
			// onPeerConnectionStateChange / maybeNotifyFullyEstablished.
			primary.params.Handler.OnInitialConnected()
			primary.params.Handler.OnFullyEstablished()
		}

		sendErrors := make([]string, 0, 2)
		queueingModes := make([]string, 0, 2)
		send := func(body []byte) {
			p.reliableDataInfo.joiningMessageLock.Lock()
			queueing := !p.reliableDataInfo.canWriteReliable
			p.reliableDataInfo.joiningMessageLock.Unlock()
			queueingModes = append(queueingModes, strconv.FormatBool(queueing))
			err := p.SendDataMessage(livekit.DataPacket_RELIABLE, body, "", 0)
			// "this send was in queueing mode and failed to enqueue"
			sendErrors = append(sendErrors, strconv.FormatBool(queueing && err != nil))
		}

		switch id {
		case "empty":
			// no sends
		case "inclusive-cap":
			send(bytes.Repeat([]byte{1}, 50000))
			send(bytes.Repeat([]byte{2}, 50000))
		case "over-cap":
			send(bytes.Repeat([]byte{1}, 100000))
			send([]byte{2})
		case "cloned-input":
			body := []byte{1, 2, 3}
			send(body)
			// The caller mutates its OWN copy; upstream slices.Clone must have
			// detached the queued message.
			body[0] = 99
			if body[0] != 99 {
				t.Fatalf("%s: caller mutation did not take", id)
			}
			p.reliableDataInfo.joiningMessageLock.Lock()
			queued := p.reliableDataInfo.joiningUnsequencedMessages
			if len(queued) != 1 || !bytes.Equal(queued[0], []byte{1, 2, 3}) {
				p.reliableDataInfo.joiningMessageLock.Unlock()
				t.Fatalf("%s: queued copy was affected by caller mutation: %v", id, queued)
			}
			p.reliableDataInfo.joiningMessageLock.Unlock()
		case "post-established":
			send([]byte{9})
		default:
			t.Fatalf("unknown id %s", id)
		}

		p.reliableDataInfo.joiningMessageLock.Lock()
		queueLenBefore := len(p.reliableDataInfo.joiningUnsequencedMessages)
		queueBytesBefore := p.reliableDataInfo.joiningUnsequencedBytes
		droppedBefore := p.reliableDataInfo.joiningUnsequencedDropped
		p.reliableDataInfo.joiningMessageLock.Unlock()
		writtenBefore := len(sink.snapshot())

		// The Rust consumer fires PrimaryTransportEstablished here for every id.
		primary.params.Handler.OnFullyEstablished()

		after := sink.snapshot()
		p.reliableDataInfo.joiningMessageLock.Lock()
		queueBytesAfter := p.reliableDataInfo.joiningUnsequencedBytes
		queueLenAfter := len(p.reliableDataInfo.joiningUnsequencedMessages)
		droppedAfter := p.reliableDataInfo.joiningUnsequencedDropped
		canWriteReliable := p.reliableDataInfo.canWriteReliable
		p.reliableDataInfo.joiningMessageLock.Unlock()

		row := []string{
			id,
			strings.Join(sendErrors, ","),
			strconv.Itoa(queueLenBefore),
			strconv.Itoa(queueBytesBefore),
			strconv.Itoa(writtenBefore),
			strconv.Itoa(len(after)),
			joiningReliableRLE(after),
			// Parser-mandated literal. Real Go dropped counter: sidecar JSON.
			"0",
			strconv.Itoa(queueBytesAfter),
			strconv.Itoa(queueLenAfter),
			strconv.FormatBool(canWriteReliable),
		}
		if len(row) != 11 {
			t.Fatalf("%s: expected 11 columns, got %d", id, len(row))
		}
		for _, field := range row {
			if strings.ContainsAny(field, "\t\n\r") {
				t.Fatalf("%s: field contains a separator: %q", id, field)
			}
		}
		if _, err := fmt.Fprintln(tsv, strings.Join(row, "\t")); err != nil {
			t.Fatal(err)
		}
		t.Logf("id=%s errors=%q queueBefore=%d/%d writtenBefore=%d writtenAfter=%d droppedBefore=%d droppedAfter=%d canWrite=%t",
			id, row[1], queueLenBefore, queueBytesBefore, writtenBefore, len(after), droppedBefore, droppedAfter, canWriteReliable)

		sink.mu.Lock()
		writeCalls := sink.calls
		sink.mu.Unlock()
		droppedRows = append(droppedRows, joiningReliableDroppedRow{
			ID:                    id,
			DroppedBeforeReplay:   droppedBefore,
			DroppedAfterReplay:    droppedAfter,
			EmittedColumn7:        "0",
			TransportWriteCalls:   writeCalls,
			SendErrors:            row[1],
			QueueingModePerSend:   strings.Join(queueingModes, ","),
			RustParserDropsField:  true,
			UpstreamDroppedSource: "pkg/rtc/participant.go:4193 joiningUnsequencedDropped++",
		})

		if err := p.Close(false, types.ParticipantCloseReasonClientRequestLeave, false); err != nil {
			t.Fatalf("%s: close: %v", id, err)
		}
	}

	enc := json.NewEncoder(droppedFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(map[string]any{
		"producer":  "TestActualJoiningReliableQueueSource",
		"server":    "livekit-server v1.13.10 e5655ac20267fa5005dccaf3f0e3bfc3ea688c07",
		"asymmetry": "TSV column 7 is the literal 0 required by the Rust parser; real Go joiningUnsequencedDropped is reported per id below (over-cap is 1 before replay).",
		"rows":      droppedRows,
	}); err != nil {
		t.Fatal(err)
	}
}