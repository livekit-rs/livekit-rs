// SPEC-0074 C15 agent-wt online wire differential client.
//
// This is the canonical WebTransport agent client, built on the same
// `quic-go/webtransport-go` stack the official server uses, so it handshakes
// natively with the Go oracle and exercises the *Rust* server against a
// reference client. It performs the Extended CONNECT upgrade to /agent, opens
// one bidirectional control stream, writes a length-prefixed protobuf
// `WorkerMessage{Register}` (the Go `wire.WriteControlMessage` framing), reads
// one length-prefixed `ServerMessage{Register}` reply and prints a canonical
// JSON observation of the externally visible contract, identical in shape to
// the Rust `wt_probe`.
//
// `--mode` selects which part of the session lifecycle the probe exercises.
// Every mode reports the same observation keys, so the same probe can be run
// against the Go oracle and the Rust server and the two results compared
// field by field:
//
//	register  - upgrade + control stream + Register round trip, then check
//	            that the session stays open (the agent registration is live);
//	idle      - upgrade only, never open a control stream: the server must end
//	            the session on its `agent.RegisterTimeout` deadline. The close
//	            reason and how it was carried (a WT_CLOSE_SESSION capsule or a
//	            QUIC CONNECTION_CLOSE) are the observable contract;
//	stream    - upgrade + Register, then open a *second* client-initiated
//	            WebTransport stream and write a payload: the official listener
//	            accepts extra streams into the session's incoming queue and
//	            leaves them open, so the peer must not observe a reset;
//	close     - upgrade + Register, then open a *second* client-initiated
//	            WebTransport stream and end the agent control plane (a FIN on
//	            the control stream): the server must end the session, carry the
//	            WT_CLOSE_SESSION capsule and terminate the session's remaining
//	            streams with WTSessionGoneErrorCode;
//	datagram  - upgrade + Register, then send one WebTransport datagram
//	            (RFC 9297): the session must survive it;
//	data_integrity - write 48 bytes on a bidi stream, FIN it, verify no reset;
//	unidir    - open a unidirectional stream, write data, FIN, verify accepted;
//	reconnect - register, close session, re-connect, register again;
//	reliable_prefix - write two chunks, reset stream, verify session survives.
//
// Non-deterministic identity (minted worker id, node id) is reported as
// structure so parity means "same contract, same shape".
package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
	"google.golang.org/protobuf/proto"
)

const idAlphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// The observation window used by the modes that only check that the session
// survives: long enough to see a peer-side teardown, short enough to keep the
// differential quick.
const aliveWindow = 1500 * time.Millisecond

// The idle mode waits for the server's `agent.RegisterTimeout` (10s) plus
// enough slack for the close to travel.
const idleWindow = 25 * time.Second

func containsByte(b byte) bool {
	for i := 0; i < len(idAlphabet); i++ {
		if idAlphabet[i] == b {
			return true
		}
	}
	return false
}

// idShape reports the worker id only by observable structure (prefix, total
// length, and that the suffix is drawn from the upstream guid alphabet).
func idShape(id string) map[string]any {
	const prefix = "AW_"
	rest := ""
	if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
		rest = id[len(prefix):]
	}
	ok := rest != ""
	for i := 0; i < len(rest); i++ {
		if !containsByte(rest[i]) {
			ok = false
			break
		}
	}
	return map[string]any{"prefix": prefix, "len": len(id), "alphabet_ok": ok}
}

// writeFrame writes a 4-byte big-endian length + body (Go WriteControlMessage).
func writeFrame(w io.Writer, body []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// readFrame reads a 4-byte big-endian length + body (Go ReadControlMessage).
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > 1<<20 {
		return nil, fmt.Errorf("frame too large: %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// closeObs maps the cause of the session's cancellation onto the observation
// vocabulary the Rust probe produces as well:
//
//	session_error    - the peer ended the session with a WT_CLOSE_SESSION
//	                   capsule (the connection stays open);
//	connection_closed - the peer tore the whole QUIC connection down;
//	none             - the session was still open when the window elapsed.
func closeObs(cause error, elapsed time.Duration) map[string]any {
	millis := elapsed.Milliseconds()
	obs := map[string]any{
		"session_closed":  false,
		"close_kind":      "none",
		"close_code":      nil,
		"close_message":   nil,
		"closed_after_ms": millis,
		// The rounded second, the same value the Rust probe reports: the
		// driver's `idle` comparison covers the registration deadline on
		// both clients instead of comparing an absent field against a
		// number.
		"closed_after_s": (millis + 500) / 1000,
	}
	switch e := cause.(type) {
	case *webtransport.SessionError:
		obs["session_closed"] = true
		obs["close_kind"] = "session_error"
		obs["close_code"] = uint64(e.ErrorCode)
		obs["close_message"] = e.Message
	case *quic.ApplicationError:
		obs["session_closed"] = true
		obs["close_kind"] = "connection_closed"
		obs["close_code"] = uint64(e.ErrorCode)
		obs["close_message"] = e.ErrorMessage
	case nil:
		// The session context was never cancelled.
	default:
		if errors.Is(cause, context.DeadlineExceeded) || errors.Is(cause, context.Canceled) {
			// The observation window elapsed with the session still open.
			break
		}
		obs["session_closed"] = true
		obs["close_kind"] = "other"
		obs["close_message"] = cause.Error()
	}
	return obs
}

// observe waits up to `window` for the session to end and reports how it ended.
//
// The observation point is `Session.AcceptStream`: it is the only public API
// that surfaces the session's own termination cause. `Session.Context()` is
// cancelled with a plain `context.Canceled` when the session's capsule reader
// goroutine returns, so it cannot tell a WT_CLOSE_SESSION capsule apart from a
// transport error. `AcceptStream` returns the cause the session was closed
// with (`*webtransport.SessionError` for a capsule, `*quic.ApplicationError`
// for a connection teardown), or the context error when the window elapses.
func observe(sess *webtransport.Session, window time.Duration) map[string]any {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	_, err := sess.AcceptStream(ctx)
	return closeObs(err, time.Since(start))
}

func main() {
	addr := flag.String("addr", "", "host:port of the WebTransport listener")
	token := flag.String("token", "", "access token with the video.agent grant")
	path := flag.String("path", "/agent", "upgrade path")
	mode := flag.String("mode", "register", "probe mode: register|idle|stream|close|datagram|data_integrity|unidir|reconnect|reliable_prefix")
	flag.Parse()

	dialer := &webtransport.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "localhost",
			NextProtos:         []string{"h3"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	url := fmt.Sprintf("https://%s%s", *addr, *path)
	header := http.Header{}
	header.Set("Authorization", "Bearer "+*token)

	resp, sess, err := dialer.Dial(ctx, url, header)
	if err != nil {
		panic(fmt.Errorf("wt dial failed: %w", err))
	}
	var draft any
	if v := resp.Header.Get("sec-webtransport-http3-draft"); v != "" {
		draft = v
	}
	upgrade := map[string]any{"status": resp.StatusCode, "sec_webtransport_http3_draft": draft}
	obs := map[string]any{
		"mode":   *mode,
		"upgrade": upgrade,
	}

	// The control stream stays open for the whole run, the way a real agent
	// keeps it: the session must survive as long as the agent's control plane
	// is up. It is kept alive explicitly so the observation cannot depend on
	// the garbage collector.
	var control *webtransport.Stream
	switch *mode {
	case "register":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		for k, v := range observe(sess, aliveWindow) {
			obs[k] = v
		}
	case "idle":
		// Never open a control stream: the server must end the session on
		// its own deadline.
		for k, v := range observe(sess, idleWindow) {
			obs[k] = v
		}
	case "stream":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		stream, err := sess.OpenStreamSync(ctx)
		if err != nil {
			panic(fmt.Errorf("open second stream: %w", err))
		}
		payload := []byte("c15-stream-probe")
		if _, err := stream.Write(payload); err != nil {
			panic(fmt.Errorf("write second stream: %w", err))
		}
		obs["stream_opened"] = true
		obs["stream_write_bytes"] = len(payload)
		readStream(sess, stream, obs)
		closed := observe(sess, aliveWindow)
		for k, v := range closed {
			obs[k] = v
		}
		refineTermination(obs, closed)
	case "close":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		stream, err := sess.OpenStreamSync(ctx)
		if err != nil {
			panic(fmt.Errorf("open second stream: %w", err))
		}
		payload := []byte("c15-close-probe")
		if _, err := stream.Write(payload); err != nil {
			panic(fmt.Errorf("write second stream: %w", err))
		}
		obs["stream_opened"] = true
		obs["stream_write_bytes"] = len(payload)
		// The agent ends its control plane: the server must close the session
		// and terminate the streams the session still owns.
		if err := control.Close(); err != nil {
			panic(fmt.Errorf("close control stream: %w", err))
		}
		readStream(sess, stream, obs)
		closed := observe(sess, aliveWindow)
		for k, v := range closed {
			obs[k] = v
		}
		refineTermination(obs, closed)
	case "datagram":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		payload := []byte("c15-datagram-probe")
		if err := sess.SendDatagram(payload); err != nil {
			panic(fmt.Errorf("send datagram: %w", err))
		}
		obs["datagram_sent"] = true
		obs["datagram_bytes"] = len(payload)
		obs["datagram_echo"] = false
		dgCtx, dgCancel := context.WithTimeout(sess.Context(), aliveWindow)
		if b, err := sess.ReceiveDatagram(dgCtx); err == nil {
			obs["datagram_echo"] = len(b) > 0
			obs["datagram_echo_bytes"] = len(b)
		}
		dgCancel()
		for k, v := range observe(sess, aliveWindow) {
			obs[k] = v
		}
	case "data_integrity":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		stream, err := sess.OpenStreamSync(ctx)
		if err != nil {
			panic(fmt.Errorf("open data stream: %w", err))
		}
		chunk := []byte("c15-integrity-ck")
		total := 0
		for i := 0; i < 3; i++ {
			if _, err := stream.Write(chunk); err != nil {
				panic(fmt.Errorf("write chunk %d: %w", i, err))
			}
			total += len(chunk)
		}
		if err := stream.Close(); err != nil {
			panic(fmt.Errorf("close data stream: %w", err))
		}
		obs["stream_opened"] = true
		obs["stream_write_bytes"] = total
		obs["stream_fin_sent"] = true
		readStream(sess, stream, obs)
		closed := observe(sess, aliveWindow)
		for k, v := range closed {
			obs[k] = v
		}
	case "unidir":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		uni, err := sess.OpenUniStreamSync(ctx)
		if err != nil {
			panic(fmt.Errorf("open unidir stream: %w", err))
		}
		payload := []byte("c15-unidir-probe")
		if _, err := uni.Write(payload); err != nil {
			panic(fmt.Errorf("write unidir: %w", err))
		}
		if err := uni.Close(); err != nil {
			panic(fmt.Errorf("close unidir: %w", err))
		}
		obs["unidir_opened"] = true
		obs["unidir_write_bytes"] = len(payload)
		obs["unidir_finish_ok"] = true
		closed := observe(sess, aliveWindow)
		for k, v := range closed {
			obs[k] = v
		}
	case "reconnect":
		// First connection: register.
		regObs1, ctrl1, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl1
		for k, v := range regObs1 {
			obs[k] = v
		}
		w1 := regObs1["worker_id"]
		s1 := regObs1["server_info"]
		closed1 := observe(sess, aliveWindow)
		// Close first session.
		_ = ctrl1.Close()
		_ = sess.CloseWithError(0, "reconnect-test")
		time.Sleep(200 * time.Millisecond)
		// Second connection.
		sess2, upgrade2 := dialOnce(ctx, dialer, *addr, *path, *token)
		regObs2, ctrl2, err := register(ctx, sess2)
		if err != nil {
			panic(err)
		}
		w2 := regObs2["worker_id"]
		s2 := regObs2["server_info"]
		closed2 := observe(sess2, aliveWindow)
		_ = ctrl2.Close()
		_ = sess2.CloseWithError(0, "")
		obs["reconnect_worker_id_shape_match"] = fmt.Sprintf("%v", w1) == fmt.Sprintf("%v", w2)
		obs["reconnect_server_info_match"] = fmt.Sprintf("%v", s1) == fmt.Sprintf("%v", s2)
		obs["second_register"] = regObs2
		obs["second_upgrade"] = upgrade2
		obs["first_session_closed"] = closed1["session_closed"]
		obs["first_close_kind"] = closed1["close_kind"]
		obs["second_session_closed"] = closed2["session_closed"]
		obs["second_close_kind"] = closed2["close_kind"]
		// Skip the final CloseWithError below — already closed.
		out, _ := json.Marshal(obs)
		fmt.Println(string(out))
		return
	case "reliable_prefix":
		registerObs, ctrl, err := register(ctx, sess)
		if err != nil {
			panic(err)
		}
		control = ctrl
		for k, v := range registerObs {
			obs[k] = v
		}
		stream, err := sess.OpenStreamSync(ctx)
		if err != nil {
			panic(fmt.Errorf("open prefix stream: %w", err))
		}
		chunk1 := []byte("c15-reliable-pfx-1")
		chunk2 := []byte("c15-reliable-pfx-2")
		if _, err := stream.Write(chunk1); err != nil {
			panic(fmt.Errorf("write chunk 1: %w", err))
		}
		if _, err := stream.Write(chunk2); err != nil {
			panic(fmt.Errorf("write chunk 2: %w", err))
		}
		stream.CancelWrite(0x170d7b68)
		obs["stream_opened"] = true
		obs["stream_write_bytes"] = len(chunk1) + len(chunk2)
		obs["stream_reset_sent"] = true
		obs["stream_reset_code"] = uint64(0x170d7b68)
		closed := observe(sess, aliveWindow)
		for k, v := range closed {
			obs[k] = v
		}
	default:
		panic(fmt.Errorf("unknown mode %q", *mode))
	}

	runtime.KeepAlive(control)
	_ = sess.CloseWithError(0, "")
	out, _ := json.Marshal(obs)
	fmt.Println(string(out))
}

// readStream reads the extra stream and records how the peer terminated it:
// the echoed byte count, the termination class and - when the peer sent a
// reset - the error code it used.
//
// The termination classes are the vocabulary both probes speak:
//
//	none         - the peer delivered data and left the stream open;
//	eof          - the peer finished the stream (FIN) without a reset;
//	reset        - the peer sent RESET_STREAM with an error code;
//	session_gone - the peer tore the session down and the stream died with
//	               it (WTSessionGoneErrorCode);
//	deadline     - the read deadline elapsed with the stream still open;
//	other        - anything else.
//
// `webtransport-go` v0.12.0 absorbs `WTSessionGoneErrorCode` inside
// `ReceiveStream.Read`: the error is routed to `handleSessionGoneError()`,
// which waits for the session to close and then reports the session's own
// close error instead of the stream error code. The Go probe can therefore
// classify a session teardown (`session_gone`), but it cannot report the
// numeric reset code the way the Rust probe does, because quinn surfaces the
// code verbatim. The driver publishes both observations side by side, so the
// difference is visible and attributable instead of hidden.
func readStream(sess *webtransport.Session, stream *webtransport.Stream, obs map[string]any) {
	_ = stream.SetReadDeadline(time.Now().Add(aliveWindow))
	buf := make([]byte, 256)
	n, err := stream.Read(buf)
	obs["stream_echo_bytes"] = n
	obs["stream_reset"] = false
	obs["stream_reset_code"] = nil
	termination := "other"
	switch {
	case err == nil:
		// The peer echoed data back.
		termination = "none"
	case errors.Is(err, io.EOF):
		termination = "eof"
	case errors.Is(err, os.ErrDeadlineExceeded):
		// The read deadline elapsed. quic-go reports a deadline as
		// `os.ErrDeadlineExceeded`; `webtransport-go` reports the very
		// same error when it absorbed a WTSessionGoneErrorCode and the
		// session never closed, so the session's own state is what tells
		// the two situations apart.
		if sess.Context().Err() != nil {
			termination = "session_gone"
		} else {
			termination = "deadline"
		}
	default:
		var se *webtransport.StreamError
		if errors.As(err, &se) {
			obs["stream_reset"] = true
			obs["stream_reset_code"] = uint64(se.ErrorCode)
			termination = "reset"
			break
		}
		var qse *quic.StreamError
		if errors.As(err, &qse) {
			// A reset with a code outside the WebTransport private range:
			// webtransport-go refuses to convert those into a StreamError.
			obs["stream_reset"] = true
			obs["stream_reset_code"] = uint64(qse.ErrorCode)
			termination = "reset"
			break
		}
		var sessErr *webtransport.SessionError
		if errors.As(err, &sessErr) {
			// The session was closed while the stream was being read: the
			// peer tore the whole session down.
			termination = "session_gone"
			break
		}
	}
	obs["stream_termination"] = termination
}

// refineTermination reconciles the stream's termination class with the
// session's own fate. A read deadline that expires a moment before the
// session teardown is processed is still a session teardown, and the session
// observation (taken right after the stream read) is the authoritative
// witness of it.
// dialOnce establishes one WebTransport session to the server.
func dialOnce(ctx context.Context, dialer *webtransport.Transport, addr, path, token string) (*webtransport.Session, map[string]any) {
	url := fmt.Sprintf("https://%s%s", addr, path)
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	resp, sess, err := dialer.Dial(ctx, url, header)
	if err != nil {
		panic(fmt.Errorf("wt dial failed: %w", err))
	}
	var draft any
	if v := resp.Header.Get("sec-webtransport-http3-draft"); v != "" {
		draft = v
	}
	return sess, map[string]any{"status": resp.StatusCode, "sec_webtransport_http3_draft": draft}
}

// reconnectOnce performs a full register + observe cycle on a fresh connection.
func reconnectOnce(ctx context.Context, dialer *webtransport.Transport, addr, path, token string) (map[string]any, *webtransport.Stream, *webtransport.Session, map[string]any) {
	sess, upgrade := dialOnce(ctx, dialer, addr, path, token)
	regObs, ctrl, err := register(ctx, sess)
	if err != nil {
		panic(err)
	}
	regObs["upgrade"] = upgrade
	return regObs, ctrl, sess, upgrade
}

func refineTermination(obs map[string]any, closed map[string]any) {
	if obs["stream_termination"] != "deadline" {
		return
	}
	switch closed["close_kind"] {
	case "session_error", "connection_closed":
		obs["stream_termination"] = "session_gone"
	}
}

// register performs the agent registration handshake on a fresh control
// stream and returns the contract observation together with the control
// stream. The stream is returned so the caller can keep it open for the rest
// of the session, exactly like a real agent: letting it go out of scope would
// end the agent's control plane, which is a different scenario the `close`
// mode exercises on purpose.
func register(ctx context.Context, sess *webtransport.Session) (map[string]any, *webtransport.Stream, error) {
	control, err := sess.OpenStreamSync(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open control stream: %w", err)
	}

	req := &livekit.WorkerMessage{
		Message: &livekit.WorkerMessage_Register{
			Register: &livekit.RegisterWorkerRequest{
				Type:      livekit.JobType_JT_ROOM,
				Version:   "1.0.0",
				AgentName: "diff-agent",
			},
		},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, nil, err
	}
	if err := writeFrame(control, body); err != nil {
		return nil, nil, fmt.Errorf("write register: %w", err)
	}

	reply, err := readFrame(control)
	if err != nil {
		return nil, nil, fmt.Errorf("read register response: %w", err)
	}
	msg := &livekit.ServerMessage{}
	if err := proto.Unmarshal(reply, msg); err != nil {
		return nil, nil, fmt.Errorf("decode response: %w", err)
	}

	if reg, ok := msg.Message.(*livekit.ServerMessage_Register); ok {
		info := reg.Register.GetServerInfo()
		return map[string]any{
			"response_type": "register",
			"worker_id":     idShape(reg.Register.GetWorkerId()),
			"server_info": map[string]any{
				"edition":         int32(info.GetEdition()),
				"version":         info.GetVersion(),
				"protocol":        info.GetProtocol(),
				"agent_protocol":  info.GetAgentProtocol(),
				"region":          info.GetRegion(),
				"node_id_present": info.GetNodeId() != "",
				"debug_info":      info.GetDebugInfo(),
			},
			"endpoint_settings_present": reg.Register.GetEndpointSettings() != nil,
		}, control, nil
	}
	return map[string]any{
		"response_type": fmt.Sprintf("unexpected:%T", msg.Message),
	}, control, nil
}
