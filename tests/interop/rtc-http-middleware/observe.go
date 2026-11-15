// SPEC-0077 C36 — launched-MAIN HTTP_BEHAVIOR:middleware.body_limit +
// HTTP_BEHAVIOR:middleware.path_normalization observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon per --group
// and drives that group's deterministic, MEDIA-FREE, UNAUTHENTICATED HTTP probes
// against the main API port, recording the exact status/body/content-type/location
// semantics of each. No track is published, no media flows, no WebSocket join.
//
// Unlike C35 CORS (unconditional middleware, one launch), body_limit IS config
// driven (limit.max_api_request_body_size), so the harness launches the daemon
// once per config group. path_normalization is unconditional but is exercised
// under its own default-limit group for stimulus isolation. Three groups:
//
//	body-limit-small     limit.max_api_request_body_size=1024
//	body-limit-zero      limit.max_api_request_body_size=0   (unbounded)
//	path-normalization   limit.max_api_request_body_size=10<<20 (default)
//
// Upstream contract (locked Go v1.13.10 e5655ac2):
//
//	pkg/service/utils.go:
//	  :49       ErrRequestBodyTooLarge = errors.New("request body too large")
//	  :103-113  RemoveDoubleSlashes: if strings.HasPrefix(r.URL.EscapedPath(),"//")
//	            { r.URL.Path = r.URL.Path[1:]; if RawPath!="" { RawPath = RawPath[1:] } }
//	            (strip exactly ONE leading slash, Path and RawPath move together).
//	  :117-121  WithPathNormalization wraps RemoveDoubleSlashes ahead of the handler.
//	  :131-154  RequestBodyLimiter: if maxBytes<=0 || Body==nil { next; return };
//	            if r.ContentLength > maxBytes { HandleError(413, ErrRequestBodyTooLarge);
//	            return }; r.Body = http.MaxBytesReader(w, r.Body, maxBytes); next.
//	pkg/service/server.go:
//	  :490      NewRequestBodyLimiter(conf.Limit.MaxAPIRequestBodySize)
//	  :484-503  api negroni chain: Recovery -> CORS -> RequestBodyLimiter -> APIKeyAuth
//	            -> mux (body limiter BEFORE auth; declared-size 413 precedes decode).
//	  :515      return WithPathNormalization(dispatch) (normalization outermost).
//	  :119,135  mux = http.NewServeMux(); mux.HandleFunc("/", defaultHandler).
//	pkg/config/config.go:
//	  :104,397  Limit.MaxAPIRequestBodySize int64 `yaml:"max_api_request_body_size"`
//	  :631      default 10 << 20 (10 MiB); 0 disables the limit (unbounded).
//
// Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
//
//	lk-compat/src/http.rs:
//	  :37-56    router(): api=middleware_chain(routes, keys, config.http.max_request_body);
//	            normalize_path_layer outermost.
//	  :94-104   middleware_chain: recover -> cors -> body_limit -> auth -> routes.
//	  :149-164  normalize_path_layer / strip_double_slash: strip ONE leading '/' when
//	            path_and_query starts with "//" (PathAndQuery::try_from(&pq[1..])).
//	  :273-287  body_limit_layer: if max>0 { declared Content-Length > max -> 413
//	            "request body too large"; else wrap body with lazy bound max }.
//	  :132-142  not_found(): 404 "404 page not found\n".
//	lk-compat/src/twirp/mod.rs:
//	  :102-112  handle(): Service::for_path else not_found (404).
//	  :114-196  serve(): non-POST -> bad_route(404); body read with limit
//	            (config.http.max_request_body, 0 -> usize::MAX) BEFORE service auth.
//	  :188-196  body_reader::json(body, limit): ReadError::Limit -> codec.body_read_error().
//	lk-compat/src/twirp/codec.rs:42-47  body_read_error(): Json -> malformed(400).
//	lk-compat/src/twirp/error.rs:65     Code::Malformed -> HTTP 400.
//	lk-config/src/official.rs:1049      max_api_request_body_size: i64 = 10 << 20.
//	lk-config/src/normalize.rs:437      max_request_body = limit.max_api_request_body_size.max(0).
//	rtc-core/src/config.rs:127,935      pub max_request_body: u64 (default 10 << 20).
//
// WHY media-free HTTP probes (load-bearing): both middlewares are deterministic
// HTTP request-shape consumers on the main API port. body_limit rejects an
// over-limit DECLARED Content-Length with 413 before auth/decode; path_normalization
// strips one leading slash so //twirp/<svc>/<method> reaches the real route. Neither
// depends on rtc config, ICE, SDP or a published track.
//
// The eight probes (request stimulus varies; daemon config per group):
//
//	body-oversize-declared      [body-limit-small]  POST /twirp/.../ListRooms CL=2048
//	                            -> 413 "request body too large" (before auth)  [DISCRIMINATING]
//	body-undersize-declared     [body-limit-small]  POST /twirp/.../ListRooms CL=2 {}
//	                            -> not 413 (auth/decode path)                  [GATING]
//	body-boundary-equal         [body-limit-small]  POST /twirp/.../ListRooms CL=1024
//	                            -> not 413 (Go check is '>', not '>=')         [GATING]
//	body-zero-unlimited         [body-limit-zero]   POST /twirp/.../ListRooms CL=2048
//	                            -> not 413 (limit 0 disables the limiter)      [GATING]
//	path-double-slash-twirp     [path-normalization] POST //twirp/.../ListRooms {}
//	                            -> normalized /twirp -> real route (non-404)   [DISCRIMINATING]
//	path-single-slash-twirp     [path-normalization] POST /twirp/.../ListRooms {}
//	                            -> real route (non-404)                        [CONTROL]
//	path-triple-slash-twirp     [path-normalization] POST ///twirp/.../ListRooms {}
//	                            -> strip ONE -> //twirp (exact-one boundary)   [BOUNDARY]
//	path-double-slash-unknown   [path-normalization] POST //nonexistent-path {}
//	                            -> normalized /nonexistent -> 404              [GATING]
//
// Falsifiability: a body_limit accept-but-ignore mutant (body_limit_layer no-op)
// drops the declared-size 413; the oversize request then reaches the twirp body
// reader whose own limit yields malformed(400) — so body-oversize-declared FAILS
// (413 -> 400) while the three gating body probes SURVIVE. A path_normalization
// accept-but-ignore mutant (strip_double_slash -> None) leaves //twirp unnormalized
// -> 404, so path-double-slash-twirp FAILS (non-404 -> 404) while the single-slash
// control and the unknown-path gating probe SURVIVE. Specific, not indiscriminate.
//
// This observer is a SEPARATE copy from the C35 rtc-http-cors observer: it swaps
// the CORS header stimulus for body-limit/path-normalization request-shape probes
// and names the two driven items, keeping the C35 observer identity unchanged
// (anti-splicing).
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const apiKey = "ownedmw"
const apiSecret = "fixture-secret-only-for-local-middleware-tests"

// twirpPath is a real registered RoomService method. An unauthenticated POST reaches
// the service auth check (deterministic non-404); an over-limit declared body is
// rejected with 413 before the route is even reached.
const twirpPath = "/twirp/livekit.RoomService/ListRooms"

// mwProbe is one deterministic media-free HTTP stimulus. body is the exact request
// payload (Content-Length is set to len(body) by net/http).
type mwProbe struct {
	name        string
	method      string
	path        string
	body        []byte
	contentType string
}

// jsonBodyOfSize builds a single valid JSON object of exactly n bytes whose closing
// brace is the final byte, so the twirp JSON frame scanner must consume the whole
// body (it cannot early-return on a completed container before the limit).
func jsonBodyOfSize(n int) []byte {
	const prefix = `{"pad":"`
	const suffix = `"}`
	pad := n - len(prefix) - len(suffix)
	if pad < 0 {
		pad = 0
	}
	return []byte(prefix + strings.Repeat("x", pad) + suffix)
}

var groupLimit = map[string]int64{
	"body-limit-small":   1024,
	"body-limit-zero":    0,
	"path-normalization": 10 << 20,
}

var groupProbes = map[string][]mwProbe{
	"body-limit-small": {
		{"body-oversize-declared", http.MethodPost, twirpPath, jsonBodyOfSize(2048), "application/json"},
		{"body-undersize-declared", http.MethodPost, twirpPath, []byte("{}"), "application/json"},
		{"body-boundary-equal", http.MethodPost, twirpPath, jsonBodyOfSize(1024), "application/json"},
	},
	"body-limit-zero": {
		{"body-zero-unlimited", http.MethodPost, twirpPath, jsonBodyOfSize(2048), "application/json"},
	},
	"path-normalization": {
		{"path-double-slash-twirp", http.MethodPost, "//" + twirpPath[1:], []byte("{}"), "application/json"},
		{"path-single-slash-twirp", http.MethodPost, twirpPath, []byte("{}"), "application/json"},
		{"path-triple-slash-twirp", http.MethodPost, "///" + twirpPath[1:], []byte("{}"), "application/json"},
		{"path-double-slash-unknown", http.MethodPost, "//nonexistent-path", []byte("{}"), "application/json"},
	},
}

var groupItems = map[string][]string{
	"body-limit-small":   {"HTTP_BEHAVIOR:middleware.body_limit"},
	"body-limit-zero":    {"HTTP_BEHAVIOR:middleware.body_limit"},
	"path-normalization": {"HTTP_BEHAVIOR:middleware.path_normalization"},
}

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	group := flag.String("group", "", "config group: body-limit-small|body-limit-zero|path-normalization")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	flag.Parse()
	if *binary == "" || *out == "" || *group == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --group G [--enable-loopback=true|false]")
		os.Exit(2)
	}
	if _, ok := groupProbes[*group]; !ok {
		fmt.Fprintf(os.Stderr, "unknown --group %q\n", *group)
		os.Exit(2)
	}
	record := map[string]any{
		"group":                     *group,
		"enable_loopback_candidate": *enableLoopback,
		"configured_body_limit":     groupLimit[*group],
		"driven_items":              groupItems[*group],
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fatal(record, *out, err)
	}
	before, err := hashFile(*binary)
	if err != nil {
		fatal(record, *out, err)
	}
	record["binary_sha256_before"] = before
	runErr := runProbes(*binary, *out, *group, *enableLoopback, record)
	after, hashErr := hashFile(*binary)
	record["binary_sha256_after"] = after
	if hashErr != nil {
		runErr = errors.Join(runErr, hashErr)
	}
	record["binary_unchanged"] = before == after
	if runErr != nil {
		record["error"] = runErr.Error()
	}
	writeResult(record, *out)
	if runErr != nil {
		os.Exit(1)
	}
}

func fatal(record map[string]any, out string, err error) {
	record["error"] = err.Error()
	writeResult(record, out)
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func writeResult(record map[string]any, out string) {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(out, "result.json"), append(data, '\n'), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func hashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:]), nil
}

// reserveTCP binds a free TCP port on loopback for the daemon HTTP/signal port (not
// a media port under test, so an ephemeral port is fine).
func reserveTCP() (int, net.Listener, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, nil, err
	}
	return l.Addr().(*net.TCPAddr).Port, l, nil
}

// Media ports are reserved in a LOW band (40000-40999), decisively below the macOS
// ephemeral range (49152-65535). The media-free probes never bind them, but a valid
// isolated rtc config keeps the daemon launch deterministic and off others' ports.
const lowBandStart = 40000
const lowBandEnd = 40999

func reserveLowUDP() (int, net.PacketConn, error) {
	for p := lowBandStart; p <= lowBandEnd; p++ {
		c, err := net.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			return p, c, nil
		}
	}
	return 0, nil, fmt.Errorf("no free UDP port in low band %d-%d", lowBandStart, lowBandEnd)
}

func reserveLowTCP() (int, net.Listener, error) {
	for p := lowBandStart; p <= lowBandEnd; p++ {
		l, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			return p, l, nil
		}
	}
	return 0, nil, fmt.Errorf("no free TCP port in low band %d-%d", lowBandStart, lowBandEnd)
}

func runProbes(binary, out, group string, enableLoopback bool, record map[string]any) (err error) {
	httpPort, httpListener, err := reserveTCP()
	if err != nil {
		return err
	}
	defer httpListener.Close()
	address := httpListener.Addr().String()

	rtc := map[string]any{
		"node_ip":                   "127.0.0.1",
		"use_external_ip":           false,
		"enable_loopback_candidate": enableLoopback,
	}
	var closers []net.Listener
	var packetClosers []net.PacketConn
	rebindTCP := []string{address}
	defer func() {
		for _, c := range closers {
			c.Close()
		}
		for _, c := range packetClosers {
			c.Close()
		}
	}()

	p, c, e := reserveLowUDP()
	if e != nil {
		return e
	}
	packetClosers = append(packetClosers, c)
	rtc["port_range_start"] = p
	rtc["port_range_end"] = p + 9
	record["configured_port_range_start"] = p
	record["configured_port_range_end"] = p + 9

	tp, tl, e := reserveLowTCP()
	if e != nil {
		return e
	}
	closers = append(closers, tl)
	rtc["tcp_port"] = tp
	rebindTCP = append(rebindTCP, fmt.Sprintf("127.0.0.1:%d", tp))
	record["configured_tcp_port"] = tp

	config := map[string]any{
		"port":           httpPort,
		"bind_addresses": []string{"127.0.0.1"},
		"keys":           map[string]string{apiKey: apiSecret},
		"rtc":            rtc,
		"limit":          map[string]any{"max_api_request_body_size": groupLimit[group]},
	}
	cfg, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	cfgPath := filepath.Join(out, "server.yaml")
	if err = os.WriteFile(cfgPath, cfg, 0600); err != nil {
		return err
	}
	record["config_sha256"] = fmt.Sprintf("%x", sha256.Sum256(cfg))

	logPath := filepath.Join(out, "server.log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	command := exec.Command(binary, "--config", cfgPath)
	command.Stdout, command.Stderr = file, file
	for _, varEntry := range os.Environ() {
		if strings.HasPrefix(varEntry, "OTEL_") || strings.HasPrefix(varEntry, "SSL_CERT_") {
			continue
		}
		command.Env = append(command.Env, varEntry)
	}
	// Release every reservation so the daemon can bind the configured ports.
	if err = httpListener.Close(); err != nil {
		return err
	}
	for _, c := range closers {
		if e := c.Close(); e != nil {
			return e
		}
	}
	for _, c := range packetClosers {
		if e := c.Close(); e != nil {
			return e
		}
	}
	if err = command.Start(); err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	early := false
	defer func() {
		var shutdown error
		if !early {
			shutdown = command.Process.Signal(syscall.SIGTERM)
			select {
			case e := <-exited:
				shutdown = errors.Join(shutdown, e)
			case <-time.After(8 * time.Second):
				shutdown = errors.Join(shutdown, fmt.Errorf("daemon shutdown timeout"), command.Process.Kill(), <-exited)
			}
		}
		record["server_exit"] = command.ProcessState.ExitCode()
		var bindErr error
		for _, addr := range rebindTCP {
			rebound, e := net.Listen("tcp4", addr)
			if e == nil {
				e = rebound.Close()
			}
			bindErr = errors.Join(bindErr, e)
		}
		record["server_ports_released"] = bindErr == nil
		if data, e := os.ReadFile(logPath); e == nil {
			record["server_log"] = string(data)
		}
		err = errors.Join(err, shutdown, bindErr)
	}()

	base := "http://" + address
	// Do NOT follow redirects: a 301 from the Go ServeMux path cleaner is itself an
	// observable (the exact-one-slash boundary), so the client must record it raw.
	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	until := time.Now().Add(8 * time.Second)
	ready := false
	for {
		select {
		case e := <-exited:
			early = true
			record["refused"] = true
			record["early_exit_error"] = fmt.Sprintf("%v", e)
			record["probes"] = map[string]any{}
			return nil
		default:
		}
		response, e := client.Get(base + "/")
		if e == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		if time.Now().After(until) {
			return fmt.Errorf("daemon readiness timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	record["refused"] = false
	if !ready {
		return fmt.Errorf("daemon never became ready")
	}
	// The Go daemon serves HTTP (server.go:327-332) BEFORE it finishes Start():
	// there is a 100ms sleep (server.go:343) then running.Store(true) (:345). A
	// SIGTERM that lands in that window makes Stop() early-return at :390
	// (!running.Swap(false)) without closing doneChan, so Start() blocks forever.
	// Settle past the init window before probing/shutting down (mirrors C35).
	time.Sleep(1 * time.Second)
	return exerciseProbes(client, base, group, record)
}

// exerciseProbes drives every deterministic media-free probe of the group against
// the launched daemon and records the normalized response semantics of each.
func exerciseProbes(client *http.Client, base, group string, record map[string]any) error {
	probes := map[string]any{}
	var firstErr error
	for _, probe := range groupProbes[group] {
		obs, err := runOneProbe(client, base, probe)
		probes[probe.name] = obs
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("probe %s: %w", probe.name, err)
		}
	}
	record["probes"] = probes
	record["probe_names"] = probeNames(group)
	return firstErr
}

func probeNames(group string) []string {
	names := make([]string, 0, len(groupProbes[group]))
	for _, p := range groupProbes[group] {
		names = append(names, p.name)
	}
	return names
}

func runOneProbe(client *http.Client, base string, probe mwProbe) (map[string]any, error) {
	req, err := http.NewRequest(probe.method, base+probe.path, bytes.NewReader(probe.body))
	if err != nil {
		return map[string]any{"error": err.Error()}, err
	}
	if probe.contentType != "" {
		req.Header.Set("Content-Type", probe.contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]any{"error": err.Error()}, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if readErr != nil {
		return map[string]any{"error": readErr.Error()}, readErr
	}
	return normalizeMiddleware(resp.StatusCode, resp.Header, body), nil
}

// normalizeMiddleware extracts ONLY the deterministic response semantics: status,
// the raw body text, Content-Type and Location (a ServeMux path-clean redirect is
// itself an observable). Header names are canonicalized by net/http.
func normalizeMiddleware(status int, h http.Header, body []byte) map[string]any {
	return map[string]any{
		"status":       status,
		"body":         string(body),
		"content_type": firstOrEmpty(h, "Content-Type"),
		"location":     firstOrEmpty(h, "Location"),
	}
}

func firstOrEmpty(h http.Header, name string) string {
	v := h.Values(name)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}