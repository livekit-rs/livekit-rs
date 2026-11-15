// SPEC-0077 C37 — launched-MAIN HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect
// observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon (the cleanPath
// redirect is unconditional, not config driven) and drives deterministic, MEDIA-FREE,
// UNAUTHENTICATED HTTP probes against the main API port, recording the exact
// status/body/content-type/location semantics of each. No track is published, no media
// flows, no WebSocket join.
//
// This is the DISTINCT downstream router concern split out of C36: the
// strip-one-leading-slash MIDDLEWARE (HTTP_BEHAVIOR:middleware.path_normalization) is
// faithful on both sides (///twirp -> //twirp), but Go's http.ServeMux then cleanPath's
// the residual //twirp and issues a 307 to /twirp, whereas the pre-C37 Rust axum router
// had no ServeMux path cleaner and returned 404. C37 adds the faithful Rust cleanPath
// redirect layer; this observer proves the reconciled cross-side parity.
//
// Upstream contract (locked Go v1.13.10 e5655ac2, Go 1.26 stdlib):
//
//	net/http/server.go:
//	  :2593-2612 cleanPath: prepend root if missing, path.Clean, restore a trailing
//	             slash that path.Clean removed (except root). Operates on the ESCAPED
//	             path, so percent-encoded bytes stay literal.
//	  :2676-2697 findHandler (non-CONNECT): path = cleanPath(escapedPath);
//	             matchOrRedirect (trailing-slash) first; then if path != escapedPath
//	             -> RedirectHandler(url.URL{Path: path, RawQuery}.String(),
//	             StatusTemporaryRedirect). BOTH redirects are 307 UNCONDITIONALLY
//	             (GODEBUG=httpmuxgo121 would select the old 301; livekit does not set it).
//	  :2367-2419 Redirect: Location = hexEscapeNonASCII(url); GET/HEAD with no prior
//	             Content-Type get "text/html; charset=utf-8"; only GET writes
//	             `<a href="htmlEscape(url)">Temporary Redirect</a>.\n` + Fprintln newline.
//	  :2665      CONNECT is authority-form and is NOT canonicalized.
//	pkg/service/server.go:
//	  :484-503   api negroni chain: Recovery -> CORS -> RequestBodyLimiter -> APIKeyAuth
//	             -> mux. The mux (cleanPath) is INNERMOST, after auth.
//	  :505-514   agentFront is a SEPARATE chain (AgentRecovery -> AgentCORS -> auth),
//	             NO mux, so agent endpoint paths are NOT cleanPath-redirected.
//	  :515       WithPathNormalization(dispatch) outermost (strip ONE leading slash).
//	  :119,135   mux = http.NewServeMux(); "/" -> defaultHandler. The only registered
//	             pattern ending in "/" is the root catch-all; twirp/rtc/agent are exact
//	             and WHIP is exact + {participant_id} wildcard, so the trailing-slash
//	             matchOrRedirect is vacuous (no exact pattern P+"/" exists for P != "").
//
// Rust mirror (C37 consumer chain, all anchors re-verified):
//
//	lk-compat/src/http.rs:
//	  middleware_chain: recover -> cors -> body_limit -> auth -> serve_mux_redirect ->
//	            routes (serve_mux_redirect_layer is the INNERMOST layer, at the mux
//	            boundary, after auth, before route dispatch).
//	  serve_mux_redirect_layer: CONNECT skip; clean_path(escaped); if cleaned != escaped
//	            -> serve_mux_redirect(method, redirect_url(cleaned, query)).
//	  clean_path / path_clean_rooted: Go cleanPath + rooted path.Clean segment stack.
//	  serve_mux_redirect: 307; GET -> text/html CT + exact html body; HEAD -> CT no body;
//	            other -> Location only. hex_escape_non_ascii / html_escape mirror Go.
//	  agent_endpoint_router: auth -> agent_cors -> recover, NO serve_mux_redirect (Go
//	            agents chain has no mux).
//
// WHY media-free HTTP probes (load-bearing): the cleanPath redirect is a deterministic
// HTTP request-shape consumer on the main API port. It depends only on the request
// method and escaped path, not on rtc config, ICE, SDP or a published track.
//
// The ten probes (request stimulus varies; one default-limit daemon launch per side):
//
//	triple-slash-post            POST ///twirp/.../ListRooms {}
//	                             -> strip ONE -> //twirp -> cleanPath -> 307 /twirp [DISCRIMINATING]
//	triple-slash-get             GET  ///twirp/.../ListRooms
//	                             -> 307 + text/html + exact Go html body            [DISCRIMINATING]
//	triple-slash-head            HEAD ///twirp/.../ListRooms
//	                             -> 307 + text/html, no body                        [DISCRIMINATING]
//	internal-double-slash-post   POST /twirp//livekit.RoomService/ListRooms {}
//	                             -> cleanPath collapses internal // -> 307          [DISCRIMINATING]
//	dot-segment-post             POST /twirp/./livekit.RoomService/ListRooms {}
//	                             -> cleanPath drops "." -> 307                      [DISCRIMINATING]
//	dotdot-segment-post          POST /twirp/foo/../livekit.RoomService/ListRooms {}
//	                             -> cleanPath resolves ".." -> 307                  [DISCRIMINATING]
//	trailing-slash-redirect-post POST /twirp//livekit.RoomService/ListRooms/ {}
//	                             -> 307 Location keeps the trailing slash           [DISCRIMINATING]
//	query-preservation-post      POST /twirp//livekit.RoomService/ListRooms?room=y&x=1 {}
//	                             -> 307 Location keeps the raw query                [DISCRIMINATING]
//	                             (query is NON-auth: an access_token would be rejected
//	                              401 by APIKeyAuth BEFORE the mux cleanPath redirect)
//	clean-path-control-post      POST /twirp/livekit.RoomService/ListRooms {}
//	                             -> cleanPath identity -> real route 401 (non-307)  [GATING]
//	double-slash-control-post    POST //twirp/livekit.RoomService/ListRooms {}
//	                             -> strip ONE -> /twirp -> identity -> 401 (non-307)[GATING]
//
// Falsifiability: a cleanPath accept-but-ignore mutant (serve_mux_redirect_layer always
// next.run) drops every 307; the residual //twirp then 404s and the internal-clean paths
// reach the twirp route (401/404), so all eight DISCRIMINATING profiles FAIL (307 -> non-307)
// while the two GATING controls (already-clean / strip-one-to-clean) SURVIVE at 401.
// Specific, not indiscriminate.
//
// This observer is a SEPARATE copy from the C36 rtc-http-middleware observer: it swaps
// the body-limit/strip-one-slash stimulus for the downstream ServeMux cleanPath redirect
// probes and names the one driven item, keeping the C36 observer identity unchanged
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

const apiKey = "ownedmux"
const apiSecret = "fixture-secret-only-for-local-serve-mux-tests"

// twirpPath is a real registered RoomService method. An unauthenticated POST reaches
// the service auth check (deterministic 401); a path that cleans to a different path is
// 307-redirected by the ServeMux BEFORE the route is reached.
const twirpPath = "/twirp/livekit.RoomService/ListRooms"

// twirpSuffix is twirpPath without the "/twirp/" prefix, so the cleaning stimuli can
// inject an internal "//", "/./" or "/foo/../" between the prefix and the method.
const twirpSuffix = "livekit.RoomService/ListRooms"

// muxProbe is one deterministic media-free HTTP stimulus. body is the exact request
// payload (Content-Length is set to len(body) by net/http); nil for GET/HEAD.
type muxProbe struct {
	name        string
	method      string
	path        string
	body        []byte
	contentType string
}

// The cleanPath redirect is unconditional; one default-limit group suffices.
var groupLimit = map[string]int64{
	"serve-mux-redirect": 10 << 20,
}

var groupProbes = map[string][]muxProbe{
	"serve-mux-redirect": {
		{"triple-slash-post", http.MethodPost, "///twirp/" + twirpSuffix, []byte("{}"), "application/json"},
		{"triple-slash-get", http.MethodGet, "///twirp/" + twirpSuffix, nil, ""},
		{"triple-slash-head", http.MethodHead, "///twirp/" + twirpSuffix, nil, ""},
		{"internal-double-slash-post", http.MethodPost, "/twirp//" + twirpSuffix, []byte("{}"), "application/json"},
		{"dot-segment-post", http.MethodPost, "/twirp/./" + twirpSuffix, []byte("{}"), "application/json"},
		{"dotdot-segment-post", http.MethodPost, "/twirp/foo/../" + twirpSuffix, []byte("{}"), "application/json"},
		{"trailing-slash-redirect-post", http.MethodPost, "/twirp//" + twirpSuffix + "/", []byte("{}"), "application/json"},
		{"query-preservation-post", http.MethodPost, "/twirp//" + twirpSuffix + "?room=y&x=1", []byte("{}"), "application/json"},
		{"clean-path-control-post", http.MethodPost, twirpPath, []byte("{}"), "application/json"},
		{"double-slash-control-post", http.MethodPost, "//twirp/" + twirpSuffix, []byte("{}"), "application/json"},
	},
}

var groupItems = map[string][]string{
	"serve-mux-redirect": {"HTTP_BEHAVIOR:routing.serve_mux_path_clean_redirect"},
}

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	group := flag.String("group", "", "config group: serve-mux-redirect")
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
	// Do NOT follow redirects: the 307 from the Go ServeMux path cleaner is itself the
	// observable under test, so the client must record it raw.
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
	// Settle past the init window before probing/shutting down (mirrors C35/C36).
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

func runOneProbe(client *http.Client, base string, probe muxProbe) (map[string]any, error) {
	var bodyReader io.Reader
	if probe.body != nil {
		bodyReader = bytes.NewReader(probe.body)
	}
	req, err := http.NewRequest(probe.method, base+probe.path, bodyReader)
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
	return normalizeMux(resp.StatusCode, resp.Header, body), nil
}

// normalizeMux extracts ONLY the deterministic response semantics: status, the raw
// body text, Content-Type and Location (the ServeMux path-clean redirect target is
// itself the observable). Header names are canonicalized by net/http.
func normalizeMux(status int, h http.Header, body []byte) map[string]any {
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