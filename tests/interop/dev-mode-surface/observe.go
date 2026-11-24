// SPEC-0077 P2 — YAML:debug_handler / YAML:debug_handler.port / base_flags:--dev
// launched-MAIN observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle v1.13.10 or the Rust livecloud release candidate). It
// launches the REAL main server once per group and drives deterministic, MEDIA-FREE
// HTTP probes against BOTH the main API port and, when configured, the dedicated
// debug-handler port, recording the dispatch semantics of each response.
//
// Upstream contract (locked Go v1.13.10 e5655ac2, Go 1.26 stdlib):
//
//	pkg/config/config.go:107        Development bool `yaml:"development,omitempty"`.
//	pkg/config/config.go:1106-1107  `--dev` CLI flag -> conf.Development = c.Bool("dev").
//	pkg/service/server.go:119-125   mux := http.NewServeMux(); if conf.Development {
//	                                      mux = http.DefaultServeMux
//	                                      mux.HandleFunc("/debug/goroutine", s.debugGoroutines)
//	                                      mux.HandleFunc("/debug/rooms", s.debugInfo) }
//	                               Development therefore swaps the API mux to the
//	                               process DefaultServeMux, which net/http/pprof's init()
//	                               already filled with /debug/pprof/{,cmdline,profile,
//	                               symbol,trace}. Production builds a FRESH mux with none
//	                               of them, so /debug/* falls through to mux.HandleFunc("/",
//	                               s.defaultHandler) -> http.NotFound (404 "404 page not
//	                               found\n" + X-Content-Type-Options: nosniff).
//	pkg/service/server.go:170-186   if conf.DebugHandler.Port > 0 -> a SEPARATE debugMux
//	                               (fresh, not DefaultServeMux) with the five pprof
//	                               patterns + /debug/goroutine + /debug/rooms, served by
//	                               s.debugServer on that port. debug_handler.port is thus
//	                               mode-INDEPENDENT, and because that mux registers no "/"
//	                               catch-all, an unknown path there gets the ServeMux's own
//	                               notFound (same 404 rendering as defaultHandler).
//	pkg/service/server.go:422-441   debugGoroutines = pprof.Lookup("goroutine").WriteTo(w,2);
//	                               debugInfo = json.Marshal([]map[string]any of
//	                               room.DebugInfo()) written WITHOUT Content-Type, so Go
//	                               sniffs it: a bare JSON array -> "text/plain; charset=utf-8".
//	net/http/server.go:2676-2697    ServeMux cleanPath + subtree matchOrRedirect: an exact
//	                               request for "/debug/pprof" against the "/debug/pprof/"
//	                               pattern is a 307 Temporary Redirect to "/debug/pprof/"
//	                               (new ServeMux default; GODEBUG=httpmuxgo121 is not set).
//	net/http/pprof/pprof.go init()  registers its five patterns on DefaultServeMux with a
//	                               "GET " method prefix unless GODEBUG=httpmuxgo121=1, which
//	                               livekit never sets; net/http's HEAD-shares-GET rule makes
//	                               HEAD match too, but POST/PUT/OPTIONS match NO pprof
//	                               pattern. ServeMux answers 405 for a method-only miss only
//	                               when nothing else matches, and the API mux also registers
//	                               the rooted "/" catch-all, so the lookup finds that and the
//	                               response is the plain 404 page. The subtree's trailing-slash
//	                               307 is gated by the same registration, so POST
//	                               /debug/pprof is 404 while HEAD is the 307. livekit's own
//	                               mux.HandleFunc calls (server.go:172-178) pass no method
//	                               prefix, so on the DEDICATED debug port every method reaches
//	                               the handler and the 307 is offered to POST too.
//	                               Measured on the locked oracle (dev mode, API port):
//
//	                      GET   HEAD  POST  PUT   OPTIONS
//	/debug/pprof          307   307   404   404   404
//	/debug/pprof/         200   200   404   404   404
//	/debug/pprof/cmdline  200   200   404   404   404
//	/debug/pprof/heap     200   200   404   404   404
//	/debug/rooms          200   200   200   200   200
//	/debug/goroutine      200   200   200   200   200
//	/debug/nope, /other   404   404   404   404   404
//
//	HEAD bodies are suppressed on the wire by both sides (Go's ResponseWriter, and hyper's
//	server encoder forcing a zero-length body for HEAD at vendor/hyper/src/proto/h1/role.rs:977)
//	while the Content-Length the handler produced survives, so body and content-length stay
//	comparable there.
//	pkg/service/server.go:484-503   the api chain is Recovery->CORS->BodyLimiter->APIKeyAuth
//	                               ->mux, so the debug patterns sit INSIDE auth: a request
//	                               with no credentials reaches them (Go's auth middleware
//	                               only rejects a request that CARRIES a token), while a
//	                               malformed bearer token is rejected 401 before dispatch.
//
// Rust mirror (this candidate):
//
//	livecloud/src/server.rs:141-156 http_app(state, manager, development) merges
//		livecloud/src/debug.rs:46-72 debug::api_routes(manager) — the same seven patterns
//		plus the explicit "/debug/pprof" slash redirect — into the API route table ONLY when
//		development, then hands the result to
//		lk-compat/src/http.rs:39     api_router(state, mux) = middleware_chain(...(mux)) —
//		auth/CORS/body-limit/recovery and the ServeMux 404 fallback stay OUTSIDE, as upstream.
//	livecloud/src/server.rs:764,775-777 debug_handler.port -> a bound listener; :944-945
//		crate::debug::router(manager) serves the dedicated debug mux on it.
//
// Groups (the launch stimulus is the mode/port config; the request stimulus is the probe):
//
//	api-prod           production daemon, no debug_handler.port. Every /debug/* on the API
//	                   port must be the 404 page.
//	api-dev-flag       the same daemon started with the `--dev` CLI flag: /debug/rooms,
//	                   /debug/goroutine, /debug/pprof/ become live and /debug/pprof
//	                   becomes a 307.
//	api-dev-yaml       `development: true` in the config file, no flag: must behave exactly
//	                   like api-dev-flag (the flag and the YAML leaf feed one consumer).
//	debug-port-set     production daemon WITH debug_handler.port: API port still 404s while
//	                   the debug port serves the same patterns — mode independence.
//	debug-port-zero  production daemon without it: the debug port has no listener at all
//	                   (connect failure), while the API port still 404s.
//
// Falsifiability: a mutant that always passes development=false (or drops the debug merge)
// keeps api-prod and debug-port-set green and FAILS every api-dev-* debug probe (307/200
// -> 404); a mutant that ignores debug_handler.port FAILS only debug-port-set. Specific.
//
// Known intentional divergence, recorded and NOT asserted equal: the goroutine/pprof
// PROFILE bodies are implementation-specific state (Go runtime goroutines vs the Rust
// rpprof native-thread sampler, whose header says so) and some named profiles are still
// 501 in Rust. Those probes record the measured status/content-type on both sides for
// transparency, and the harness keeps them out of the equality set with the reason.
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

const apiKey = "owneddevmux"
const apiSecret = "fixture-secret-only-for-local-dev-mux-tests"

const twirpPath = "/twirp/livekit.RoomService/ListRooms"

type probe struct {
	name   string
	target string // "api" or "debug"
	method string
	path   string
	body   []byte
	header map[string]string
}

type group struct {
	devFlag   bool
	devYAML   bool
	debugPort bool
	// devGenFlag and debugPortFlag stimulate the generated CLI spellings of the same two
	// settings (`--development=true`, `--debug_handler.port=N`) with nothing in the config
	// file, so a parser that only reads the YAML half cannot pass these groups.
	devGenFlag    bool
	debugPortFlag bool
	probes        []probe
}

// apiProbes is the shared API-port stimulus. The first two entries are GATING (they must
// be identical in every mode: the health route and the unauthenticated twirp error), the
// /debug/* entries are DISCRIMINATING (their dispatch depends on the mode), and
// rooms-bearer checks that the debug patterns sit inside the auth middleware.
var apiProbes = []probe{
	{"health-get", "api", http.MethodGet, "/", nil, nil},
	{"twirp-unauth-post", "api", http.MethodPost, twirpPath, []byte("{}"), nil},
	{"rooms-get", "api", http.MethodGet, "/debug/rooms", nil, nil},
	{"rooms-head", "api", http.MethodHead, "/debug/rooms", nil, nil},
	{"rooms-post", "api", http.MethodPost, "/debug/rooms", nil, nil},
	{"rooms-put", "api", http.MethodPut, "/debug/rooms", nil, nil},
	{"goroutine-get", "api", http.MethodGet, "/debug/goroutine", nil, nil},
	{"goroutine-head", "api", http.MethodHead, "/debug/goroutine", nil, nil},
	{"goroutine-post", "api", http.MethodPost, "/debug/goroutine", nil, nil},
	{"pprof-slash-get", "api", http.MethodGet, "/debug/pprof", nil, nil},
	{"pprof-slash-head", "api", http.MethodHead, "/debug/pprof", nil, nil},
	{"pprof-slash-post", "api", http.MethodPost, "/debug/pprof", nil, nil},
	{"pprof-slash-options", "api", http.MethodOptions, "/debug/pprof", nil, nil},
	{"pprof-index-get", "api", http.MethodGet, "/debug/pprof/", nil, nil},
	{"pprof-index-head", "api", http.MethodHead, "/debug/pprof/", nil, nil},
	{"pprof-index-post", "api", http.MethodPost, "/debug/pprof/", nil, nil},
	{"pprof-index-put", "api", http.MethodPut, "/debug/pprof/", nil, nil},
	{"pprof-cmdline-get", "api", http.MethodGet, "/debug/pprof/cmdline", nil, nil},
	{"pprof-cmdline-head", "api", http.MethodHead, "/debug/pprof/cmdline", nil, nil},
	{"pprof-cmdline-post", "api", http.MethodPost, "/debug/pprof/cmdline", nil, nil},
	{"pprof-cmdline-options", "api", http.MethodOptions, "/debug/pprof/cmdline", nil, nil},
	{"pprof-heap-get", "api", http.MethodGet, "/debug/pprof/heap", nil, nil},
	{"pprof-heap-head", "api", http.MethodHead, "/debug/pprof/heap", nil, nil},
	{"pprof-heap-post", "api", http.MethodPost, "/debug/pprof/heap", nil, nil},
	{"unknown-debug-get", "api", http.MethodGet, "/debug/nope", nil, nil},
	{"unknown-debug-post", "api", http.MethodPost, "/debug/nope", nil, nil},
	{"other-get", "api", http.MethodGet, "/other", nil, nil},
	{"other-post", "api", http.MethodPost, "/other", nil, nil},
	{"rooms-bearer-get", "api", http.MethodGet, "/debug/rooms", nil,
		map[string]string{"Authorization": "Bearer not-a-jwt"}},
}

var debugProbes = []probe{
	{"debug-rooms-get", "debug", http.MethodGet, "/debug/rooms", nil, nil},
	{"debug-rooms-post", "debug", http.MethodPost, "/debug/rooms", nil, nil},
	{"debug-goroutine-get", "debug", http.MethodGet, "/debug/goroutine", nil, nil},
	{"debug-goroutine-post", "debug", http.MethodPost, "/debug/goroutine", nil, nil},
	{"debug-pprof-slash-get", "debug", http.MethodGet, "/debug/pprof", nil, nil},
	{"debug-pprof-slash-post", "debug", http.MethodPost, "/debug/pprof", nil, nil},
	{"debug-pprof-index-get", "debug", http.MethodGet, "/debug/pprof/", nil, nil},
	{"debug-pprof-index-post", "debug", http.MethodPost, "/debug/pprof/", nil, nil},
	{"debug-pprof-cmdline-get", "debug", http.MethodGet, "/debug/pprof/cmdline", nil, nil},
	{"debug-pprof-cmdline-head", "debug", http.MethodHead, "/debug/pprof/cmdline", nil, nil},
	{"debug-pprof-cmdline-post", "debug", http.MethodPost, "/debug/pprof/cmdline", nil, nil},
	{"debug-pprof-heap-post", "debug", http.MethodPost, "/debug/pprof/heap", nil, nil},
	{"debug-unknown-get", "debug", http.MethodGet, "/debug/nope", nil, nil},
}

var groups = map[string]group{
	"api-prod":       {probes: apiProbes},
	"api-dev-flag":   {devFlag: true, probes: apiProbes},
	"api-dev-yaml":   {devYAML: true, probes: apiProbes},
	"api-dev-genflag": {devGenFlag: true, probes: apiProbes},
	"debug-port-set": {debugPort: true, probes: append(append([]probe{}, apiProbes...), debugProbes...)},
	"debug-port-genflag": {debugPortFlag: true, probes: append(append([]probe{}, apiProbes...), debugProbes...)},
	"debug-port-zero":    {probes: append(append([]probe{}, apiProbes...), debugProbes...)},
}

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	name := flag.String("group", "", "group: api-prod|api-dev-flag|api-dev-yaml|api-dev-genflag|debug-port-set|debug-port-genflag|debug-port-zero")
	flag.Parse()
	if *binary == "" || *out == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --group G")
		os.Exit(2)
	}
	g, ok := groups[*name]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown --group %q\n", *name)
		os.Exit(2)
	}
	record := map[string]any{
		"group":             *name,
		"dev_flag":          g.devFlag,
		"dev_yaml":          g.devYAML,
		"dev_gen_flag":      g.devGenFlag,
		"debug_port_set":    g.debugPort,
		"debug_port_genflag": g.debugPortFlag,
		"probe_names":       probeNames(g),
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fatal(record, *out, err)
	}
	before, err := hashFile(*binary)
	if err != nil {
		fatal(record, *out, err)
	}
	record["binary_sha256_before"] = before
	runErr := runGroup(*binary, *out, g, record)
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
	if err = os.WriteFile(filepath.Join(out, "result.json"), append(data, '\n'), 0600); err != nil {
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

func probeNames(g group) []string {
	names := make([]string, 0, len(g.probes))
	for _, p := range g.probes {
		names = append(names, p.name)
	}
	return names
}

// reserveTCP binds a free loopback TCP port so the daemon's API port is isolated
// without racing another lane.
func reserveTCP() (int, net.Listener, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, nil, err
	}
	return l.Addr().(*net.TCPAddr).Port, l, nil
}

// Media and debug ports are reserved in a LOW band (40000-40999), decisively below
// the macOS ephemeral range (49152-65535).
const lowBandStart = 40000
const lowBandEnd = 40999

func reserveLow(kind string) (int, io.Closer, error) {
	for p := lowBandStart; p <= lowBandEnd; p++ {
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		if kind == "udp" {
			if c, err := net.ListenPacket("udp4", addr); err == nil {
				return p, c, nil
			}
			continue
		}
		if l, err := net.Listen("tcp4", addr); err == nil {
			return p, l, nil
		}
	}
	return 0, nil, fmt.Errorf("no free %s port in low band %d-%d", kind, lowBandStart, lowBandEnd)
}

func runGroup(binary, out string, g group, record map[string]any) (err error) {
	httpPort, httpListener, err := reserveTCP()
	if err != nil {
		return err
	}
	defer httpListener.Close()
	address := httpListener.Addr().String()

	var closers []io.Closer
	rebindTCP := []string{address}
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	rtc := map[string]any{
		"node_ip":                   "127.0.0.1",
		"use_external_ip":           false,
		"enable_loopback_candidate": true,
	}
	udpPort, udpCloser, e := reserveLow("udp")
	if e != nil {
		return e
	}
	closers = append(closers, udpCloser)
	rtc["port_range_start"] = udpPort
	rtc["port_range_end"] = udpPort + 9
	record["configured_port_range_start"] = udpPort
	record["configured_port_range_end"] = udpPort + 9

	tcpPort, tcpCloser, e := reserveLow("tcp")
	if e != nil {
		return e
	}
	closers = append(closers, tcpCloser)
	rtc["tcp_port"] = tcpPort
	rebindTCP = append(rebindTCP, fmt.Sprintf("127.0.0.1:%d", tcpPort))
	record["configured_tcp_port"] = tcpPort

	config := map[string]any{
		"port":           httpPort,
		"bind_addresses": []string{"127.0.0.1"},
		"keys":           map[string]string{apiKey: apiSecret},
		"rtc":            rtc,
		"limit":          map[string]any{"max_api_request_body_size": 10 << 20},
	}
	if g.devYAML {
		config["development"] = true
	}
	debugAddress := ""
	debugPort := 0
	if g.debugPort || g.debugPortFlag {
		reserved, closer, e2 := reserveLow("tcp")
		if e2 != nil {
			return e2
		}
		closers = append(closers, closer)
		debugPort = reserved
		debugAddress = fmt.Sprintf("127.0.0.1:%d", debugPort)
		rebindTCP = append(rebindTCP, debugAddress)
		if g.debugPort {
			config["debug_handler"] = map[string]any{"port": debugPort}
		}
		record["configured_debug_port"] = debugPort
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

	args := []string{"--config", cfgPath}
	if g.devFlag {
		args = append(args, "--dev")
	}
	if g.devGenFlag {
		args = append(args, "--development=true")
	}
	if g.debugPortFlag {
		args = append(args, fmt.Sprintf("--debug_handler.port=%d", debugPort))
	}
	record["daemon_args"] = args
	command := exec.Command(binary, args...)
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
	debugBase := ""
	if debugAddress != "" {
		debugBase = "http://" + debugAddress
	}
	// Do NOT follow redirects: the 307 that the ServeMux subtree match issues for
	// /debug/pprof is itself one of the observables under test.
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	until := time.Now().Add(15 * time.Second)
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
	// The Go daemon serves HTTP before it finishes Start() (a 100ms sleep then
	// running.Store(true)); settle past the init window so SIGTERM is not lost,
	// as in C35/C36/C37.
	time.Sleep(1 * time.Second)

	probes := map[string]any{}
	var firstErr error
	for _, p := range g.probes {
		target := base
		if p.target == "debug" {
			target = debugBase
		}
		obs, e := runOneProbe(client, target, p)
		probes[p.name] = obs
		if e != nil && firstErr == nil {
			firstErr = fmt.Errorf("probe %s: %w", p.name, e)
		}
	}
	record["probes"] = probes
	return firstErr
}

func runOneProbe(client *http.Client, base string, p probe) (map[string]any, error) {
	if base == "" {
		return map[string]any{"outcome": "no_listener_configured"}, nil
	}
	var bodyReader io.Reader
	if p.body != nil {
		bodyReader = bytes.NewReader(p.body)
	}
	req, err := http.NewRequest(p.method, base+p.path, bodyReader)
	if err != nil {
		return map[string]any{"outcome": "request_error", "error": err.Error()}, err
	}
	if p.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range p.header {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		// A refused connection is an OBSERVABLE (no listener), not a harness error:
		// debug_handler.port absent must mean nothing is bound there.
		return map[string]any{"outcome": "connect_failure", "error": err.Error()}, nil
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16384))
	if readErr != nil {
		return map[string]any{"outcome": "read_error", "error": readErr.Error()}, readErr
	}
	return map[string]any{
		"outcome":      "response",
		"status":       resp.StatusCode,
		"body":         string(body),
		"body_class":   classify(resp.StatusCode, firstOrEmpty(resp.Header, "Content-Type"), body),
		"content_type": firstOrEmpty(resp.Header, "Content-Type"),
		"location":     firstOrEmpty(resp.Header, "Location"),
		"nosniff":      firstOrEmpty(resp.Header, "X-Content-Type-Options"),
		// Content-Length is the declared payload size, and it is the observable that
		// survives HEAD body suppression, so the comparator can pin the
		// "headers as if GET, body suppressed" property instead of pretending the
		// bodies are equal. X-Go-Pprof is recorded for transparency and NOT asserted
		// equal: the Rust candidate sets it only on its own pprof error path, where Go
		// sets it on every pprof handler response.
		"content_length": firstOrEmpty(resp.Header, "Content-Length"),
		"go_pprof":       firstOrEmpty(resp.Header, "X-Go-Pprof"),
		"body_len":       len(body),
	}, nil
}

// classify reduces a response body to the deterministic property that the dev-mode
// mux decision controls, so the two sides can be compared without pretending that
// process-specific profile payloads are equal.
func classify(status int, contentType string, body []byte) string {
	text := string(body)
	switch {
	case status == http.StatusNotFound && text == "404 page not found\n":
		return "serve_mux_404_page"
	case status == http.StatusTemporaryRedirect &&
		strings.HasPrefix(text, `<a href="`) &&
		strings.HasSuffix(text, `">Temporary Redirect</a>.\n\n`):
		return "serve_mux_307_html"
	case status == http.StatusOK && text == "[]":
		return "empty_json_array_body"
	case status == http.StatusOK && strings.Contains(text, "Types of profiles available"):
		return "pprof_index_html"
	case status == http.StatusOK && strings.HasPrefix(contentType, "text/plain") &&
		len(text) > 0:
		return "text_plain_body"
	case status == http.StatusNotImplemented:
		return "not_implemented"
	default:
		return "other"
	}
}

func firstOrEmpty(h http.Header, name string) string {
	v := h.Values(name)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}
