// SPEC-0077 C35 — launched-MAIN HTTP_BEHAVIOR:middleware.cors observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon and drives
// five deterministic, MEDIA-FREE, UNAUTHENTICATED HTTP CORS probes against the
// main API port, recording the exact CORS response-header semantics of each. No
// track is published, no media flows, no WebSocket join: the observable is purely
// the rs/cors middleware response surface, which is unconditional (not config
// driven), so a single daemon launch per side exercises every profile.
//
// Upstream contract (locked Go v1.13.10 e5655ac2, vendored rs/cors v1.11.1):
//
//	pkg/service/server.go:
//	  :488      API cors.New(corsOptions(["OPTIONS","HEAD","GET","POST","PATCH","DELETE"]))
//	  :525-536  corsOptions: AllowOriginFunc=func(string)bool{return true},
//	            AllowedMethods=methods, AllowedHeaders=["*"], ExposedHeaders=["*"],
//	            MaxAge=86400.
//	rs/cors@v1.11.1/cors.go (negroni ServeHTTP, the form server.go uses):
//	  :308-325  if OPTIONS && Access-Control-Request-Method!="" { handlePreflight;
//	            WriteHeader(204) } else { handleActualRequest; next }
//	  :329-397  handlePreflight: ALWAYS Vary=preflightVary; if origin=="" return;
//	            if !isMethodAllowed(reqMethod) return; ACAO=echoed Origin,
//	            ACAM=echoed ACRM, ACAH=echoed ACRH (if non-empty), ACMA=86400.
//	  :400-444  handleActualRequest: ALWAYS Vary=Origin; if origin=="" return;
//	            if !isMethodAllowed(method) return; ACAO=echoed Origin,
//	            Expose-Headers=["*"].
//	  :234      preflightVary=["Origin, Access-Control-Request-Method, Access-Control-Request-Headers"]
//
// Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
//
//	lk-compat/src/http.rs:
//	  :31-35    CORS_MAX_AGE="86400", CORS_ALLOWED_METHODS=[OPTIONS,HEAD,GET,POST,PATCH,DELETE],
//	            PREFLIGHT_VARY="Origin, Access-Control-Request-Method, Access-Control-Request-Headers".
//	  :94-104   middleware_chain: recover -> cors -> body_limit -> auth -> routes
//	            (CORS runs BEFORE auth; preflight short-circuits, never reaches auth).
//	  :183-262  cors(): preflight (OPTIONS+ACRM) -> 204 + Vary=preflight, then if
//	            origin_present && method_ok: ACAO=origin, ACAM=acrm, ACAH=request_headers,
//	            ACMA=86400. Actual -> Vary=Origin (unconditional), then if origin &&
//	            method_ok: ACAO=origin, Expose="*".
//	  :132-142  not_found(): 404 "404 page not found\n" (the deterministic actual-request route).
//	auth/mod.rs:208-216 authenticate_token(None)=Ok(None): an unauthenticated actual
//	            request passes auth and reaches the 404 fallback on both sides.
//
// WHY a media-free HTTP probe (load-bearing): CORS is an unconditional HTTP
// middleware on the main API port. It does not depend on rtc config, ICE, SDP or a
// published track. The clean deterministic observable is the response-header set
// of an OPTIONS preflight (204, short-circuited before auth) and an actual GET to a
// nonexistent path (404 on both sides, CORS headers merged regardless of auth).
//
// The five profiles (request stimulus varies; daemon config is identical):
//
//	preflight-allowed           OPTIONS /  Origin+ACRM=POST+ACRH=X-Custom-Header
//	                            -> 204, ACAO=origin, ACAM=POST, ACAH=X-Custom-Header,
//	                               ACMA=86400, Vary=preflight          [DISCRIMINATING]
//	preflight-disallowed-method OPTIONS /  Origin+ACRM=TRACE
//	                            -> 204, Vary=preflight ONLY (method gate) [GATING]
//	preflight-no-origin         OPTIONS /  ACRM=POST+ACRH (NO Origin)
//	                            -> 204, Vary=preflight ONLY (origin gate) [GATING]
//	actual-get-origin           GET /cors-probe-absent  Origin
//	                            -> 404, ACAO=origin, Expose=*, Vary=Origin [DISCRIMINATING]
//	actual-no-origin            GET /cors-probe-absent  (NO Origin)
//	                            -> 404, Vary=Origin ONLY (origin gate)    [GATING]
//
// An accept-but-ignore mutant that shadows the request Origin to None keeps the
// structural 204+Vary (preflight) and 404+Vary:Origin (actual) but drops every
// origin-conditional header (ACAO/ACAM/ACAH/ACMA, ACAO/Expose). The two
// DISCRIMINATING profiles then FAIL while the three GATING profiles SURVIVE —
// specific, not indiscriminate.
//
// This observer is a SEPARATE copy from the C34 rtc-use-ice-lite observer: it swaps
// the media-free signal-join stimulus for media-free HTTP CORS probes and names
// HTTP_BEHAVIOR:middleware.cors as the driven item, keeping the C34 observer
// identity unchanged (anti-splicing).
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const apiKey = "ownedcors"
const apiSecret = "fixture-secret-only-for-local-cors-tests"

// probeOrigin is a deterministic non-loopback origin. rs/cors AllowOriginFunc
// returns true for any origin, so it is allowed and echoed verbatim on both sides.
const probeOrigin = "https://probe.example"
const probeACRH = "X-Custom-Header"
const preflightVary = "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"

// corsProbe is one deterministic media-free HTTP stimulus. Empty origin/acrm/acrh
// means the header is ABSENT (not sent).
type corsProbe struct {
	name   string
	method string
	path   string
	origin string
	acrm   string
	acrh   string
}

var corsProbes = []corsProbe{
	{"preflight-allowed", http.MethodOptions, "/", probeOrigin, "POST", probeACRH},
	{"preflight-disallowed-method", http.MethodOptions, "/", probeOrigin, "TRACE", ""},
	{"preflight-no-origin", http.MethodOptions, "/", "", "POST", probeACRH},
	{"actual-get-origin", http.MethodGet, "/cors-probe-absent", probeOrigin, "", ""},
	{"actual-no-origin", http.MethodGet, "/cors-probe-absent", "", "", ""},
}

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	flag.Parse()
	if *binary == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR [--enable-loopback=true|false]")
		os.Exit(2)
	}
	record := map[string]any{
		"enable_loopback_candidate": *enableLoopback,
		"driven_items":              []string{"HTTP_BEHAVIOR:middleware.cors"},
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fatal(record, *out, err)
	}
	before, err := hashFile(*binary)
	if err != nil {
		fatal(record, *out, err)
	}
	record["binary_sha256_before"] = before
	runErr := runProbes(*binary, *out, *enableLoopback, record)
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
// ephemeral range (49152-65535). The media-free CORS probes never bind them, but a
// valid isolated rtc config keeps the daemon launch deterministic and off others'
// ports.
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

func runProbes(binary, out string, enableLoopback bool, record map[string]any) (err error) {
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
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
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
	// Settle past the init window before probing/shutting down (mirrors C34).
	time.Sleep(1 * time.Second)
	return exerciseCORSProbes(client, base, record)
}

// exerciseCORSProbes drives every deterministic media-free CORS probe against the
// launched daemon and records the normalized response-header semantics of each.
func exerciseCORSProbes(client *http.Client, base string, record map[string]any) error {
	probes := map[string]any{}
	var firstErr error
	for _, probe := range corsProbes {
		obs, err := runOneProbe(client, base, probe)
		probes[probe.name] = obs
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("probe %s: %w", probe.name, err)
		}
	}
	record["probes"] = probes
	record["probe_names"] = probeNames()
	return firstErr
}

func probeNames() []string {
	names := make([]string, 0, len(corsProbes))
	for _, p := range corsProbes {
		names = append(names, p.name)
	}
	return names
}

func runOneProbe(client *http.Client, base string, probe corsProbe) (map[string]any, error) {
	req, err := http.NewRequest(probe.method, base+probe.path, nil)
	if err != nil {
		return map[string]any{"error": err.Error()}, err
	}
	if probe.origin != "" {
		req.Header.Set("Origin", probe.origin)
	}
	if probe.acrm != "" {
		req.Header.Set("Access-Control-Request-Method", probe.acrm)
	}
	if probe.acrh != "" {
		req.Header.Set("Access-Control-Request-Headers", probe.acrh)
	}
	resp, err := client.Do(req)
	if err != nil {
		return map[string]any{"error": err.Error()}, err
	}
	defer resp.Body.Close()
	// Drain the body so the connection is reusable/clean; the body is not an observable.
	buf := make([]byte, 4096)
	for {
		if _, e := resp.Body.Read(buf); e != nil {
			break
		}
	}
	return normalizeCORS(resp.StatusCode, resp.Header), nil
}

// normalizeCORS extracts ONLY the deterministic CORS-relevant response semantics.
// Scalar headers capture the first value (null when absent); list headers capture
// every value (empty when absent). Header names are canonicalized by net/http.
func normalizeCORS(status int, h http.Header) map[string]any {
	return map[string]any{
		"status":          status,
		"acao":            firstOrNil(h, "Access-Control-Allow-Origin"),
		"acam":            firstOrNil(h, "Access-Control-Allow-Methods"),
		"acah":            valuesOrEmpty(h, "Access-Control-Allow-Headers"),
		"acma":            firstOrNil(h, "Access-Control-Max-Age"),
		"expose":          firstOrNil(h, "Access-Control-Expose-Headers"),
		"credentials":     firstOrNil(h, "Access-Control-Allow-Credentials"),
		"private_network": firstOrNil(h, "Access-Control-Allow-Private-Network"),
		"vary":            valuesOrEmpty(h, "Vary"),
	}
}

func firstOrNil(h http.Header, name string) any {
	v := h.Values(name)
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

func valuesOrEmpty(h http.Header, name string) []string {
	v := h.Values(name)
	out := append([]string{}, v...)
	sort.Strings(out)
	return out
}