// SPEC-0077 C51 — launched-MAIN rtc.enable_warp SNAP (SCTP-in-SDP) observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon with a
// configurable rtc.enable_warp, drives an authenticated, MEDIA-FREE signal join
// over WebSocket /rtc with a modern client (protocol>5), and records whether the
// daemon's subscriber SDP OFFER carries the `a=sctp-init:<base64 INIT>` attribute
// (SNAP = SCTP INIT in SDP, the data-channel half of WARP). The observer publishes
// no track and sends no media: the offer is the daemon's own subscriber
// peer-connection negotiation, so the SNAP flag originates from the daemon and
// depends on enable_warp being consumed end-to-end by the assembled MAIN server:
//
//	Go (v1.13.10 e5655ac2, pinned pion):
//	  pkg/config/config.go:135-138        EnableWarp bool `yaml:"enable_warp"` (default false)
//	  pkg/service/roommanager.go:487      enableWarp := !useOneShotSignallingMode && r.config.RTC.EnableWarp
//	  pkg/rtc/transport.go:389-393        se.EnableSped(true); se.EnableSctpSnap(true) + Debugw("enable warp")
//	  pion peerconnection.go:2894-2895    localSctpInit only when dataChannelsRequested != 0 && sctp.enableSnap
//	  pion sdp.go:396-397                 a=sctp-init:<base64 INIT chunk>
//	Rust (complete launched-MAIN consumer chain):
//	  lk-config/src/official.rs:320       enable_warp: bool = false (parse)
//	  lk-config/src/normalize.rs:605      enable_warp passthrough -> ServerConfig.rtc.enable_warp
//	  rtc-media/src/engine.rs:310         let warp = rtc_cfg.enable_warp && !spec.one_shot
//	  rtc-media/src/engine.rs:312         tracing::debug!("enable warp")
//	  rtc-media/src/engine.rs:323         .set_snap_enabled(warp)   (str0m a=sctp-init)
//	  rtc-media/src/engine.rs:324         .set_sped_enabled(warp)   (SPED, attested separately by C05)
//	  str0m src/sctp/snap.rs, src/sdp/data.rs:1505 (write), src/change/sdp.rs (offer)
//	  vendor/str0m PATCHES.md Patch 24    snap-answer-requires-local-optin (answer gating parity)
//
// WHY NOT WHIP / one-shot: the warp gate is `enable_warp && !one_shot` (Go
// !useOneShotSignallingMode). The media-free /rtc signal join is the NORMAL
// (non-one-shot) signalling path, so warp == enable_warp here; this is the real
// launched-MAIN full-server consumer surface. SPED (DTLS-in-STUN) is the other
// WARP half and is only observable on the wire with a WARP client — it is attested
// separately by the C05 media-engine paired Go/Pion differential, NOT by this
// SNAP-SDP observer. This harness proves the SNAP half of the full-server wiring.
//
// Discriminator (load-bearing): sctp_init_present = (an `a=sctp-init:` attribute
// line in the daemon subscriber offer SDP). application_present = (an `m=application`
// media section, i.e. data channels were negotiated — the precondition for SNAP).
// With node_ip=127.0.0.1 + use_external_ip=false the Go NAT1To1 catch-all masks
// candidate IPs to 127.0.0.1 but does NOT touch the a=sctp-init attribute. Profiles:
//
//	enable_warp=true  -> a=sctp-init PRESENT (SNAP opt-in consumed by the MAIN server)
//	enable_warp=false -> a=sctp-init ABSENT  (default off)
//
// An accept-but-ignore mutant that ignores enable_warp never calls set_snap_enabled,
// so a=sctp-init is absent even when enabled: warp-on FAILS, warp-off SURVIVES —
// specific, not indiscriminate. The a=sctp-init base64 INIT blob is nondeterministic
// (verification tag / timestamps), so only its PRESENCE is cross-compared, never its
// bytes; the raw line + SDP hash are recorded per side for transparency only.
//
// This observer is a SEPARATE copy from the C34 rtc-use-ice-lite observer: it swaps
// the ice-lite stimulus for enable_warp and names rtc.enable_warp as the driven item,
// keeping the C34 observer identity unchanged (anti-splicing).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

const apiKey = "ownedwarpsnap"
const apiSecret = "fixture-secret-only-for-local-warp-snap-tests"

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	profile := flag.String("profile", "", "profile name (label only)")
	enableWarp := flag.Bool("enable-warp", false, "rtc.enable_warp value")
	browser := flag.String("browser", "Chrome", "client browser query param (Chrome|Firefox)")
	protocol := flag.Int("protocol", 15, "client protocol version query param")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	flag.Parse()
	if *binary == "" || *out == "" || *profile == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --profile P [--enable-warp] [--browser Chrome|Firefox] [--protocol N] [--enable-loopback=true|false]")
		os.Exit(2)
	}
	record := map[string]any{
		"profile":                   *profile,
		"enable_warp":               *enableWarp,
		"browser":                   *browser,
		"protocol":                  *protocol,
		"enable_loopback_candidate": *enableLoopback,
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fatal(record, *out, err)
	}
	before, err := hashFile(*binary)
	if err != nil {
		fatal(record, *out, err)
	}
	record["binary_sha256_before"] = before
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runErr := runProfile(ctx, *binary, *out, *profile, *enableWarp, *browser, *protocol, *enableLoopback, record)
	runErr = errors.Join(runErr, ctx.Err())
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
// ephemeral range (49152-65535), so host candidates are unambiguous.
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

func enumerateInterfaces(record map[string]any) error {
	ifaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	var table []map[string]any
	loopback := ""
	for _, iface := range ifaces {
		up := iface.Flags&net.FlagUp != 0
		lb := iface.Flags&net.FlagLoopback != 0
		table = append(table, map[string]any{"name": iface.Name, "up": up, "loopback": lb})
		if lb && up && loopback == "" {
			loopback = iface.Name
		}
	}
	record["enumerated_interfaces"] = table
	record["loopback_name"] = loopback
	if loopback == "" {
		return fmt.Errorf("no UP loopback interface found")
	}
	return nil
}

func runProfile(ctx context.Context, binary, out, profile string, enableWarp bool, browser string, protocol int, enableLoopback bool, record map[string]any) (err error) {
	if err := enumerateInterfaces(record); err != nil {
		return err
	}

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
		"enable_warp":               enableWarp,
	}
	var closers []net.Listener
	var packetClosers []net.PacketConn
	rebindTCP := []string{address}
	rebindUDP := []string{}
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
	rangeStart, rangeEnd := p, p+9
	rtc["port_range_start"] = rangeStart
	rtc["port_range_end"] = rangeEnd
	record["configured_port_range_start"] = rangeStart
	record["configured_port_range_end"] = rangeEnd

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
	record["driven_items"] = []string{"YAML:rtc.enable_warp", "CONFIG_ENABLED_CAPABILITY:rtc.enable_warp"}

	logPath := filepath.Join(out, "server.log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	command := exec.CommandContext(ctx, binary, "--config", cfgPath)
	command.Cancel = func() error { return command.Process.Signal(syscall.SIGTERM) }
	command.WaitDelay = 8 * time.Second
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
		for _, addr := range rebindUDP {
			rebound, e := net.ListenPacket("udp4", addr)
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
			record["join_received"] = false
			record["offer_received"] = false
			record["sctp_init_present"] = false
			record["application_present"] = false
			record["offer_sdp"] = ""
			record["candidate_types"] = []string{}
			record["candidate_transports"] = []string{}
			record["host_candidate_count"] = 0
			record["early_exit_error"] = fmt.Sprintf("%v", e)
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
	joinErr := exerciseSignalJoin(address, browser, protocol, record)
	time.Sleep(1 * time.Second)
	return joinErr
}

// exerciseSignalJoin dials the legacy /rtc WebSocket with the join stimulus on the
// query and an Authorization Bearer JWT, then reads the daemon's SignalResponse
// stream until the subscriber Offer arrives. It publishes no track and sends no
// media; the offer is the daemon's own subscriber PC negotiation. The a=sctp-init
// attribute (SNAP) is the load-bearing observable; m=application confirms the
// data-channel precondition.
func exerciseSignalJoin(address, browser string, protocol int, record map[string]any) (err error) {
	token, err := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity("owned-warp-snap-subscriber").
		SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-warp-snap-room"}).
		ToJWT()
	if err != nil {
		return err
	}
	query := url.Values{}
	query.Set("protocol", strconv.Itoa(protocol))
	query.Set("sdk", "js")
	query.Set("auto_subscribe", "true")
	query.Set("browser", browser)
	wsURL := "ws://" + address + "/rtc?" + query.Encode()
	record["signal_url_path"] = "/rtc"
	record["signal_query"] = query.Encode()

	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, resp, err := dialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			record["signal_upgrade_status"] = resp.StatusCode
		}
		return fmt.Errorf("signal websocket dial: %w", err)
	}
	if resp != nil {
		record["signal_upgrade_status"] = resp.StatusCode
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	conn.SetPongHandler(func(string) error { return nil })

	deadline := time.Now().Add(8 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	joinReceived := false
	offerReceived := false
	for time.Now().Before(deadline) {
		msgType, payload, readErr := conn.ReadMessage()
		if readErr != nil {
			if offerReceived {
				return nil
			}
			return fmt.Errorf("signal read before offer: %w", readErr)
		}
		if msgType == websocket.PingMessage {
			_ = conn.WriteMessage(websocket.PongMessage, nil)
			continue
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		var sr livekit.SignalResponse
		if e := proto.Unmarshal(payload, &sr); e != nil {
			continue
		}
		switch msg := sr.Message.(type) {
		case *livekit.SignalResponse_Join:
			joinReceived = true
			record["join_received"] = true
			if msg.Join != nil && msg.Join.Participant != nil {
				record["participant_sid"] = msg.Join.Participant.Sid
			}
		case *livekit.SignalResponse_Offer:
			offerReceived = true
			record["offer_received"] = true
			sdp := ""
			if msg.Offer != nil {
				sdp = msg.Offer.Sdp
			}
			record["offer_sdp"] = sdp
			record["offer_sdp_sha256"] = fmt.Sprintf("%x", sha256.Sum256([]byte(sdp)))
			record["sctp_init_present"] = sctpInitPresent(sdp)
			record["sctp_init_lines"] = sctpInitLines(sdp)
			record["application_present"] = applicationPresent(sdp)
			obs := hostCandidates(sdp)
			record["udp_host_ports"] = obs.udpPorts
			record["tcp_host_ports"] = obs.tcpPorts
			record["host_candidate_ips"] = obs.ips
			record["host_candidate_count"] = obs.count
			record["candidate_types"] = obs.types
			record["candidate_transports"] = obs.transports
			return nil
		}
	}
	record["join_received"] = joinReceived
	record["offer_received"] = offerReceived
	if !offerReceived {
		return fmt.Errorf("no subscriber offer received before deadline (join_received=%t)", joinReceived)
	}
	return nil
}

// sctpInitPresent reports whether the SDP carries an `a=sctp-init:<base64 INIT>`
// attribute (SNAP, pion sdp.go:396-397 / str0m sctp/snap.rs). It is a trimmed-line
// prefix match; the base64 INIT blob is nondeterministic so only presence is signal.
func sctpInitPresent(sdp string) bool {
	for _, line := range strings.Split(sdp, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "a=sctp-init:") {
			return true
		}
	}
	return false
}

func sctpInitLines(sdp string) []string {
	var out []string
	for _, line := range strings.Split(sdp, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "a=sctp-init:") {
			out = append(out, t)
		}
	}
	if out == nil {
		out = []string{}
	}
	return out
}

// applicationPresent reports whether the SDP carries an m=application media section
// (SCTP data channels negotiated) — the precondition for SNAP localSctpInit.
func applicationPresent(sdp string) bool {
	for _, line := range strings.Split(sdp, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "m=application") {
			return true
		}
	}
	return false
}

type candidateObs struct {
	udpPorts   []int
	tcpPorts   []int
	ips        []string
	types      []string
	transports []string
	count      int
}

// hostCandidates extracts the sorted unique HOST candidate ports per transport, the
// connection-address set, the candidate-type catalog and the transport catalog from
// an SDP offer. Only `typ host` lines contribute. Grammar:
//
//	a=candidate:<foundation> <component> <transport> <priority> <ip> <port> typ <type> ...
//	fields[2]=transport fields[4]=ip fields[5]=port fields[7]=type
func hostCandidates(sdp string) candidateObs {
	udpSet := map[int]bool{}
	tcpSet := map[int]bool{}
	ipSet := map[string]bool{}
	typeSet := map[string]bool{}
	transportSet := map[string]bool{}
	count := 0
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "a=candidate:") {
			continue
		}
		fields := strings.Fields(line)
		typ := ""
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] == "typ" {
				typ = fields[i+1]
				break
			}
		}
		if typ == "" || len(fields) < 6 {
			continue
		}
		transport := strings.ToLower(fields[2])
		port, perr := strconv.Atoi(fields[5])
		if perr != nil {
			continue
		}
		typeSet[typ] = true
		transportSet[transport] = true
		if typ != "host" {
			continue
		}
		count++
		ipSet[fields[4]] = true
		switch transport {
		case "udp":
			udpSet[port] = true
		case "tcp":
			tcpSet[port] = true
		}
	}
	obs := candidateObs{count: count}
	for p := range udpSet {
		obs.udpPorts = append(obs.udpPorts, p)
	}
	for p := range tcpSet {
		obs.tcpPorts = append(obs.tcpPorts, p)
	}
	for ip := range ipSet {
		obs.ips = append(obs.ips, ip)
	}
	for t := range typeSet {
		obs.types = append(obs.types, t)
	}
	for t := range transportSet {
		obs.transports = append(obs.transports, t)
	}
	sort.Ints(obs.udpPorts)
	sort.Ints(obs.tcpPorts)
	sort.Strings(obs.ips)
	sort.Strings(obs.types)
	sort.Strings(obs.transports)
	if obs.udpPorts == nil {
		obs.udpPorts = []int{}
	}
	if obs.tcpPorts == nil {
		obs.tcpPorts = []int{}
	}
	if obs.ips == nil {
		obs.ips = []string{}
	}
	if obs.types == nil {
		obs.types = []string{}
	}
	if obs.transports == nil {
		obs.transports = []string{}
	}
	return obs
}
