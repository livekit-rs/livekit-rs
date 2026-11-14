// SPEC-0077 C34 — launched-MAIN rtc.use_ice_lite ICE-lite SDP observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon with a
// configurable rtc.use_ice_lite, drives an authenticated, MEDIA-FREE signal join
// over WebSocket /rtc with a modern non-Firefox client (protocol>5), and records
// whether the daemon's subscriber SDP OFFER carries the session-level `a=ice-lite`
// attribute. The observer publishes no track and sends no media: the offer is the
// daemon's own subscriber peer-connection negotiation, so the ICE-lite flag
// originates from the daemon and depends on use_ice_lite being consumed:
//
//	Go (pinned mediatransportutil-f234b534b095) rtcconfig/webrtc_config.go:
//	  :223-224  if rtcConf.UseICELite { s.SetLite(true) }
//	  :225-235  else if (NodeIP empty/auto) && !UseExternalIP { add STUN servers }
//	pkg/service/roommanager.go:
//	  :446-447  if pi.DisableICELite { rtcConf.SettingEngine.SetLite(false) }
//	pkg/rtc/transport.go:
//	  :414-422  if !ProtocolVersion.SupportsICELite() || !ClientInfo.SupportsPrflxOverRelay()
//	              { se.SetLite(false) }   // Firefox-only disable
//	Rust:
//	  lk-config/src/official.rs:303        use_ice_lite: bool = false (parse)
//	  lk-config/src/normalize.rs:587       use_ice_lite passthrough
//	  rtc-core/src/config.rs:109           policy.use_ice_lite = cfg.rtc.use_ice_lite
//	  rtc-core/src/room/join.rs:72-73      ice_lite = policy.use_ice_lite
//	                                         && !params.disable_ice_lite
//	                                         && params.capabilities.ice_lite
//	  rtc-core/src/room/join.rs:183        -> TransportSpec.ice_lite
//	  lk-compat/src/profile.rs:41          capabilities.ice_lite = protocol>5 && !firefox
//	  rtc-media/src/engine.rs:321          .set_ice_lite(spec.ice_lite)  (str0m a=ice-lite)
//	  rtc-media/src/engine.rs:233          gather_srflx = gather_srflx && !ice_lite
//
// WHY NOT WHIP: both Go (whipservice.go:196 DisableICELite:true) and Rust
// (whip/mod.rs:374, whip/runtime.rs:142 disable_ice_lite=true) FORCE ICE-lite off
// on the WHIP ingress path, so a WHIP offer/answer never carries a=ice-lite on
// either side regardless of rtc.use_ice_lite. The signal join path is the real
// launched-MAIN consumer surface for this leaf.
//
// Discriminator (load-bearing): ice_lite_present = (`a=ice-lite` session attribute
// in the daemon subscriber offer SDP). With node_ip=127.0.0.1 + use_external_ip=false
// the Go NAT1To1 catch-all (webrtc_config.go:111-116) masks candidate IPs to
// 127.0.0.1 but does NOT touch the session-level a=ice-lite attribute, which is the
// surviving observable. The four profiles:
//
//	use_ice_lite=true  + Chrome  + disable_ice_lite=false -> a=ice-lite PRESENT
//	use_ice_lite=false + Chrome  + disable_ice_lite=false -> a=ice-lite ABSENT
//	use_ice_lite=true  + Firefox + disable_ice_lite=false -> a=ice-lite ABSENT (capability gate)
//	use_ice_lite=true  + Chrome  + disable_ice_lite=true  -> a=ice-lite ABSENT (param gate)
//
// The browser/protocol/disable_ice_lite stimulus is carried on the legacy /rtc query
// (Go rtcservice.go:240 ParseClientInfo + :253 DisableICELite FormValue; Rust
// signal/params.rs:164 protocol, :180 browser, :151 disable_ice_lite, :359 augment
// only fills an EMPTY browser so an explicit query browser is preserved on both sides).
//
// This observer is a SEPARATE copy from the C33 rtc-stun-port-as-ice observer: it
// swaps the WHIP stimulus for a media-free signal join and names use_ice_lite as the
// driven item, keeping the C33 observer identity unchanged (anti-splicing).
package main

import (
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

const apiKey = "ownedicelite"
const apiSecret = "fixture-secret-only-for-local-ice-lite-tests"

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	profile := flag.String("profile", "", "profile name (label only)")
	useICELite := flag.Bool("use-ice-lite", false, "rtc.use_ice_lite value")
	browser := flag.String("browser", "Chrome", "client browser query param (Chrome|Firefox)")
	disableICELite := flag.Bool("disable-ice-lite", false, "disable_ice_lite join query param")
	protocol := flag.Int("protocol", 15, "client protocol version query param")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	flag.Parse()
	if *binary == "" || *out == "" || *profile == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --profile P [--use-ice-lite] [--browser Chrome|Firefox] [--disable-ice-lite] [--protocol N] [--enable-loopback=true|false]")
		os.Exit(2)
	}
	record := map[string]any{
		"profile":                   *profile,
		"use_ice_lite":              *useICELite,
		"browser":                   *browser,
		"disable_ice_lite":          *disableICELite,
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
	runErr := runProfile(*binary, *out, *profile, *useICELite, *browser, *disableICELite, *protocol, *enableLoopback, record)
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

func runProfile(binary, out, profile string, useICELite bool, browser string, disableICELite bool, protocol int, enableLoopback bool, record map[string]any) (err error) {
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
		"use_ice_lite":              useICELite,
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
	record["driven_items"] = []string{"YAML:rtc.use_ice_lite"}

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
			record["ice_lite_present"] = false
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
	joinErr := exerciseSignalJoin(address, browser, disableICELite, protocol, record)
	time.Sleep(1 * time.Second)
	return joinErr
}

// exerciseSignalJoin dials the legacy /rtc WebSocket with the join stimulus on the
// query and an Authorization Bearer JWT, then reads the daemon's SignalResponse
// stream until the subscriber Offer arrives. It publishes no track and sends no
// media; the offer is the daemon's own subscriber PC negotiation. The session-level
// a=ice-lite attribute is the load-bearing observable.
func exerciseSignalJoin(address, browser string, disableICELite bool, protocol int, record map[string]any) (err error) {
	token, err := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity("owned-ice-lite-subscriber").
		SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-ice-lite-room"}).
		ToJWT()
	if err != nil {
		return err
	}
	query := url.Values{}
	query.Set("protocol", strconv.Itoa(protocol))
	query.Set("sdk", "js")
	query.Set("auto_subscribe", "true")
	query.Set("browser", browser)
	query.Set("disable_ice_lite", strconv.FormatBool(disableICELite))
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
			record["ice_lite_present"] = iceLitePresent(sdp)
			record["ice_lite_lines"] = iceLiteLines(sdp)
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

// iceLitePresent reports whether the SDP carries the session-level a=ice-lite
// attribute (RFC 5245 §15.4 / pion SettingEngine.SetLite). It is an exact trimmed
// line match, not a substring of another attribute.
func iceLitePresent(sdp string) bool {
	for _, line := range strings.Split(sdp, "\n") {
		if strings.TrimSpace(line) == "a=ice-lite" {
			return true
		}
	}
	return false
}

func iceLiteLines(sdp string) []string {
	var out []string
	for _, line := range strings.Split(sdp, "\n") {
		t := strings.TrimSpace(line)
		if t == "a=ice-lite" {
			out = append(out, t)
		}
	}
	if out == nil {
		out = []string{}
	}
	return out
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