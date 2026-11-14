// SPEC-0077 C32 — launched-MAIN rtc media-port family ICE candidate-port observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon with a
// configurable rtc.udp_port / rtc.port_range_start,end / rtc.tcp_port media-port
// selection, drives an authenticated WHIP Create then DELETE with a Pion offer,
// and records the HOST ICE candidate PORT set (per transport) the daemon gathered
// into its SDP answer. The Pion driver configures no interface filter and no port
// preference of its own, so every recorded host candidate port originates from the
// daemon and depends on the media-port config being consumed:
//
//	Go (pinned mediatransportutil-f234b534b095) rtcconfig/webrtc_config.go:
//	  :127-130  ICEPortRangeStart/End != 0 -> SetEphemeralUDPPortRange(start,end)
//	  :131-171  else UDPPort.Valid()       -> CreateUDPMuxesFromPorts -> SetICEUDPMux
//	  :180-185  TCPPort != 0               -> net.ListenTCP(TCPAddr{Port}) TCP mux
//	Rust:
//	  livecloud/src/interfaces.rs:337-346 net_config (UdpMode::Mux->udp_port,
//	    UdpMode::Range->port_range, tcp_port), :260-271 mux_ports_for_cpu
//	  livecloud/src/server.rs:332-343 tcp_port + startup_mux_ports -> start_with_configured_ports
//	  rtc-runtime/src/net/mod.rs:344/367 (mux bind), :396-403 (listen_tcp_wildcard),
//	    :662 bind_dedicated_host -> udp::bind_in_range(port_range)
//
// Discriminator (load-bearing): with node_ip=127.0.0.1 + use_external_ip=false the
// Go daemon installs a catch-all NAT1To1 rewrite to 127.0.0.1
// (webrtc_config.go:111-116) that masks every candidate IP, but it does NOT touch
// the candidate PORT. The port is therefore the observable that survives the
// rewrite:
//
//	udp_port=U (mux)        -> every UDP host candidate port == U
//	port_range=[A,B]        -> every UDP host candidate port in [A,B]
//	port_range over udp_port-> range wins (Go :127 before :131); U not used
//	tcp_port=T (+force_tcp) -> every TCP host candidate port == T, no UDP
//	ephemeral (all unset)   -> UDP host candidate ports are OS-chosen (outside the
//	                           reserved media band), the accept-but-ignore baseline
//
// Media ports are reserved dynamically (UDP/TCP :0) and released immediately before
// the daemon starts, so no fixed host port is assumed free; the observer RECORDS the
// actual configured values (configured_udp_port / configured_port_range_start,end /
// configured_tcp_port) and the harness asserts the gathered candidate ports against
// those recorded values. The reserved band is kept below the macOS ephemeral range
// (49152-65535) by preferring low free ports, so an accept-but-ignore mutant that
// falls back to OS ephemeral ports is decisively outside the configured band.
//
// This observer is a SEPARATE copy from the C31 rtc-loopback-candidate observer: it
// adds the media-port flags and names the four port leaves as the driven items,
// keeping the C31 observer identity unchanged (anti-splicing).
package main

import (
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
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/pion/webrtc/v4"
)

const apiKey = "ownedports"
const apiSecret = "fixture-secret-only-for-local-media-port-tests"

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	profile := flag.String("profile", "", "profile name (label only)")
	udpMode := flag.String("udp-mode", "ephemeral", "mux|range|precedence|ephemeral")
	tcpMode := flag.String("tcp-mode", "off", "off (default 7881)|on (reserve a free low TCP port)|zero (explicit tcp_port=0, no TCP mux)")
	forceTCP := flag.Bool("force-tcp", false, "rtc.force_tcp value")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	ifIncludes := flag.String("if-includes", "", "comma-separated rtc.interfaces.includes names (@loopback/@all tokens)")
	ifExcludes := flag.String("if-excludes", "", "comma-separated rtc.interfaces.excludes names (@loopback/@all tokens)")
	flag.Parse()
	if *binary == "" || *out == "" || *profile == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --profile P [--udp-mode mux|range|precedence|ephemeral] [--tcp-mode off|on|zero] [--force-tcp] [--enable-loopback=true|false] [--if-includes a,b] [--if-excludes c,d]")
		os.Exit(2)
	}
	record := map[string]any{
		"profile":                  *profile,
		"udp_mode":                 *udpMode,
		"tcp_mode":                 *tcpMode,
		"force_tcp":                *forceTCP,
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
	runErr := runProfile(*binary, *out, *profile, *udpMode, *tcpMode, *forceTCP, *enableLoopback, splitList(*ifIncludes), splitList(*ifExcludes), record)
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

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func enumerateInterfaces(record map[string]any) (loopback string, all []string, err error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", nil, err
	}
	var table []map[string]any
	for _, iface := range ifaces {
		up := iface.Flags&net.FlagUp != 0
		lb := iface.Flags&net.FlagLoopback != 0
		table = append(table, map[string]any{"name": iface.Name, "up": up, "loopback": lb})
		all = append(all, iface.Name)
		if lb && up && loopback == "" {
			loopback = iface.Name
		}
	}
	sort.Strings(all)
	record["enumerated_interfaces"] = table
	record["loopback_name"] = loopback
	if loopback == "" {
		return "", nil, fmt.Errorf("no UP loopback interface found")
	}
	return loopback, all, nil
}

func resolveNames(names, all []string, loopback string) []string {
	var out []string
	seen := map[string]bool{}
	for _, n := range names {
		switch n {
		case "@loopback":
			if !seen[loopback] {
				out = append(out, loopback)
				seen[loopback] = true
			}
		case "@all":
			for _, a := range all {
				if !seen[a] {
					out = append(out, a)
					seen[a] = true
				}
			}
		default:
			if !seen[n] {
				out = append(out, n)
				seen[n] = true
			}
		}
	}
	return out
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

// reserveTCP binds a free TCP port on loopback and returns the port plus the
// listener (held until just before the daemon starts, then closed). Used for the
// daemon HTTP port (not a media port under test, so an ephemeral port is fine).
func reserveTCP() (int, net.Listener, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, nil, err
	}
	return l.Addr().(*net.TCPAddr).Port, l, nil
}

// Media ports are reserved in a LOW band (40000-40999), decisively below the
// macOS ephemeral range (49152-65535, confirmed via sysctl). An accept-but-ignore
// mutant that falls back to OS-chosen ephemeral ports therefore lands OUTSIDE the
// configured band, making the port discriminator robust rather than probabilistic.
const lowBandStart = 40000
const lowBandEnd = 40999

func reserveLowUDP() (int, net.PacketConn, error) {
	return reserveLowUDPFrom(lowBandStart)
}

// reserveLowUDPFrom scans the low band starting at `start` so the precedence
// profile can reserve its mux port U strictly above the range band [R,R+9].
// Scanning always from lowBandStart would keep returning the first free port
// inside the held range, so the start offset is load-bearing.
func reserveLowUDPFrom(start int) (int, net.PacketConn, error) {
	for p := start; p <= lowBandEnd; p++ {
		c, err := net.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", p))
		if err == nil {
			return p, c, nil
		}
	}
	return 0, nil, fmt.Errorf("no free UDP port in low band %d-%d", start, lowBandEnd)
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

func runProfile(binary, out, profile, udpMode, tcpMode string, forceTCP, enableLoopback bool, ifIncludes, ifExcludes []string, record map[string]any) (err error) {
	loopback, all, err := enumerateInterfaces(record)
	if err != nil {
		return err
	}
	includes := resolveNames(ifIncludes, all, loopback)
	excludes := resolveNames(ifExcludes, all, loopback)
	record["resolved_if_includes"] = includes
	record["resolved_if_excludes"] = excludes

	// Reserve the daemon HTTP port plus the media ports under test. All
	// reservations are released immediately before command.Start() so the daemon
	// can bind them; the actual values are recorded for the harness contract.
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
		"force_tcp":                 forceTCP,
		"use_stun_port_as_ice":      false,
	}
	var closers []io.Closer
	rebindTCP := []string{address}
	rebindUDP := []string{}
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	configuredUDPPort := 0
	rangeStart, rangeEnd := 0, 0
	switch udpMode {
	case "mux":
		p, c, e := reserveLowUDP()
		if e != nil {
			return e
		}
		closers = append(closers, c)
		configuredUDPPort = p
		rtc["udp_port"] = p
		rebindUDP = append(rebindUDP, fmt.Sprintf("127.0.0.1:%d", p))
	case "range":
		p, c, e := reserveLowUDP()
		if e != nil {
			return e
		}
		closers = append(closers, c)
		rangeStart, rangeEnd = p, p+9
		rtc["port_range_start"] = rangeStart
		rtc["port_range_end"] = rangeEnd
	case "precedence":
		// Reserve a range base R and a distinct mux port U strictly above the
		// range band [R,R+9] so the two are disjoint: range-wins puts the
		// candidate port inside [R,R+9], mux-wins puts it exactly at U.
		r, rc, e := reserveLowUDP()
		if e != nil {
			return e
		}
		closers = append(closers, rc)
		rangeStart, rangeEnd = r, r+9
		u, uc, e := reserveLowUDPFrom(rangeEnd + 1)
		if e != nil {
			return e
		}
		closers = append(closers, uc)
		configuredUDPPort = u
		rtc["udp_port"] = u
		rtc["port_range_start"] = rangeStart
		rtc["port_range_end"] = rangeEnd
		rebindUDP = append(rebindUDP, fmt.Sprintf("127.0.0.1:%d", u))
	case "ephemeral":
		// No udp_port, no port_range: OS-chosen ephemeral per-transport sockets.
	default:
		return fmt.Errorf("unknown udp-mode %q", udpMode)
	}
	record["configured_udp_port"] = configuredUDPPort
	record["configured_port_range_start"] = rangeStart
	record["configured_port_range_end"] = rangeEnd

	configuredTCPPort := 0
	switch tcpMode {
	case "on":
		p, l, e := reserveLowTCP()
		if e != nil {
			return e
		}
		closers = append(closers, l)
		configuredTCPPort = p
		rtc["tcp_port"] = p
		rebindTCP = append(rebindTCP, fmt.Sprintf("127.0.0.1:%d", p))
	case "zero":
		// Explicit tcp_port=0 overrides the Go default (7881): no TCP mux, no TCP
		// host candidates. Used by the ephemeral baseline so it carries NO
		// configured media port and survives an accept-but-ignore mutant.
		rtc["tcp_port"] = 0
	case "off":
		// Leave tcp_port unset: the daemon default (Go config.go:572 TCPPort 7881)
		// applies. Not used by the final profiles (avoids the shared-host 7881).
	default:
		return fmt.Errorf("unknown tcp-mode %q", tcpMode)
	}
	record["configured_tcp_port"] = configuredTCPPort

	ifaces := map[string]any{}
	if len(includes) > 0 {
		ifaces["includes"] = includes
	}
	if len(excludes) > 0 {
		ifaces["excludes"] = excludes
	}
	if len(ifaces) > 0 {
		rtc["interfaces"] = ifaces
	}
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
	record["driven_items"] = []string{
		"YAML:rtc.udp_port", "YAML:rtc.tcp_port",
		"YAML:rtc.port_range_start", "YAML:rtc.port_range_end",
	}

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
			record["whip_create_status"] = 0
			record["whip_delete_status"] = 0
			record["udp_host_ports"] = []int{}
			record["tcp_host_ports"] = []int{}
			record["host_candidate_ips"] = []string{}
			record["host_candidate_count"] = 0
			record["candidate_types"] = []string{}
			record["candidate_transports"] = []string{}
			record["answer_sdp"] = ""
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
	whipErr := exerciseWHIP(client, base, record)
	time.Sleep(1 * time.Second)
	return whipErr
}

func exerciseWHIP(client *http.Client, url string, record map[string]any) (err error) {
	token, err := auth.NewAccessToken(apiKey, apiSecret).SetIdentity("owned-ports-publisher").SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-ports-room"}).ToJWT()
	if err != nil {
		return err
	}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, pc.Close()) }()
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly}); err != nil {
		return err
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		return err
	}
	select {
	case <-gathered:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("owned ICE gather timeout")
	}
	request, err := http.NewRequest("POST", url+"/whip/v1", strings.NewReader(pc.LocalDescription().SDP))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/sdp")
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(response.Body)
	if err = errors.Join(readErr, response.Body.Close()); err != nil {
		return err
	}
	record["whip_create_status"] = response.StatusCode
	location := response.Header.Get("Location")
	answer := string(body)
	record["answer_sdp"] = answer
	obs := hostCandidates(answer)
	record["udp_host_ports"] = obs.udpPorts
	record["tcp_host_ports"] = obs.tcpPorts
	record["host_candidate_ips"] = obs.ips
	record["host_candidate_count"] = obs.count
	record["candidate_types"] = obs.types
	record["candidate_transports"] = obs.transports
	record["all_host_candidates"] = obs.all
	if response.StatusCode != http.StatusCreated || !strings.HasPrefix(location, "/whip/v1/") {
		record["whip_delete_status"] = 0
		return nil
	}
	defer func() {
		request, e := http.NewRequest("DELETE", url+location, nil)
		if e != nil {
			err = errors.Join(err, e)
			return
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, e := client.Do(request)
		if e != nil {
			err = errors.Join(err, e)
			return
		}
		record["whip_delete_status"] = response.StatusCode
		err = errors.Join(err, response.Body.Close())
	}()
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		return err
	}
	return nil
}

type candidateObs struct {
	udpPorts   []int
	tcpPorts   []int
	ips        []string
	types      []string
	transports []string
	count      int
	all        []map[string]any
}

// hostCandidates extracts the sorted unique HOST candidate ports per transport,
// the connection-address set, the candidate-type catalog and the transport catalog
// from an SDP answer. Only `typ host` lines contribute (srflx/relay reflect
// STUN/TURN, not the local media-port selection under test). The candidate
// attribute grammar is:
//
//	a=candidate:<foundation> <component> <transport> <priority> <ip> <port> typ <type> ...
//	fields[2]=transport fields[4]=ip fields[5]=port fields[7]=type
func hostCandidates(sdp string) candidateObs {
	udpSet := map[int]bool{}
	tcpSet := map[int]bool{}
	ipSet := map[string]bool{}
	typeSet := map[string]bool{}
	transportSet := map[string]bool{}
	var all []map[string]any
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
		all = append(all, map[string]any{"transport": transport, "ip": fields[4], "port": port, "typ": typ})
		switch transport {
		case "udp":
			udpSet[port] = true
		case "tcp":
			tcpSet[port] = true
		}
	}
	obs := candidateObs{count: count, all: all}
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
	if obs.all == nil {
		obs.all = []map[string]any{}
	}
	return obs
}