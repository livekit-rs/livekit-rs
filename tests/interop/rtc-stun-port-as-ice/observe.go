// SPEC-0077 C33 — launched-MAIN rtc.use_stun_port_as_ice ICE candidate-port observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon with a
// configurable rtc.use_stun_port_as_ice plus the rtc.udp_port / rtc.port_range /
// rtc.force_tcp media-port selection that gates it, drives an authenticated WHIP
// Create then DELETE with a Pion offer, and records the HOST ICE candidate PORT
// set (per transport) the daemon gathered into its SDP answer. The Pion driver
// configures no interface filter and no port preference of its own, so every
// recorded host candidate port originates from the daemon and depends on
// use_stun_port_as_ice being consumed:
//
//	Go (pinned mediatransportutil-f234b534b095) rtcconfig/webrtc_config.go:
//	  :123      if !rtcConf.ForceTCP {                       // UDP block guard
//	  :127-130    ICEPortRangeStart/End != 0 -> SetEphemeralUDPPortRange (range)
//	  :131        else if rtcConf.UDPPort.Valid() {          // mux mode
//	  :162-167      if rtcConf.UseStunPortAsICE {
//	                  CreateUDPMuxesFromPorts([]int{3478})   // standalone STUN mux
//	                }
//	  :169          NewMultiPortsUDPMux(muxes, standalonePortMuxes)
//	  :180-185    TCPPort != 0 -> net.ListenTCP(TCPAddr{Port}) TCP mux
//	Rust:
//	  livecloud/src/interfaces.rs:274-277 standalone_stun_mux_port =
//	    (!force_tcp && use_stun_port_as_ice && UdpMode::Mux).then_some(3478)
//	  livecloud/src/server.rs:339 standalone_stun_mux_port -> start_with_configured_ports
//	  rtc-runtime/src/net/mod.rs:365 two mux lists [(ports,false),(standalone,true)],
//	    :384-390 the standalone 3478 bind is pushed into shared_candidates,
//	    :477 shared_candidates advertised via open_transport.
//
// Discriminator (load-bearing): with node_ip=127.0.0.1 + use_external_ip=false the
// Go daemon installs a catch-all NAT1To1 rewrite to 127.0.0.1
// (webrtc_config.go:111-116) that masks every candidate IP, but it does NOT touch
// the candidate PORT. The port is therefore the observable that survives the
// rewrite. use_stun_port_as_ice adds a STANDALONE UDP mux on the fixed STUN port
// 3478 ALONGSIDE the configured mux port, so:
//
//	use_stun_port_as_ice=true  + udp_port=U (mux) -> UDP host ports {U, 3478}
//	use_stun_port_as_ice=false + udp_port=U (mux) -> UDP host ports {U} (no 3478)
//	use_stun_port_as_ice=true  + port_range=[A,B] -> no 3478 (range, not mux)
//	use_stun_port_as_ice=true  + force_tcp        -> no UDP at all (no 3478)
//
// 3478 is the fixed STUN port (NOT reserved by this observer); the configured mux
// port is reserved in a LOW band (40000-40999, decisively below the macOS ephemeral
// range 49152-65535) and released before the daemon starts. The observer verifies
// 3478 is FREE before launch (when the daemon will bind it) and RELEASED after, so
// an accept-but-ignore mutant that never binds 3478 is unambiguously distinguishable
// from the clean daemon that does.
//
// This observer is a SEPARATE copy from the C32 rtc-media-ports observer: it adds
// the use-stun-port-as-ice flag and names that single leaf as the driven item,
// keeping the C32 observer identity unchanged (anti-splicing).
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

const apiKey = "ownedstunport"
const apiSecret = "fixture-secret-only-for-local-stun-port-tests"

// stunPort is the fixed STUN port upstream binds as a standalone ICE mux when
// UseStunPortAsICE is set (webrtc_config.go:163 CreateUDPMuxesFromPorts([]int{3478})).
const stunPort = 3478

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	profile := flag.String("profile", "", "profile name (label only)")
	udpMode := flag.String("udp-mode", "mux", "mux|range|ephemeral")
	tcpMode := flag.String("tcp-mode", "on", "on (reserve a free low TCP port)|zero (explicit tcp_port=0)")
	forceTCP := flag.Bool("force-tcp", false, "rtc.force_tcp value")
	useStunPortAsICE := flag.Bool("use-stun-port-as-ice", false, "rtc.use_stun_port_as_ice value")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	flag.Parse()
	if *binary == "" || *out == "" || *profile == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --profile P [--udp-mode mux|range|ephemeral] [--tcp-mode on|zero] [--force-tcp] [--use-stun-port-as-ice] [--enable-loopback=true|false]")
		os.Exit(2)
	}
	record := map[string]any{
		"profile":                   *profile,
		"udp_mode":                  *udpMode,
		"tcp_mode":                  *tcpMode,
		"force_tcp":                 *forceTCP,
		"use_stun_port_as_ice":      *useStunPortAsICE,
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
	runErr := runProfile(*binary, *out, *profile, *udpMode, *tcpMode, *forceTCP, *useStunPortAsICE, *enableLoopback, record)
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

// reserveTCP binds a free TCP port on loopback for the daemon HTTP port (not a
// media port under test, so an ephemeral port is fine).
func reserveTCP() (int, net.Listener, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, nil, err
	}
	return l.Addr().(*net.TCPAddr).Port, l, nil
}

// Media ports are reserved in a LOW band (40000-40999), decisively below the
// macOS ephemeral range (49152-65535). The fixed STUN port 3478 is NOT reserved.
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

func runProfile(binary, out, profile, udpMode, tcpMode string, forceTCP, useStunPortAsICE, enableLoopback bool, record map[string]any) (err error) {
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
		"force_tcp":                 forceTCP,
		"use_stun_port_as_ice":      useStunPortAsICE,
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
		rtc["tcp_port"] = 0
	default:
		return fmt.Errorf("unknown tcp-mode %q", tcpMode)
	}
	record["configured_tcp_port"] = configuredTCPPort

	// The daemon binds the standalone STUN mux on 3478 only when
	// use_stun_port_as_ice && mux mode && !force_tcp (Go webrtc_config.go:123/131/162,
	// Rust interfaces.rs:274-277). Verify 3478 is FREE before launch so a busy port
	// is a fast infrastructure error, never a silent daemon refusal, and check it is
	// RELEASED after shutdown.
	stunPortBound := useStunPortAsICE && udpMode == "mux" && !forceTCP
	record["stun_port_expected_bound"] = stunPortBound
	if stunPortBound {
		probe, e := net.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", stunPort))
		if e != nil {
			return fmt.Errorf("STUN port %d not free before launch: %w", stunPort, e)
		}
		probe.Close()
		rebindUDP = append(rebindUDP, fmt.Sprintf("127.0.0.1:%d", stunPort))
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
	record["driven_items"] = []string{"YAML:rtc.use_stun_port_as_ice"}

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
	token, err := auth.NewAccessToken(apiKey, apiSecret).SetIdentity("owned-stun-port-publisher").SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-stun-port-room"}).ToJWT()
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
// from an SDP answer. Only `typ host` lines contribute. Grammar:
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