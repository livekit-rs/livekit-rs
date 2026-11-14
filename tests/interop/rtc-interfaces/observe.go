// SPEC-0077 C30 — launched-MAIN rtc.interfaces.{includes,excludes} ICE host-candidate observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon with a
// configurable rtc.interfaces.{includes,excludes} NAME filter, drives an
// authenticated WHIP Create then DELETE with a Pion offer, and records the HOST
// ICE candidate IP set the daemon gathered into its SDP answer. The Pion driver
// configures no interface filter of its own, so every recorded host candidate
// originates from the daemon and depends on rtc.interfaces.{includes,excludes}
// being consumed by InterfaceFilterFromConf (mediatransportutil
// webrtc_config.go:70-72,422-439) / Rust named_allowed
// (livecloud/src/interfaces.rs:41-47, applied media_interfaces_from:103 and
// nat_discovery_addresses:180).
//
// Interface names are host-specific, so the observer resolves two tokens against
// the live OS interface table (net.Interfaces) and records both the enumerated
// table and the resolved name lists for transparency:
//
//	@loopback -> the UP loopback interface name (lo0 on macOS)
//	@all      -> every interface name (UP or down), so an exclude of @all admits
//	             no interface at all
//
// Any other token is a literal interface name. Unlike CIDRs, interface names are
// never parsed, so there is NO malformed-refusal profile: the name filter is an
// exact-string slices.Contains / n==name match (NOT a glob), with includes taking
// precedence over excludes.
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
	"strings"
	"syscall"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/pion/webrtc/v4"
)

const apiKey = "ownedifaces"
const apiSecret = "fixture-secret-only-for-local-iface-tests"

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	profile := flag.String("profile", "", "profile name (label only)")
	ifIncludes := flag.String("if-includes", "", "comma-separated rtc.interfaces.includes names (@loopback/@all tokens)")
	ifExcludes := flag.String("if-excludes", "", "comma-separated rtc.interfaces.excludes names (@loopback/@all tokens)")
	flag.Parse()
	if *binary == "" || *out == "" || *profile == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --profile P [--if-includes a,b] [--if-excludes c,d]")
		os.Exit(2)
	}
	record := map[string]any{"profile": *profile}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fatal(record, *out, err)
	}
	before, err := hashFile(*binary)
	if err != nil {
		fatal(record, *out, err)
	}
	record["binary_sha256_before"] = before
	runErr := runProfile(*binary, *out, *profile, splitList(*ifIncludes), splitList(*ifExcludes), record)
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

// enumerateInterfaces records the live OS interface table and returns the UP
// loopback name plus every interface name, so @loopback / @all resolve against
// the same host the daemon runs on.
func enumerateInterfaces(record map[string]any) (loopback string, all []string, err error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", nil, err
	}
	var table []map[string]any
	for _, iface := range ifaces {
		up := iface.Flags&net.FlagUp != 0
		lb := iface.Flags&net.FlagLoopback != 0
		// Names + flags only: raw machine addresses stay private (the
		// differential needs the name list, not the host IP topology).
		table = append(table, map[string]any{
			"name": iface.Name, "up": up, "loopback": lb,
		})
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

// resolveNames expands @loopback / @all tokens against the live interface table.
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

func runProfile(binary, out, profile string, ifIncludes, ifExcludes []string, record map[string]any) (err error) {
	loopback, all, err := enumerateInterfaces(record)
	if err != nil {
		return err
	}
	includes := resolveNames(ifIncludes, all, loopback)
	excludes := resolveNames(ifExcludes, all, loopback)
	record["resolved_if_includes"] = includes
	record["resolved_if_excludes"] = excludes

	// Reserve exclusive loopback ports for the daemon HTTP and media TCP, then
	// release them so the daemon can bind. The UDP media range is fixed
	// 50000-60000 (shared host), so profiles MUST run sequentially.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	media, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer media.Close()
	address := listener.Addr().String()
	mediaAddress := media.Addr().String()

	rtc := map[string]any{
		"node_ip":                   "127.0.0.1",
		"enable_loopback_candidate": true,
		"use_external_ip":           false,
		"tcp_port":                  media.Addr().(*net.TCPAddr).Port,
		"port_range_start":          50000,
		"port_range_end":            60000,
	}
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
		"port":           listener.Addr().(*net.TCPAddr).Port,
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
	driven := "YAML:rtc.interfaces.excludes"
	if len(includes) > 0 {
		driven = "YAML:rtc.interfaces.includes"
	}
	record["driven_items"] = []string{driven}

	logPath := filepath.Join(out, "server.log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	command := exec.Command(binary, "--config", cfgPath)
	command.Stdout, command.Stderr = file, file
	// Strip inherited OTEL_/SSL_CERT_ so no ambient telemetry/trust leaks in.
	for _, varEntry := range os.Environ() {
		if strings.HasPrefix(varEntry, "OTEL_") || strings.HasPrefix(varEntry, "SSL_CERT_") {
			continue
		}
		command.Env = append(command.Env, varEntry)
	}
	if err = errors.Join(listener.Close(), media.Close()); err != nil {
		return err
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
		for _, addr := range []string{address, mediaAddress} {
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
			record["whip_create_status"] = 0
			record["whip_delete_status"] = 0
			record["host_candidate_ips"] = []string{}
			record["host_candidate_count"] = 0
			record["candidate_types"] = []string{}
			record["answer_sdp"] = ""
			// Interface names never refuse startup; an early exit here is an
			// infrastructure failure the harness judges, not an expected profile.
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
	// Let the daemon fully tear down the WHIP session (async participant close)
	// before SIGTERM, so graceful shutdown does not race the session teardown.
	time.Sleep(1 * time.Second)
	return whipErr
}

func exerciseWHIP(client *http.Client, url string, record map[string]any) (err error) {
	token, err := auth.NewAccessToken(apiKey, apiSecret).SetIdentity("owned-ifaces-publisher").SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-ifaces-room"}).ToJWT()
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
	ips, types := hostCandidates(answer)
	record["host_candidate_ips"] = ips
	record["host_candidate_count"] = len(ips)
	record["candidate_types"] = types
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

// hostCandidates extracts the sorted unique HOST candidate connection-addresses
// and the sorted unique candidate-type catalog from an SDP answer. Only `typ
// host` lines contribute IPs (srflx/relay reflect STUN/TURN, not the local
// interface name allowlist under test). The candidate attribute grammar is:
//
//	a=candidate:<foundation> <component> <transport> <priority> <ip> <port> typ <type> ...
func hostCandidates(sdp string) (ips []string, types []string) {
	ipSet := map[string]bool{}
	typeSet := map[string]bool{}
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
		if typ == "" {
			continue
		}
		typeSet[typ] = true
		if typ == "host" && len(fields) >= 5 {
			ipSet[fields[4]] = true
		}
	}
	for ip := range ipSet {
		ips = append(ips, ip)
	}
	for t := range typeSet {
		types = append(types, t)
	}
	sort.Strings(ips)
	sort.Strings(types)
	if ips == nil {
		ips = []string{}
	}
	if types == nil {
		types = []string{}
	}
	return ips, types
}