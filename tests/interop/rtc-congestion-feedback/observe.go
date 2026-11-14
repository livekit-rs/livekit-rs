// SPEC-0077 C35 — launched-MAIN congestion_control feedback-mode SDP observer.
//
// Run from the fixed Go module (references/livekit-stable) with either immutable
// daemon (locked Go oracle or Rust livecloud). It launches ONE daemon with a
// configurable rtc.congestion_control.{use_send_side_bwe,use_send_side_bwe_interceptor},
// drives an authenticated, MEDIA-FREE signal join over WebSocket /rtc with a modern
// non-Firefox client (protocol>5), and records the daemon's subscriber SDP OFFER
// per-media-kind RTCP-feedback catalog and RTP header-extension URI catalog. The
// observer publishes no track and sends no media: the offer is the daemon's own
// subscriber peer-connection negotiation, so the feedback selection originates from
// the daemon and depends on the congestion_control flags being consumed:
//
//	Go (locked v1.13.10 e5655ac2) pkg/rtc/config.go:
//	  :90/:99  Subscriber = getSubscriberConfig(UseSendSideBWEInterceptor || UseSendSideBWE)
//	  :175-207 getSubscriberConfig(enableTWCC bool):
//	           base video ext [dd, abs-capture-time], base video fb [ccm fir, nack, nack pli]
//	           enableTWCC=true  -> +TransportCCURI ext, +transport-cc fb
//	           enableTWCC=false -> +ABSSendTimeURI ext, +goog-remb fb
//	           audio ext [abs-capture-time], audio fb [nack] (never TWCC/ABS/REMB)
//	Rust:
//	  lk-config/src/official.rs:385-386   use_send_side_bwe[_interceptor]: bool = false (parse)
//	  lk-config/src/normalize.rs:227-228  passthrough into CongestionControl
//	  rtc-core/src/config.rs:267-275      effective_feedback_mode:
//	                                        use_send_side_bwe            -> NativeSendSide
//	                                        else use_send_side_bwe_interceptor -> InterceptorEstimate
//	                                        else                          -> ReceiverEstimate
//	  rtc-media/src/codecs.rs:136-151     base video fb (transport-cc + remb), Opus fb
//	  rtc-media/src/codecs.rs:171-198     base extension map (TWCC + ABS for sends)
//	  rtc-media/src/engine.rs:352-456     per-mode subscriber fb/ext normalization
//
// Discriminator (load-bearing): the subscriber offer's VIDEO m-line RTCP-feedback
// catalog (transport-cc vs goog-remb) and VIDEO header-extension URI catalog
// (transport-wide-cc vs abs-send-time), plus the AUDIO catalogs. These are
// deterministic per launch (payload-type numbers and extmap ids may vary, so we
// compare the feedback TYPE catalog and the extension URI catalog, not raw ids).
// The three profiles:
//
//	receiver-estimate    (bwe=false, interceptor=false) -> video goog-remb + abs-send-time
//	interceptor-estimate (bwe=false, interceptor=true)  -> video transport-cc + transport-wide-cc
//	native-send-side     (bwe=true,  interceptor=false) -> video transport-cc + transport-wide-cc
//
// This observer is a SEPARATE copy from the C34 rtc-use-ice-lite observer: it swaps
// the a=ice-lite discriminator for the per-media-kind rtcp-fb/extmap catalogs and
// names the congestion_control feedback leaves as the driven items, keeping the C34
// observer identity unchanged (anti-splicing).
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

const apiKey = "ownedcongestionfb"
const apiSecret = "fixture-secret-only-for-local-congestion-feedback-tests"

func main() {
	binary := flag.String("binary", "", "absolute path to the immutable daemon")
	out := flag.String("output", "", "absolute fresh output directory")
	profile := flag.String("profile", "", "profile name (label only)")
	useSendSideBWE := flag.Bool("use-send-side-bwe", false, "rtc.congestion_control.use_send_side_bwe value")
	useInterceptor := flag.Bool("use-send-side-bwe-interceptor", false, "rtc.congestion_control.use_send_side_bwe_interceptor value")
	protocol := flag.Int("protocol", 15, "client protocol version query param")
	enableLoopback := flag.Bool("enable-loopback", true, "rtc.enable_loopback_candidate value")
	flag.Parse()
	if *binary == "" || *out == "" || *profile == "" {
		fmt.Fprintln(os.Stderr, "usage: observe --binary B --output DIR --profile P [--use-send-side-bwe] [--use-send-side-bwe-interceptor] [--protocol N] [--enable-loopback=true|false]")
		os.Exit(2)
	}
	record := map[string]any{
		"profile":                       *profile,
		"use_send_side_bwe":             *useSendSideBWE,
		"use_send_side_bwe_interceptor": *useInterceptor,
		"protocol":                      *protocol,
		"enable_loopback_candidate":     *enableLoopback,
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fatal(record, *out, err)
	}
	before, err := hashFile(*binary)
	if err != nil {
		fatal(record, *out, err)
	}
	record["binary_sha256_before"] = before
	runErr := runProfile(*binary, *out, *profile, *useSendSideBWE, *useInterceptor, *protocol, *enableLoopback, record)
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

func runProfile(binary, out, profile string, useSendSideBWE bool, useInterceptor bool, protocol int, enableLoopback bool, record map[string]any) (err error) {
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
		"congestion_control": map[string]any{
			"use_send_side_bwe":             useSendSideBWE,
			"use_send_side_bwe_interceptor": useInterceptor,
		},
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
	record["driven_items"] = []string{
		"YAML:rtc.congestion_control.use_send_side_bwe",
		"YAML:rtc.congestion_control.use_send_side_bwe_interceptor",
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
			record["offer_sdp"] = ""
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
	joinErr := exerciseSignalJoin(address, protocol, record)
	time.Sleep(1 * time.Second)
	return joinErr
}

// exerciseSignalJoin dials the legacy /rtc WebSocket with the join stimulus on the
// query and an Authorization Bearer JWT, then reads the daemon's SignalResponse
// stream until the subscriber Offer arrives. It publishes no track and sends no
// media; the offer is the daemon's own subscriber PC negotiation. The per-media-kind
// rtcp-fb and extmap catalogs are the load-bearing observables.
func exerciseSignalJoin(address string, protocol int, record map[string]any) (err error) {
	token, err := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity("owned-congestion-fb-subscriber").
		SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-congestion-fb-room"}).
		ToJWT()
	if err != nil {
		return err
	}
	query := url.Values{}
	query.Set("protocol", strconv.Itoa(protocol))
	query.Set("sdk", "js")
	query.Set("auto_subscribe", "true")
	query.Set("browser", "Chrome")
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
			recordFeedbackObservables(sdp, record)
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

// mediaSection holds the rtcp-fb type catalog and extmap URI catalog for one media
// kind. Payload-type numbers and extmap ids are nondeterministic per launch, so we
// keep only the feedback TYPE (with parameter) and the extension URI.
type mediaSection struct {
	fb   map[string]bool
	uris map[string]bool
}

func newMediaSection() *mediaSection {
	return &mediaSection{fb: map[string]bool{}, uris: map[string]bool{}}
}

// parseSDP splits an SDP into per-media-kind sections. Session-level attributes
// (before the first m=) are ignored: the feedback contract is per media kind.
func parseSDP(sdp string) map[string]*mediaSection {
	out := map[string]*mediaSection{}
	var cur *mediaSection
	for _, raw := range strings.Split(sdp, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "m=") {
			kind := strings.Fields(line)[0][2:] // m=audio / m=video
			cur = newMediaSection()
			if _, ok := out[kind]; !ok {
				out[kind] = cur
			} else {
				cur = out[kind]
			}
			continue
		}
		if cur == nil {
			continue
		}
		if strings.HasPrefix(line, "a=rtcp-fb:") {
			// a=rtcp-fb:<pt> <type> [<param>]
			rest := strings.TrimSpace(line[len("a=rtcp-fb:"):])
			fields := strings.Fields(rest)
			if len(fields) >= 2 {
				fbType := strings.Join(fields[1:], " ")
				cur.fb[fbType] = true
			}
			continue
		}
		if strings.HasPrefix(line, "a=extmap:") {
			// a=extmap:<id>[/<direction>] <uri>
			rest := strings.TrimSpace(line[len("a=extmap:"):])
			fields := strings.Fields(rest)
			if len(fields) >= 2 {
				cur.uris[fields[1]] = true
			}
			continue
		}
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func hasURI(uris map[string]bool, substr string) bool {
	for u := range uris {
		if strings.Contains(u, substr) {
			return true
		}
	}
	return false
}

// recordFeedbackObservables extracts the deterministic per-media-kind feedback and
// extension catalogs and derives the load-bearing booleans. The video transport-cc
// vs goog-remb feedback and the transport-wide-cc vs abs-send-time extension are the
// feedback-mode discriminator; the audio catalogs prove audio carries neither.
func recordFeedbackObservables(sdp string, record map[string]any) {
	sections := parseSDP(sdp)
	video := sections["video"]
	audio := sections["audio"]
	if video == nil {
		video = newMediaSection()
	}
	if audio == nil {
		audio = newMediaSection()
	}
	record["video_mline_present"] = sections["video"] != nil
	record["audio_mline_present"] = sections["audio"] != nil
	record["video_rtcp_fb"] = sortedKeys(video.fb)
	record["audio_rtcp_fb"] = sortedKeys(audio.fb)
	record["video_extmap_uris"] = sortedKeys(video.uris)
	record["audio_extmap_uris"] = sortedKeys(audio.uris)

	record["video_transport_cc_fb"] = video.fb["transport-cc"]
	record["video_goog_remb_fb"] = video.fb["goog-remb"]
	record["video_nack_fb"] = video.fb["nack"]
	record["video_nack_pli_fb"] = video.fb["nack pli"]
	record["video_ccm_fir_fb"] = video.fb["ccm fir"]
	record["video_transport_cc_ext"] = hasURI(video.uris, "transport-wide-cc")
	record["video_abs_send_time_ext"] = hasURI(video.uris, "abs-send-time")

	record["audio_transport_cc_fb"] = audio.fb["transport-cc"]
	record["audio_goog_remb_fb"] = audio.fb["goog-remb"]
	record["audio_nack_fb"] = audio.fb["nack"]
	record["audio_transport_cc_ext"] = hasURI(audio.uris, "transport-wide-cc")
	record["audio_abs_send_time_ext"] = hasURI(audio.uris, "abs-send-time")
}