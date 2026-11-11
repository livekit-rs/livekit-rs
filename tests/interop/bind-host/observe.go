// Run from the pinned LiveKit Go module. Tests real HTTP and QUIC sockets.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
)

func run(binary, fixture, output, profile string, record map[string]any) (err error) {
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	var ports []int
	for range 3 {
		listener, e := net.Listen("tcp4", "127.0.0.1:0")
		if e != nil {
			return e
		}
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}
	udp, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		return e
	}
	defer udp.Close()
	wtPort := udp.LocalAddr().(*net.UDPAddr).Port
	media, e := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		return e
	}
	defer media.Close()
	mediaPort := media.Addr().(*net.TCPAddr).Port
	config := map[string]any{
		"port":          ports[0],
		"prometheus":    map[string]any{"port": ports[1]},
		"debug_handler": map[string]any{"port": ports[2]},
		"webtransport":  map[string]any{"port": wtPort, "tls_cert_file": filepath.Join(fixture, "server.pem"), "tls_key_file": filepath.Join(fixture, "server-key.pem")},
		"rtc":           map[string]any{"node_ip": "127.0.0.1", "enable_loopback_candidate": true, "use_external_ip": false, "tcp_port": mediaPort, "port_range_start": 50000, "port_range_end": 60000},
		"keys":          map[string]string{"fixture": "fixture-secret-that-is-only-used-by-tests"},
	}
	var bindArg string
	switch profile {
	case "numeric":
		config["bind_addresses"] = []string{"127.0.0.1"}
	case "hostname":
		config["bind_addresses"] = []string{"localhost"}
	case "hostname-cli":
		config["bind_addresses"] = []string{"127.0.0.1"}
		bindArg = "localhost"
	case "empty-list":
		config["bind_addresses"] = []string{}
	case "wildcard":
		config["bind_addresses"] = []string{""}
	case "nil-list":
	case "nonlocal":
		// TEST-NET address is not configured locally. Bind needs no packets.
		config["bind_addresses"] = []string{"127.0.0.1", "192.0.2.123"}
	case "malformed":
		config["bind_addresses"] = []string{"127.0.0.1", "[::1]"}
	default:
		return fmt.Errorf("unknown profile %s", profile)
	}
	raw, e := json.MarshalIndent(config, "", "  ")
	if e != nil {
		return e
	}
	cfgPath := filepath.Join(output, "server.yaml")
	if e := os.WriteFile(cfgPath, raw, 0600); e != nil {
		return e
	}
	log, e := os.OpenFile(filepath.Join(output, "server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer log.Close()
	args := []string{"--config", cfgPath}
	if bindArg != "" {
		args = append(args, "--bind", bindArg)
	}
	command := exec.Command(binary, args...)
	command.Stdout, command.Stderr = log, log
	for _, listener := range listeners {
		if e := listener.Close(); e != nil {
			return e
		}
	}
	if e := errors.Join(media.Close(), udp.Close()); e != nil {
		return e
	}
	if e := command.Start(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	alreadyExited := false
	defer func() {
		var cleanup error
		if !alreadyExited {
			cleanup = command.Process.Signal(syscall.SIGTERM)
			select {
			case exited := <-done:
				cleanup = errors.Join(cleanup, exited)
			case <-time.After(5 * time.Second):
				cleanup = errors.Join(cleanup, fmt.Errorf("owned daemon did not exit"), command.Process.Kill(), <-done)
			}
		}
		record["server_exit"] = command.ProcessState.ExitCode()
		var bindErr error
		for _, port := range append(ports, mediaPort) {
			listener, e := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
			if e == nil {
				e = listener.Close()
			}
			bindErr = errors.Join(bindErr, e)
		}
		rebound, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: wtPort})
		if e == nil {
			e = rebound.Close()
		}
		bindErr = errors.Join(bindErr, e)
		record["ports_released"] = bindErr == nil
		err = errors.Join(err, cleanup, bindErr)
	}()
	if profile == "nonlocal" || profile == "malformed" {
		select {
		case <-done:
			alreadyExited = true
		case <-time.After(5 * time.Second):
			return fmt.Errorf("invalid host did not stop startup")
		}
		observed, e := os.ReadFile(filepath.Join(output, "server.log"))
		if e != nil {
			return e
		}
		output := string(observed)
		var expected string
		if profile == "nonlocal" {
			errno := "cannot assign requested address"
			if runtime.GOOS == "darwin" {
				errno = "can't assign requested address"
			}
			expected = fmt.Sprintf("listen tcp 192.0.2.123:%d: bind: %s", ports[0], errno)
		} else {
			expected = fmt.Sprintf("listen tcp: address [[::1]]:%d: missing port in address", ports[0])
		}
		record["expected_diagnostic"] = expected
		if command.ProcessState.ExitCode() != 1 || !strings.Contains(output, expected) || strings.Contains(output, "starting LiveKit server") {
			return fmt.Errorf("unexpected invalid-host startup outcome")
		}
		return nil
	}
	pem, e := os.ReadFile(filepath.Join(fixture, "root.pem"))
	if e != nil {
		return e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return fmt.Errorf("invalid public fixture root")
	}
	start := time.Now()
	var connection *quic.Conn
	for time.Since(start) < 8*time.Second {
		select {
		case exited := <-done:
			alreadyExited = true
			return fmt.Errorf("daemon exited before QUIC: %v", exited)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		connection, e = quic.DialAddr(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(wtPort)), &tls.Config{RootCAs: roots, ServerName: "localhost", NextProtos: []string{"h3"}}, &quic.Config{})
		cancel()
		if e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e != nil {
		return fmt.Errorf("verified QUIC handshake: %w", e)
	}
	if connection.ConnectionState().TLS.NegotiatedProtocol != "h3" {
		_ = connection.CloseWithError(0, "unexpected ALPN")
		return fmt.Errorf("expected QUIC h3 ALPN")
	}
	record["verified_quic_h3"] = true
	if e := connection.CloseWithError(0, "fixture complete"); e != nil {
		return e
	}
	client := &http.Client{Timeout: 500 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	paths := []string{"/", "/metrics", "/debug/rooms"}
	for i, path := range paths {
		address := net.JoinHostPort("127.0.0.1", strconv.Itoa(ports[i]))
		response, e := client.Get("http://" + address + path)
		if profile == "empty-list" {
			if e == nil {
				response.Body.Close()
				return fmt.Errorf("unexpected HTTP listener %s", path)
			}
			if !errors.Is(e, syscall.ECONNREFUSED) {
				return fmt.Errorf("expected no HTTP listener for %s: %w", path, e)
			}
			continue
		}
		if e != nil {
			return fmt.Errorf("%s: %w", path, e)
		}
		response.Body.Close()
		record[path] = response.StatusCode
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("%s returned %d", path, response.StatusCode)
		}
		if profile == "hostname" || profile == "hostname-cli" || profile == "numeric" {
			v6 := net.JoinHostPort("::1", strconv.Itoa(ports[i]))
			response, e = client.Get("http://" + v6 + path)
			if e == nil {
				response.Body.Close()
				return fmt.Errorf("%s unexpectedly listened on both families", path)
			}
			if !errors.Is(e, syscall.ECONNREFUSED) {
				return fmt.Errorf("expected no IPv6 HTTP listener for %s: %w", path, e)
			}
		}
	}
	if profile == "hostname" || profile == "hostname-cli" {
		logs, e := os.ReadFile(filepath.Join(output, "server.log"))
		if e != nil {
			return e
		}
		if !strings.Contains(string(logs), `"bindAddresses": ["localhost"]`) {
			return fmt.Errorf("startup log did not preserve the configured hostname")
		}
	}
	time.Sleep(250 * time.Millisecond) // avoid racing Go's post-listen running flag
	return nil
}

func main() {
	binary := flag.String("binary", "", "immutable daemon")
	fixture := flag.String("fixture", "", "public TLS fixture directory")
	output := flag.String("out", "", "fresh output directory")
	profile := flag.String("profile", "", "configuration case")
	flag.Parse()
	if !filepath.IsAbs(*binary) || !filepath.IsAbs(*fixture) || !filepath.IsAbs(*output) {
		panic("absolute binary, fixture and output required")
	}
	if e := os.Mkdir(*output, 0700); e != nil {
		panic(e)
	}
	payload, e := os.ReadFile(*binary)
	if e != nil {
		panic(e)
	}
	digest := sha256.Sum256(payload)
	record := map[string]any{"binary": *binary, "binary_sha256": fmt.Sprintf("%x", digest), "profile": *profile}
	err := run(*binary, *fixture, *output, *profile, record)
	after, e := os.ReadFile(*binary)
	if e != nil || sha256.Sum256(after) != digest {
		err = errors.Join(err, fmt.Errorf("daemon changed during test"))
	}
	record["passed"] = err == nil
	if err != nil {
		record["error"] = err.Error()
	}
	data, e := json.MarshalIndent(record, "", "  ")
	if e != nil {
		panic(e)
	}
	if e := os.WriteFile(filepath.Join(*output, "result.json"), append(data, '\n'), 0600); e != nil {
		panic(e)
	}
	fmt.Println(string(data))
	if err != nil {
		os.Exit(1)
	}
}
