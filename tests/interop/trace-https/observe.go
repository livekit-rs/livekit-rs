// Run from the fixed Go module with either immutable daemon.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/pion/webrtc/v4"
	otlp "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

const apiKey = "ownedtrace"
const apiSecret = "fixture-secret-only-for-local-trace-tests"

type collector struct {
	server              *http.Server
	address             string
	done                chan error
	hellos              atomic.Int64
	mu                  sync.Mutex
	spans               []string
	requests            int
	protocol            string
	authenticatedClient bool
}

func startCollector(fixture, profile string, clientRoot *x509.CertPool) (*collector, error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	c := &collector{address: ln.Addr().String(), done: make(chan error, 1)}
	c.server = &http.Server{ReadHeaderTimeout: time.Second, ErrorLog: log.New(io.Discard, "", 0)}
	c.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			http.Error(w, "unexpected OTLP request", http.StatusBadRequest)
			return
		}
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		export := &otlp.ExportTraceServiceRequest{}
		if err = proto.Unmarshal(data, export); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		c.requests++
		c.protocol = r.Proto
		c.authenticatedClient = r.TLS != nil && len(r.TLS.VerifiedChains) > 0
		for _, rs := range export.GetResourceSpans() {
			validResource := false
			for _, attribute := range rs.GetResource().GetAttributes() {
				if attribute.GetKey() == "service.name" && attribute.GetValue().GetStringValue() == "livekit" {
					validResource = true
				}
			}
			if !validResource {
				continue
			}
			for _, ss := range rs.GetScopeSpans() {
				for _, s := range ss.GetSpans() {
					if len(s.GetTraceId()) == 16 && len(s.GetSpanId()) == 8 {
						c.spans = append(c.spans, s.GetName())
					}
				}
			}
		}
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})
	if profile == "http" {
		go func() { c.done <- c.server.Serve(ln) }()
	} else {
		cert, err := tls.LoadX509KeyPair(filepath.Join(fixture, "server.pem"), filepath.Join(fixture, "server-key.pem"))
		if err != nil {
			ln.Close()
			return nil, err
		}
		c.server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { c.hellos.Add(1); return nil, nil }}
		if clientRoot != nil {
			c.server.TLSConfig.ClientCAs = clientRoot
			c.server.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		go func() { c.done <- c.server.ServeTLS(ln, "", "") }()
	}
	return c, nil
}

func (c *collector) snapshot(record map[string]any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record["collector_requests"] = c.requests
	record["collector_span_names"] = append([]string{}, c.spans...)
	record["tls_client_hellos"] = c.hellos.Load()
	record["collector_http_protocol"] = c.protocol
	record["collector_authenticated_client"] = c.authenticatedClient
	for _, s := range c.spans {
		if s == "Sent.WHIP.Create" {
			return true
		}
	}
	return false
}

func exerciseWHIP(client *http.Client, url string, record map[string]any) (err error) {
	token, err := auth.NewAccessToken(apiKey, apiSecret).SetIdentity("owned-whip-publisher").SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: "owned-trace-room"}).ToJWT()
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
	if response.StatusCode != http.StatusCreated || !strings.HasPrefix(location, "/whip/v1/") {
		return fmt.Errorf("WHIP response %d: %s", response.StatusCode, body)
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
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
			err = errors.Join(err, fmt.Errorf("unexpected WHIP delete status %d", response.StatusCode))
		}
	}()
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}); err != nil {
		return err
	}
	return nil
}

func clientIdentity(out string) (roots *x509.CertPool, certPath, keyPath string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", "", err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, "", "", err
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, "", "", err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, "", "", err
	}
	certPath, keyPath = filepath.Join(out, "owned-client.pem"), filepath.Join(out, "owned-client-key.pem")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		return nil, "", "", err
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		return nil, "", "", err
	}
	roots = x509.NewCertPool()
	roots.AddCert(certificate)
	return roots, certPath, keyPath, nil
}

func run(binary, fixture, out, profile string, record map[string]any) (err error) {
	var clientRoots *x509.CertPool
	var certPath, keyPath string
	if profile == "mtls" {
		clientRoots, certPath, keyPath, err = clientIdentity(out)
		if err != nil {
			return err
		}
	}
	c, err := startCollector(fixture, profile, clientRoots)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closeErr := c.server.Shutdown(ctx)
		if closeErr != nil {
			closeErr = errors.Join(closeErr, c.server.Close())
		}
		serveErr := <-c.done
		c.snapshot(record)
		rebound, bindErr := net.Listen("tcp4", c.address)
		if bindErr == nil {
			bindErr = rebound.Close()
		}
		record["collector_port_released"] = bindErr == nil
		if !errors.Is(serveErr, http.ErrServerClosed) {
			err = errors.Join(err, serveErr)
		}
		err = errors.Join(err, closeErr, bindErr)
	}()
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
	mediaAddress := media.Addr().String()
	address := listener.Addr().String()
	collectorURL := "https://" + c.address
	if profile == "http" {
		collectorURL = "http://" + c.address
	}
	config := map[string]any{
		"port":           listener.Addr().(*net.TCPAddr).Port,
		"bind_addresses": []string{"127.0.0.1"},
		"keys":           map[string]string{apiKey: apiSecret},
		"rtc":            map[string]any{"node_ip": "127.0.0.1", "enable_loopback_candidate": true, "use_external_ip": false, "tcp_port": media.Addr().(*net.TCPAddr).Port, "port_range_start": 50000, "port_range_end": 60000},
		"trace":          map[string]any{"jaeger_url": collectorURL},
	}
	cfg, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	cfgPath := filepath.Join(out, "server.yaml")
	if err = os.WriteFile(cfgPath, cfg, 0600); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(out, "server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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
	command.Env = append(command.Env, "OTEL_BSP_SCHEDULE_DELAY=250")
	if profile == "trusted" || profile == "mtls" {
		command.Env = append(command.Env, "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE="+filepath.Join(fixture, "root.pem"))
	}
	if profile == "mtls" {
		command.Env = append(command.Env, "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE="+certPath, "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY="+keyPath)
	}
	if profile == "wrong-root" {
		command.Env = append(command.Env, "OTEL_EXPORTER_OTLP_CERTIFICATE="+filepath.Join(fixture, "root.pem"),
			"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE="+filepath.Join(fixture, "p384-root.pem"))
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
		if command.ProcessState.ExitCode() != 0 {
			shutdown = errors.Join(shutdown, fmt.Errorf("server exit %d", command.ProcessState.ExitCode()))
		}
		var bindErr error
		for _, addr := range []string{address, mediaAddress} {
			rebound, e := net.Listen("tcp4", addr)
			if e == nil {
				e = rebound.Close()
			}
			bindErr = errors.Join(bindErr, e)
		}
		record["server_ports_released"] = bindErr == nil
		err = errors.Join(err, shutdown, bindErr)
	}()
	base := "http://" + address
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	until := time.Now().Add(8 * time.Second)
	for {
		select {
		case e := <-exited:
			early = true
			return fmt.Errorf("daemon exited before ready: %v", e)
		default:
		}
		response, e := client.Get(base + "/")
		if e == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(until) {
			return fmt.Errorf("daemon readiness timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err = exerciseWHIP(client, base, record); err != nil {
		return err
	}
	until = time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		found := c.snapshot(record)
		if profile == "http" || profile == "trusted" || profile == "mtls" {
			if found {
				if profile != "http" && (c.hellos.Load() == 0 || record["collector_http_protocol"] != "HTTP/2.0") {
					return fmt.Errorf("HTTPS positive did not negotiate TLS HTTP/2: %v", record["collector_http_protocol"])
				}
				if profile == "mtls" && record["collector_authenticated_client"] != true {
					return fmt.Errorf("mTLS positive did not verify client identity")
				}
				return nil
			}
		} else if c.hellos.Load() > 0 {
			time.Sleep(500 * time.Millisecond)
			c.snapshot(record)
			if record["collector_requests"] != 0 {
				return fmt.Errorf("untrusted TLS delivered a request")
			}
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("no required OTLP wire observation")
}

func main() {
	binary := flag.String("binary", "", "absolute immutable daemon")
	out := flag.String("out", "", "fresh absolute output directory")
	fixture := flag.String("fixture", "", "public TLS fixtures")
	profile := flag.String("profile", "", "http, trusted, mtls, wrong-root or untrusted")
	flag.Parse()
	if !filepath.IsAbs(*binary) || !filepath.IsAbs(*out) || !filepath.IsAbs(*fixture) {
		panic("absolute paths required")
	}
	switch *profile {
	case "http", "trusted", "mtls", "wrong-root", "untrusted":
	default:
		panic("invalid profile")
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		panic(err)
	}
	raw, err := os.ReadFile(*binary)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(raw)
	record := map[string]any{"profile": *profile, "binary": *binary, "binary_sha256": fmt.Sprintf("%x", digest)}
	err = run(*binary, *fixture, *out, *profile, record)
	later, readErr := os.ReadFile(*binary)
	if readErr != nil || sha256.Sum256(later) != digest {
		err = errors.Join(err, fmt.Errorf("daemon binary changed"))
	}
	record["passed"] = err == nil
	if err != nil {
		record["error"] = err.Error()
	}
	data, err2 := json.MarshalIndent(record, "", "  ")
	if err2 != nil {
		panic(err2)
	}
	if err2 = os.WriteFile(filepath.Join(*out, "result.json"), append(data, '\n'), 0600); err2 != nil {
		panic(err2)
	}
	fmt.Println(string(data))
	if err != nil {
		os.Exit(1)
	}
}
