// Run against the fixed Go or integrated Rust daemon. The pinned Pion TURN
// client and fixed Go public credential generator drive both binaries.
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
	"syscall"
	"time"

	"github.com/pion/stun/v3"
	"github.com/pion/turn/v5"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/auth"
)

const apiKey = "turn-probe-fixture"
const apiSecret = "turn-probe-fixture-secret-32-characters"

func socket(network, ip string) (*net.UDPConn, error) {
	return net.ListenUDP(network, &net.UDPAddr{IP: net.ParseIP(ip)})
}
func port(socket *net.UDPConn) int { return socket.LocalAddr().(*net.UDPAddr).Port }

// Pion Client.Allocate omits REQUESTED-ADDRESS-FAMILY for IPv4. The
// negative fixture must emit the explicit attribute for both families.
type allocationAttributes byte

func (a allocationAttributes) AddTo(msg *stun.Message) error {
	msg.Add(stun.AttrRequestedTransport, []byte{17, 0, 0, 0})
	msg.Add(stun.AttrRequestedAddressFamily, []byte{byte(a), 0, 0, 0})
	return nil
}

func rejectOppositeFamily(client *turn.Client, network, server, username, password string, family turn.RequestedAddressFamily, result map[string]any) error {
	address, err := net.ResolveUDPAddr(network, server)
	if err != nil {
		return err
	}
	allocate := stun.NewType(stun.MethodAllocate, stun.ClassRequest)
	attrs := allocationAttributes(family)
	message, err := stun.Build(stun.TransactionID, allocate, attrs, stun.Fingerprint)
	if err != nil {
		return err
	}
	first, err := client.PerformTransaction(message, address, false)
	if err != nil {
		return err
	}
	var code stun.ErrorCodeAttribute
	if err := code.GetFrom(first.Msg); err != nil || code.Code != stun.CodeUnauthorized {
		return fmt.Errorf("opposite-family unauthenticated challenge: code=%d error=%v", code.Code, err)
	}
	var nonce stun.Nonce
	var realm stun.Realm
	if err := errors.Join(nonce.GetFrom(first.Msg), realm.GetFrom(first.Msg)); err != nil {
		return err
	}
	message, err = stun.Build(stun.TransactionID, allocate, attrs,
		stun.NewUsername(username), realm, nonce,
		stun.NewLongTermIntegrity(username, realm.String(), password), stun.Fingerprint)
	if err != nil {
		return err
	}
	second, err := client.PerformTransaction(message, address, false)
	if err != nil {
		return err
	}
	if err := code.GetFrom(second.Msg); err != nil {
		return fmt.Errorf("opposite-family authenticated response %s has no error code: %w", second.Msg.Type, err)
	}
	result["opposite_family_allocate_error"] = code.Code
	if code.Code != stun.CodeInsufficientCapacity {
		return fmt.Errorf("expected opposite-family wire error 508, got %d", code.Code)
	}
	return nil
}

func exercise(network, transport, server, clientIP, relayIP, advertisedIP string, peer *net.UDPConn, oppositeFamily turn.RequestedAddressFamily, result map[string]any) error {
	creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	username, expiry := creds.CreateUsername(apiKey, "PA_turnBindProbe", 300)
	password, err := creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
	if err != nil {
		return err
	}
	var connection net.PacketConn
	if transport == "udp" {
		connection, err = socket(network, clientIP)
	} else {
		var stream net.Conn
		stream, err = net.DialTimeout("tcp"+network[3:], server, time.Second)
		if err == nil {
			connection = turn.NewSTUNConn(stream)
		}
	}
	if err != nil {
		return err
	}
	defer connection.Close()
	client, err := turn.NewClient(&turn.ClientConfig{STUNServerAddr: server, TURNServerAddr: server, Username: username, Password: password, Realm: service.LivekitRealm, Conn: connection, RTO: 100 * time.Millisecond})
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Listen(); err != nil {
		return err
	}
	if oppositeFamily != 0 {
		return rejectOppositeFamily(client, network, server, username, password, oppositeFamily, result)
	}
	relay, err := client.Allocate()
	if err != nil {
		return fmt.Errorf("authenticated ALLOCATE: %w", err)
	}
	defer relay.Close()
	advertised := relay.LocalAddr().(*net.UDPAddr)
	result["advertised"] = advertised.String()
	if !advertised.IP.Equal(net.ParseIP(advertisedIP)) {
		return fmt.Errorf("unexpected advertised IP %s", advertised)
	}
	if err := client.CreatePermission(peer.LocalAddr()); err != nil {
		return err
	}
	if _, err := relay.WriteTo([]byte("turn-wire"), peer.LocalAddr()); err != nil {
		return err
	}
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	b := make([]byte, 64)
	n, from, err := peer.ReadFromUDP(b)
	if err != nil {
		return err
	}
	result["observed_relay_source"] = from.String()
	if string(b[:n]) != "turn-wire" || !from.IP.Equal(net.ParseIP(relayIP)) || from.Port != advertised.Port {
		return fmt.Errorf("actual relay source %s or payload %q diverged from bind %s and advertised port %d", from, b[:n], relayIP, advertised.Port)
	}
	if _, err := peer.WriteTo([]byte("turn-reply"), from); err != nil {
		return err
	}
	if err := relay.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	n, reverse, err := relay.ReadFrom(b)
	if err != nil {
		return err
	}
	if string(b[:n]) != "turn-reply" || reverse.String() != peer.LocalAddr().String() {
		return fmt.Errorf("reverse relay payload %q from %v", b[:n], reverse)
	}
	result["reverse_payload_verified"] = true
	return nil
}

func run(binary, output, profile, bind, transport string, result map[string]any) (err error) {
	network, clientIP, listenerIP, relayIP, advertisedIP := "udp4", "127.0.0.1", bind, bind, "127.0.0.1"
	switch profile {
	case "explicit-v4":
		if ip := net.ParseIP(bind); ip.To4() == nil || ip.Equal(net.ParseIP(clientIP)) || ip.IsUnspecified() {
			return fmt.Errorf("explicit-v4 needs an assigned IPv4 different from loopback")
		}
	case "wildcard-v4":
		if ip := net.ParseIP(bind); ip.To4() == nil || ip.Equal(net.ParseIP(clientIP)) || ip.IsUnspecified() {
			return fmt.Errorf("wildcard-v4 needs a connection destination different from the peer")
		}
		// The TCP stream's local address is this explicit destination,
		// whereas the configured listener/relay binds are wildcard.
		relayIP = clientIP
	case "explicit-v6":
		network, clientIP, listenerIP, relayIP, advertisedIP = "udp6", "::1", "::1", "::1", "::1"
	default:
		return fmt.Errorf("invalid profile %q", profile)
	}
	result["expected_actual_relay_ip"] = relayIP
	result["configured_advertised_ip"] = advertisedIP
	allowed, err := socket(network, clientIP)
	if err != nil {
		return err
	}
	defer allowed.Close()
	relayPort, err := socket(network, relayIP)
	if err != nil {
		return err
	}
	defer relayPort.Close()
	var listener net.Listener
	var packet net.PacketConn
	var turnAddr string
	if transport == "udp" {
		p, err := socket(network, listenerIP)
		if err != nil {
			return err
		}
		packet, turnAddr = p, p.LocalAddr().String()
		defer packet.Close()
	} else if transport == "tcp" {
		l, err := net.Listen("tcp"+network[3:], net.JoinHostPort(listenerIP, "0"))
		if err != nil {
			return err
		}
		listener, turnAddr = l, l.Addr().String()
		defer listener.Close()
	} else {
		return fmt.Errorf("unknown transport %s", transport)
	}
	_, portString, err := net.SplitHostPort(turnAddr)
	if err != nil {
		return err
	}
	var turnPort int
	if _, err := fmt.Sscan(portString, &turnPort); err != nil {
		return err
	}
	result["listener_destination"] = turnAddr
	if profile == "wildcard-v4" {
		result["configured_bind"] = "default wildcard"
	} else {
		result["configured_bind"] = listenerIP
	}
	httpListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer httpListener.Close()
	if port(allowed) == 65535 || turnPort == port(allowed)+1 || port(relayPort) == port(allowed)+1 {
		return fmt.Errorf("fixture address selection collided with ICE range")
	}
	httpAddr := httpListener.Addr().String()
	turnCfg := map[string]any{"enabled": true, "relay_range_start": port(relayPort), "relay_range_end": port(relayPort),
		"allow_restricted_peer_cidrs": []string{"127.0.0.0/8", "::1/128"}}
	if profile != "wildcard-v4" {
		turnCfg["bind_addresses"] = []string{listenerIP}
	}
	if transport == "tcp" {
		turnCfg["tls_port"], turnCfg["domain"], turnCfg["external_tls"] = turnPort, "turn.example.com", true
	} else {
		turnCfg["udp_port"] = turnPort
	}
	config := map[string]any{"port": httpListener.Addr().(*net.TCPAddr).Port, "bind_addresses": []string{"127.0.0.1"},
		"keys": map[string]string{apiKey: apiSecret},
		"rtc": map[string]any{"node_ip": advertisedIP, "use_external_ip": false, "enable_loopback_candidate": true,
			"tcp_port": 0, "port_range_start": port(allowed), "port_range_end": port(allowed) + 1}, "turn": turnCfg}
	cfg, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	cfgPath := filepath.Join(output, "server.yaml")
	if err := os.WriteFile(cfgPath, cfg, 0600); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(output, "server.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	command := exec.Command(binary, "--config", cfgPath)
	command.Stdout, command.Stderr = log, log
	if err := httpListener.Close(); err != nil {
		return err
	}
	if packet != nil {
		err = packet.Close()
	} else {
		err = listener.Close()
	}
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	earlyExit := false
	releaseAddr := relayPort.LocalAddr().String()
	defer func() {
		var stop error
		if !earlyExit {
			stop = command.Process.Signal(syscall.SIGTERM)
			select {
			case exit := <-exited:
				stop = errors.Join(stop, exit)
			case <-time.After(10 * time.Second):
				stop = errors.Join(stop, fmt.Errorf("owned daemon failed to stop"), command.Process.Kill(), <-exited)
			}
		}
		result["server_exit"] = command.ProcessState.ExitCode()
		if command.ProcessState.ExitCode() != 0 {
			stop = errors.Join(stop, fmt.Errorf("daemon exit %d", command.ProcessState.ExitCode()))
		}
		var bindErr error
		if transport == "udp" {
			s, e := net.ListenPacket(network, turnAddr)
			bindErr = e
			if e == nil {
				bindErr = s.Close()
			}
		} else {
			l, e := net.Listen("tcp"+network[3:], turnAddr)
			bindErr = e
			if e == nil {
				bindErr = l.Close()
			}
		}
		if s, e := net.ListenPacket(network, releaseAddr); e != nil {
			bindErr = errors.Join(bindErr, e)
		} else {
			bindErr = errors.Join(bindErr, s.Close())
		}
		result["owned_listener_and_relay_released"] = bindErr == nil
		err = errors.Join(err, stop, bindErr)
	}()
	client := &http.Client{Timeout: 300 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(8 * time.Second)
	for {
		select {
		case exit := <-exited:
			earlyExit = true
			return fmt.Errorf("daemon failed before ready: %v", exit)
		default:
		}
		r, e := client.Get("http://" + httpAddr + "/")
		if e == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("daemon readiness timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The HTTP listener becomes responsive before Go marks the process
	// running. Let startup settle before the very short wire exchange so
	// SIGTERM tests normal shutdown, not the earlier startup race.
	time.Sleep(250 * time.Millisecond)
	if err := relayPort.Close(); err != nil {
		return err
	}
	if err := exercise(network, transport, turnAddr, clientIP, relayIP, advertisedIP, allowed, 0, result); err != nil {
		return err
	}
	opposite := turn.RequestedAddressFamilyIPv6
	if network == "udp6" {
		opposite = turn.RequestedAddressFamilyIPv4
	}
	return exercise(network, transport, turnAddr, clientIP, relayIP, advertisedIP, allowed, opposite, result)
}

func main() {
	binary := flag.String("binary", "", "absolute daemon binary")
	out := flag.String("out", "", "new absolute output directory")
	profile := flag.String("profile", "explicit-v4", "explicit-v4, wildcard-v4 or explicit-v6")
	bind := flag.String("v4-bind", "", "explicit local IPv4 address distinct from 127.0.0.1")
	transport := flag.String("transport", "udp", "udp or tcp (external TLS)")
	flag.Parse()
	if !filepath.IsAbs(*binary) || !filepath.IsAbs(*out) {
		panic("absolute -binary and -out required")
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		panic(err)
	}
	result := map[string]any{"binary": *binary, "observer": "pion-turn/v5"}
	result["profile"], result["transport"] = *profile, *transport
	raw, err := os.ReadFile(*binary)
	if err != nil {
		panic(err)
	}
	result["binary_sha256"] = fmt.Sprintf("%x", sha256.Sum256(raw))
	err = run(*binary, *out, *profile, *bind, *transport, result)
	after, hashErr := os.ReadFile(*binary)
	if hashErr != nil || sha256.Sum256(raw) != sha256.Sum256(after) {
		err = errors.Join(err, fmt.Errorf("daemon binary changed during test"))
	}
	result["passed"] = err == nil
	if err != nil {
		result["error"] = err.Error()
	}
	b, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		panic(marshalErr)
	}
	if writeErr := os.WriteFile(filepath.Join(*out, "result.json"), append(b, '\n'), 0600); writeErr != nil {
		panic(writeErr)
	}
	fmt.Println(string(b))
	if err != nil {
		os.Exit(1)
	}
}
