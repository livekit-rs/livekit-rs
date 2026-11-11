// Run against the fixed Go or integrated Rust daemon. The pinned Pion TURN
// client and fixed Go public credential generator drive both binaries.
package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
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
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pion/stun/v3"
	"github.com/pion/turn/v5"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
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

func proxyTLS(stream net.Conn, server, mode string, result map[string]any) (net.Conn, string, bool, error) {
	expected := stream.LocalAddr().String()
	port := stream.LocalAddr().(*net.TCPAddr).Port
	dest := stream.RemoteAddr().(*net.TCPAddr).Port
	if strings.HasPrefix(mode, "v1") {
		header := fmt.Sprintf("PROXY TCP4 203.0.113.9 127.0.0.1 %d %d\r\n", port, dest)
		splitAt := 0
		switch mode {
		case "v1-inet6":
			header = fmt.Sprintf("PROXY TCP6 2001:db8::9 ::1 %d %d\r\n", port, dest)
			expected = net.JoinHostPort("2001:db8::9", fmt.Sprint(port))
		case "v1", "v1-segment-prefix", "v1-segment-signature", "v1-segment-tuple", "v1-segment-crlf":
			expected = net.JoinHostPort("203.0.113.9", fmt.Sprint(port))
			switch mode {
			case "v1-segment-prefix":
				splitAt = 4
			case "v1-segment-signature":
				splitAt = 5
			case "v1-segment-tuple":
				splitAt = 20
			case "v1-segment-crlf":
				splitAt = len(header) - 1
			}
		case "v1-unknown-opaque":
			header = "PROXY UNKNOWN \xff\xfe\x00 opaque\r\n"
		case "v1-unknown-bare-lf":
			header = "PROXY UNKNOWN opaque\ntrailer\r\n"
		case "v1-limit", "v1-oversized":
			length := 107
			if mode == "v1-oversized" {
				length++
			}
			const prefix = "PROXY UNKNOWN "
			header = prefix + strings.Repeat("x", length-len(prefix)-2) + "\r\n"
			result["proxy_v1_header_bytes"] = length
		case "v1-double-space":
			header = strings.Replace(header, "PROXY ", "PROXY  ", 1)
		case "v1-padded-port":
			header = fmt.Sprintf("PROXY TCP4 203.0.113.9 127.0.0.1 0%d %d\r\n", port, dest)
		default:
			return nil, "", false, fmt.Errorf("invalid proxy profile %q", mode)
		}
		if splitAt > 0 {
			if err := stream.(*net.TCPConn).SetNoDelay(true); err != nil {
				return nil, "", false, err
			}
			if _, err := fmt.Fprint(stream, header[:splitAt]); err != nil {
				return nil, "", false, err
			}
			time.Sleep(200 * time.Millisecond)
			result["split_at"] = splitAt
			result["split_delay_ms"] = 200
		}
		if _, err := fmt.Fprint(stream, header[splitAt:]); err != nil {
			if splitAt >= len("PROXY") && (errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)) {
				// The complete signature was sent in the first write.
				// An early close can prevent sending the final fragment.
				result["proxy_tls_outcome"] = "closed_before_final_header_fragment"
				result["proxy_rejected_before_verified_tls"] = true
				result["observed_error"] = err.Error()
				return nil, "", true, nil
			}
			return nil, "", false, err
		}
	} else if mode != "missing" {
		command, family := byte(0x21), byte(0x11)
		body := []byte{203, 0, 113, 9, 127, 0, 0, 1, 0, 0, 0, 0}
		binary.BigEndian.PutUint16(body[8:10], uint16(port))
		binary.BigEndian.PutUint16(body[10:12], uint16(dest))
		switch mode {
		case "v2", "v2-limit", "v2-oversized":
			expected = net.JoinHostPort("203.0.113.9", fmt.Sprint(port))
		case "local-full", "local-limit", "local-oversized":
			command = 0x20
		case "local-empty":
			command = 0x20
			body = nil
		case "unix-stream", "unix-dgram", "unix-short", "unix-local", "unix-local-short":
			family = 0x31
			if mode == "unix-dgram" {
				family = 0x32
			}
			if strings.HasPrefix(mode, "unix-local") {
				command = 0x20
			}
			body = make([]byte, 216)
			sourceName, destinationName := "owned-source", "owned-destination"
			if os.Getenv("LK_TURN_UNIX_OWNERSHIP") != "" && os.Getenv("LK_TURN_UNIX_NAMES") != "same" {
				sourceName = fmt.Sprintf("%s-%d", sourceName, port)
				destinationName = fmt.Sprintf("%s-%d", destinationName, port)
			}
			copy(body[:108], sourceName)
			copy(body[108:], destinationName)
			result["unix_source_name"] = sourceName
			result["unix_destination_name"] = destinationName
			result["raw_tcp_peer"] = stream.LocalAddr().String()
			if strings.HasSuffix(mode, "short") {
				body = body[:215]
			}
		case "bad-transport", "retire-tls":
			family = 0x13
		case "proxy-unspec":
			family = 0x00
			body = nil
		case "untrusted":
		default:
			return nil, "", false, fmt.Errorf("invalid proxy profile %q", mode)
		}
		switch mode {
		case "v2-limit", "local-limit", "v2-oversized", "local-oversized":
			length := 4096
			if mode == "v2-oversized" || mode == "local-oversized" {
				length++
			}
			body = append(body, make([]byte, length-len(body))...)
			result["proxy_v2_payload_bytes"] = length
		}
		header := append([]byte("\r\n\r\n\x00\r\nQUIT\n"), command, family)
		header = binary.BigEndian.AppendUint16(header, uint16(len(body)))
		if _, err := stream.Write(append(header, body...)); err != nil {
			return nil, "", false, err
		}
	}
	root, err := os.ReadFile(filepath.Join(os.Getenv("LK_TURN_TLS_FIXTURE"), "root.pem"))
	if err != nil {
		return nil, "", false, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(root) {
		return nil, "", false, fmt.Errorf("invalid fixture CA")
	}
	conn := tls.Client(stream, &tls.Config{ServerName: "localhost", RootCAs: roots, MinVersion: tls.VersionTLS12})
	if err := stream.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return nil, "", false, err
	}
	err = conn.Handshake()
	reject := mode == "missing" || mode == "bad-transport" || mode == "proxy-unspec" || mode == "untrusted" ||
		mode == "v2-oversized" || mode == "local-oversized" ||
		mode == "v1-oversized" || mode == "v1-double-space" || mode == "v1-padded-port" ||
		mode == "v1-segment-signature" || mode == "v1-segment-tuple" || mode == "v1-segment-crlf" ||
		mode == "v1-unknown-bare-lf" || mode == "unix-short" || mode == "retire-tls"
	if reject {
		if err == nil {
			result["verified_tls"] = len(conn.ConnectionState().VerifiedChains) > 0
			return nil, "", false, fmt.Errorf("invalid proxy path negotiated TLS")
		}
		var timeout net.Error
		var verify *tls.CertificateVerificationError
		if errors.As(err, &verify) {
			return nil, "", false, fmt.Errorf("certificate failure is not a proxy rejection: %w", err)
		}
		if errors.As(err, &timeout) && timeout.Timeout() {
			// Fixed Go can leave malformed trusted connections quiet.
			// Record this separately from an observed close.
			result["proxy_tls_outcome"] = "quiet_until_probe_deadline"
		} else {
			result["proxy_tls_outcome"] = "closed_before_verified_tls"
		}
		result["proxy_rejected_before_verified_tls"] = true
		result["observed_error"] = err.Error()
		return nil, "", true, nil
	}
	if err != nil {
		result["handshake_error"] = err.Error()
		return nil, "", false, err
	}
	if err = stream.SetDeadline(time.Time{}); err != nil {
		return nil, "", false, err
	}
	result["verified_tls"] = len(conn.ConnectionState().VerifiedChains) > 0
	return conn, expected, false, nil
}

func exchangeSTUN(stream net.Conn, message *stun.Message) (*stun.Message, error) {
	if err := stream.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		return nil, err
	}
	if _, err := stream.Write(message.Raw); err != nil {
		return nil, err
	}
	head := make([]byte, 20)
	if _, err := io.ReadFull(stream, head); err != nil {
		return nil, err
	}
	body := make([]byte, int(binary.BigEndian.Uint16(head[2:4])))
	if _, err := io.ReadFull(stream, body); err != nil {
		return nil, err
	}
	reply := &stun.Message{Raw: append(head, body...)}
	if err := reply.Decode(); err != nil {
		return nil, err
	}
	if reply.TransactionID != message.TransactionID {
		return nil, fmt.Errorf("unexpected transaction")
	}
	return reply, nil
}

// AF_UNIX has no IP/port for STUN XOR-MAPPED-ADDRESS. Probe the actual TURN
// behavior after verified TLS rather than treating handshake as relay success.
type unixAllocateReply struct {
	Code        stun.ErrorCode
	Attributes  []string
	Transaction string
}

type unixAllocateProbe func(reuseTransaction bool) (unixAllocateReply, error)

type unixClient struct {
	stream             net.Conn
	username, password string
	realm              stun.Realm
	nonce              stun.Nonce
}

func (c unixClient) request(method stun.Method, attrs ...stun.Setter) (*stun.Message, error) {
	setters := []stun.Setter{stun.TransactionID, stun.NewType(method, stun.ClassRequest)}
	setters = append(setters, attrs...)
	return stun.Build(append(setters, stun.NewUsername(c.username), c.realm, c.nonce,
		stun.NewLongTermIntegrity(c.username, c.realm.String(), c.password), stun.Fingerprint)...)
}

func attributeNames(reply *stun.Message) []string {
	names := make([]string, 0, len(reply.Attributes))
	for _, attribute := range reply.Attributes {
		names = append(names, attribute.Type.String())
	}
	return names
}

func openUnixClient(stream net.Conn, username, password string, result map[string]any) (*unixClient, error) {
	bind, err := stun.Build(stun.TransactionID, stun.BindingRequest, stun.Fingerprint)
	if err != nil {
		return nil, err
	}
	reply, err := exchangeSTUN(stream, bind)
	var timeout net.Error
	result["binding_quiet"] = errors.As(err, &timeout) && timeout.Timeout()
	if result["binding_quiet"] != true {
		return nil, fmt.Errorf("UNIX Binding response %v: %v", reply, err)
	}
	allocate := stun.NewType(stun.MethodAllocate, stun.ClassRequest)
	attrs := allocationAttributes(1)
	request, err := stun.Build(stun.TransactionID, allocate, attrs, stun.Fingerprint)
	if err != nil {
		return nil, err
	}
	challenge, err := exchangeSTUN(stream, request)
	if err != nil {
		return nil, err
	}
	var code stun.ErrorCodeAttribute
	if err := code.GetFrom(challenge); err != nil || code.Code != stun.CodeUnauthorized {
		return nil, fmt.Errorf("challenge %v: %v", code, err)
	}
	result["allocate_challenge"] = code.Code
	result["allocate_challenge_attributes"] = attributeNames(challenge)
	if challenge.Type != stun.NewType(stun.MethodAllocate, stun.ClassErrorResponse) ||
		!slices.Equal(attributeNames(challenge), []string{"ERROR-CODE", "NONCE", "REALM"}) {
		return nil, fmt.Errorf("unexpected UNIX Allocate challenge %v", challenge)
	}
	var realm stun.Realm
	var nonce stun.Nonce
	if err := errors.Join(realm.GetFrom(challenge), nonce.GetFrom(challenge)); err != nil {
		return nil, err
	}
	return &unixClient{stream: stream, username: username, password: password, realm: realm, nonce: nonce}, nil
}

func unixAllocator(stream net.Conn, username, password string, result map[string]any) (unixAllocateProbe, error) {
	c, err := openUnixClient(stream, username, password, result)
	if err != nil {
		return nil, err
	}
	return c.allocate(), nil
}

func (c unixClient) allocate(extra ...stun.Setter) unixAllocateProbe {
	var lastRequest *stun.Message
	return func(reuseTransaction bool) (unixAllocateReply, error) {
		if !reuseTransaction || lastRequest == nil {
			request, err := c.request(stun.MethodAllocate, append([]stun.Setter{allocationAttributes(1)}, extra...)...)
			if err != nil {
				return unixAllocateReply{}, err
			}
			if request.TransactionID == [stun.TransactionIDSize]byte{} ||
				(lastRequest != nil && request.TransactionID == lastRequest.TransactionID) {
				return unixAllocateReply{}, fmt.Errorf("expected a fresh nonzero transaction")
			}
			lastRequest = request
		}
		reply, err := exchangeSTUN(c.stream, lastRequest)
		if err != nil {
			return unixAllocateReply{}, err
		}
		if reply.Type != stun.NewType(stun.MethodAllocate, stun.ClassErrorResponse) ||
			reply.Contains(stun.AttrXORMappedAddress) || reply.Contains(stun.AttrXORRelayedAddress) {
			return unixAllocateReply{}, fmt.Errorf("unexpected UNIX Allocate response %v", reply)
		}
		var code stun.ErrorCodeAttribute
		if err := code.GetFrom(reply); err != nil {
			return unixAllocateReply{}, err
		}
		return unixAllocateReply{
			Code:        code.Code,
			Attributes:  attributeNames(reply),
			Transaction: fmt.Sprintf("%x", reply.TransactionID),
		}, nil
	}
}

func expectUnixAllocate(allocate unixAllocateProbe, reuseTransaction bool, want stun.ErrorCode, label string, result map[string]any) error {
	reply, err := allocate(reuseTransaction)
	result[label] = reply.Code
	result[label+"_attributes"] = reply.Attributes
	result[label+"_transaction"] = reply.Transaction
	if err != nil || reply.Code != want || !slices.Equal(reply.Attributes, []string{"ERROR-CODE"}) {
		return fmt.Errorf("%s: expected error-only %v, got %v %v: %v", label, want, reply.Code, reply.Attributes, err)
	}
	return nil
}

func exerciseUnix(stream net.Conn, username, password string, result map[string]any) error {
	allocate, err := unixAllocator(stream, username, password, result)
	if err != nil {
		return err
	}
	return expectUnixAllocate(allocate, false, stun.CodeBadRequest, "authenticated_allocate_error", result)
}

func waitUnixRelayPorts(addresses []string, want int, label string, result map[string]any) ([]string, error) {
	return waitUnixRelayPortsFor(addresses, want, label, result, time.Second)
}

func waitUnixRelayPortsFor(addresses []string, want int, label string, result map[string]any, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	for {
		var busy []string
		for _, address := range addresses {
			probe, err := net.ListenPacket("udp", address)
			switch {
			case err == nil:
				if err := probe.Close(); err != nil {
					return nil, err
				}
			case errors.Is(err, syscall.EADDRINUSE):
				busy = append(busy, address)
			default:
				return nil, err
			}
		}
		if len(busy) == want || time.Now().After(deadline) {
			result[label] = map[string]any{"occupied_count": len(busy), "occupied_ports": busy}
			if len(busy) != want {
				return nil, fmt.Errorf("%s: %d relay ports occupied, expected %d", label, len(busy), want)
			}
			return busy, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func exerciseUnixOwnership(server, mode, ownership string, relayAddresses []string, result map[string]any) error {
	creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	username, expiry := creds.CreateUsername(apiKey, "PA_turnBindProbe", 300)
	password, err := creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
	if err != nil {
		return err
	}
	open := func(row map[string]any) (net.Conn, error) {
		raw, err := net.DialTimeout("tcp4", server, time.Second)
		if err != nil {
			return nil, err
		}
		conn, _, rejected, err := proxyTLS(raw, server, mode, row)
		if err != nil || rejected || row["verified_tls"] != true {
			raw.Close()
			return nil, fmt.Errorf("UNIX TLS: rejected=%t, %v", rejected, err)
		}
		return conn, nil
	}
	first := map[string]any{}
	second := map[string]any{}
	result["first_connection"], result["second_connection"] = first, second
	a, err := open(first)
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := open(second)
	if err != nil {
		return err
	}
	defer b.Close()
	sameNames := os.Getenv("LK_TURN_UNIX_NAMES") == "same"
	if first["raw_tcp_peer"] == second["raw_tcp_peer"] ||
		(first["unix_source_name"] == second["unix_source_name"]) != sameNames ||
		(first["unix_destination_name"] == second["unix_destination_name"]) != sameNames {
		return fmt.Errorf("unexpected physical or logical connection identity")
	}
	result["unix_ownership_expectation"] = ownership
	result["same_forwarded_names"] = sameNames

	allocA, err := unixAllocator(a, username, password, first)
	if err != nil {
		return err
	}
	allocB, err := unixAllocator(b, username, password, second)
	if err != nil {
		return err
	}
	if _, err := waitUnixRelayPorts(relayAddresses, 0, "after_challenges", result); err != nil {
		return err
	}
	if err := expectUnixAllocate(allocA, false, stun.CodeBadRequest, "first_allocate", first); err != nil {
		return err
	}
	originalPorts, err := waitUnixRelayPorts(relayAddresses, 1, "after_first_allocate", result)
	if err != nil {
		return err
	}
	if err := expectUnixAllocate(allocA, true, stun.CodeAllocMismatch, "same_transaction", first); err != nil {
		return err
	}
	if first["same_transaction_transaction"] != first["first_allocate_transaction"] {
		return fmt.Errorf("retransmission did not preserve the original transaction")
	}
	if err := expectUnixAllocate(allocA, false, stun.CodeAllocMismatch, "new_transaction", first); err != nil {
		return err
	}
	secondCode, afterCloseCode := stun.CodeBadRequest, stun.CodeAllocMismatch
	bothPorts, afterClosePorts := 2, 1
	if ownership == "go-alias" {
		secondCode, afterCloseCode = stun.CodeAllocMismatch, stun.CodeBadRequest
		bothPorts, afterClosePorts = 1, 0
	}
	if err := expectUnixAllocate(allocB, false, secondCode, "first_allocate", second); err != nil {
		return err
	}
	if _, err := waitUnixRelayPorts(relayAddresses, bothPorts, "after_second_allocate", result); err != nil {
		return err
	}
	if err := expectUnixAllocate(allocB, false, stun.CodeAllocMismatch, "new_transaction", second); err != nil {
		return err
	}
	if err := b.Close(); err != nil {
		return err
	}
	remainingPorts, err := waitUnixRelayPorts(relayAddresses, afterClosePorts, "after_second_closed", result)
	if err != nil {
		return err
	}
	if ownership == "independent" {
		result["first_original_port_preserved"] = remainingPorts[0] == originalPorts[0]
		if result["first_original_port_preserved"] != true {
			return fmt.Errorf("closing second connection changed first relay port")
		}
	}
	if err := expectUnixAllocate(allocA, false, afterCloseCode, "after_second_closed", first); err != nil {
		return err
	}
	if _, err := waitUnixRelayPorts(relayAddresses, 1, "after_first_retried", result); err != nil {
		return err
	}
	if err := a.Close(); err != nil {
		return err
	}
	if _, err := waitUnixRelayPorts(relayAddresses, 0, "after_both_closed", result); err != nil {
		return err
	}
	fresh := map[string]any{}
	result["fresh_connection"] = fresh
	c, err := open(fresh)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := exerciseUnix(c, username, password, fresh); err != nil {
		return err
	}
	if _, err := waitUnixRelayPorts(relayAddresses, 1, "after_fresh_allocate", result); err != nil {
		return err
	}
	if err := c.Close(); err != nil {
		return err
	}
	_, err = waitUnixRelayPorts(relayAddresses, 0, "after_fresh_closed", result)
	return err
}

type turnLifetime uint32

func (l turnLifetime) AddTo(msg *stun.Message) error {
	msg.Add(stun.AttrLifetime, binary.BigEndian.AppendUint32(nil, uint32(l)))
	return nil
}

func (c unixClient) refreshZero(result map[string]any, label string, wantQuiet bool) error {
	request, err := c.request(stun.MethodRefresh, turnLifetime(0))
	if err != nil {
		return err
	}
	reply, err := exchangeSTUN(c.stream, request)
	if wantQuiet {
		var timeout net.Error
		quiet := errors.As(err, &timeout) && timeout.Timeout()
		result[label] = map[string]any{"quiet": quiet, "error": fmt.Sprint(err), "reply": fmt.Sprint(reply)}
		if !quiet {
			return fmt.Errorf("%s: expected no response on healthy connection: %v %v", label, reply, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	attrs := attributeNames(reply)
	lifetime, lifetimeErr := reply.Get(stun.AttrLifetime)
	zero := lifetimeErr == nil && len(lifetime) == 4 && binary.BigEndian.Uint32(lifetime) == 0
	integrity := stun.NewLongTermIntegrity(c.username, c.realm.String(), c.password).Check(reply)
	result[label] = map[string]any{"type": reply.Type.String(), "attributes": attrs, "lifetime_zero": zero, "valid_integrity": integrity == nil}
	if reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) || !zero || integrity != nil ||
		!slices.Equal(attrs, []string{"LIFETIME", "MESSAGE-INTEGRITY"}) {
		return fmt.Errorf("%s: expected Refresh success, lifetime zero and exact authenticated attrs, got %v", label, result[label])
	}
	return nil
}

func exerciseUnixLifecycle(server, mode, lifecycle string, relayAddresses []string, result map[string]any) error {
	creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	username, expiry := creds.CreateUsername(apiKey, "PA_turnBindProbe", 300)
	password, err := creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
	if err != nil {
		return err
	}
	raw, err := net.DialTimeout("tcp4", server, time.Second)
	if err != nil {
		return err
	}
	defer raw.Close()
	stream, _, rejected, err := proxyTLS(raw, server, mode, result)
	if err != nil || rejected || result["verified_tls"] != true {
		return fmt.Errorf("UNIX TLS: rejected=%t, %v", rejected, err)
	}
	client, err := openUnixClient(stream, username, password, result)
	if err != nil {
		return err
	}
	if _, err = waitUnixRelayPorts(relayAddresses, 0, "after_challenge", result); err != nil {
		return err
	}
	allocate := client.allocate()
	if lifecycle == "expiry" {
		result["requested_lifetime_seconds"] = 2
		allocate = client.allocate(turnLifetime(2))
	}
	start := time.Now()
	if err = expectUnixAllocate(allocate, false, stun.CodeBadRequest, "first_allocate", result); err != nil {
		return err
	}
	occupied, err := waitUnixRelayPorts(relayAddresses, 1, "after_allocate", result)
	if err != nil {
		return err
	}
	if err = expectUnixAllocate(allocate, false, stun.CodeAllocMismatch, "retry_before_release", result); err != nil {
		return err
	}

	switch lifecycle {
	case "refresh-zero":
		wrong := *client
		wrong.username, expiry = creds.CreateUsername(apiKey, "PA_wrongOwner", 300)
		wrong.password, err = creds.CreatePassword(apiKey, "PA_wrongOwner", expiry)
		if err != nil {
			return err
		}
		wrongErr := wrong.refreshZero(result, "wrong_owner_refresh", true)
		remaining, err := waitUnixRelayPorts(relayAddresses, 1, "after_wrong_owner_refresh", result)
		if err != nil {
			return errors.Join(wrongErr, err)
		}
		if !slices.Equal(occupied, remaining) {
			return errors.Join(wrongErr, fmt.Errorf("wrong owner refresh changed the original relay port"))
		}
		// Rotating the TURN credential username does not change the authenticated user ID.
		rotated := *client
		rotated.username, expiry = creds.CreateUsername(apiKey, "PA_turnBindProbe", 360)
		rotated.password, err = creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
		if err != nil {
			return err
		}
		if rotated.username == client.username {
			return fmt.Errorf("credential username must rotate")
		}
		result["refresh_credential_rotated"] = true
		rotatedErr := rotated.refreshZero(result, "rotated_owner_refresh_zero", false)
		// Observe release before any fallback on the baseline. If the baseline rejects
		// rotation, use the original credential separately to capture its response.
		_, rotatedReleaseErr := waitUnixRelayPorts(relayAddresses, 0, "after_rotated_refresh_zero", result)
		if rotatedReleaseErr != nil {
			originalErr := client.refreshZero(result, "original_owner_refresh_zero_fallback", false)
			wrongErr = errors.Join(wrongErr, originalErr)
		}
		refreshErr := errors.Join(wrongErr, rotatedErr, rotatedReleaseErr)
		_, releaseErr := waitUnixRelayPorts(relayAddresses, 0, "after_refresh_zero", result)
		if releaseErr != nil {
			return errors.Join(refreshErr, releaseErr)
		}
		quietErr := client.refreshZero(result, "refresh_after_release", true)
		if err = expectUnixAllocate(client.allocate(), false, stun.CodeBadRequest, "new_allocate_after_refresh", result); err != nil {
			return errors.Join(refreshErr, quietErr, err)
		}
		if _, err = waitUnixRelayPorts(relayAddresses, 1, "after_new_allocate", result); err != nil {
			return errors.Join(refreshErr, quietErr, err)
		}
		if err = raw.Close(); err != nil {
			return errors.Join(refreshErr, quietErr, err)
		}
		_, err = waitUnixRelayPorts(relayAddresses, 0, "after_disconnect", result)
		return errors.Join(refreshErr, quietErr, err)
	case "expiry":
		time.Sleep(500 * time.Millisecond)
		nowOccupied, err := waitUnixRelayPorts(relayAddresses, 1, "before_expiry", result)
		if err != nil {
			return err
		}
		if !slices.Equal(occupied, nowOccupied) {
			return fmt.Errorf("original relay did not survive the first half second")
		}
		_, err = waitUnixRelayPortsFor(relayAddresses, 0, "after_expiry", result, 8*time.Second)
		result["release_elapsed_ms"] = time.Since(start).Milliseconds()
		if err != nil {
			return err
		}
		if time.Since(start) < 1800*time.Millisecond {
			return fmt.Errorf("relay expired earlier than the requested two-second lifetime")
		}
		if err = expectUnixAllocate(client.allocate(), false, stun.CodeBadRequest, "new_allocate_after_expiry", result); err != nil {
			return err
		}
		if _, err = waitUnixRelayPorts(relayAddresses, 1, "after_new_allocate", result); err != nil {
			return err
		}
	case "disconnect":
		// The raw socket close sends no TLS close_notify.
		result["raw_disconnect_without_close_notify"] = true
	default:
		return fmt.Errorf("invalid UNIX lifecycle mode %q", lifecycle)
	}
	if err = raw.Close(); err != nil {
		return err
	}
	_, err = waitUnixRelayPorts(relayAddresses, 0, "after_disconnect", result)
	return err
}

func exerciseRetirement(server string, mode string, result map[string]any) (net.Conn, error) {
	bad, err := net.DialTimeout("tcp4", server, time.Second)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (net.Conn, error) { bad.Close(); return nil, err }
	bind, err := stun.Build(stun.TransactionID, stun.BindingRequest, stun.Fingerprint)
	if err != nil {
		return fail(err)
	}
	if mode == "retire-tls" {
		_, _, rejected, err := proxyTLS(bad, server, mode, result)
		if err != nil || !rejected {
			return fail(fmt.Errorf("bad TLS header: rejected=%t: %v", rejected, err))
		}
	} else {
		badHeader := append([]byte("\r\n\r\n\x00\r\nQUIT\n"), 0x21, 0x13, 0x00, 0x00)
		if _, err := bad.Write(badHeader); err != nil {
			return fail(err)
		}
		if _, err := exchangeSTUN(bad, bind); err == nil {
			return fail(fmt.Errorf("bad plaintext header yielded a TURN response"))
		} else {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				result["proxy_plain_outcome"] = "quiet_until_probe_deadline"
			} else {
				result["proxy_plain_outcome"] = "closed_before_turn_response"
			}
			result["observed_error"] = err.Error()
		}
	}
	fresh, err := net.DialTimeout("tcp4", server, time.Second)
	if err != nil {
		return fail(err)
	}
	defer fresh.Close()
	expected := fresh.LocalAddr().String()
	if mode == "retire-tls" {
		control := map[string]any{}
		var rejected bool
		fresh, expected, rejected, err = proxyTLS(fresh, server, "v1", control)
		result["fresh_verified_tls"] = control["verified_tls"]
		if err != nil || rejected {
			return fail(fmt.Errorf("fresh TLS control: %v", err))
		}
	} else if _, err := fmt.Fprint(fresh, "PROXY UNKNOWN\r\n"); err != nil {
		return fail(err)
	}
	reply, err := exchangeSTUN(fresh, bind)
	if err != nil {
		return fail(fmt.Errorf("fresh Binding: %w", err))
	}
	var mapped stun.XORMappedAddress
	if err := mapped.GetFrom(reply); err != nil || mapped.String() != expected {
		return fail(fmt.Errorf("fresh Binding address %s expected %s: %v", mapped.String(), expected, err))
	}
	result["fresh_binding_mapped"] = mapped.String()
	return bad, nil
}

func exercise(network, transport, server, clientIP, relayIP, advertisedIP string, peer *net.UDPConn, oppositeFamily turn.RequestedAddressFamily, result map[string]any) error {
	creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	username, expiry := creds.CreateUsername(apiKey, "PA_turnBindProbe", 300)
	password, err := creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
	if err != nil {
		return err
	}
	var connection net.PacketConn
	var mappedExpected string
	if transport == "udp" {
		connection, err = socket(network, clientIP)
	} else {
		var stream net.Conn
		stream, err = net.DialTimeout("tcp"+network[3:], server, time.Second)
		if err == nil {
			defer stream.Close()
			var rejected bool
			stream, mappedExpected, rejected, err = proxyTLS(stream, server, os.Getenv("LK_TURN_PROXY_MODE"), result)
			if rejected {
				return nil
			}
			if err == nil {
				if mode := os.Getenv("LK_TURN_PROXY_MODE"); mode == "unix-stream" || mode == "unix-dgram" {
					return exerciseUnix(stream, username, password, result)
				}
				connection = turn.NewSTUNConn(stream)
			}
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
	mapped, err := client.SendBindingRequest()
	if err != nil {
		return fmt.Errorf("Binding over verified TLS: %w", err)
	}
	result["mapped"] = mapped.String()
	result["expected_mapped"] = mappedExpected
	if mapped.String() != mappedExpected {
		return fmt.Errorf("wrong mapped address %v, want %s", mapped, mappedExpected)
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
	case "proxy-v4":
		listenerIP, relayIP = "127.0.0.1", "127.0.0.1"
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
	ownership := os.Getenv("LK_TURN_UNIX_OWNERSHIP")
	lifecycle := os.Getenv("LK_TURN_UNIX_LIFECYCLE")
	mode := os.Getenv("LK_TURN_PROXY_MODE")
	relayAddresses := []string{relayPort.LocalAddr().String()}
	relayEnd := port(relayPort)
	var companion net.PacketConn
	if ownership != "" {
		if transport != "tcp" || profile != "proxy-v4" ||
			(mode != "unix-stream" && mode != "unix-dgram") ||
			(ownership != "go-alias" && ownership != "independent") {
			return fmt.Errorf("invalid UNIX ownership fixture inputs")
		}
		if relayEnd == 65535 || relayEnd+1 == port(allowed)+1 {
			return fmt.Errorf("invalid fixture neighbor relay port")
		}
		relayEnd++
		address := net.JoinHostPort(relayIP, fmt.Sprint(relayEnd))
		companion, err = net.ListenPacket(network, address)
		if err != nil {
			return fmt.Errorf("reserve fixture neighbor relay port: %w", err)
		}
		defer companion.Close()
		relayAddresses = append(relayAddresses, address)
	}
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
	turnCfg := map[string]any{"enabled": true, "relay_range_start": port(relayPort), "relay_range_end": relayEnd,
		"allow_restricted_peer_cidrs": []string{"127.0.0.0/8", "::1/128"}}
	if profile != "wildcard-v4" {
		turnCfg["bind_addresses"] = []string{listenerIP}
	}
	if transport == "tcp" {
		plainINET := os.Getenv("LK_TURN_INET_LIFETIME") != "" && os.Getenv("LK_TURN_INET_WIRE") == "tcp"
		turnCfg["tls_port"], turnCfg["domain"], turnCfg["external_tls"] = turnPort, "turn.example.com", mode == "retire-plain" || plainINET
		fixture := os.Getenv("LK_TURN_TLS_FIXTURE")
		turnCfg["cert_file"] = filepath.Join(fixture, "server.pem")
		turnCfg["key_file"] = filepath.Join(fixture, "server-key.pem")
		turnCfg["proxy_protocol"] = !plainINET
		trusted := "127.0.0.1/32"
		if network == "udp6" {
			trusted = "::1/128"
		}
		if os.Getenv("LK_TURN_PROXY_MODE") == "untrusted" {
			trusted = "198.51.100.0/24"
		}
		turnCfg["proxy_protocol_trusted_cidrs"] = []string{trusted}
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
	var heldInvalid net.Conn
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
		if heldInvalid != nil {
			deadlineErr := heldInvalid.SetReadDeadline(time.Now().Add(time.Second))
			if deadlineErr == nil {
				n, err := heldInvalid.Read(make([]byte, 1))
				var timeout net.Error
				closed := n == 0 && err != nil && !(errors.As(err, &timeout) && timeout.Timeout())
				result["invalid_socket_closed_after_daemon_stop"] = closed
				if !closed {
					stop = errors.Join(stop, fmt.Errorf("held invalid socket was not closed after daemon stop: %v", err))
				}
			} else {
				stop = errors.Join(stop, deadlineErr)
			}
			heldInvalid.Close()
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
		for _, address := range relayAddresses {
			if s, e := net.ListenPacket(network, address); e != nil {
				bindErr = errors.Join(bindErr, e)
			} else {
				bindErr = errors.Join(bindErr, s.Close())
			}
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
	if companion != nil {
		if err := companion.Close(); err != nil {
			return err
		}
	}
	if lifecycle != "" {
		if ownership != "" || transport != "tcp" || profile != "proxy-v4" || (mode != "unix-stream" && mode != "unix-dgram") {
			return fmt.Errorf("invalid UNIX lifecycle fixture inputs")
		}
		result["unix_lifecycle"] = lifecycle
		return exerciseUnixLifecycle(turnAddr, mode, lifecycle, relayAddresses, result)
	}
	if ownership != "" {
		return exerciseUnixOwnership(turnAddr, mode, ownership, relayAddresses, result)
	}
	if mode == "retire-tls" || mode == "retire-plain" {
		heldInvalid, err = exerciseRetirement(turnAddr, mode, result)
		return err
	}
	if requested := os.Getenv("LK_TURN_INET_LIFETIME"); requested != "" {
		return exerciseINETLifetime(turnAddr, transport, relayAddresses, requested, result, allowed)
	}
	if err := exercise(network, transport, turnAddr, clientIP, relayIP, advertisedIP, allowed, 0, result); err != nil {
		return err
	}
	return nil
}

type inetObserver func(unixClient, func(*stun.Message) (*stun.Message, error), []string, []string, map[string]any, *net.UDPConn) error

var observeINETControl, observeINETSend inetObserver

func exerciseINETLifetime(server, transport string, relayAddresses []string, requested string, result map[string]any, allowed *net.UDPConn) error {
	creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	username, expiry := creds.CreateUsername(apiKey, "PA_turnBindProbe", 300)
	password, err := creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
	if err != nil {
		return err
	}
	wire := os.Getenv("LK_TURN_INET_WIRE")
	if (wire != "udp" && wire != "tcp" && wire != "tls") ||
		(wire == "udp") != (transport == "udp") {
		return fmt.Errorf("INET lifetime mode %q does not match listener %q", wire, transport)
	}
	result["inet_wire"] = wire
	family := relayAddressFamily(relayAddresses[0])
	raw, err := net.DialTimeout(transport, server, time.Second)
	if err != nil {
		return err
	}
	defer raw.Close()
	stream, expected := raw, raw.LocalAddr().String()
	if wire == "tls" {
		var rejected bool
		proxy := "v1"
		if family == 2 {
			proxy = "v1-inet6"
		}
		stream, expected, rejected, err = proxyTLS(raw, server, proxy, result)
		if err != nil || rejected || result["verified_tls"] != true {
			return fmt.Errorf("verified TLS unavailable: %t %v", rejected, err)
		}
	}
	exchange := func(request *stun.Message) (*stun.Message, error) {
		if wire != "udp" {
			return exchangeSTUN(stream, request)
		}
		if err := stream.SetDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
			return nil, err
		}
		if _, err := stream.Write(request.Raw); err != nil {
			return nil, err
		}
		buf := make([]byte, 1500)
		n, err := stream.Read(buf)
		if err != nil {
			return nil, err
		}
		reply := &stun.Message{Raw: buf[:n]}
		if err := reply.Decode(); err != nil {
			return nil, err
		}
		if reply.TransactionID != request.TransactionID {
			return nil, fmt.Errorf("unexpected UDP transaction")
		}
		return reply, nil
	}
	allocate := stun.NewType(stun.MethodAllocate, stun.ClassRequest)
	request, err := stun.Build(stun.TransactionID, allocate, family, stun.Fingerprint)
	if err != nil {
		return err
	}
	challenge, err := exchange(request)
	if err != nil {
		return err
	}
	var realm stun.Realm
	var nonce stun.Nonce
	var code stun.ErrorCodeAttribute
	if err := errors.Join(realm.GetFrom(challenge), nonce.GetFrom(challenge), code.GetFrom(challenge)); err != nil || code.Code != stun.CodeUnauthorized {
		return fmt.Errorf("unexpected challenge %v %v", code, err)
	}
	client := unixClient{stream: stream, username: username, password: password, realm: realm, nonce: nonce}
	attrs := []stun.Setter{family}
	operation := os.Getenv("LK_TURN_INET_OPERATION")
	if operation != "allocate" && operation != "refresh" && operation != "extend" {
		return fmt.Errorf("unsupported INET operation %q", operation)
	}
	result["inet_operation"] = operation
	refresh := operation == "refresh"
	extend := operation == "extend"
	var wanted []stun.Setter
	if requested != "default" {
		seconds, err := strconv.ParseUint(requested, 10, 32)
		if err != nil {
			return err
		}
		wanted = []stun.Setter{turnLifetime(seconds)}
		if !refresh {
			attrs = append(attrs, wanted...)
		}
	}
	result["requested"] = requested
	request, err = client.request(stun.MethodAllocate, attrs...)
	if err != nil {
		return err
	}
	start := time.Now()
	reply, err := exchange(request)
	if err != nil {
		return err
	}
	if requested == "0" && reply.Type == stun.NewType(stun.MethodAllocate, stun.ClassErrorResponse) {
		if err := code.GetFrom(reply); err != nil {
			return err
		}
		result["allocate_error"] = int(code.Code)
		_, err := waitUnixRelayPorts(relayAddresses, 0, "after_allocate_zero", result)
		return err
	}
	if reply.Type != stun.NewType(stun.MethodAllocate, stun.ClassSuccessResponse) {
		return fmt.Errorf("expected Allocate success, got %v", reply)
	}
	lifetime, err := reply.Get(stun.AttrLifetime)
	if err != nil || len(lifetime) != 4 {
		return fmt.Errorf("invalid LIFETIME: %v", err)
	}
	result["advertised_seconds"] = binary.BigEndian.Uint32(lifetime)
	var mapped stun.XORMappedAddress
	if err := mapped.GetFrom(reply); err != nil || mapped.String() != expected {
		return fmt.Errorf("unexpected mapped address %s %v", mapped.String(), err)
	}
	if err := stun.NewLongTermIntegrity(username, realm.String(), password).Check(reply); err != nil {
		return err
	}
	result["verified_allocate_integrity"] = true
	occupied, err := waitUnixRelayPorts(relayAddresses, 1, "allocation", result)
	if err != nil {
		return err
	}
	var relayed stun.XORMappedAddress
	if err := relayed.GetFromAs(reply, stun.AttrXORRelayedAddress); err != nil || relayed.String() != occupied[0] {
		return fmt.Errorf("unexpected relay address %s vs actual %s: %v", relayed.String(), occupied[0], err)
	}
	if os.Getenv("LK_TURN_INET_CONTROL") == "1" {
		if observeINETControl == nil {
			return fmt.Errorf("observer was built without control-observe.go")
		}
		return observeINETControl(client, exchange, relayAddresses, occupied, result, allowed)
	}
	if os.Getenv("LK_TURN_INET_SEND") == "1" {
		if observeINETSend == nil {
			return fmt.Errorf("observer was built without send-observe.go")
		}
		return observeINETSend(client, exchange, relayAddresses, occupied, result, allowed)
	}
	if os.Getenv("LK_TURN_INET_REFRESH_OWNER") == "1" || os.Getenv("LK_TURN_INET_REFRESH_FAMILY") == "1" {
		return exerciseINETRefreshOwner(client, exchange, relayAddresses, occupied, result)
	}
	if extend {
		result["initial_advertised_seconds"] = result["advertised_seconds"]
		time.Sleep(900 * time.Millisecond)
		wanted = []stun.Setter{turnLifetime(4)}
	}
	originalDeadline := start.Add(2 * time.Second)
	if refresh || extend {
		request, err := client.request(stun.MethodRefresh, wanted...)
		if err != nil {
			return err
		}
		start = time.Now()
		reply, err := exchange(request)
		if err != nil {
			return err
		}
		if reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) {
			return fmt.Errorf("unexpected Refresh response %v", reply)
		}
		lifetime, err := reply.Get(stun.AttrLifetime)
		if err != nil || len(lifetime) != 4 {
			return fmt.Errorf("invalid Refresh LIFETIME: %v", err)
		}
		result["advertised_seconds"] = binary.BigEndian.Uint32(lifetime)
		if err := stun.NewLongTermIntegrity(username, realm.String(), password).Check(reply); err != nil {
			return err
		}
		result["verified_refresh_integrity"] = true
	}
	if requested == "2" {
		timeout := 3 * time.Second
		if extend {
			time.Sleep(time.Until(originalDeadline.Add(200 * time.Millisecond)))
			stillOccupied, err := waitUnixRelayPortsFor(relayAddresses, 1, "after_original_deadline", result, 0)
			result["retained_same_port_after_original_deadline"] = err == nil && stillOccupied[0] == occupied[0]
			timeout = time.Until(start.Add(5 * time.Second))
		}
		_, released := waitUnixRelayPortsFor(relayAddresses, 0, "short_deadline", result, timeout)
		result["released_with_connection_open"] = released == nil
		result["elapsed_ms"] = time.Since(start).Milliseconds()
		if released == nil && ((extend && result["elapsed_ms"].(int64) < 3950) || (!extend && result["elapsed_ms"].(int64) < 1950)) {
			return fmt.Errorf("relay was released early at %dms", result["elapsed_ms"])
		}
		if released == nil {
			if os.Getenv("LK_TURN_INET_LATE_REFRESH") == "1" {
				late, err := client.request(stun.MethodRefresh, turnLifetime(2))
				if err != nil {
					return err
				}
				lateReply, err := exchange(late)
				var netErr net.Error
				switch {
				case errors.As(err, &netErr) && netErr.Timeout():
					result["late_refresh"] = "no_response"
				case err != nil:
					return err
				case lateReply.Type != stun.NewType(stun.MethodRefresh, stun.ClassErrorResponse):
					return fmt.Errorf("unexpected late Refresh response %v", lateReply)
				default:
					var lateCode stun.ErrorCodeAttribute
					if err := lateCode.GetFrom(lateReply); err != nil {
						return err
					}
					result["late_refresh"] = int(lateCode.Code)
				}
				if _, err := waitUnixRelayPorts(relayAddresses, 0, "after_late_refresh", result); err != nil {
					return err
				}
			}
			fresh, err := client.request(stun.MethodAllocate, family)
			if err != nil {
				return err
			}
			reply, err := exchange(fresh)
			if err != nil || reply.Type != stun.NewType(stun.MethodAllocate, stun.ClassSuccessResponse) {
				return fmt.Errorf("fresh Allocate on the same socket: %v %v", reply, err)
			}
			if err := stun.NewLongTermIntegrity(username, realm.String(), password).Check(reply); err != nil {
				return err
			}
			if _, err := waitUnixRelayPorts(relayAddresses, 1, "after_fresh_allocate", result); err != nil {
				return err
			}
			result["same_socket_reallocate"] = true
		}
	}

	zero, err := client.request(stun.MethodRefresh, turnLifetime(0))
	if err != nil {
		return err
	}
	reply, err = exchange(zero)
	if err != nil || reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) {
		return fmt.Errorf("authenticated cleanup Refresh(0): %v %v", reply, err)
	}
	if err := stun.NewLongTermIntegrity(username, realm.String(), password).Check(reply); err != nil {
		return err
	}
	lifetime, err = reply.Get(stun.AttrLifetime)
	if err != nil || len(lifetime) != 4 || binary.BigEndian.Uint32(lifetime) != 0 {
		return fmt.Errorf("cleanup Refresh(0) returned invalid lifetime: %x %v", lifetime, err)
	}
	_, err = waitUnixRelayPorts(relayAddresses, 0, "after_refresh_zero", result)
	return err
}

type refreshFamily []byte

func relayAddressFamily(address string) allocationAttributes {
	host, _, _ := net.SplitHostPort(address)
	if net.ParseIP(host).To4() == nil {
		return 2
	}
	return 1
}

func (family refreshFamily) AddTo(msg *stun.Message) error {
	msg.Add(stun.AttrRequestedAddressFamily, family)
	return nil
}

func exerciseINETRefreshOwner(client unixClient, exchange func(*stun.Message) (*stun.Message, error), addresses, occupied []string, result map[string]any) error {
	creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	credential := func(id livekit.ParticipantID, ttl int) (unixClient, error) {
		copy := client
		username, expiry := creds.CreateUsername(apiKey, id, ttl)
		copy.username = username
		password, err := creds.CreatePassword(apiKey, id, expiry)
		copy.password = password
		return copy, err
	}
	observe := func(c unixClient, seconds uint32, attrs ...stun.Setter) (map[string]any, error) {
		request, err := c.request(stun.MethodRefresh, append([]stun.Setter{turnLifetime(seconds)}, attrs...)...)
		if err != nil {
			return nil, err
		}
		reply, err := exchange(request)
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return map[string]any{"quiet": true}, nil
		}
		if err != nil {
			return nil, err
		}
		attributes := attributeNames(reply)
		if reply.Type == stun.NewType(stun.MethodRefresh, stun.ClassErrorResponse) {
			var code stun.ErrorCodeAttribute
			if err := code.GetFrom(reply); err != nil {
				return nil, err
			}
			return map[string]any{"error_code": int(code.Code), "reason": string(code.Reason), "attributes": attributes}, nil
		}
		if reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) {
			return nil, fmt.Errorf("unexpected Refresh response %v", reply)
		}
		lifetime, err := reply.Get(stun.AttrLifetime)
		if err != nil || len(lifetime) != 4 || binary.BigEndian.Uint32(lifetime) != seconds {
			return nil, fmt.Errorf("unexpected Refresh LIFETIME %x: %v", lifetime, err)
		}
		if err := stun.NewLongTermIntegrity(c.username, c.realm.String(), c.password).Check(reply); err != nil {
			return nil, err
		}
		return map[string]any{"lifetime": seconds, "attributes": attributes, "valid_integrity": true}, nil
	}
	if os.Getenv("LK_TURN_INET_REFRESH_FAMILY") == "1" {
		rows := map[string]any{}
		result["refresh_family"] = rows
		family := byte(relayAddressFamily(occupied[0]))
		for _, check := range []struct {
			name    string
			family  refreshFamily
			seconds uint32
		}{
			{"matching_family", refreshFamily{family, 0, 0, 0}, 600},
			{"reserved_bytes_ignored", refreshFamily{family, 1, 2, 3}, 600},
			{"mismatched_family", refreshFamily{3 - family, 0, 0, 0}, 0},
			{"unknown_family", refreshFamily{3, 0, 0, 0}, 600},
			{"short_attribute", refreshFamily{family, 0, 0}, 0},
			{"long_attribute", refreshFamily{family, 0, 0, 0, 0}, 0},
		} {
			reply, err := observe(client, check.seconds, check.family)
			if err != nil {
				return fmt.Errorf("%s: %w", check.name, err)
			}
			rows[check.name] = reply
			remaining, err := waitUnixRelayPorts(addresses, 1, "after_"+check.name, result)
			if err != nil || !slices.Equal(remaining, occupied) {
				return fmt.Errorf("%s changed the original relay: %v", check.name, err)
			}
		}
	}
	wrong, err := credential("PA_wrongOwner", 300)
	if err != nil {
		return err
	}
	rotated, err := credential("PA_turnBindProbe", 360)
	if err != nil {
		return err
	}
	if rotated.username == client.username || wrong.username == client.username {
		return fmt.Errorf("fixture must use different credential usernames")
	}
	for _, check := range []struct {
		name    string
		client  unixClient
		seconds uint32
	}{
		{"wrong_owner_refresh", wrong, 0},
		{"original_owner_refresh", client, 600},
		{"rotated_owner_refresh", rotated, 0},
	} {
		reply, err := observe(check.client, check.seconds)
		if err != nil {
			return fmt.Errorf("%s: %w", check.name, err)
		}
		result[check.name] = reply
		if check.name != "rotated_owner_refresh" {
			remaining, err := waitUnixRelayPorts(addresses, 1, "after_"+check.name, result)
			if err != nil || !slices.Equal(remaining, occupied) {
				return fmt.Errorf("%s changed the original relay: %v", check.name, err)
			}
		}
	}
	_, releaseErr := waitUnixRelayPortsFor(addresses, 0, "after_rotated_owner_refresh", result, time.Second)
	result["rotated_owner_released_relay"] = releaseErr == nil
	if releaseErr != nil {
		if _, err := observe(client, 0); err != nil {
			return err
		}
		if _, err := waitUnixRelayPorts(addresses, 0, "after_fallback_cleanup", result); err != nil {
			return err
		}
	}
	fresh, err := client.request(stun.MethodAllocate, relayAddressFamily(occupied[0]))
	if err != nil {
		return err
	}
	reply, err := exchange(fresh)
	if err != nil || reply.Type != stun.NewType(stun.MethodAllocate, stun.ClassSuccessResponse) {
		return fmt.Errorf("fresh Allocate on same socket: %v %v", reply, err)
	}
	if err := stun.NewLongTermIntegrity(client.username, client.realm.String(), client.password).Check(reply); err != nil {
		return err
	}
	if _, err := waitUnixRelayPorts(addresses, 1, "fresh_allocate", result); err != nil {
		return err
	}
	result["same_socket_reallocate"] = true
	if _, err := observe(client, 0); err != nil {
		return err
	}
	_, err = waitUnixRelayPorts(addresses, 0, "after_cleanup", result)
	return err
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
