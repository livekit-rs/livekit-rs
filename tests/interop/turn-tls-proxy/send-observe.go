package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/stun/v3"
)

func init() {
	observeINETSend = exerciseINETSend
}

type sendAttribute struct {
	kind  stun.AttrType
	value []byte
}

func (attr sendAttribute) AddTo(message *stun.Message) error {
	message.Add(attr.kind, attr.value)
	return nil
}

type sendPeer struct {
	address *net.UDPAddr
}

func (peer sendPeer) AddTo(message *stun.Message) error {
	return (stun.XORMappedAddress{IP: peer.address.IP, Port: peer.address.Port}).AddToAs(message, stun.AttrXORPeerAddress)
}

func exerciseINETSend(client unixClient, exchange func(*stun.Message) (*stun.Message, error), relayAddresses, occupied []string, result map[string]any, peer *net.UDPConn) error {
	target := sendPeer{peer.LocalAddr().(*net.UDPAddr)}
	wire, _ := result["inet_wire"].(string)
	network := "tcp4"
	if wire == "udp" {
		network = "udp4"
	}
	otherRaw, err := net.DialTimeout(network, client.stream.RemoteAddr().String(), time.Second)
	if err != nil {
		return err
	}
	defer otherRaw.Close()
	other := otherRaw
	if wire == "tls" {
		details := map[string]any{}
		var rejected bool
		other, _, rejected, err = proxyTLS(otherRaw, client.stream.RemoteAddr().String(), "v1", details)
		if err != nil || rejected || details["verified_tls"] != true {
			return fmt.Errorf("unallocated TLS tuple must negotiate verified TLS: %t %v", rejected, err)
		}
	}
	if otherRaw.LocalAddr().String() == client.stream.LocalAddr().String() {
		return fmt.Errorf("unallocated socket did not get a distinct tuple")
	}
	result["send_unallocated_tuple_distinct"] = true

	authHandler := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	credentials := func(id livekit.ParticipantID, ttl int) ([]stun.Setter, string, error) {
		user, expiry := authHandler.CreateUsername(apiKey, id, ttl)
		password, err := authHandler.CreatePassword(apiKey, id, expiry)
		if err != nil {
			return nil, "", err
		}
		return []stun.Setter{stun.NewUsername(user), client.realm, client.nonce,
			stun.NewLongTermIntegrity(user, client.realm.String(), password)}, user, nil
	}
	rotated, rotatedUser, err := credentials("PA_turnBindProbe", 360)
	if err != nil {
		return err
	}
	wrong, wrongUser, err := credentials("PA_wrongOwner", 300)
	if err != nil {
		return err
	}
	if rotatedUser == client.username || wrongUser == client.username || wrongUser == rotatedUser {
		return fmt.Errorf("credential controls must have distinct usernames")
	}
	result["send_rotated_username_distinct"] = true
	result["send_wrong_owner_username_distinct"] = true

	const readTimeout = 150 * time.Millisecond
	rows := make(map[string]any)
	result["send_cases"] = rows
	observe := func(label string, conn net.Conn, class stun.MessageClass, mutate func(*stun.Message), attrs ...stun.Setter) error {
		setters := []stun.Setter{stun.TransactionID, stun.NewType(stun.MethodSend, class)}
		setters = append(setters, attrs...)
		message, err := stun.Build(append(setters, stun.Fingerprint)...)
		if err != nil {
			return err
		}
		if mutate != nil {
			mutate(message)
		}
		if err := conn.SetDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}
		if n, err := conn.Write(message.Raw); err != nil || n != len(message.Raw) {
			return fmt.Errorf("%s: write Send indication: %d %v", label, n, err)
		}
		if err := peer.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}
		buf := make([]byte, 1500)
		n, from, err := peer.ReadFromUDP(buf)
		row := map[string]any{"peer_received": false}
		rows[label] = row
		var timeout net.Error
		if err == nil {
			if from.String() != occupied[0] {
				return fmt.Errorf("%s: wrong relay source %s, want %s", label, from, occupied[0])
			}
			row["peer_received"] = true
			row["peer_payload_hex"] = hex.EncodeToString(buf[:n])
			row["actual_relay_source"] = true
		} else if !errors.As(err, &timeout) || !timeout.Timeout() {
			return fmt.Errorf("%s: read peer UDP: %w", label, err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}
		var response []byte
		if wire == "udp" {
			n, err = conn.Read(buf)
			response = buf[:n]
		} else {
			n, err = io.ReadFull(conn, buf[:20])
			response = buf[:n]
			if err == nil {
				length := int(binary.BigEndian.Uint16(buf[2:4]))
				if length > len(buf)-20 {
					return fmt.Errorf("%s: oversized STUN reply: %d", label, length)
				}
				n, err = io.ReadFull(conn, buf[20:20+length])
				response = buf[:20+n]
			}
		}
		if len(response) == 0 && errors.As(err, &timeout) && timeout.Timeout() {
			row["client_quiet"] = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: read STUN response: %w", label, err)
		}
		reply := &stun.Message{Raw: response}
		if err := reply.Decode(); err != nil {
			return fmt.Errorf("%s: malformed STUN response: %w", label, err)
		}
		if reply.TransactionID != message.TransactionID || reply.Type != stun.NewType(stun.MethodSend, stun.ClassErrorResponse) {
			return fmt.Errorf("%s: unexpected STUN response %v", label, reply.Type)
		}
		var code stun.ErrorCodeAttribute
		if err := code.GetFrom(reply); err != nil {
			return err
		}
		row["client_quiet"] = false
		row["client_error"] = int(code.Code)
		row["client_reason"] = string(code.Reason)
		row["client_attributes"] = attributeNames(reply)
		if value, err := reply.Get(stun.AttrUnknownAttributes); err == nil {
			row["unknown_attributes_hex"] = hex.EncodeToString(value)
		}
		return nil
	}
	indicate := func(label string, conn net.Conn, mutate func(*stun.Message), attrs ...stun.Setter) error {
		return observe(label, conn, stun.ClassIndication, mutate, attrs...)
	}

	data := func(value string) sendAttribute { return sendAttribute{stun.AttrData, []byte(value)} }
	if err := indicate("without_permission", client.stream, nil, target, data("without-permission")); err != nil {
		return err
	}
	permission, err := client.request(stun.MethodCreatePermission, target)
	if err != nil {
		return err
	}
	reply, err := exchange(permission)
	if err != nil || reply.Type != stun.NewType(stun.MethodCreatePermission, stun.ClassSuccessResponse) {
		return fmt.Errorf("create real peer permission: %v %v", reply, err)
	}
	if err := stun.NewLongTermIntegrity(client.username, client.realm.String(), client.password).Check(reply); err != nil {
		return fmt.Errorf("CreatePermission integrity: %w", err)
	}
	result["send_permission_verified"] = true

	original := []stun.Setter{stun.NewUsername(client.username), client.realm, client.nonce,
		stun.NewLongTermIntegrity(client.username, client.realm.String(), client.password)}
	invalidIntegrity := stun.NewLongTermIntegrity(client.username, client.realm.String(), "invalid fixture password")
	for _, check := range []struct {
		label  string
		conn   net.Conn
		mutate func(*stun.Message)
		attrs  []stun.Setter
	}{
		{"unauthenticated_same_tuple", client.stream, nil, []stun.Setter{target, data("unauthenticated")}},
		{"original_credential_same_tuple", client.stream, nil, append([]stun.Setter{target, data("original")}, original...)},
		{"rotated_credential_same_tuple", client.stream, nil, append([]stun.Setter{target, data("rotated")}, rotated...)},
		{"wrong_owner_credential_same_tuple", client.stream, nil, append([]stun.Setter{target, data("wrong-owner")}, wrong...)},
		{"invalid_integrity_same_tuple", client.stream, nil, []stun.Setter{target, data("invalid-integrity"), stun.NewUsername(client.username), client.realm, client.nonce, invalidIntegrity}},
		{"original_credential_different_tuple", other, nil, append([]stun.Setter{target, data("different-tuple")}, original...)},
		{"missing_data", client.stream, nil, []stun.Setter{target}},
		{"missing_peer", client.stream, nil, []stun.Setter{data("missing-peer")}},
		{"short_peer", client.stream, nil, []stun.Setter{sendAttribute{stun.AttrXORPeerAddress, []byte{0, 1, 0, 0}}, data("short-peer")}},
		{"invalid_peer_family", client.stream, nil, []stun.Setter{sendAttribute{stun.AttrXORPeerAddress, []byte{0, 3, 0, 0, 0, 0, 0, 0}}, data("invalid-family")}},
		{"empty_data", client.stream, nil, []stun.Setter{target, data("")}},
		{"unknown_required_attribute", client.stream, nil, []stun.Setter{target, data("unknown-required"), sendAttribute{0x002f, []byte{1, 2, 3, 4}}}},
		{"unknown_optional_attribute", client.stream, nil, []stun.Setter{target, data("unknown-optional"), sendAttribute{0x80ff, []byte{1, 2, 3, 4}}}},
		{"invalid_fingerprint", client.stream, func(m *stun.Message) { m.Raw[len(m.Raw)-1] ^= 1 }, []stun.Setter{target, data("invalid-fingerprint")}},
		{"data_after_message_integrity", client.stream, nil, append(append([]stun.Setter{target}, original...), data("post-integrity-data"))},
		{"usable_after_malformed", client.stream, nil, []stun.Setter{target, data("still-usable")}},
	} {
		if err := indicate(check.label, check.conn, check.mutate, check.attrs...); err != nil {
			return err
		}
	}
	if err := observe("send_request", client.stream, stun.ClassRequest, nil, target, data("unexpected-request")); err != nil {
		return err
	}
	usable, _ := rows["usable_after_malformed"].(map[string]any)
	if usable["peer_received"] != true || usable["peer_payload_hex"] != hex.EncodeToString([]byte("still-usable")) {
		return fmt.Errorf("valid Send control did not reach the peer after malformed cases")
	}
	zero, err := client.request(stun.MethodRefresh, turnLifetime(0))
	if err != nil {
		return err
	}
	reply, err = exchange(zero)
	if err != nil || reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) {
		return fmt.Errorf("release original relay: %v %v", reply, err)
	}
	if _, err := waitUnixRelayPorts(relayAddresses, 0, "after_send_cleanup", result); err != nil {
		return err
	}
	result["send_relay_released"] = true
	if err := indicate("after_release", client.stream, nil, target, data("released")); err != nil {
		return err
	}
	if wire == "tls" {
		result["send_different_tuple_verified_tls"] = true
	}
	return nil
}
