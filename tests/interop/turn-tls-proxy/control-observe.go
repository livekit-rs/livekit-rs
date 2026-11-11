// Opt-in raw TURN permission and channel oracle. Build alongside observe.go.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"time"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/stun/v3"
)

func init() {
	observeINETControl = exerciseINETControl
	if os.Getenv("LK_TURN_CHANNEL_FRAME") == "1" {
		observeINETControl = exerciseChannelFrame
	}
}

type controlAttribute struct {
	kind  stun.AttrType
	value []byte
}

func (a controlAttribute) AddTo(message *stun.Message) error {
	message.Add(a.kind, a.value)
	return nil
}

type controlPeer struct{ stun.XORMappedAddress }

func (p controlPeer) AddTo(message *stun.Message) error {
	return p.AddToAs(message, stun.AttrXORPeerAddress)
}

func controlChannel(number uint16) stun.Setter {
	return controlAttribute{stun.AttrChannelNumber, binary.BigEndian.AppendUint32(nil, uint32(number)<<16)}
}

func exerciseChannelFrame(client unixClient, exchange func(*stun.Message) (*stun.Message, error), addresses, occupied []string, result map[string]any, peer *net.UDPConn) error {
	const channel = 0x4000
	addr := peer.LocalAddr().(*net.UDPAddr)
	bind, err := client.request(stun.MethodChannelBind, controlChannel(channel),
		controlPeer{stun.XORMappedAddress{IP: addr.IP, Port: addr.Port}})
	if err != nil {
		return err
	}
	reply, err := exchange(bind)
	if err != nil || reply.Type != stun.NewType(stun.MethodChannelBind, stun.ClassSuccessResponse) {
		return fmt.Errorf("ChannelBind failed: %v %v", reply, err)
	}
	if err := stun.NewLongTermIntegrity(client.username, client.realm.String(), client.password).Check(reply); err != nil {
		return err
	}
	rows := map[int]any{}
	result["channel_frame_rows"] = rows
	for _, size := range []int{0, 1, 4, 5, 8} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(16*size + i)
		}
		frame := binary.BigEndian.AppendUint16(nil, channel)
		frame = binary.BigEndian.AppendUint16(frame, uint16(size))
		frame = append(frame, payload...)
		if os.Getenv("LK_TURN_INET_WIRE") != "udp" {
			frame = append(frame, make([]byte, (4-size%4)%4)...)
		}
		if err := client.stream.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		if n, err := client.stream.Write(frame); err != nil || n != len(frame) {
			return fmt.Errorf("write ChannelData size %d: %d %v", size, n, err)
		}
		read := func(timeout time.Duration) (bool, error) {
			if err := peer.SetReadDeadline(time.Now().Add(timeout)); err != nil {
				return false, err
			}
			var buf [32]byte
			n, src, err := peer.ReadFromUDP(buf[:])
			var timeoutError net.Error
			if errors.As(err, &timeoutError) && timeoutError.Timeout() {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if src.String() != occupied[0] || !slices.Equal(buf[:n], payload) {
				return false, fmt.Errorf("unexpected relayed ChannelData %x from %v", buf[:n], src)
			}
			return true, nil
		}
		before, err := read(300 * time.Millisecond)
		if err != nil {
			return err
		}
		binding, err := stun.Build(stun.TransactionID, stun.BindingRequest, stun.Fingerprint)
		if err != nil {
			return err
		}
		reply, err := exchange(binding)
		if err != nil || reply.Type != stun.BindingSuccess {
			return fmt.Errorf("subsequent Binding failed: %v %v", reply, err)
		}
		wait := 100 * time.Millisecond
		if !before {
			wait = time.Second
		}
		after, err := read(wait)
		if err != nil {
			return err
		}
		if before == after {
			return fmt.Errorf("expected exactly one delivery for ChannelData size %d", size)
		}
		rows[size] = map[string]any{"frame_bytes": len(frame), "before_binding": before, "after_binding": after}
	}
	refresh, err := client.request(stun.MethodRefresh, turnLifetime(0))
	if err != nil {
		return err
	}
	reply, err = exchange(refresh)
	if err != nil || reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) {
		return fmt.Errorf("cleanup Refresh failed: %v %v", reply, err)
	}
	_, err = waitUnixRelayPorts(addresses, 0, "after_channel_frame_refresh_zero", result)
	return err
}

func exerciseINETControl(client unixClient, exchange func(*stun.Message) (*stun.Message, error), addresses, occupied []string, result map[string]any, peer *net.UDPConn) error {
	credentials := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
	credential := func(participant livekit.ParticipantID, ttl int) (unixClient, error) {
		copy := client
		username, expiry := credentials.CreateUsername(apiKey, participant, ttl)
		password, err := credentials.CreatePassword(apiKey, participant, expiry)
		copy.username, copy.password = username, password
		return copy, err
	}
	rotated, err := credential("PA_turnBindProbe", 360)
	if err != nil {
		return err
	}
	wrong, err := credential("PA_wrongOwner", 300)
	if err != nil {
		return err
	}
	if rotated.username == client.username || wrong.username == client.username {
		return fmt.Errorf("control fixture must use different credential usernames")
	}
	other, err := socket("udp4", "127.0.0.1")
	if err != nil {
		return err
	}
	defer other.Close()
	peerAttr := func(conn *net.UDPConn) controlPeer {
		addr := conn.LocalAddr().(*net.UDPAddr)
		return controlPeer{stun.XORMappedAddress{IP: addr.IP, Port: addr.Port}}
	}
	local := peerAttr(peer)
	otherLocal := peerAttr(other)
	ipv6 := controlPeer{stun.XORMappedAddress{IP: net.ParseIP("::1"), Port: local.Port}}
	blocked := controlPeer{stun.XORMappedAddress{IP: net.ParseIP("10.0.0.1"), Port: local.Port}}
	malformed := controlAttribute{stun.AttrXORPeerAddress, []byte{0, 1, 0}}
	badChannel := controlAttribute{stun.AttrChannelNumber, []byte{0x40, 0, 0}}
	rows := make(map[string]any)
	result["control_rows"] = rows
	inspect := func(label string, owner unixClient, method stun.Method, request *stun.Message) error {
		reply, err := exchange(request)
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			rows[label] = map[string]any{"quiet": true}
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		row := map[string]any{"attributes": attributeNames(reply)}
		rows[label] = row
		switch reply.Type {
		case stun.NewType(method, stun.ClassSuccessResponse):
			row["success"] = true
		case stun.NewType(method, stun.ClassErrorResponse):
			var code stun.ErrorCodeAttribute
			if err := code.GetFrom(reply); err != nil {
				return err
			}
			row["error_code"] = int(code.Code)
			row["reason"] = string(code.Reason)
		default:
			return fmt.Errorf("%s unexpected response %v", label, reply)
		}
		if reply.Contains(stun.AttrMessageIntegrity) {
			if err := stun.NewLongTermIntegrity(owner.username, owner.realm.String(), owner.password).Check(reply); err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			row["valid_integrity"] = true
		}
		return nil
	}
	response := func(label string, owner unixClient, method stun.Method, attrs ...stun.Setter) error {
		request, err := owner.request(method, attrs...)
		if err != nil {
			return err
		}
		return inspect(label, owner, method, request)
	}
	sequence := uint32(0)
	probe := func(label string, channel uint16, expected bool) error {
		// An eight-byte payload clears Pion's nine-byte minimum stream
		// detection window, and the sequence disambiguates earlier probes.
		sequence++
		payload := binary.BigEndian.AppendUint32(nil, sequence)
		payload = append(payload, "turn"...)
		var packet []byte
		if channel == 0 {
			msg, err := stun.Build(stun.TransactionID, stun.NewType(stun.MethodSend, stun.ClassIndication),
				local, controlAttribute{stun.AttrData, payload}, stun.Fingerprint)
			if err != nil {
				return err
			}
			packet = msg.Raw
		} else {
			packet = binary.BigEndian.AppendUint16(nil, channel)
			packet = binary.BigEndian.AppendUint16(packet, uint16(len(payload)))
			packet = append(packet, payload...)
		}
		if err := client.stream.SetDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		if n, err := client.stream.Write(packet); err != nil || n != len(packet) {
			return fmt.Errorf("%s write %d/%d: %v", label, n, len(packet), err)
		}
		timeout := 250 * time.Millisecond
		if expected {
			timeout = time.Second
		}
		if err := peer.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		for {
			var buffer [32]byte
			n, from, err := peer.ReadFromUDP(buffer[:])
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				rows[label] = false
				return nil
			}
			if err != nil || n != len(payload) || !slices.Equal(buffer[4:n], payload[4:]) {
				return fmt.Errorf("%s read %d bytes from %v: %v", label, n, from, err)
			}
			received := binary.BigEndian.Uint32(buffer[:n])
			if received < sequence && slices.Contains(addresses, from.String()) {
				key := "delayed_payload_sequences"
				values, _ := result[key].([]uint32)
				result[key] = append(values, received)
				continue
			}
			if received != sequence || from.String() != occupied[0] {
				return fmt.Errorf("%s received sequence %d from %v, expected %d from %s", label, received, from, sequence, occupied[0])
			}
			rows[label] = true
			return nil
		}
	}
	perm := stun.MethodCreatePermission
	bind := stun.MethodChannelBind
	for _, method := range []struct {
		label string
		kind  stun.Method
		attrs []stun.Setter
	}{
		{"permission", perm, []stun.Setter{local}},
		{"channel", bind, []stun.Setter{controlChannel(0x4000), local}},
	} {
		setters := []stun.Setter{stun.TransactionID, stun.NewType(method.kind, stun.ClassRequest)}
		setters = append(setters, method.attrs...)
		request, err := stun.Build(append(setters, stun.Fingerprint)...)
		if err != nil {
			return err
		}
		if err := inspect(method.label+"_unauthenticated", client, method.kind, request); err != nil {
			return err
		}
		bad := client
		bad.password = "fixture-invalid-password"
		if err := response(method.label+"_bad_integrity", bad, method.kind, method.attrs...); err != nil {
			return err
		}
	}
	checks := []struct {
		label  string
		owner  unixClient
		method stun.Method
		attrs  []stun.Setter
	}{
		{"permission_wrong_owner", wrong, perm, []stun.Setter{local}},
		{"permission_empty", client, perm, nil},
		{"permission_malformed", client, perm, []stun.Setter{malformed}},
		{"permission_opposite_family", client, perm, []stun.Setter{ipv6}},
		{"permission_blocked_peer", client, perm, []stun.Setter{blocked}},
		{"channel_wrong_owner", wrong, bind, []stun.Setter{controlChannel(0x4000), local}},
		{"channel_missing_number", client, bind, []stun.Setter{local}},
		{"channel_missing_peer", client, bind, []stun.Setter{controlChannel(0x4000)}},
		{"channel_malformed_number", client, bind, []stun.Setter{badChannel, local}},
		{"channel_malformed_peer", client, bind, []stun.Setter{controlChannel(0x4000), malformed}},
		{"channel_opposite_family", client, bind, []stun.Setter{controlChannel(0x4000), ipv6}},
		{"channel_blocked_peer", client, bind, []stun.Setter{controlChannel(0x4000), blocked}},
	}
	for _, check := range checks {
		if err := response(check.label, check.owner, check.method, check.attrs...); err != nil {
			return err
		}
	}
	if err := probe("rejected_permission_relay", 0, false); err != nil {
		return err
	}
	if err := probe("rejected_channel_relay", 0x4000, false); err != nil {
		return err
	}
	if err := response("permission_rotated_owner", rotated, perm, local); err != nil {
		return err
	}
	if err := probe("rotated_permission_relay", 0, true); err != nil {
		return err
	}
	if err := response("permission_original_owner", client, perm, local); err != nil {
		return err
	}
	if err := response("channel_rotated_owner", rotated, bind, controlChannel(0x4000), local); err != nil {
		return err
	}
	if err := probe("rotated_channel_relay", 0x4000, true); err != nil {
		return err
	}
	if err := response("channel_original_owner", client, bind, controlChannel(0x4000), local); err != nil {
		return err
	}
	if err := response("channel_same_peer_conflict", client, bind, controlChannel(0x4001), local); err != nil {
		return err
	}
	if err := response("channel_same_number_conflict", client, bind, controlChannel(0x4000), otherLocal); err != nil {
		return err
	}
	if err := probe("original_channel_after_conflicts_relay", 0x4000, true); err != nil {
		return err
	}
	if err := response("channel_out_of_range", client, bind, controlChannel(0x3fff), otherLocal); err != nil {
		return err
	}
	encodedPort := uint16(local.Port) ^ 0x2112
	highByte := controlAttribute{stun.AttrXORPeerAddress, []byte{
		1, 1, byte(encodedPort >> 8), byte(encodedPort), 127 ^ 0x21, 0x12, 0xa4, 1 ^ 0x42,
	}}
	underlength := controlAttribute{stun.AttrXORPeerAddress, []byte{
		0, 1, byte(encodedPort >> 8), byte(encodedPort), 127 ^ 0x21,
	}}
	if err := response("permission_peer_reserved_family_byte", client, perm, highByte); err != nil {
		return err
	}
	if err := response("channel_peer_reserved_family_byte", client, bind, controlChannel(0x4000), highByte); err != nil {
		return err
	}
	if err := response("permission_peer_underlength_ipv4", client, perm, underlength); err != nil {
		return err
	}
	if err := response("channel_peer_underlength_ipv4", client, bind, controlChannel(0x4002), underlength); err != nil {
		return err
	}
	cleanup := func() error {
		request, err := client.request(stun.MethodRefresh, turnLifetime(0))
		if err != nil {
			return err
		}
		reply, err := exchange(request)
		if err != nil || reply.Type != stun.NewType(stun.MethodRefresh, stun.ClassSuccessResponse) {
			return fmt.Errorf("control fixture Refresh(0): %v %v", reply, err)
		}
		_, err = waitUnixRelayPorts(addresses, 0, "control_cleanup", result)
		return err
	}
	if err := cleanup(); err != nil {
		return err
	}
	if err := response("permission_no_allocation", client, perm, local); err != nil {
		return err
	}
	if err := response("channel_no_allocation", client, bind, controlChannel(0x4000), local); err != nil {
		return err
	}

	// A fresh allocation exposes whether a valid prefix takes effect before a
	// subsequent malformed peer makes the whole CreatePermission report 400.
	request, err := client.request(stun.MethodAllocate, allocationAttributes(1))
	if err != nil {
		return err
	}
	reply, err := exchange(request)
	if err != nil || reply.Type != stun.NewType(stun.MethodAllocate, stun.ClassSuccessResponse) {
		return fmt.Errorf("control fixture fresh Allocate: %v %v", reply, err)
	}
	occupied, err = waitUnixRelayPorts(addresses, 1, "control_fresh_relay", result)
	if err != nil {
		return err
	}
	if err := response("permission_malformed_then_valid", client, perm, malformed, local); err != nil {
		return err
	}
	if err := probe("malformed_then_valid_relay", 0, false); err != nil {
		return err
	}
	if err := response("permission_valid_then_malformed", client, perm, local, malformed); err != nil {
		return err
	}
	if err := probe("valid_then_malformed_relay", 0, true); err != nil {
		return err
	}
	return cleanup()
}
