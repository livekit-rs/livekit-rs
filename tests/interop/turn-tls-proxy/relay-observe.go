// Opt-in TURN relay oracle. Build alongside observe.go.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"time"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/auth"
	"github.com/pion/stun/v3"
)

func init() { observeINETControl = exerciseRelayWire }

type relayAttr struct {
	kind  stun.AttrType
	value []byte
}

func (a relayAttr) AddTo(message *stun.Message) error {
	message.Add(a.kind, a.value)
	return nil
}

type relayPeer struct{ stun.XORMappedAddress }

func (p relayPeer) AddTo(message *stun.Message) error {
	return p.AddToAs(message, stun.AttrXORPeerAddress)
}

func exerciseRelayWire(client unixClient, exchange func(*stun.Message) (*stun.Message, error), addresses, occupied []string, result map[string]any, peer *net.UDPConn) error {
	const channel = 0x4000
	relay, err := net.ResolveUDPAddr("udp", occupied[0])
	if err != nil {
		return err
	}
	addr := peer.LocalAddr().(*net.UDPAddr)
	peerAttr := relayPeer{stun.XORMappedAddress{IP: addr.IP, Port: addr.Port}}
	udp := os.Getenv("LK_TURN_INET_WIRE") == "udp"
	request := func(method stun.Method, attrs ...stun.Setter) error {
		message, err := client.request(method, attrs...)
		if err != nil {
			return err
		}
		reply, err := exchange(message)
		if err != nil {
			return err
		}
		if reply.Type != stun.NewType(method, stun.ClassSuccessResponse) {
			return fmt.Errorf("expected %s success, got %v", method, reply)
		}
		return stun.NewLongTermIntegrity(client.username, client.realm.String(), client.password).Check(reply)
	}
	write := func(frame []byte) error {
		if err := client.stream.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		n, err := client.stream.Write(frame)
		if err == nil && n != len(frame) {
			return io.ErrShortWrite
		}
		return err
	}
	readDownstream := func(payload []byte, bound bool) (map[string]any, error) {
		if err := client.stream.SetReadDeadline(time.Now().Add(400 * time.Millisecond)); err != nil {
			return nil, err
		}
		if _, err := peer.WriteToUDP(payload, relay); err != nil {
			return nil, err
		}
		frame, err := readRelayPacket(client.stream, udp)
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return map[string]any{"delivered": false}, nil
		}
		if err != nil {
			return nil, err
		}
		row := map[string]any{"delivered": true, "frame_bytes": len(frame)}
		if bound {
			length := int(binary.BigEndian.Uint16(frame[2:4]))
			if binary.BigEndian.Uint16(frame[:2]) != channel || length != len(payload) ||
				len(frame) < 4+length || !slices.Equal(frame[4:4+length], payload) {
				return nil, fmt.Errorf("unexpected downstream ChannelData %x", frame)
			}
			row["padding"] = fmt.Sprintf("%x", frame[4+length:])
			return row, nil
		}
		message := &stun.Message{Raw: frame}
		if err := message.Decode(); err != nil {
			return nil, err
		}
		data, err := message.Get(stun.AttrData)
		var reportedPeer stun.XORMappedAddress
		if err != nil || message.Type != stun.NewType(stun.MethodData, stun.ClassIndication) ||
			!slices.Equal(data, payload) ||
			reportedPeer.GetFromAs(message, stun.AttrXORPeerAddress) != nil || reportedPeer.String() != addr.String() {
			return nil, fmt.Errorf("unexpected downstream Data indication %v", message)
		}
		row["attributes"] = attributeNames(message)
		return row, nil
	}
	readUpstream := func(payload []byte, bound bool) (bool, error) {
		var frame []byte
		if bound {
			frame = binary.BigEndian.AppendUint16(nil, channel)
			frame = binary.BigEndian.AppendUint16(frame, uint16(len(payload)))
			frame = append(frame, payload...)
			if !udp {
				frame = append(frame, make([]byte, (4-len(payload)%4)%4)...)
			}
		} else {
			msg, err := stun.Build(stun.TransactionID, stun.NewType(stun.MethodSend, stun.ClassIndication), peerAttr, relayAttr{stun.AttrData, payload})
			if err != nil {
				return false, err
			}
			frame = msg.Raw
		}
		if err := write(frame); err != nil {
			return false, err
		}
		if err := peer.SetReadDeadline(time.Now().Add(400 * time.Millisecond)); err != nil {
			return false, err
		}
		var data [64]byte
		n, source, err := peer.ReadFromUDP(data[:])
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if source.String() != relay.String() || !slices.Equal(data[:n], payload) {
			return false, fmt.Errorf("unexpected upstream payload %x from %s", data[:n], source)
		}
		return true, nil
	}
	if err := request(stun.MethodCreatePermission, peerAttr); err != nil {
		return err
	}
	data, err := readDownstream([]byte("relay-data"), false)
	if err != nil || data["delivered"] != true {
		return fmt.Errorf("Data indication control failed: %v %v", data, err)
	}
	result["data_indication"] = data
	if err := request(stun.MethodChannelBind, relayAttr{stun.AttrChannelNumber, []byte{0x40, 0, 0, 0}}, peerAttr); err != nil {
		return err
	}
	boundAt := time.Now()
	rows := map[int]any{}
	result["channel_data"] = rows
	for _, size := range []int{1, 4, 5} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(size + i)
		}
		row, err := readDownstream(payload, true)
		if err != nil || row["delivered"] != true {
			return fmt.Errorf("ChannelData control %d failed: %v %v", size, row, err)
		}
		rows[size] = row
	}
	if os.Getenv("LK_TURN_CHANNEL_EXPIRY") == "1" {
		for _, bound := range []bool{false, true} {
			if received, err := readUpstream([]byte("before-expiry"), bound); err != nil || !received {
				return fmt.Errorf("upstream before expiry failed for bound=%v: %t %v", bound, received, err)
			}
		}
		time.Sleep(time.Until(boundAt.Add(305 * time.Second)))
		up, err := readUpstream([]byte("after-channel"), true)
		if err != nil {
			return err
		}
		send, err := readUpstream([]byte("after-send"), false)
		if err != nil || send {
			return fmt.Errorf("Send without live permission forwarded: %t %v", send, err)
		}
		down, err := readDownstream([]byte("after-channel"), true)
		if err != nil {
			return err
		}
		result["after_permission_expiry"] = map[string]any{"elapsed_seconds": time.Since(boundAt).Seconds(),
			"upstream_channel_delivered": up, "downstream_channel_delivered": down["delivered"], "send_delivered": send}
		creds := service.NewTURNAuthHandler(auth.NewSimpleKeyProvider(apiKey, apiSecret))
		username, expiry := creds.CreateUsername(apiKey, "PA_turnBindProbe", 300)
		password, err := creds.CreatePassword(apiKey, "PA_turnBindProbe", expiry)
		if err != nil {
			return err
		}
		client.username, client.password = username, password
	}
	if err := request(stun.MethodRefresh, turnLifetime(0)); err != nil {
		return err
	}
	_, err = waitUnixRelayPorts(addresses, 0, "after_relay_refresh_zero", result)
	return err
}

func readRelayPacket(stream net.Conn, udp bool) ([]byte, error) {
	if udp {
		buf := make([]byte, 2048)
		n, err := stream.Read(buf)
		if err == nil && n < 4 {
			return nil, io.ErrUnexpectedEOF
		}
		return buf[:n], err
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(stream, head); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint16(head[2:4]))
	rest := 16 + length
	if head[0]&0xc0 == 0x40 {
		rest = (length + 3) &^ 3
	}
	body := make([]byte, rest)
	_, err := io.ReadFull(stream, body)
	return append(head, body...), err
}
