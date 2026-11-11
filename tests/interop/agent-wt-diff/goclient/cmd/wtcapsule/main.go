// wtcapsule observes HTTP/3 CONNECT and owned-stream reset codes separately.
package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
	"google.golang.org/protobuf/proto"
)

const closeSession = 0x2843

type capsuleCase struct {
	Name  string
	Wire  []byte
	Fin   bool
	Owned bool
}

type observation struct {
	Name         string  `json:"name"`
	Registered   bool    `json:"registered"`
	ConnectReset uint64  `json:"connect_reset"`
	OwnedReset   *uint64 `json:"owned_reset"`
}

func capsule(kind uint64, payload []byte) []byte {
	header := quicvarint.Append(nil, kind)
	header = quicvarint.Append(header, uint64(len(payload)))
	return append(header, payload...)
}

func cases() []capsuleCase {
	valid := capsule(closeSession, []byte{0, 0, 0, 7})
	return []capsuleCase{
		{Name: "short_close_before_register", Wire: capsule(closeSession, []byte{1, 2, 3})},
		{Name: "short_close_after_register", Wire: capsule(closeSession, []byte{1, 2, 3}), Owned: true},
		{Name: "truncated_close", Wire: capsule(closeSession, []byte{0, 0, 0, 7})[:5], Fin: true, Owned: true},
		{Name: "connect_fin", Fin: true, Owned: true},
		{Name: "http2_only_max_stream_data", Wire: capsule(0x190b4d3e, nil), Owned: true},
		{Name: "http2_only_stream_data_blocked", Wire: capsule(0x190b4d42, nil), Owned: true},
		{Name: "max_data_trailing_byte", Wire: capsule(0x190b4d3d, []byte{42, 0}), Owned: true},
		{Name: "max_streams_empty_value", Wire: capsule(0x190b4d3f, nil), Owned: true},
		{Name: "streams_blocked_out_of_range", Wire: capsule(0x190b4d44, quicvarint.Append(nil, 1<<60+1)), Owned: true},
		{Name: "valid_flow_control_then_close", Wire: append(capsule(0x190b4d3f, []byte{42}), valid...), Owned: true},
		{Name: "unknown_then_close", Wire: append(capsule(63, []byte{1, 2, 3}), valid...), Owned: true},
		{Name: "valid_close", Wire: valid, Owned: true},
	}
}

func register(ctx context.Context, conn *quic.Conn, sessionID quic.StreamID) (*quic.Stream, error) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	stream.SetDeadline(time.Now().Add(3 * time.Second))
	request := &livekit.WorkerMessage{Message: &livekit.WorkerMessage_Register{
		Register: &livekit.RegisterWorkerRequest{AgentName: "capsule-probe", Version: "1.0.0"},
	}}
	body, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	frame := quicvarint.Append(nil, 0x41)
	frame = quicvarint.Append(frame, uint64(sessionID))
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
	if _, err := stream.Write(append(frame, body...)); err != nil {
		return nil, err
	}
	var size uint32
	if err := binary.Read(stream, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size > 1<<20 {
		return nil, fmt.Errorf("invalid register response size %d", size)
	}
	reply := make([]byte, size)
	if _, err := io.ReadFull(stream, reply); err != nil {
		return nil, err
	}
	var msg livekit.ServerMessage
	if err := proto.Unmarshal(reply, &msg); err != nil {
		return nil, err
	}
	if msg.GetRegister().GetWorkerId() == "" {
		return nil, errors.New("missing worker registration")
	}
	return stream, nil
}

func resetCode(reader io.Reader) (uint64, error) {
	var buffer [1]byte
	if _, err := reader.Read(buffer[:]); err != nil {
		var streamErr *quic.StreamError
		var h3Err *http3.Error
		if errors.As(err, &streamErr) && streamErr.Remote {
			return uint64(streamErr.ErrorCode), nil
		}
		if errors.As(err, &h3Err) && h3Err.Remote {
			return uint64(h3Err.ErrorCode), nil
		}
		return 0, err
	}
	return 0, errors.New("unexpected response data instead of reset")
}

func observe(ctx context.Context, h3 *http3.ClientConn, conn *quic.Conn, addr, token string, tc capsuleCase) (observation, error) {
	result := observation{Name: tc.Name}
	request, err := http.NewRequestWithContext(ctx, http.MethodConnect, "https://"+addr+"/agent", nil)
	if err != nil {
		return result, err
	}
	request.Proto = "webtransport"
	request.Header.Set("Authorization", "Bearer "+token)
	connect, err := h3.OpenRequestStream(ctx)
	if err != nil {
		return result, err
	}
	connect.SetDeadline(time.Now().Add(3 * time.Second))
	if err := connect.SendRequestHeader(request); err != nil {
		return result, err
	}
	response, err := connect.ReadResponse()
	if err != nil {
		return result, err
	}
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("CONNECT response: %s", response.Status)
	}
	var owned *quic.Stream
	if tc.Owned {
		owned, err = register(ctx, conn, connect.StreamID())
		if err != nil {
			return result, err
		}
		result.Registered = true
	}
	if len(tc.Wire) > 0 {
		if _, err := connect.Write(tc.Wire); err != nil {
			return result, err
		}
	}
	if tc.Fin {
		if err := connect.Close(); err != nil {
			return result, err
		}
	}
	result.ConnectReset, err = resetCode(connect)
	if err != nil {
		return result, fmt.Errorf("CONNECT read: %w", err)
	}
	if owned != nil {
		code, err := resetCode(owned)
		if err != nil {
			return result, fmt.Errorf("owned stream read: %w", err)
		}
		result.OwnedReset = &code
	}
	return result, nil
}

func run(ctx context.Context, addr, token string) error {
	conn, err := quic.DialAddr(ctx, addr, &tls.Config{
		InsecureSkipVerify: true, // test daemon uses the development certificate
		NextProtos:         []string{http3.NextProtoH3},
	}, &quic.Config{EnableDatagrams: true})
	if err != nil {
		return err
	}
	defer conn.CloseWithError(0, "")
	transport := &http3.Transport{
		EnableDatagrams:    true,
		AdditionalSettings: map[uint64]uint64{0x2c7cf000: 1},
	}
	h3 := transport.NewClientConn(conn)
	encode := json.NewEncoder(os.Stdout)
	var failures []error
	for _, tc := range cases() {
		result, err := observe(ctx, h3, conn, addr, token, tc)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", tc.Name, err))
			continue
		}
		if err := encode.Encode(result); err != nil {
			return err
		}
	}
	return errors.Join(failures...)
}

func main() {
	addr := flag.String("addr", "", "development HTTP/3 listener host:port")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := run(ctx, *addr, os.Getenv("LIVEKIT_WT_PROBE_TOKEN")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
