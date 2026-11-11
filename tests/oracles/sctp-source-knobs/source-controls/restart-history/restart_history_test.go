// SPDX-License-Identifier: MIT
// Fixture only. All restart parsing, cookie authentication, TCB commit, stream
// rollback and pending-writer retirement execute the original Pion library.
package sctp

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func restartHistoryOutput(t *testing.T) *json.Encoder {
	t.Helper()
	file, e := os.OpenFile(os.Getenv("GO_SCTP_RESTART_HISTORY"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, e)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	return json.NewEncoder(file)
}
func restartHistoryWire(t *testing.T, a *Association, p *packet) string {
	t.Helper()
	raw, e := a.marshalPacket(p)
	require.NoError(t, e)
	sum := sha256.Sum256(raw)
	require.NoError(t, a.handleInbound(raw))
	return hex.EncodeToString(sum[:])
}
func restartHistoryTCB(t *testing.T) *Association {
	t.Helper()
	left, right := net.Pipe()
	cfg, e := buildClientConfig(WithNetConn(left), WithMTU(1228), WithFastRtxWnd(0))
	require.NoError(t, e)
	a := createAssociationFromConfigWithTsn(cfg, 100)
	t.Cleanup(func() { _ = a.close(); _ = right.Close() })
	a.setState(established)
	a.sourcePort = 5000
	a.destinationPort = 5000
	a.myVerificationTag = 111
	a.setPeerInit(&chunkInitCommon{initiateTag: 222, initialTSN: 200, numInboundStreams: 100, numOutboundStreams: 100, advertisedReceiverWindowCredit: 1 << 20})
	return a
}
func restartHistoryInit(a *Association, tag uint32) *packet {
	return &packet{sourcePort: a.destinationPort, destinationPort: a.sourcePort, chunks: []chunk{&chunkInit{chunkInitCommon: chunkInitCommon{initiateTag: tag, initialTSN: 500, numInboundStreams: 100, numOutboundStreams: 100, advertisedReceiverWindowCredit: 1 << 20}}}}
}
func restartHistoryEcho(a *Association, tag uint32, cookie []byte) *packet {
	return &packet{sourcePort: a.destinationPort, destinationPort: a.sourcePort, verificationTag: tag, chunks: []chunk{&chunkCookieEcho{cookie: cookie}}}
}
func restartHistoryChallenge(t *testing.T, a *Association, tag uint32) (uint32, []byte) {
	t.Helper()
	restartHistoryWire(t, a, restartHistoryInit(a, tag))
	a.lock.Lock()
	replies := a.controlQueue.popAll()
	a.lock.Unlock()
	require.Len(t, replies, 1)
	raw, e := a.marshalPacket(replies[0])
	require.NoError(t, e)
	var decoded packet
	require.NoError(t, decoded.unmarshal(true, raw))
	ack, ok := decoded.chunks[0].(*chunkInitAck)
	require.True(t, ok)
	for _, p := range ack.params {
		if cookie, ok := p.(*paramStateCookie); ok {
			return ack.initiateTag, append([]byte(nil), cookie.cookie...)
		}
	}
	t.Fatal("original INIT_ACK lacks cookie")
	return 0, nil
}
func TestSourceRestartAuthenticationWireObserver(t *testing.T) {
	out := restartHistoryOutput(t)
	a := restartHistoryTCB(t)
	identity := a
	stream, e := a.OpenStream(1, PayloadTypeWebRTCBinary)
	require.NoError(t, e)
	// Real Stream packetization accounts the buffer; original send queue then
	// serializes each primary DATA, exactly as in production.
	chunks, _ := stream.packetize([]byte("old-inflight"), PayloadTypeWebRTCBinary)
	a.lock.Lock()
	a.pendingQueue.push(chunks[0])
	a.movePendingDataChunkToInflightQueue(chunks[0])
	a.lock.Unlock()
	chunks, _ = stream.packetize([]byte("old-pending"), PayloadTypeWebRTCBinary)
	a.lock.Lock()
	a.pendingQueue.push(chunks[0])
	a.lock.Unlock()
	buffered := stream.BufferedAmount()
	require.Positive(t, buffered)
	beforeTag, beforePeer := a.myVerificationTag, a.peerVerificationTag
	duplicateHash := restartHistoryWire(t, a, restartHistoryInit(a, beforePeer))
	require.Zero(t, a.controlQueue.size())
	require.Nil(t, a.restartCookieKey)
	require.NoError(t, out.Encode(map[string]any{"case": "wire", "phase": "duplicate-init", "generation": a.restartGeneration, "no_challenge": a.controlQueue.size() == 0, "buffer_preserved": stream.BufferedAmount() == buffered, "wire_sha256": duplicateHash}))
	tag, cookie := restartHistoryChallenge(t, a, 333)
	otherTag, otherCookie := restartHistoryChallenge(t, a, 444)
	require.Equal(t, beforeTag, a.myVerificationTag)
	require.Equal(t, buffered, stream.BufferedAmount())
	forged := append([]byte(nil), cookie...)
	forged[len(forged)-1] ^= 1
	hash := restartHistoryWire(t, a, restartHistoryEcho(a, tag, forged))
	require.Zero(t, a.restartGeneration)
	require.Equal(t, beforeTag, a.myVerificationTag)
	require.Equal(t, beforePeer, a.peerVerificationTag)
	require.Equal(t, buffered, stream.BufferedAmount())
	require.NoError(t, out.Encode(map[string]any{"case": "wire", "phase": "invalid-cookie", "generation": a.restartGeneration, "buffer_preserved": stream.BufferedAmount() == buffered, "tags_preserved": a.myVerificationTag == beforeTag && a.peerVerificationTag == beforePeer, "wire_sha256": hash}))
	wrongPort := restartHistoryEcho(a, tag, cookie)
	wrongPort.sourcePort++
	hash = restartHistoryWire(t, a, wrongPort)
	require.Zero(t, a.restartGeneration)
	require.Equal(t, buffered, stream.BufferedAmount())
	require.NoError(t, out.Encode(map[string]any{"case": "wire", "phase": "wrong-port-cookie", "generation": a.restartGeneration, "buffer_preserved": stream.BufferedAmount() == buffered, "wire_sha256": hash}))
	hash = restartHistoryWire(t, a, restartHistoryEcho(a, tag, cookie))
	require.Same(t, identity, a)
	require.Equal(t, uint64(1), a.restartGeneration)
	require.Equal(t, tag, a.myVerificationTag)
	require.Equal(t, uint32(333), a.peerVerificationTag)
	require.Equal(t, StreamStateOpen, stream.State())
	require.Zero(t, stream.BufferedAmount())
	require.Same(t, stream, a.streams[1])
	require.Zero(t, a.pendingQueue.size())
	require.Zero(t, a.inflightQueue.size())
	require.False(t, a.writePending)
	require.NoError(t, out.Encode(map[string]any{"case": "wire", "phase": "commit", "generation": a.restartGeneration, "same_tcb": identity == a, "same_stream": stream == a.streams[1], "stream_open": stream.State() == StreamStateOpen, "buffer_released": stream.BufferedAmount() == 0, "pending_empty": a.pendingQueue.size() == 0, "inflight_empty": a.inflightQueue.size() == 0, "write_pending": a.writePending, "wire_sha256": hash}))
	restartHistoryWire(t, a, restartHistoryEcho(a, tag, cookie))
	restartHistoryWire(t, a, restartHistoryEcho(a, otherTag, otherCookie))
	require.Equal(t, uint64(1), a.restartGeneration)
	require.Equal(t, tag, a.myVerificationTag)
	require.NoError(t, out.Encode(map[string]any{"case": "wire", "phase": "duplicate-competing-cookie", "generation": a.restartGeneration, "same_tcb": identity == a, "same_stream": stream == a.streams[1], "tags_preserved": a.myVerificationTag == tag && a.peerVerificationTag == 333}))
	shutdown := restartHistoryTCB(t)
	shutdown.setState(shutdownAckSent)
	restartHistoryWire(t, shutdown, restartHistoryInit(shutdown, 333))
	require.Zero(t, shutdown.controlQueue.size())
	require.True(t, shutdown.willSendShutdownAck)
	require.Zero(t, shutdown.restartGeneration)
	require.Nil(t, shutdown.restartCookieKey)
	require.NoError(t, out.Encode(map[string]any{"case": "wire", "phase": "shutdown-ack-init", "generation": shutdown.restartGeneration, "no_challenge": shutdown.controlQueue.size() == 0, "shutdown_ack_pending": shutdown.willSendShutdownAck}))
}

// The UDP fixture is the original package's association(t,pipeDump) helper.
// The second endpoint really closes and rebinds the same local UDP address;
// persistent never gets replaced. Blocked Stream.Write uses the original native
// write loop and must return its actual ErrAssociationRestarted sentinel.
func TestSourceRestartPhysicalPeerObserver(t *testing.T) {
	out := restartHistoryOutput(t)
	persistent, peer, e := association(t, pipeDump, WithEnableInterleaving(false), WithEnableZeroChecksum(false), WithBlockWrite(true))
	require.NoError(t, e)
	defer func() { _ = persistent.Close(); _ = peer.Close() }()
	identity := persistent
	var previous *Stream
	for generation := 0; generation < 3; generation++ {
		retired := false
		released := false
		if previous != nil {
			require.Eventually(t, func() bool { return previous.BufferedAmount() == 0 }, time.Second, time.Millisecond)
			persistent.lock.Lock()
			persistent.writePending = true
			persistent.lock.Unlock()
			release := make(chan uint64, 1)
			previous.OnBufferedAmountLow(func() { release <- previous.BufferedAmount() })
			interrupted := make(chan error, 1)
			go func() { _, err := previous.Write([]byte("retired-old-generation")); interrupted <- err }()
			require.Eventually(t, func() bool { return previous.BufferedAmount() > 0 }, time.Second, time.Millisecond)
			local, ok := peer.netConn.LocalAddr().(*net.UDPAddr)
			require.True(t, ok)
			remote, ok := peer.netConn.RemoteAddr().(*net.UDPAddr)
			require.True(t, ok)
			require.NoError(t, peer.Close())
			conn, err := net.DialUDP("udp4", local, remote)
			require.NoError(t, err)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			peer, err = ClientWithOptions(WithNetConn(conn), WithEnableInterleaving(false), WithEnableZeroChecksum(false))
			require.NoError(t, err)
			require.NoError(t, conn.SetReadDeadline(time.Time{}))
			select {
			case err := <-interrupted:
				require.ErrorIs(t, err, ErrAssociationRestarted)
				retired = errors.Is(err, ErrAssociationRestarted)
			case <-time.After(5 * time.Second):
				t.Fatal("original blocked writer did not retire")
			}
			select {
			case amount := <-release:
				require.Zero(t, amount)
				released = true
			case <-time.After(2 * time.Second):
				t.Fatal("old generation buffer callback not released")
			}
			previous.OnBufferedAmountLow(nil)
			require.Zero(t, previous.BufferedAmount())
			require.Equal(t, StreamStateOpen, previous.State())
		}
		tx, err := peer.OpenStream(0, PayloadTypeWebRTCBinary)
		require.NoError(t, err)
		rx, err := persistent.OpenStream(0, PayloadTypeWebRTCBinary)
		require.NoError(t, err)
		sameStream := previous == nil || previous == rx
		if previous != nil {
			require.Same(t, previous, rx)
		}
		for _, pair := range [][2]*Stream{{tx, rx}, {rx, tx}} {
			require.NoError(t, pair[0].SetWriteDeadline(time.Now().Add(5*time.Second)))
			require.NoError(t, pair[1].SetReadDeadline(time.Now().Add(5*time.Second)))
			payload := bytes.Repeat([]byte{byte(generation), 42}, 256)
			n, err := pair[0].Write(payload)
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
			buf := make([]byte, len(payload))
			n, err = pair[1].Read(buf)
			require.NoError(t, err)
			require.Equal(t, payload, buf[:n])
		}
		require.Same(t, identity, persistent)
		require.Equal(t, uint64(generation), persistent.restartGeneration)
		require.NoError(t, out.Encode(map[string]any{"case": "physical", "phase": fmt.Sprintf("generation-%d", generation), "generation": persistent.restartGeneration, "same_tcb": identity == persistent, "same_stream": sameStream, "stream_open": rx.State() == StreamStateOpen, "old_attempt_retired": retired, "buffer_callback_released": released, "actual_bidirectional_data": true}))
		previous = rx
	}
}
