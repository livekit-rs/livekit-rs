// SPDX-License-Identifier: MIT
package sctp

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"os"
	"testing"
	"time"
)

var sourceRackClock time.Time

func sourceRackObserverNow() time.Time { return sourceRackClock }
func TestSourceFractionalAckHistory(t *testing.T) {
	out, e := os.Create(os.Getenv("GO_SCTP_FRACTIONAL_ACK_OUTPUT"))
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	samples := []time.Duration{1234567, 177777, 999999, 2345678, 333333, 1000001, 7654321, 123457}
	for _, row := range []struct {
		id      string
		initial uint32
	}{{"low", 1}, {"high", 0x90000000}, {"wrap", 0xfffffffc}} {
		left, right := net.Pipe()
		cfg, e := buildClientConfig(WithNetConn(left), WithMTU(1228), WithFastRtxWnd(0))
		if e != nil {
			t.Fatal(e)
		}
		a := createAssociationFromConfigWithTsn(cfg, row.initial)
		a.lock.Lock()
		a.setState(established)
		a.useForwardTSN = true
		a.cumulativeTSNAckPoint = row.initial - 1
		a.advancedPeerTSNAckPoint = row.initial - 1
		a.setCWND(10000)
		a.setRWND(10000)
		stream := a.createStream(0, false)
		stream.SetReliabilityParams(false, ReliabilityTypeReliable, 0)
		base := time.Now().Add(time.Hour)
		for i, rtt := range samples {
			sourceRackClock = base.Add(time.Duration(i) * 20 * time.Millisecond)
			// Use the real Stream packetizer: it owns sequence allocation and
			// bufferedAmount accounting which the subsequent real SACK releases.
			// packetize acquires association.RLock itself, so release our fixture lock.
			a.lock.Unlock()
			chunks, unordered := stream.packetize([]byte{byte(i), 2}, PayloadTypeWebRTCBinary)
			a.lock.Lock()
			if len(chunks) != 1 || unordered || stream.BufferedAmount() != 2 {
				t.Fatal("real stream packetize/buffer accounting prerequisite")
			}
			ch := chunks[0]
			a.pendingQueue.push(ch)
			a.movePendingDataChunkToInflightQueue(ch)
			wire, err := a.marshalPacket(a.createPacket([]chunk{ch}))
			if err != nil {
				t.Fatal(err)
			}
			if len(wire) < 20 || wire[12] != 0 || binary.BigEndian.Uint32(wire[16:20]) != row.initial+uint32(i) || ch.nSent != 1 {
				t.Fatal("actual primary DATA serialization prerequisite")
			}
			sourceRackClock = sourceRackClock.Add(rtt)
			if e = a.handleSack(&chunkSelectiveAck{cumulativeTSNAck: ch.tsn, advertisedReceiverWindowCredit: 10000}); e != nil {
				t.Fatal(e)
			}
			if stream.BufferedAmount() != 0 {
				t.Fatal("real cumulative SACK must release exactly the packetized bytes")
			}
			fmt.Fprintf(out, "%s\t%d\t%d\t%016x\t%d\t%d\t%d\t%t\n", row.id, i, rtt.Nanoseconds(), math.Float64bits(a.SRTT()), a.rack.rackMinRTTWnd.Len(), a.minTSN2MeasureRTT, a.rackMinRTT.Nanoseconds(), a.rackReorderingSeen)
		}
		a.lock.Unlock()
		_ = a.close()
		_ = right.Close()
	}
}
