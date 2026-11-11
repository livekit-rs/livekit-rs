// SPDX-License-Identifier: MIT
package sctp
import("encoding/binary";"fmt";"math";"net";"os";"testing";"time")
var sourceRackClock time.Time
func sourceRackObserverNow()time.Time{return sourceRackClock}
func TestSourceRealAckRttHistory(t *testing.T){
 out,e:=os.Create(os.Getenv("GO_SCTP_ACK_RTT_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close()
 for _,row:=range []struct{id string;initial uint32}{{"low",1},{"high",0x90000000},{"wrap",0xffffffff}}{
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(0));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,row.initial)
 a.lock.Lock();a.setState(established);a.useForwardTSN=true;a.cumulativeTSNAckPoint=row.initial-1;a.advancedPeerTSNAckPoint=row.initial-1;a.setCWND(10000);a.setRWND(10000)
 stream:=a.createStream(0,false);stream.SetReliabilityParams(false,ReliabilityTypeReliable,0)
 for i:=0;i<5;i++{a.pendingQueue.push(&chunkPayloadData{beginningFragment:true,endingFragment:true,streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary,userData:[]byte{byte(i),2}})}
 base:=time.Now().Add(time.Hour);sourceRackClock=base
 send:=func(at time.Duration){sourceRackClock=base.Add(at);ch:=a.pendingQueue.peek();a.movePendingDataChunkToInflightQueue(ch);ch.since=sourceRackClock}
 dump:=func(stage string){fmt.Fprintf(out,"%s\t%s\t%016x\t%d\t%d\t%d\t%t\n",row.id,stage,math.Float64bits(a.SRTT()),a.rack.rackMinRTTWnd.Len(),a.minTSN2MeasureRTT,a.rackMinRTT.Nanoseconds(),a.rackReorderingSeen)}
 send(0);head,_:=a.inflightQueue.get(row.initial);send(time.Millisecond);a.t3RTX.start(1000)
 ack:=&chunkSelectiveAck{cumulativeTSNAck:row.initial-1,advertisedReceiverWindowCredit:10000,gapAckBlocks:[]gapAckBlock{{start:2,end:2}}}
 sourceRackClock=base.Add(2*time.Millisecond);if e=a.handleSack(ack);e!=nil{t.Fatal(e)};dump("first-original")
 sourceRackClock=base.Add(2500*time.Microsecond);if e=a.handleSack(ack);e!=nil{t.Fatal(e)};dump("duplicate-ack")
 sourceRackClock=base.Add(3*time.Millisecond);var consumed bool;packets:=a.gatherDataPacketsToRetransmit(nil,nil,&consumed);actualRepeat:=false
 for _,b:=range packets{for off:=12;off+4<=len(b);{n:=int(binary.BigEndian.Uint16(b[off+2:]));if n<4||off+n>len(b){t.Fatal("invalid emitted SCTP")};if b[off]==0&&binary.BigEndian.Uint32(b[off+4:])==row.initial{actualRepeat=true};off+=(n+3)&^3}}
 if !actualRepeat||head.nSent!=2{t.Fatal("source prerequisite: real ACK-RACK ordinary repeated head missing")}
 sourceRackClock=base.Add(4*time.Millisecond);ack=&chunkSelectiveAck{cumulativeTSNAck:row.initial,advertisedReceiverWindowCredit:10000};if e=a.handleSack(ack);e!=nil{t.Fatal(e)};dump("retransmitted-ack-karn")
 send(4*time.Millisecond);sourceRackClock=base.Add(7*time.Millisecond);ack=&chunkSelectiveAck{cumulativeTSNAck:row.initial,advertisedReceiverWindowCredit:10000,gapAckBlocks:[]gapAckBlock{{start:1,end:2}}};if e=a.handleSack(ack);e!=nil{t.Fatal(e)};dump("next-original")
 a.lock.Unlock();_=a.close();_=right.Close()
 }
}
