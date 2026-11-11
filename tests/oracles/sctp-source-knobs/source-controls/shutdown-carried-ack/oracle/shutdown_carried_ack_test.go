// SPDX-License-Identifier: MIT
package sctp
import("fmt";"net";"os";"testing";"time")
var sourceRackClock time.Time
func sourceRackObserverNow()time.Time{return sourceRackClock}
func TestSourceShutdownCarriedAcknowledgement(t *testing.T){
 out,e:=os.Create(os.Getenv("GO_SCTP_SHUTDOWN_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close()
 for _,id:=range []string{"all-acknowledged","partial-acknowledged","invalid-future","stale-ack","pending-data","crossed-shutdown","repeat-shutdown","shutdown-ack"}{
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(0));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,1)
 a.lock.Lock();a.setState(established);a.useForwardTSN=true;a.cumulativeTSNAckPoint=0;a.advancedPeerTSNAckPoint=0;a.setCWND(10000);a.setRWND(9999)
 stream:=a.createStream(0,false);stream.SetReliabilityParams(false,ReliabilityTypeReliable,0)
 sourceRackClock=time.Now().Add(time.Hour)
 for i:=0;i<2;i++ {ch:=&chunkPayloadData{beginningFragment:true,endingFragment:true,streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary,userData:[]byte{byte(i),2}};a.pendingQueue.push(ch);a.movePendingDataChunkToInflightQueue(ch);wire,e:=a.marshalPacket(a.createPacket([]chunk{ch}));if e!=nil||len(wire)<28||wire[12]!=0{t.Fatal("actual serialized DATA prerequisite",e)}}
 ack:=uint32(2)
 switch id {
 case "partial-acknowledged":ack=1
 case "invalid-future":ack=3
 case "stale-ack":ack=0xffffffff
 case "pending-data":a.pendingQueue.push(&chunkPayloadData{beginningFragment:true,endingFragment:true,streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary,userData:[]byte{3,2}})
 case "crossed-shutdown":a.setState(shutdownSent);a.willSendShutdown=true
 case "repeat-shutdown":a.setState(shutdownAckSent)
 case "shutdown-ack":a.setState(shutdownSent);a.willSendShutdown=true
 }
 sourceRackClock=sourceRackClock.Add(time.Millisecond)
 var err error
 if id=="shutdown-ack" {a.handleShutdownAck(&chunkShutdownAck{})}else{err=a.handleShutdown(&chunkShutdown{cumulativeTSNAck:ack})}
 fmt.Fprintf(out,"%s\t%s\t%d\t%d\t%d\t%d\t%t\t%t\t%t\t%t\n",id,getAssociationStateString(a.getState()),a.inflightQueue.size(),a.pendingQueue.size(),a.cumulativeTSNAckPoint,a.RWND(),a.willSendShutdownAck,a.willSendShutdown,a.willSendShutdownComplete,err!=nil)
 a.lock.Unlock();_=a.close();_=right.Close()
 }
}
