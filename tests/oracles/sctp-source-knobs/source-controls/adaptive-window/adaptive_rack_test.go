// SPDX-License-Identifier: MIT
package sctp
import("bufio";"encoding/json";"fmt";"math";"net";"os";"testing";"time")
var sourceRackClock time.Time
func sourceRackObserverNow()time.Time{return sourceRackClock}
type adaptiveRackRow struct{ID string;Initial,DeliveredTSN,Highest,Keep uint32;HeadAgeNs,DeliveredAgeNs,PriorMinNs,PriorReoNs int64;Samples []struct{AgeNs,ValueNs int64};Reordered,Recovery,T3,Duplicate,Delivered,Abandoned,Retried bool}
func TestSourceAdaptiveRackDeadline(t *testing.T){
 in,e:=os.Open(os.Getenv("GO_SCTP_ADAPTIVE_RACK_CASES"));if e!=nil{t.Fatal(e)};defer in.Close();out,e:=os.Create(os.Getenv("GO_SCTP_ADAPTIVE_RACK_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close();scan:=bufio.NewScanner(in)
 for scan.Scan(){var c adaptiveRackRow;if e=json.Unmarshal(scan.Bytes(),&c);e!=nil{t.Fatal(e)}
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(0));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,c.Initial)
 a.lock.Lock();a.setState(established);a.useForwardTSN=true;a.cumulativeTSNAckPoint=c.Initial-1;a.advancedPeerTSNAckPoint=c.Initial-1
 stream:=a.createStream(0,false);stream.SetReliabilityParams(false,ReliabilityTypeReliable,0)
 sourceRackClock=time.Now().Add(time.Hour);now:=sourceRackClock
 head:=&chunkPayloadData{beginningFragment:true,endingFragment:true,streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary,userData:[]byte{1,2}}
 a.pendingQueue.push(head);a.movePendingDataChunkToInflightQueue(head);head.since=now.Add(-time.Duration(c.HeadAgeNs));if c.Retried{head.nSent=2};if c.Abandoned{head.setAbandoned(true)}
 a.rackHighestDeliveredOrigTSN=c.Highest;a.rackReorderingSeen=c.Reordered;a.rackKeepInflatedRecoveries=int(c.Keep);a.rackMinRTT=time.Duration(c.PriorMinNs);a.rackReoWnd=time.Duration(c.PriorReoNs);a.inFastRecovery=c.Recovery
 for _,sample:=range c.Samples{rtt:=time.Duration(sample.ValueNs);a.rack.rackMinRTTWnd.Push(now.Add(-time.Duration(sample.AgeNs)),rtt);srtt:=a.rtoMgr.setNewRTT(rtt.Seconds()*1000.0);a.srtt.Store(srtt)}
 if c.T3{a.t3RTX.start(1000)}
 ack:=&chunkSelectiveAck{};if c.Duplicate{ack.duplicateTSN=[]uint32{c.Initial+1}}
 delivered:=time.Time{};if c.Delivered{delivered=now.Add(-time.Duration(c.DeliveredAgeNs))}
 a.onRackAfterSACK(c.Delivered,delivered,c.DeliveredTSN,ack)
 a.timerMu.Lock();deadline:=a.rackDeadline;a.timerMu.Unlock();delta:=int64(-1);if !deadline.IsZero(){delta=deadline.Sub(now).Nanoseconds()}
 fmt.Fprintf(out,"%s\t%d\t%d\t%d\t%d\t%t\t%t\t%t\t%d\t%016x\t%d\n",c.ID,a.rackMinRTT.Nanoseconds(),a.rackReoWnd.Nanoseconds(),a.rackKeepInflatedRecoveries,a.rackHighestDeliveredOrigTSN,a.rackReorderingSeen,head.retransmit,head.rackInList,delta,math.Float64bits(a.SRTT()),a.rack.rackMinRTTWnd.Len())
 a.lock.Unlock();_=a.close();_=right.Close()
 };if e=scan.Err();e!=nil{t.Fatal(e)}
}
