// SPDX-License-Identifier: MIT
package sctp
import("encoding/binary";"fmt";"math";"net";"os";"strings";"testing";"time")
var sourceRackClock time.Time
func sourceRackObserverNow()time.Time{return sourceRackClock}
func TestSourcePtoTlrAndWriterBudget(t *testing.T){
 out,e:=os.Create(os.Getenv("GO_SCTP_PTO_TLR_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close()
 type input struct{id string;rtt time.Duration;inflight,pending int;op string}
 cases:=[]input{
 {"pto-no-rtt-single",0,1,0,"pto"},{"pto-no-rtt-multi",0,2,0,"pto"},
 {"pto-actual-rtt-single",1000001,1,0,"pto"},{"pto-actual-rtt-multi",1000001,2,0,"pto"},
 {"pto-actual-subms-single",177777,1,0,"pto"},{"pto-pending-burst",2000000,1,10,"pto"},
 {"phase-before",2000000,3,0,"before"},{"phase-equal",2000000,3,0,"equal"},
 {"first-loss-once",2000000,3,0,"first1"},{"first-loss-floor",2000000,3,0,"first5"},
 {"later-loss-once",2000000,3,0,"later1"},{"later-loss-floor",2000000,3,0,"later5"},
 {"ack-progress-phase",2000000,3,0,"progress"},{"finish-clean",2000000,3,0,"finish"},
 {"finish-with-loss",2000000,3,0,"finishloss"},{"clean-reset-sixteen",2000000,3,0,"reset16"},
 {"shared-ordinary-new-fast",2000000,4,8,"shared"},
 {"first-exceeds-budget",2000000,1,0,"oversize"},
 }
 for _,row:=range cases{
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(0));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,1)
 a.lock.Lock();a.setState(established);a.useForwardTSN=true;a.cumulativeTSNAckPoint=0;a.advancedPeerTSNAckPoint=0;a.setCWND(100000);a.setRWND(100000)
 stream:=a.createStream(0,false);stream.SetReliabilityParams(false,ReliabilityTypeReliable,0)
 base:=time.Now().Add(time.Hour);sourceRackClock=base
 enqueue:=func(n int){for i:=0;i<n;i++{a.pendingQueue.push(&chunkPayloadData{beginningFragment:true,endingFragment:true,streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary,userData:make([]byte,1200)})}}
 send:=func(){c:=a.pendingQueue.peek();a.movePendingDataChunkToInflightQueue(c);wire,err:=a.marshalPacket(a.createPacket([]chunk{c}));if err!=nil||len(wire)!=1228{t.Fatal("source actual primary serialization prerequisite",err,len(wire))}}
 if row.rtt>0{enqueue(1);send();sourceRackClock=base.Add(row.rtt);if e=a.handleSack(&chunkSelectiveAck{cumulativeTSNAck:1,advertisedReceiverWindowCredit:100000});e!=nil{t.Fatal(e)}}
 sourceRackClock=base.Add(10*time.Millisecond);enqueue(row.inflight);for i:=0;i<row.inflight;i++{send()};enqueue(row.pending)
 // Initial setup is not an asynchronous source writer iteration. Drain its
 // coalesced wakes before exercising exactly one accepted PTO/explicit wake.
 for len(a.awakeWriteLoopCh)>0{<-a.awakeWriteLoopCh}
 a.schedulePTOAfterSendLocked();ptoDelta:=a.ptoDeadline.Sub(sourceRackClock).Nanoseconds()
 var allow string;var wireTSNs []string;wireBytes:=0
 switch row.op{
 case "pto": sourceRackClock=a.ptoDeadline;a.ptoDeadline=time.Time{};a.onPTOTimerLocked()
 case "shared":
   a.fastRtxWnd=10000;a.tlrBeginLocked();start:=a.cumulativeTSNAckPoint+1
   for i:=uint32(0);i<2;i++{c,_:=a.inflightQueue.get(start+i);c.retransmit=true}
   for i:=uint32(2);i<4;i++{c,_:=a.inflightQueue.get(start+i);c.missIndicator=3};a.willRetransmitFast=true;a.awakeWriteLoop()
 default:
   a.tlrBeginLocked();
   switch row.op{
   case "before":sourceRackClock=sourceRackClock.Add(row.rtt-time.Nanosecond)
   case "equal":sourceRackClock=sourceRackClock.Add(row.rtt)
   case "first1","first5","later1","later5","finishloss":
      n:=1;if strings.HasSuffix(row.op,"5"){n=5};if strings.HasPrefix(row.op,"later"){sourceRackClock=sourceRackClock.Add(row.rtt)}
      for i:=0;i<n;i++{a.tlrApplyAdditionalLossLocked(sourceRackClock)}
   case "progress":a.tlrMaybeFinishLocked(true)
   case "finish":a.cumulativeTSNAckPoint=a.tlrEndTSN;a.tlrMaybeFinishLocked(true)
   case "reset16":a.tlrBurstFirstRTTUnits=8;a.tlrBurstLaterRTTUnits=5;for i:=0;i<16;i++{a.tlrBeginLocked();a.cumulativeTSNAckPoint=a.tlrEndTSN;a.tlrMaybeFinishLocked(true)}
   case "oversize": budget:=int64(1);consumed:=false;first:=a.tlrAllowSendLocked(&budget,&consumed,1228);second:=a.tlrAllowSendLocked(&budget,&consumed,1);allow=fmt.Sprintf("%t,%t,%d,%t",first,second,budget,consumed)
   }
   if row.op=="finishloss"{a.cumulativeTSNAckPoint=a.tlrEndTSN;a.tlrMaybeFinishLocked(true)}
 }
 budget:=a.tlrCurrentBurstBudgetScaledLocked();active,first,loss,end,fu,lu,good:=a.tlrActive,a.tlrFirstRTT,a.tlrHadAdditionalLoss,a.tlrEndTSN,a.tlrBurstFirstRTTUnits,a.tlrBurstLaterRTTUnits,a.tlrGoodOps
 wake:=len(a.awakeWriteLoopCh)
 if row.op=="pto"||row.op=="shared"{
   if wake>0{<-a.awakeWriteLoopCh}
   a.lock.Unlock();packets,_:=a.gatherOutbound();a.lock.Lock()
   for _,b:=range packets{wireBytes+=len(b);for off:=12;off+4<=len(b);{n:=int(binary.BigEndian.Uint16(b[off+2:]));if n<4||off+n>len(b){t.Fatal("invalid source packet")};if b[off]==0{wireTSNs=append(wireTSNs,fmt.Sprint(binary.BigEndian.Uint32(b[off+4:])))};off+=(n+3)&^3}}
 }
 fmt.Fprintf(out,"%s\t%d\t%016x\t%t\t%t\t%t\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%s\n",row.id,ptoDelta,math.Float64bits(a.SRTT()),active,first,loss,end,fu,lu,good,budget,wake,strings.Join(wireTSNs,","),wireBytes,allow)
 a.lock.Unlock();_=a.close();_=right.Close()
 }
}
