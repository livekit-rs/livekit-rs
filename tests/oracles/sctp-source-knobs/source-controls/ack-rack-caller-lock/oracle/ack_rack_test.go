// SPDX-License-Identifier: MIT
package sctp
import("bufio";"encoding/binary";"encoding/json";"fmt";"net";"os";"strings";"testing";"time")
func TestSourceAckRackZeroWindow(t *testing.T){
 in,e:=os.Open(os.Getenv("GO_SCTP_ACK_RACK_CASES"));if e!=nil{t.Fatal(e)};defer in.Close()
 out,e:=os.Create(os.Getenv("GO_SCTP_ACK_RACK_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close();scan:=bufio.NewScanner(in)
 for scan.Scan(){var c struct{ID,Kind string;ThirdNs int64;HeadRetransmitted,AllInflight,Duplicate,Reordered bool};if e=json.Unmarshal(scan.Bytes(),&c);e!=nil{t.Fatal(e)}
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(0));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,1);a.setState(established);a.useForwardTSN=true;a.cumulativeTSNAckPoint=0;a.advancedPeerTSNAckPoint=0;a.setCWND(10000);a.setRWND(10000)
 stream:=a.createStream(0,false);kind:=byte(ReliabilityTypeRexmit);if c.Kind=="reliable"{kind=ReliabilityTypeReliable};stream.SetReliabilityParams(false,kind,0)
 head:=&chunkPayloadData{beginningFragment:true,streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary,userData:[]byte{1,2}}
 if c.Kind=="dcep"{head.payloadType=PayloadTypeWebRTCDCEP}
 a.lock.Lock() // actual handleChunk caller ownership; ACK helpers may temporarily unlock/relock
 for i:=0;i<5;i++{ch:=head;if i!=0{ch=&chunkPayloadData{head:head,endingFragment:i==4,streamIdentifier:0,payloadType:head.payloadType,userData:[]byte{byte(i),2}}};a.pendingQueue.push(ch)}
 base:=time.Now().Add(-100*time.Millisecond)
 for i:=0;i<2;i++{ch:=a.pendingQueue.peek();a.movePendingDataChunkToInflightQueue(ch);ch.since=base}
 if c.HeadRetransmitted{head.nSent=2}
 if c.Reordered{a.rackHighestDeliveredOrigTSN=100}
 a.t3RTX.start(1000)
 ack:=&chunkSelectiveAck{cumulativeTSNAck:0,advertisedReceiverWindowCredit:10000,gapAckBlocks:[]gapAckBlock{{start:2,end:2}}}
 if e=a.handleSack(ack);e!=nil{t.Fatal(e)};first:=head.retransmit
 ch:=a.pendingQueue.peek();a.movePendingDataChunkToInflightQueue(ch);ch.since=base.Add(time.Duration(c.ThirdNs))
 if c.AllInflight{for a.pendingQueue.size()>0{tail:=a.pendingQueue.peek();a.movePendingDataChunkToInflightQueue(tail);tail.since=ch.since}}
 ack=&chunkSelectiveAck{cumulativeTSNAck:0,advertisedReceiverWindowCredit:10000,gapAckBlocks:[]gapAckBlock{{start:2,end:3}}};if c.Duplicate{ack.duplicateTSN=[]uint32{2}}
 if e=a.handleSack(ack);e!=nil{t.Fatal(e)};second:=head.retransmit
 var consumed bool;packets:=a.gatherDataPacketsToRetransmit(nil,nil,&consumed);tsns:=[]string{}
 for _,b:=range packets{for off:=12;off+4<=len(b);{n:=int(binary.BigEndian.Uint16(b[off+2:]));if n<4||off+n>len(b){t.Fatal("invalid real output")};if b[off]==0{tsns=append(tsns,fmt.Sprint(binary.BigEndian.Uint32(b[off+4:])))};off+=(n+3)&^3}}
 fmt.Fprintf(out,"%s\t%t\t%t\t%d\t%t\t%t\t%s\n",c.ID,first,second,head.nSent,head.abandoned(),a.rackReorderingSeen,strings.Join(tsns,","));a.lock.Unlock();_=a.close();_=right.Close()
 };if e=scan.Err();e!=nil{t.Fatal(e)}
}
