// SPDX-License-Identifier: MIT
// Independent original source stream.packetize/send/fast/SACK observer.
package sctp
import("bufio";"encoding/binary";"encoding/json";"net";"os";"testing";"time")
func TestSourceRtoPartialReliabilityObserver(t *testing.T){
 input,err:=os.Open(os.Getenv("GO_SCTP_RTO_INPUTS"));if err!=nil{t.Fatal(err)};defer input.Close()
 output,err:=os.Create(os.Getenv("GO_SCTP_RTO_OUTPUT"));if err!=nil{t.Fatal(err)};defer output.Close();enc:=json.NewEncoder(output);scan:=bufio.NewScanner(input)
 for scan.Scan(){
  var c struct{ID,Kind string;Value,Fast uint32};if err:=json.Unmarshal(scan.Bytes(),&c);err!=nil{t.Fatal(err)}
  left,right:=net.Pipe();cfg,err:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(c.Fast));if err!=nil{t.Fatal(err)}
  a:=createAssociationFromConfigWithTsn(cfg,1);a.useForwardTSN=true;a.setState(established);a.cumulativeTSNAckPoint=0
  stream:=a.createStream(0,false);var kind byte;switch c.Kind{case"reliable":kind=ReliabilityTypeReliable;case"rexmit":kind=ReliabilityTypeRexmit;case"timed":kind=ReliabilityTypeTimed;default:t.Fatal(c.Kind)};stream.SetReliabilityParams(false,kind,c.Value)
  // The actual packetizer builds real fragment head pointers and source buffer
  // debt.3000B fits the actual initial4380B congestion window.
  chunks,_:=stream.packetize(make([]byte,3000),PayloadTypeWebRTCBinary);if len(chunks)!=3{t.Fatal("source packetization",len(chunks))}
  snapshot:=func(stage string,packets[][]byte){
   sent,abandoned,raw,all:=make([]uint32,len(chunks)),make([]bool,len(chunks)),make([]bool,len(chunks)),make([]bool,len(chunks));sizes:=make([]int,len(packets));tsns:=[]uint32{}
   for i,ch:=range chunks{sent[i]=ch.nSent;abandoned[i]=ch.abandoned();head:=ch;if ch.head!=nil{head=ch.head};raw[i]=head._abandoned;all[i]=head._allInflight}
   for i,b:=range packets{sizes[i]=len(b);for off:=12;off<len(b);{if off+4>len(b){t.Fatal("short chunk")};n:=int(binary.BigEndian.Uint16(b[off+2:off+4]));if n<4||off+n>len(b){t.Fatal("bad chunk")};if b[off]==0{if n<16{t.Fatal("short DATA")};tsns=append(tsns,binary.BigEndian.Uint32(b[off+4:off+8]))};off+=(n+3)&^3}}
   if err:=enc.Encode(map[string]any{"id":c.ID,"stage":stage,"sent":sent,"abandoned":abandoned,"raw_head_abandoned":raw,"all_inflight":all,"buffered":stream.BufferedAmount(),"flight_bytes":a.inflightQueue.getNumBytes(),"pending":a.pendingQueue.size(),"packet_sizes":sizes,"transmitted_tsn":tsns});err!=nil{t.Fatal(err)}
  }
  for _,ch:=range chunks{a.pendingQueue.push(ch)}
  for i:=range chunks{a.lock.Lock();a.movePendingDataChunkToInflightQueue(a.pendingQueue.peek());a.lock.Unlock();snapshot([]string{"original-first","original-middle","original-last"}[i],nil)}
  for _,ch:=range chunks{if !chunks[0]._allInflight||ch.nSent!=1{t.Fatal("source full-send prerequisite")};ch.since=time.Now().Add(-2*time.Hour);ch.missIndicator=3}
  a.lock.Lock();a.setCWND(20000);a.setRWND(20000);a.inflightQueue.markAllToRetrasmit();var consumed bool;selected:=a.getDataPacketsToRetransmit(nil,&consumed);packets:=make([][]byte,0,len(selected));for _,packet:=range selected{raw,err:=a.marshalPacket(packet);if err!=nil{t.Fatal(err)};packets=append(packets,raw)};a.lock.Unlock();snapshot("rto",packets)
  a.lock.Lock();err=a.handleSack(&chunkSelectiveAck{cumulativeTSNAck:3,advertisedReceiverWindowCredit:1024*1024});a.lock.Unlock();if err!=nil{t.Fatal(err)};snapshot("sack",nil)
  _=a.close();_=right.Close()
 }
 if err:=scan.Err();err!=nil{t.Fatal(err)}
}
