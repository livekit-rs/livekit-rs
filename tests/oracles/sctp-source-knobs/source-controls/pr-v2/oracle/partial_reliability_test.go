// SPDX-License-Identifier: MIT
// Independent observer of unmodified github.com/pion/sctp v1.12.0 package methods.
package sctp

import (
 "bufio"
 "encoding/binary"
 "encoding/json"
 "net"
 "os"
 "testing"
 "time"
)

func TestSourceFastPartialReliabilityObserver(t *testing.T) {
 input, err := os.Open(os.Getenv("GO_SCTP_PR_INPUTS")); if err != nil { t.Fatal(err) }; defer input.Close()
 output, err := os.Create(os.Getenv("GO_SCTP_PR_OUTPUT")); if err != nil { t.Fatal(err) }; defer output.Close()
 scan, enc := bufio.NewScanner(input), json.NewEncoder(output)
 for scan.Scan() {
  var c struct { ID,Kind string; Value,Fast uint32 }
  if err := json.Unmarshal(scan.Bytes(), &c); err != nil { t.Fatal(err) }
  left,right := net.Pipe()
  cfg,err := buildClientConfig(WithNetConn(left), WithMTU(1228), WithFastRtxWnd(c.Fast)); if err != nil { t.Fatal(err) }
  a := createAssociationFromConfigWithTsn(cfg,1)
  a.useForwardTSN=true
  stream := a.createStream(0,false)
  var kind byte
  switch c.Kind { case "reliable": kind=ReliabilityTypeReliable; case "rexmit": kind=ReliabilityTypeRexmit; case "timed": kind=ReliabilityTypeTimed; default: t.Fatal(c.Kind) }
  stream.SetReliabilityParams(false,kind,c.Value)
  a.cumulativeTSNAckPoint=0
  // Old send timestamp makes the distinction between original deadline and
  // source fast-RTX resetting `since` observable. No observer-controlled clock.
  initialAbandoned:=make([]bool,5)
  for i:=uint32(1);i<=5;i++ {
   chunk:=&chunkPayloadData{beginningFragment:true,endingFragment:true,userData:make([]byte,500),streamIdentifier:0,payloadType:PayloadTypeWebRTCBinary}
   a.pendingQueue.push(chunk)
   // This real source send transition sets allInflight, assigns TSN/nSent,
   // performs initial PR evaluation and registers the actual inflight chunk.
   a.movePendingDataChunkToInflightQueue(a.pendingQueue.peek())
   if chunk.tsn!=i || !chunk._allInflight || chunk.nSent!=1 {t.Fatal("source send prerequisite",chunk.tsn,chunk._allInflight,chunk.nSent)}
   initialAbandoned[i-1]=chunk.abandoned()
   chunk.missIndicator=3
   chunk.since=time.Now().Add(-2*time.Hour)
  }
  a.willRetransmitFast=true
  var consumed bool
  packets:=a.gatherOutboundFastRetransmissionPackets(nil,nil,&consumed)
  sizes:=make([]int,len(packets)); transmitted:=[]uint32{}
  for i,b:=range packets {
   sizes[i]=len(b)
   for off:=12; off<len(b); {
    if off+4>len(b) {t.Fatal("short SCTP chunk header")}; n:=int(binary.BigEndian.Uint16(b[off+2:off+4])); if n<4 || off+n>len(b) {t.Fatal("bad SCTP chunk length")}
    if b[off]==0 {if n<16 {t.Fatal("short DATA chunk")}; transmitted=append(transmitted,binary.BigEndian.Uint32(b[off+4:off+8]))};off+=(n+3)&^3
   }
  }
  sent,abandoned:=make([]uint32,5),make([]bool,5)
  rawAbandoned,allInflight:=make([]bool,5),make([]bool,5)
  for i:=uint32(1);i<=5;i++ { ch,ok:=a.inflightQueue.get(i); if !ok { t.Fatal("missing inflight",i) };sent[i-1]=ch.nSent;abandoned[i-1]=ch.abandoned();rawAbandoned[i-1]=ch._abandoned;allInflight[i-1]=ch._allInflight }
  if err:=enc.Encode(map[string]any{"id":c.ID,"kind":c.Kind,"value":c.Value,"fast":c.Fast,"sent":sent,"abandoned":abandoned,"packet_sizes":sizes,"transmitted_tsn":transmitted,"initial_abandoned":initialAbandoned,"raw_abandoned":rawAbandoned,"all_inflight":allInflight});err!=nil {t.Fatal(err)}
  _=a.close();_=right.Close()
 }
 if err:=scan.Err();err!=nil { t.Fatal(err) }
}
