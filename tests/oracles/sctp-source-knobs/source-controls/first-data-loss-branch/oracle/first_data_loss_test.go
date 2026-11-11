// SPDX-License-Identifier: MIT
package sctp

import (
 "context"
 "crypto/sha256"
 "encoding/binary"
 "encoding/hex"
 "encoding/json"
 "fmt"
 "net"
 "os"
 "sync"
 "testing"
 "time"
 "github.com/pion/logging"
)

// Observe original source Tracef callsites without modifying SCTP production
// source, queue flags, source clocks, or recovery choices. No formatted log/TSN
// is retained. The embedded original logger handles all unrelated methods.
type recoveryCounts struct {mu sync.Mutex; rackACK,rackTimer,pto uint64}
type recoveryFactory struct {logging.LoggerFactory; counts *recoveryCounts}
type recoveryLogger struct {logging.LeveledLogger; counts *recoveryCounts}
func (f *recoveryFactory) NewLogger(scope string)logging.LeveledLogger{return &recoveryLogger{LeveledLogger:f.LoggerFactory.NewLogger(scope),counts:f.counts}}
func (l *recoveryLogger) Tracef(format string,args ...interface{}){
 l.counts.mu.Lock();defer l.counts.mu.Unlock()
 switch format {
 case "[%s] RACK: mark lost tsn=%d (sent=%v, delivered=%v, reoWnd=%v)":l.counts.rackACK++
 case "[%s] RACK timer: mark lost tsn=%d":l.counts.rackTimer++
 case "[%s] PTO fired: probe tsn=%d":l.counts.pto++
 }
}

// This is a real UDP transport with controlled, explicitly counted packet loss.
// It never fabricates an ACK, changes a DATA payload, calls a selector, or fires
// a timer. Original association goroutines and wall-clock timers do that work.
type firstLossUDP struct {
 *net.UDPConn
 remote *net.UDPAddr
 mu sync.Mutex
 association *Association
 dropUntilT3 bool
 dropped, dataPackets, repeated, forward, gapSacks uint64
 seen map[uint32]uint64
}
func (c *firstLossUDP) Read(p []byte)(int,error){ n,_,e:=c.ReadFromUDP(p);return n,e }
func (c *firstLossUDP) RemoteAddr()net.Addr{return c.remote}
func (c *firstLossUDP) Write(p []byte)(int,error){
 c.mu.Lock()
 data:=false
 for off:=12;off+4<=len(p); {
  size:=int(binary.BigEndian.Uint16(p[off+2:off+4]));if size<4||off+size>len(p){c.mu.Unlock();return 0,fmt.Errorf("invalid emitted SCTP framing")}
  if p[off]==192 {c.forward++}
  if p[off]==3 && size>=16 && binary.BigEndian.Uint16(p[off+12:off+14])>0 {c.gapSacks++}
  if p[off]==0 { if size<16{c.mu.Unlock();return 0,fmt.Errorf("invalid emitted DATA")};data=true;tsn:=binary.BigEndian.Uint32(p[off+4:off+8]);c.seen[tsn]++;if c.seen[tsn]>1{c.repeated++} }
  off+=(size+3)&^3
 }
 if data {c.dataPackets++}
 drop:=data&&c.dropUntilT3&&c.dropped==0
 if drop {c.dropped++;c.mu.Unlock();return len(p),nil}
 c.mu.Unlock();return c.WriteToUDP(p,c.remote)
}
func (c *firstLossUDP) snapshot()(uint64,uint64,uint64){c.mu.Lock();defer c.mu.Unlock();return c.dropped,c.dataPackets,c.repeated}
func TestSourcePhysicalUDPFirstDataLoss(t *testing.T){
 output,e:=os.Create(os.Getenv("GO_SCTP_FIRST_LOSS_OUTPUT"));if e!=nil{t.Fatal(e)};defer output.Close();enc:=json.NewEncoder(output)
 for _,row:=range []struct{id string;bytes int;reliable,loss bool}{
  {"fragmented-max0-first-data-datagram-lost",50*1024,false,true},
  {"fragmented-reliable-first-data-datagram-lost",50*1024,true,true},
 } { t.Run(row.id,func(t *testing.T){
  left,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:net.IPv4(127,0,0,1)});if e!=nil{t.Fatal(e)};defer left.Close()
  right,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:net.IPv4(127,0,0,1)});if e!=nil{t.Fatal(e)};defer right.Close()
  lc:=&firstLossUDP{UDPConn:left,remote:right.LocalAddr().(*net.UDPAddr),seen:map[uint32]uint64{}}
  rc:=&firstLossUDP{UDPConn:right,remote:left.LocalAddr().(*net.UDPAddr),seen:map[uint32]uint64{}}
  // Noninterleaved DATA is the actual native WebRTC association capability.
  // No RTO override: default original timer and actual elapsed wall time.
  type result struct{a *Association;e error};server:=make(chan result,1)
  go func(){a,e:=ServerWithOptions(WithNetConn(rc),WithMTU(1228),WithFastRtxWnd(0),WithEnableInterleaving(false));server<-result{a,e}}()
  recovery:=&recoveryCounts{}
  factory:=&recoveryFactory{LoggerFactory:logging.NewDefaultLoggerFactory(),counts:recovery}
  ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
  a,e:=ClientContext(ctx,WithLoggerFactory(factory),WithNetConn(lc),WithMTU(1228),WithFastRtxWnd(0),WithEnableInterleaving(false));if e!=nil{t.Fatal(e)};defer a.Close()
  var b *Association;select{case r:=<-server:if r.e!=nil{t.Fatal(r.e)};b=r.a;case <-ctx.Done():t.Fatal("real server handshake timeout")};defer b.Close()
  lc.mu.Lock();lc.association=a;lc.dropUntilT3=row.loss;lc.mu.Unlock()
  s,e:=a.OpenStream(0,PayloadTypeWebRTCBinary);if e!=nil{t.Fatal(e)};kind:=byte(ReliabilityTypeRexmit);if row.reliable{kind=ReliabilityTypeReliable};s.SetReliabilityParams(true,kind,0)
  payload:=make([]byte,row.bytes);for i:=range payload{payload[i]=byte(i*31+7)}
  received:=make(chan []byte,1);readErr:=make(chan error,1)
  go func(){stream,e:=b.AcceptStream();if e!=nil{readErr<-e;return};if e=stream.SetReadDeadline(time.Now().Add(12*time.Second));e!=nil{readErr<-e;return};buf:=make([]byte,65536);n,_,e:=stream.ReadSCTP(buf);if e!=nil{readErr<-e;return};received<-buf[:n]}()
  start:=time.Now();n,e:=s.WriteSCTP(payload,PayloadTypeWebRTCBinary);if e!=nil||n!=len(payload){t.Fatalf("actual write %d %v",n,e)}
  var actual []byte;var readFailure string
  select{case actual=<-received:case e:=<-readErr:readFailure=e.Error();case <-ctx.Done():readFailure="outer context deadline"}
  dropped,packets,repeats:=lc.snapshot();want:=sha256.Sum256(payload);got:=sha256.Sum256(actual)
  if len(actual)>0&&(len(actual)!=len(payload)||got!=want){t.Fatal("actual complete message corruption")}
  if !row.loss&&len(actual)!=len(payload){t.Fatal("unlost control did not deliver actual message")}
  if row.loss&&dropped!=1{t.Fatalf("first-datagram loss must drop exactly one actual DATA datagram, got %d",dropped)}
  lc.mu.Lock();forward:=lc.forward;lc.mu.Unlock();rc.mu.Lock();gapSacks:=rc.gapSacks;rc.mu.Unlock()
  a.lock.RLock();pending:=a.pendingQueue.size();flight:=a.inflightQueue.size();a.lock.RUnlock()
  recovery.mu.Lock();rackACK,rackTimer,pto:=recovery.rackACK,recovery.rackTimer,recovery.pto;recovery.mu.Unlock()
  if e=enc.Encode(map[string]any{"id":row.id,"fast_rtx_wnd":0,"mtu":1228,"interleaving":false,"requested_bytes":len(payload),"received_bytes":len(actual),"received_sha256":hex.EncodeToString(got[:]),"expected_payload_sha256":hex.EncodeToString(want[:]),"delivered":len(actual)==len(payload),"actual_t3_timeouts":a.stats.getNumT3Timeouts(),"actual_fast_retransmissions":a.stats.getNumFastRetrans(),"dropped_data_datagrams":dropped,"written_data_datagrams":packets,"repeated_data_tsns":repeats,"read_error":readFailure,"loss_policy":"first_actual_DATA_datagram_only","source_rack_ack_marks":rackACK,"source_rack_timer_marks":rackTimer,"source_pto_marks":pto,"srtt_ms":a.SRTT(),"rto_ms":a.rtoMgr.getRTO(),"forward_tsn_datagrams":forward,"gap_sack_chunks":gapSacks,"pending_chunks_terminal":pending,"flight_chunks_terminal":flight,"elapsed_ms":time.Since(start).Milliseconds()});e!=nil{t.Fatal(e)}
  // Close unblocks AcceptStream/ReadSCTP even for an abandoned single message.
  _=b.Close();_=a.Close();_=left.Close();_=right.Close()
 }) }
}
