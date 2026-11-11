// SPDX-License-Identifier: Apache-2.0
package rtc
import (
 "bytes"; "encoding/json"; "io"; "os"; "testing"; "time"
 pb "github.com/livekit/protocol/livekit"
 "github.com/livekit/livekit-server/pkg/rtc/types"
 "github.com/livekit/livekit-server/pkg/sfu/datachannel"
 "github.com/pion/webrtc/v4"
 "google.golang.org/protobuf/proto"
)
// Component observation only: original participant/TM/PCTransport/writer methods
// execute unchanged; raw writer records which selected writer receives the bytes.
// This is not an ICE/SCTP/media fixture; the separate original SDK gate is.
type openKindSink struct { bytes.Buffer }
func(s *openKindSink) Read(p []byte)(int,error){return 0,io.EOF}
func(s *openKindSink) ReadDataChannel(p []byte)(int,bool,error){return 0,false,io.EOF}
func(s *openKindSink) WriteDataChannel(p []byte,_ bool)(int,error){return s.Write(p)}
func(s *openKindSink) SetReadDeadline(time.Time)error{return nil}
func(s *openKindSink) SetWriteDeadline(time.Time)error{return nil}
func(s *openKindSink) Close()error{return nil}
func TestActualOpenDataKindSource(t *testing.T){
 f,e:=os.OpenFile(os.Getenv("OPEN_DATA_KIND_GO_RESULTS"),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 for _,active:=range []bool{true,false}{for _,raw:=range []int32{0,1,99,-1,-2147483648,2147483647}{
  // Known0 JOINING queue is a separate implementation slice, not skipped in a
  // declared common corpus: this finite corpus declares six ACTIVE + five
  // nonreliable JOINING cases at construction.
  if !active&&raw==0{continue}
  p:=newParticipantForTest("open-kind-component");primary:=p.TransportManager.getTransport(true)
  r,l:=&openKindSink{},&openKindSink{}
  primary.lock.Lock();primary.params.DatachannelSlowThreshold=0;primary.params.DataChannelMaxBufferedAmount=0
  primary.reliableDC=datachannel.NewDataChannelWriterReliable[*webrtc.DataChannel](nil,r,0)
  primary.lossyDC=datachannel.NewDataChannelWriterUnreliable[*webrtc.DataChannel](nil,l,0,0);primary.lock.Unlock()
  p.reliableDataInfo.canWriteReliable=true
  state:="active";if !active{state="joining";p.state.Store(pb.ParticipantInfo_JOINING)}
  packet:=&pb.DataPacket{Kind:pb.DataPacket_Kind(raw),Value:&pb.DataPacket_User{User:&pb.UserPacket{Payload:[]byte("actual-api-body")}}};wire,e:=proto.Marshal(packet);if e!=nil{t.Fatal(e)}
  err:=p.SendDataMessage(pb.DataPacket_Kind(raw),wire,"",0);message:="";if err!=nil{message=err.Error()}
  channel:="none";body:=[]byte(nil);if r.Len()>0{channel="_reliable";body=r.Bytes()};if l.Len()>0{if channel!="none"{t.Fatal("duplicate writer")};channel="_lossy";body=l.Bytes()}
  observed:=int32(0);if body!=nil{got:=&pb.DataPacket{};if e=proto.Unmarshal(body,got);e!=nil{t.Fatal(e)};observed=int32(got.Kind);if !bytes.Equal(body,wire){t.Fatal("writer changed body")}}
  if e=out.Encode(map[string]any{"id":state+"-"+pb.DataPacket_Kind(raw).String(),"state":state,"raw_kind":raw,"channel":channel,"wire_kind":observed,"written":body!=nil,"queued":len(p.reliableDataInfo.joiningUnsequencedMessages),"error":message});e!=nil{t.Fatal(e)}
  if e=p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false);e!=nil{t.Fatal(e)}
 }}
}
