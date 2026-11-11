// SPDX-License-Identifier: MIT
package sctp
import("encoding/binary";"encoding/json";"fmt";"hash/crc32";"math";"net";"os";"testing";"time")
func sourceWireHeartbeat(kind byte,body []byte)[]byte{
 n:=4+len(body);b:=make([]byte,12+((n+3)&^3))
 binary.BigEndian.PutUint16(b[0:2],5000);binary.BigEndian.PutUint16(b[2:4],5000)
 b[12]=kind;binary.BigEndian.PutUint16(b[14:16],uint16(n));copy(b[16:],body)
 binary.LittleEndian.PutUint32(b[8:12],crc32.Checksum(b,castagnoliTable));return b
}
func TestSourceHeartbeatActualWireAdmission(t *testing.T){
 out,e:=os.Create(os.Getenv("GO_SCTP_HEARTBEAT_WIRE_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close();enc:=json.NewEncoder(out)
 const unix int64=1700000000123456789
 param:=func(n uint64)[]byte{b:=make([]byte,12);binary.BigEndian.PutUint16(b,1);binary.BigEndian.PutUint16(b[2:],12);binary.BigEndian.PutUint64(b[4:],n);return b}
 valid:=param(uint64(unix-1234567));wrong:=[]byte{0,7,0,4}
 bodies:=[]struct{id string;body []byte}{
 {"empty",nil},{"valid-timestamp",valid},{"valid-zeros-tail",append(append([]byte{},valid...),0,0,0,0)},
 {"valid-nonzero-tail",append(append([]byte{},valid...),1,0,0,0)},
 {"valid-second-param",append(append([]byte{},valid...),param(uint64(unix-177777))...)},
 {"wrong-first",wrong},{"short-body-one",[]byte{0}},{"short-body-three",[]byte{0,1,0}},
 {"param-length-three",[]byte{0,1,0,3}},{"param-exceeds-body",[]byte{0,1,0,12}},
 {"empty-info",[]byte{0,1,0,4}},{"valid-future",param(uint64(unix+1))},{"valid-unix-zero",param(0)},
 }
 for _,kind:=range []byte{4,5}{for _,row:=range bodies{
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,1);a.lock.Lock();a.setState(established);a.sourcePort=5000;a.destinationPort=5000;sourceRackClock=time.Unix(0,unix)
 wire:=sourceWireHeartbeat(kind,row.body);var direct chunk;if kind==4{direct=&chunkHeartbeat{}}else{direct=&chunkHeartbeatAck{}}
 directErr:=direct.unmarshal(wire[12:]);directParams:=-1;if directErr==nil{if kind==4{directParams=len(direct.(*chunkHeartbeat).params)}else{directParams=len(direct.(*chunkHeartbeatAck).params)}}
 p:=&packet{};packetErr:=p.unmarshal(true,wire);typed:="";if packetErr==nil{for _,c:=range p.chunks{typed+=fmt.Sprintf("%T;",c)}}
 a.lock.Unlock();inboundErr:=a.handleInbound(wire);a.lock.Lock()
 controls:=a.controlQueue.popAll();responses:=[]map[string]any{}
 for _,p:=range controls{b,e:=a.marshalPacket(p);if e!=nil{t.Fatal(e)};responses=append(responses,map[string]any{"bytes":len(b),"type":b[12],"chunk_length":binary.BigEndian.Uint16(b[14:16])})}
 errText:=func(e error)string{if e==nil{return ""};return e.Error()}
 if e=enc.Encode(map[string]any{"id":fmt.Sprintf("%d-%s",kind,row.id),"wire_kind":kind,"wire_bytes":len(wire),"chunk_body_bytes":len(row.body),"direct_chunk_accepted":directErr==nil,"direct_chunk_error":errText(directErr),"direct_params":directParams,"whole_packet_accepted":packetErr==nil,"whole_packet_error":errText(packetErr),"typed":typed,"handle_inbound_error":errText(inboundErr),"source_state":getAssociationStateString(a.getState()),"srtt_bits":fmt.Sprintf("%016x",math.Float64bits(a.SRTT())),"rtt_samples":a.rack.rackMinRTTWnd.Len(),"responses":responses});e!=nil{t.Fatal(e)}
 a.lock.Unlock();_=a.close();_=right.Close()
 }}
}
