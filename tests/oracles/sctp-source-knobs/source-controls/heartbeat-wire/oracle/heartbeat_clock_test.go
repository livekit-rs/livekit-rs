// SPDX-License-Identifier: MIT
package sctp
import("encoding/binary";"fmt";"math";"net";"os";"testing";"time")
var sourceRackClock time.Time
func sourceRackObserverNow()time.Time{return sourceRackClock}
func TestSourceHeartbeatClockAdmissionAndProbeMetadata(t *testing.T){
 out,e:=os.Create(os.Getenv("GO_SCTP_HEARTBEAT_OUTPUT"));if e!=nil{t.Fatal(e)};defer out.Close()
 const unix int64=1700000000123456789
 for _,id:=range []string{"empty","wrong-first","length-seven","length-nine","future","above-i64","u64-max","past-fractional","past-submillisecond","now-zero-rtt","unix-epoch-zero","wrong-first-valid-second","valid-first-wrong-second","active-established","active-cookie-wait","idle-pto"}{
 left,right:=net.Pipe();cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithFastRtxWnd(0));if e!=nil{t.Fatal(e)};a:=createAssociationFromConfigWithTsn(cfg,1);a.lock.Lock();a.setState(established);sourceRackClock=time.Unix(0,unix)
 info:=func(n uint64)param{b:=make([]byte,8);binary.BigEndian.PutUint64(b,n);return &paramHeartbeatInfo{heartbeatInformation:b}}
 bad:=&paramStateCookie{cookie:[]byte{1}};ack:=&chunkHeartbeatAck{}
 switch id{
 case "empty":
 case "wrong-first":ack.params=[]param{bad}
 case "length-seven":ack.params=[]param{&paramHeartbeatInfo{heartbeatInformation:make([]byte,7)}}
 case "length-nine":ack.params=[]param{&paramHeartbeatInfo{heartbeatInformation:make([]byte,9)}}
 case "future":ack.params=[]param{info(uint64(unix+1))}
 case "above-i64":ack.params=[]param{info(uint64(math.MaxInt64)+1)}
 case "u64-max":ack.params=[]param{info(math.MaxUint64)}
 case "past-fractional":ack.params=[]param{info(uint64(unix-1234567))}
 case "past-submillisecond":ack.params=[]param{info(uint64(unix-177777))}
 case "now-zero-rtt":ack.params=[]param{info(uint64(unix))}
 case "unix-epoch-zero":ack.params=[]param{info(0)}
 case "wrong-first-valid-second":ack.params=[]param{bad,info(uint64(unix-1234567))}
 case "valid-first-wrong-second":ack.params=[]param{info(uint64(unix-1234567)),bad}
 case "active-established":a.lock.Unlock();a.ActiveHeartbeat();a.lock.Lock()
 case "active-cookie-wait":a.setState(cookieWait);a.lock.Unlock();a.ActiveHeartbeat();a.lock.Lock()
 case "idle-pto":a.onPTOTimerLocked()
 }
 if len(id)<6||id[:6]!="active" {if id!="idle-pto"{a.handleHeartbeatAck(ack)}}
 wire:="";packets:=a.controlQueue.popAll()
 for _,p:=range packets{b,e:=a.marshalPacket(p);if e!=nil{t.Fatal(e)};ctype,body,ptype,plen:=-1,-1,-1,-1;if len(b)>=16{ctype=int(b[12]);body=int(binary.BigEndian.Uint16(b[14:16]))-4};if len(b)>=20{ptype=int(binary.BigEndian.Uint16(b[16:18]));plen=int(binary.BigEndian.Uint16(b[18:20]))};t.Logf("HB_SOURCE_METADATA id=%s queue=%d len=%d type=%d body=%d paramtype=%d paramlen=%d",id,len(packets),len(b),ctype,body,ptype,plen);if ctype==4&&body>=12&&len(b)>=28{wire=fmt.Sprintf("%016x",binary.BigEndian.Uint64(b[20:28]))}}
 fmt.Fprintf(out,"%s\t%016x\t%d\t%d\t%d\t%s\n",id,math.Float64bits(a.SRTT()),a.rack.rackMinRTTWnd.Len(),a.rack.rackMinRTTWnd.Min(sourceRackClock).Nanoseconds(),len(packets),wire)
 a.lock.Unlock();_=a.close();_=right.Close()
 }
}
