// Direct fixed Pion real-socket observer. No copied cache/close algorithm.
// It records actual operations; no authored Rust expected values are embedded.
package main
import("encoding/binary";"encoding/json";"errors";"io";"net";"os";"time";ice "github.com/pion/ice/v4")
func must(e error){if e!=nil{panic(e)}}
func stun(user string)[]byte{p:=(len(user)+3)&^3;b:=make([]byte,24+p);b[1]=1;binary.BigEndian.PutUint16(b[2:4],uint16(4+p));copy(b[4:8],[]byte{0x21,0x12,0xa4,0x42});for i:=8;i<20;i++{b[i]=7};binary.BigEndian.PutUint16(b[20:22],6);binary.BigEndian.PutUint16(b[22:24],uint16(len(user)));copy(b[24:],user);return b}
func read(c net.PacketConn,wait time.Duration)(map[string]any,net.Addr){must(c.SetReadDeadline(time.Now().Add(wait)));buf:=make([]byte,2048);n,a,e:=c.ReadFrom(buf);m:=map[string]any{"bytes":n,"error":"","timeout":false};if e!=nil{m["error"]=e.Error();var ne net.Error;m["timeout"]=errors.As(e,&ne)&&ne.Timeout()}else{m["payload_hex"]=hex(buf[:n])};return m,a}
func hex(b []byte)string{const d="0123456789abcdef";v:=make([]byte,len(b)*2);for i,x:=range b{v[2*i]=d[x>>4];v[2*i+1]=d[x&15]};return string(v)}
func emit(id string,v map[string]any){v["id"]=id;must(json.NewEncoder(os.Stdout).Encode(v))}
func udp(){
 raw,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:net.IPv4(127,0,0,1)});must(e);mux:=ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn:raw});defer mux.Close()
 a,e:=mux.GetConn("owner-a",raw.LocalAddr());must(e);defer a.Close();b,e:=mux.GetConn("owner-b",raw.LocalAddr());must(e);defer b.Close();peer,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:net.IPv4(127,0,0,1)});must(e);defer peer.Close()
 _,e=peer.WriteTo(stun("owner-a:peer"),raw.LocalAddr());must(e);r,_:=read(a,2*time.Second);emit("udp-initial-stun",r)
 _,e=peer.WriteTo([]byte("before-any-owner-write"),raw.LocalAddr());must(e);r,_=read(a,100*time.Millisecond);emit("udp-before-write-nonstun",r)
 _,e=a.WriteTo([]byte("actual-a-write"),peer.LocalAddr());must(e);r,_=read(peer,2*time.Second);emit("udp-actual-a-write",r)
 _,e=peer.WriteTo(stun("owner-b:peer"),raw.LocalAddr());must(e);r,_=read(a,2*time.Second);emit("udp-cached-a-beats-b-username",r);r,_=read(b,100*time.Millisecond);emit("udp-b-not-selected-by-incoming-username",r)
 _,e=b.WriteTo([]byte("actual-b-write"),peer.LocalAddr());must(e);r,_=read(peer,2*time.Second);emit("udp-actual-b-write",r)
 _,e=peer.WriteTo([]byte("after-b-write"),raw.LocalAddr());must(e);r,_=read(b,2*time.Second);emit("udp-b-registered-after-write",r)
 must(a.Close());_,e=peer.WriteTo([]byte("old-a-close-cannot-remove-b"),raw.LocalAddr());must(e);r,_=read(b,2*time.Second);emit("udp-close-old-owner-keeps-new",r)
 must(b.Close());fresh,e:=mux.GetConn("owner-b",raw.LocalAddr());must(e);defer fresh.Close();_,e=b.WriteTo([]byte("stale"),peer.LocalAddr());emit("udp-retired-write",map[string]any{"error":func()string{if e!=nil{return e.Error()};return ""}(),"closed_pipe":errors.Is(e,io.ErrClosedPipe)})
 _,e=peer.WriteTo([]byte("new-owner-needs-own-write"),raw.LocalAddr());must(e);r,_=read(fresh,100*time.Millisecond);emit("udp-replacement-not-pre-registered",r)
}
func tcp(){
 listener,e:=net.ListenTCP("tcp4",&net.TCPAddr{IP:net.IPv4(127,0,0,1)});must(e);mux:=ice.NewTCPMuxDefault(ice.TCPMuxParams{Listener:listener,ReadBufferSize:32,WriteBufferSize:0});defer mux.Close();conn,e:=mux.GetConnByUfrag("tcp-owner",false,net.IPv4(127,0,0,1));must(e);defer conn.Close()
 peer,e:=net.DialTCP("tcp4",nil,listener.Addr().(*net.TCPAddr));must(e);defer peer.Close();body:=stun("tcp-owner:peer");frame:=make([]byte,2+len(body));binary.BigEndian.PutUint16(frame[:2],uint16(len(body)));copy(frame[2:],body);_,e=peer.Write(frame);must(e);r,_:=read(conn,2*time.Second);emit("tcp-initial-owner-frame",r)
 mux.RemoveConnByUfrag("tcp-owner");must(peer.SetReadDeadline(time.Now().Add(2*time.Second)));buf:=make([]byte,1);n,e:=peer.Read(buf);var ne net.Error;emit("tcp-owner-removal-closes-physical-peer",map[string]any{"bytes":n,"closed":e!=nil&&!(errors.As(e,&ne)&&ne.Timeout()),"timeout":errors.As(e,&ne)&&ne.Timeout(),"eof":errors.Is(e,io.EOF)})
 _,e=conn.WriteTo([]byte("stale"),peer.LocalAddr());emit("tcp-retired-write",map[string]any{"closed_pipe":errors.Is(e,io.ErrClosedPipe),"error":func()string{if e!=nil{return e.Error()};return ""}()})
}
func main(){udp();tcp()}
