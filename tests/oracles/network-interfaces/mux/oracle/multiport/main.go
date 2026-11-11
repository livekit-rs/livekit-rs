// Actual fixed MTU MultiPortsUDPMux + full Pion Agent observer. No copied
// selection algorithm or precomputed observations. Parent owns execution.
package main
import (
 "encoding/json"
 "fmt"
 "net"
 "os"
 "runtime"
 "sync"
 "time"
 transport "github.com/livekit/mediatransportutil/pkg/transport"
 ice "github.com/pion/ice/v4"
)
type calls struct {ice.UDPMux; mu sync.Mutex; conns []net.PacketConn; addresses []net.Addr}
func(m *calls)GetConn(u string,a net.Addr)(net.PacketConn,error){c,e:=m.UDPMux.GetConn(u,a);if e==nil{m.mu.Lock();m.conns=append(m.conns,c);m.addresses=append(m.addresses,a);m.mu.Unlock()};return c,e}
func index(ports []int,p int)int{for i,v:=range ports{if v==p{return i}};panic("unconfigured actual source port")}
func main(){
 if runtime.NumCPU()<2{panic("this genuine two-configured-port profile requires source runtime.NumCPU >= 2")}
 ip:=net.ParseIP("127.0.0.1");first,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:ip});if e!=nil{panic(e)};second,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:ip});if e!=nil{first.Close();panic(e)}
 ports:=[]int{first.LocalAddr().(*net.UDPAddr).Port,second.LocalAddr().(*net.UDPAddr).Port};first.Close();second.Close()
 muxes,e:=transport.CreateUDPMuxesFromPorts(ports,transport.UDPMuxFromPortWithNetworks(ice.NetworkTypeUDP4),transport.UDPMuxFromPortWithLoopback(),transport.UDPMuxFromPortWithIPFilter(func(v net.IP)bool{return v.Equal(ip)}));if e!=nil{panic(e)}
 mux:=&calls{UDPMux:transport.NewMultiPortsUDPMux(muxes,nil)};defer mux.Close()
 peer,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:ip});if e!=nil{panic(e)};defer peer.Close();enc:=json.NewEncoder(os.Stdout)
 for iteration:=0;iteration<4;iteration++{
  mux.mu.Lock();mux.conns=nil;mux.addresses=nil;mux.mu.Unlock()
  a,e:=ice.NewAgentWithOptions(ice.WithUDPMux(mux),ice.WithNetworkTypes([]ice.NetworkType{ice.NetworkTypeUDP4}),ice.WithCandidateTypes([]ice.CandidateType{ice.CandidateTypeHost}),ice.WithMulticastDNSMode(ice.MulticastDNSModeDisabled),ice.WithIncludeLoopback());if e!=nil{panic(e)}
  done:=make(chan struct{});var mu sync.Mutex;var candidates []ice.Candidate
  e=a.OnCandidate(func(c ice.Candidate){if c==nil{close(done);return};mu.Lock();candidates=append(candidates,c);mu.Unlock()});if e!=nil{a.Close();panic(e)}
  if e=a.GatherCandidates();e!=nil{a.Close();panic(e)}
  select{case<-done:case<-time.After(5*time.Second):a.Close();panic("actual full Agent gather timeout")}
  mux.mu.Lock();conns:=append([]net.PacketConn(nil),mux.conns...);addresses:=append([]net.Addr(nil),mux.addresses...);mux.mu.Unlock()
  mu.Lock();cs:=append([]ice.Candidate(nil),candidates...);mu.Unlock()
  if len(conns)!=1||len(cs)!=1{a.Close();panic(fmt.Sprintf("registered=%d candidates=%d",len(conns),len(cs)))}
  body:=[]byte(fmt.Sprintf("actual-multiport-reply-%d",iteration));if _,e=conns[0].WriteTo(body,peer.LocalAddr());e!=nil{a.Close();panic(e)}
  peer.SetReadDeadline(time.Now().Add(2*time.Second));buf:=make([]byte,128);n,source,e:=peer.ReadFromUDP(buf);if e!=nil{a.Close();panic(e)};if string(buf[:n])!=string(body){a.Close();panic("actual reply payload mismatch")}
  addr:=addresses[0].(*net.UDPAddr);if source.Port!=addr.Port||!source.IP.Equal(ip){a.Close();panic("actual physical source mismatch")}
  row:=map[string]any{"id":fmt.Sprintf("multiport-full-agent-%d",iteration),"source_num_cpu":runtime.NumCPU(),"configured_ports_count":len(ports),"bound_muxes_count":len(muxes),"registered_count":len(conns),"candidate_count":len(cs),"selected_configured_port_index":index(ports,addr.Port),"candidate_configured_port_index":index(ports,cs[0].Port()),"actual_reply_configured_port_index":index(ports,source.Port),"actual_reply_payload":string(buf[:n])}
  if e=enc.Encode(row);e!=nil{a.Close();panic(e)};if e=a.Close();e!=nil{panic(e)}
 }
}
