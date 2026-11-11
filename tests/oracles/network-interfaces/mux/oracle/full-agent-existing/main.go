// Execute the fixed full Pion Agent gather path, not GetConn in isolation.
// Source dependency selection is inherited from ../go.mod. No expected rows
// are precomputed here; callbacks and actual mux registrations are observed.
package main

import (
 "encoding/json"
 "fmt"
 "net"
 "os"
 "sync"
 "time"

 ice "github.com/pion/ice/v4"
)

type udpCalls struct { ice.UDPMux; mu sync.Mutex; calls []string }
func (m *udpCalls) GetConn(u string, a net.Addr) (net.PacketConn,error) {
 m.mu.Lock();m.calls=append(m.calls,a.String());m.mu.Unlock()
 return m.UDPMux.GetConn(u,a)
}
type tcpCalls struct { ice.TCPMux; listener net.Listener; mu sync.Mutex; calls []string }
func(m *tcpCalls) LocalAddr() net.Addr {return m.listener.Addr()}
func(m *tcpCalls) GetConnByUfrag(u string,v6 bool,local net.IP)(net.PacketConn,error){
 m.mu.Lock();m.calls=append(m.calls,local.String());m.mu.Unlock()
 return m.TCPMux.GetConnByUfrag(u,v6,local)
}
func gather(opts ...ice.AgentOption)([]string,error){
 a,e:=ice.NewAgentWithOptions(opts...);if e!=nil{return nil,e};defer a.Close()
 done:=make(chan struct{});var mu sync.Mutex;var candidates []string
 if e=a.OnCandidate(func(c ice.Candidate){if c==nil{close(done);return};mu.Lock();candidates=append(candidates,c.Marshal());mu.Unlock()});e!=nil{return nil,e}
 if e=a.GatherCandidates();e!=nil{return nil,e}
 select{case <-done:case <-time.After(5*time.Second):return nil,fmt.Errorf("gather did not complete in 5s")}
 mu.Lock();defer mu.Unlock();return append([]string(nil),candidates...),nil
}
func main(){
 if len(os.Args)!=2 {panic("usage: full-agent-existing udp-existing|tcp-wildcard")}
 profile:=os.Args[1];if profile!="udp-existing"&&profile!="tcp-wildcard" {panic("unknown profile")}
 enc:=json.NewEncoder(os.Stdout)
 if profile=="udp-existing" {
 // Use two existing host interfaces, without changing OS network settings.
 firstIP:=net.ParseIP("127.0.0.1")
 var secondIP net.IP
 devices,e:=net.Interfaces();if e!=nil{panic(e)}
 for _,device:=range devices { if device.Flags&net.FlagUp==0||device.Flags&net.FlagLoopback!=0{continue};addrs,e:=device.Addrs();if e!=nil{panic(e)};for _,addr:=range addrs {ip,_,e:=net.ParseCIDR(addr.String());if e==nil&&ip.To4()!=nil&&!ip.IsUnspecified()&&!ip.IsMulticast()&&!ip.IsLoopback() {secondIP=ip;break}};if secondIP!=nil{break} }
 if secondIP==nil {panic("existing UP non-loopback IPv4 interface required")}

 // Two actual bound sockets on existing interfaces, same rewritten public address and port.
 // Missing inventory or bind failure fails the profile; it never reports skipped success.
 first,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:firstIP});if e!=nil{panic(e)}
 second,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:secondIP,Port:first.LocalAddr().(*net.UDPAddr).Port});if e!=nil{first.Close();panic(e)}
 raw:=ice.NewMultiUDPMuxDefault(ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn:first}),ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn:second}))
 mux:=&udpCalls{UDPMux:raw}
 cs,e:=gather(ice.WithUDPMux(mux),ice.WithNetworkTypes([]ice.NetworkType{ice.NetworkTypeUDP4}),ice.WithCandidateTypes([]ice.CandidateType{ice.CandidateTypeHost}),ice.WithMulticastDNSMode(ice.MulticastDNSModeDisabled),ice.WithIncludeLoopback(),ice.WithAddressRewriteRules(ice.AddressRewriteRule{External:[]string{"198.51.100.8"},AsCandidateType:ice.CandidateTypeHost}))
 mux.Close();if e!=nil{panic(e)}
 if e=enc.Encode(map[string]any{"id":"full-agent-rewritten-udp-existing-two-bases","candidates":cs,"registered_bases":mux.calls,"bound_bases":[]string{first.LocalAddr().String(),second.LocalAddr().String()}});e!=nil{panic(e)}
 return
 }
 // A real wildcard TCP listener. Agent selects actual interface addresses;
 // 0.0.0.0 is never supplied as the candidate's physical base to GetConn.
 listener,e:=net.Listen("tcp4","0.0.0.0:0");if e!=nil{panic(e)}
 rawTCP:=ice.NewTCPMuxDefault(ice.TCPMuxParams{Listener:listener})
 tm:=&tcpCalls{TCPMux:rawTCP,listener:listener}
 cs,e:=gather(ice.WithTCPMux(tm),ice.WithNetworkTypes([]ice.NetworkType{ice.NetworkTypeTCP4}),ice.WithCandidateTypes([]ice.CandidateType{ice.CandidateTypeHost}),ice.WithMulticastDNSMode(ice.MulticastDNSModeDisabled),ice.WithIncludeLoopback(),ice.WithIPFilter(func(ip net.IP)bool{return ip.Equal(net.ParseIP("127.0.0.1"))}))
 tm.Close();if e!=nil{panic(e)}
 if e=enc.Encode(map[string]any{"id":"full-agent-wildcard-tcp-concrete-loopback-base","listener":listener.Addr().String(),"candidates":cs,"registered_bases":tm.calls});e!=nil{panic(e)}
}
