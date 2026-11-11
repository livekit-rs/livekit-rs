// Independent unmodified MTU predicates and real UDP mux binding oracle.
package main
import (
 "encoding/json"
 "net"
 "os"
 "sort"
 rtcconfig "github.com/livekit/mediatransportutil/pkg/rtcconfig"
 mtu "github.com/livekit/mediatransportutil/pkg/transport"
)
type Case struct {ID string `json:"id"`;Includes []string `json:"includes"`;Excludes []string `json:"excludes"`;Names []string `json:"names"`}
func main(){
 if len(os.Args)!=2 {panic("usage: oracle cases.json")};bytes,err:=os.ReadFile(os.Args[1]);if err!=nil {panic(err)};var cases []Case;if err=json.Unmarshal(bytes,&cases);err!=nil {panic(err)};enc:=json.NewEncoder(os.Stdout)
 for _,c:=range cases {filter:=rtcconfig.InterfaceFilterFromConf(rtcconfig.InterfacesConfig{Includes:c.Includes,Excludes:c.Excludes});result:=map[string]bool{};for _,name:=range c.Names {result[name]=filter(name)};if err=enc.Encode(map[string]any{"id":c.ID,"allowed":result});err!=nil {panic(err)}}
 // This section actually binds the original MTU UDP sockets, not a mock Net.
 interfaces,err:=net.Interfaces();if err!=nil {panic(err)};loopback:="";for _,i:=range interfaces {if i.Flags&net.FlagUp!=0&&i.Flags&net.FlagLoopback!=0 {loopback=i.Name;break}};if loopback=="" {panic("up loopback interface required")}
 for _,c:=range []Case{{ID:"actual-loopback-bind",Includes:[]string{loopback}},{ID:"actual-no-interface-selected",Includes:[]string{"f08-no-such-interface"}}} {
  filter:=rtcconfig.InterfaceFilterFromConf(rtcconfig.InterfacesConfig{Includes:c.Includes});muxes,err:=mtu.CreateUDPMuxesFromPorts([]int{0},mtu.UDPMuxFromPortWithInterfaceFilter(filter),mtu.UDPMuxFromPortWithLoopback());result:=map[string]any{"id":c.ID,"interface_name":loopback};if err!=nil {result["error"]=err.Error()}else{hosts:=[]string{};for _,mux:=range muxes {for _,a:=range mux.GetListenAddresses(){host,_,e:=net.SplitHostPort(a.String());if e!=nil {panic(e)};hosts=append(hosts,host)};if e:=mux.Close();e!=nil {panic(e)}};sort.Strings(hosts);result["actual_bound_hosts"]=hosts;result["mux_count"]=len(muxes)};if err=enc.Encode(result);err!=nil {panic(err)}
 }
 for _,includeLoopback:=range []bool{true,false} {
  id:="actual-all-without-loopback"; opts:=[]mtu.UDPMuxFromPortOption{};if includeLoopback{id="actual-all-with-loopback";opts=append(opts,mtu.UDPMuxFromPortWithLoopback())}
  muxes,e:=mtu.CreateUDPMuxesFromPorts([]int{0},opts...);result:=map[string]any{"id":id};if e!=nil{result["error"]=e.Error()}else{hosts:=[]string{};for _,mux:=range muxes{for _,a:=range mux.GetListenAddresses(){host,_,err:=net.SplitHostPort(a.String());if err!=nil{panic(err)};hosts=append(hosts,host)};if err:=mux.Close();err!=nil{panic(err)}};sort.Strings(hosts);result["actual_bound_hosts"]=hosts;result["mux_count"]=len(muxes)};if err=enc.Encode(result);err!=nil{panic(err)}
 }

}
