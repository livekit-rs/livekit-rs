// Actual fixed MTU predicates and actual OS binds; only interface inventory
// metadata is controlled to reproduce duplicates without mutating host devices.
package main
import("encoding/json";"errors";"net";"os";"syscall";"github.com/pion/transport/v4";"github.com/pion/transport/v4/stdnet";mtu "github.com/livekit/mediatransportutil/pkg/transport";rtc "github.com/livekit/mediatransportutil/pkg/rtcconfig")
type inventory struct{transport.Net; devices []*transport.Interface}
func(n inventory)Interfaces()([]*transport.Interface,error){return n.devices,nil}
func main(){enc:=json.NewEncoder(os.Stdout)
 for _,cidr:=range []string{"192.0.2.0/24","::ffff:0:0/96","::ffff:192.0.2.0/120","::ffff:192.0.2.9/128","::ffff:192.0.2.0/95"}{for _,ip:=range []string{"192.0.2.9","192.0.3.9","::ffff:192.0.2.9","2001:db8::1"}{for _,exclude:=range []bool{false,true}{config:=rtc.IPsConfig{};if exclude{config.Excludes=[]string{cidr}}else{config.Includes=[]string{cidr}};filter,err:=rtc.IPFilterFromConf(config);if err!=nil{panic(err)};if err=enc.Encode(map[string]any{"family":"mapped-cidr","cidr":cidr,"ip":ip,"exclude":exclude,"allowed":filter(net.ParseIP(ip))});err!=nil{panic(err)}}}}
 base,err:=stdnet.NewNet();if err!=nil{panic(err)};devices:=[]*transport.Interface{};for index,name:=range []string{"first","second"}{i:=transport.NewInterface(net.Interface{Index:index+1,Name:name,Flags:net.FlagUp|net.FlagLoopback});i.AddAddress(&net.IPNet{IP:net.ParseIP("127.0.0.1"),Mask:net.CIDRMask(8,32)});devices=append(devices,i)}
 reserved,err:=net.ListenUDP("udp4",&net.UDPAddr{IP:net.IPv4(127,0,0,1)});if err!=nil{panic(err)};port:=reserved.LocalAddr().(*net.UDPAddr).Port;if err=reserved.Close();err!=nil{panic(err)}
 muxes,err:=mtu.CreateUDPMuxesFromPorts([]int{port},mtu.UDPMuxFromPortWithNet(inventory{base,devices}),mtu.UDPMuxFromPortWithLoopback());for _,mux:=range muxes{if e:=mux.Close();e!=nil{panic(e)}}
 row:=map[string]any{"family":"duplicate-occurrence-fixed-port","actual_error":err!=nil,"addr_in_use":errors.Is(err,syscall.EADDRINUSE),"returned_muxes":len(muxes)}
 again,e:=net.ListenUDP("udp4",&net.UDPAddr{IP:net.IPv4(127,0,0,1),Port:port});row["first_socket_released"]=e==nil;if e==nil{if e=again.Close();e!=nil{panic(e)}};if e=enc.Encode(row);e!=nil{panic(e)}
}
