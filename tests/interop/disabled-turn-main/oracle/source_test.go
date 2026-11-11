// SPDX-License-Identifier: Apache-2.0
package service
import("encoding/json";"os";"testing";"github.com/livekit/livekit-server/pkg/config")
func TestActualDisabledTurnFirst(t *testing.T){
 f,e:=os.OpenFile(os.Getenv("DISABLED_TURN_GO_RESULTS"),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 tails:=[]string{"  bind_addresses: [not-an-ip]\n","  tls_port: 70000\n","  udp_port: 70000\n","  allow_restricted_peer_cidrs: [not-a-cidr]\n","  deny_peer_cidrs: [not-a-cidr]\n","  bind_addresses: [not-an-ip]\n  tls_port: 70000\n  udp_port: 70000\n  allow_restricted_peer_cidrs: [not-a-cidr]\n  deny_peer_cidrs: [not-a-cidr]\n"}
 for i,tail:=range tails{
  conf,err:=config.NewConfig("rtc:\n  node_ip: 127.0.0.1\nturn:\n  enabled: false\n"+tail,true,nil,nil);message:="";if err!=nil{message=err.Error()}
  disabled:=false;if err==nil{server,err:=NewTurnServer(conf,nil,true);disabled=server==nil;if err!=nil{message=err.Error()};if server!=nil{server.Close()}}
  if e=out.Encode(map[string]any{"id":i,"input_tail":tail,"error":message,"no_turn_server":disabled});e!=nil{t.Fatal(e)}
 }
}
