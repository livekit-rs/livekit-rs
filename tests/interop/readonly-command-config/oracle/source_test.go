// SPDX-License-Identifier: Apache-2.0
package config
import("encoding/json";"os";"testing")
func TestActualReadOnlyGetConfigAdmission(t *testing.T){
 input,e:=os.ReadFile(os.Getenv("READONLY_COMMAND_CASES"));if e!=nil{t.Fatal(e)};var rows []struct{ID string `json:"id"`;Body string `json:"body"`};if e=json.Unmarshal(input,&rows);e!=nil{t.Fatal(e)}
 output,e:=os.OpenFile(os.Getenv("READONLY_COMMAND_GO_RESULTS"),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{t.Fatal(e)};defer output.Close();out:=json.NewEncoder(output);seen:=map[string]bool{}
 for _,row:=range rows{if seen[row.ID]{t.Fatal("duplicate case")};seen[row.ID]=true;conf,err:=NewConfig(row.Body,true,nil,nil);message:="";value:=map[string]any{};if err!=nil{message=err.Error()}else{value=map[string]any{"port":conf.Port,"bind_addresses":conf.BindAddresses,"tcp_port":conf.RTC.TCPPort,"udp_start":conf.RTC.UDPPort.Start,"udp_end":conf.RTC.UDPPort.End,"ip_includes":conf.RTC.IPs.Includes,"turn_servers":conf.RTC.TURNServers}}
 if e=out.Encode(map[string]any{"id":row.ID,"accepted":err==nil,"error":message,"official":value});e!=nil{t.Fatal(e)}}
 if len(seen)!=12{t.Fatal("complete12 required")}
}
