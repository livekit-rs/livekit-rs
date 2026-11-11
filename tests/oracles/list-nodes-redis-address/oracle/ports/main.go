// Independent fixed Go stdlib resolution controls, not a rewritten parser.
package main
import("encoding/json";"net";"os")
func main(){data,err:=os.ReadFile(os.Args[1]);if err!=nil{panic(err)};var rows []struct{ID string `json:"id"`;Address string `json:"address"`};if err=json.Unmarshal(data,&rows);err!=nil{panic(err)};out:=json.NewEncoder(os.Stdout);seen:=map[string]bool{};for _,row:=range rows{if seen[row.ID]{panic("duplicate id")};seen[row.ID]=true;value:=map[string]any{"id":row.ID,"address":row.Address};address,err:=net.ResolveTCPAddr("tcp",row.Address);if err!=nil{value["accepted"]=false;value["error"]="dial tcp: "+err.Error()}else{value["accepted"]=true;value["endpoint"]=address.String();value["port"]=address.Port};if err=out.Encode(value);err!=nil{panic(err)}}}
