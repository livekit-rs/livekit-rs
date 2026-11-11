package main
import("encoding/json";"os")
func main(){e:=json.NewEncoder(os.Stdout);for _,rtt:=range []int64{-1,0,1,95,4294967295,4294967296,4294967297,9223372036854775807}{v:=map[string]any{"input":rtt,"forward":rtt>0};if rtt>0{v["converted"]=uint32(rtt)};if err:=e.Encode(v);err!=nil{panic(err)}}}
