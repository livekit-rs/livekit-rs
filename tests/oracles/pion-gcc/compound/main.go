// Direct pinned source admission observer. It does not model a fake worker,
// estimator callback, crypto boundary or public WebRTC success.
package main
import("bufio";"encoding/hex";"encoding/json";"fmt";"os";interceptor "github.com/pion/interceptor")
func main(){if len(os.Args)!=2{panic("usage: observer cases.jsonl")};f,e:=os.Open(os.Args[1]);if e!=nil{panic(e)};defer f.Close();s:=bufio.NewScanner(f);enc:=json.NewEncoder(os.Stdout);for s.Scan(){var c struct{ID string `json:"id"`;Hex string `json:"raw_hex"`};if e=json.Unmarshal(s.Bytes(),&c);e!=nil{panic(e)};raw,e:=hex.DecodeString(c.Hex);if e!=nil{panic(e)};packets,e:= (interceptor.Attributes{}).GetRTCPPackets(raw);types:=[]string{};for _,p:=range packets{types=append(types,fmt.Sprintf("%T",p))};message:="";if e!=nil{message=e.Error()};if e=enc.Encode(map[string]any{"id":c.ID,"accepted":e==nil,"error":message,"packets":types,"packet_count":len(packets)});e!=nil{panic(e)}};if e=s.Err();e!=nil{panic(e)}}
