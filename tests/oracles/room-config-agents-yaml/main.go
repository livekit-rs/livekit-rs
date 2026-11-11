// Development source oracle only. Root executes with the locked official server
// module replacement; this never imports or rewrites the Rust implementation.
package main
import (
 "bufio"
 "encoding/json"
 "fmt"
 "os"
 "github.com/livekit/livekit-server/pkg/config"
 "google.golang.org/protobuf/encoding/protojson"
)
type Input struct{Name string `json:"name"`; YAML string `json:"yaml"`; Strict bool `json:"strict"`}
type Output struct{Name string `json:"name"`; Panic string `json:"panic,omitempty"`; Error string `json:"error,omitempty"`; Configuration json.RawMessage `json:"configuration,omitempty"`; AgentsNil *bool `json:"agents_nil,omitempty"`; AgentCount int `json:"agent_count"`; AgentNullIndexes []int `json:"agent_null_indexes"`; Presence bool `json:"presence"`}
func one(in Input)(out Output){
 out.Name=in.Name
 defer func(){if e:=recover();e!=nil{out.Panic=fmt.Sprint(e)}}()
 conf,err:=config.NewConfig(in.YAML,in.Strict,nil,nil);if err!=nil{out.Error=err.Error();return}
 preset:=conf.Room.RoomConfigurations["record"];if preset==nil{return};out.Presence=true; nilAgents:=preset.Agents==nil; out.AgentsNil=&nilAgents; out.AgentCount=len(preset.Agents); out.AgentNullIndexes=[]int{}; for i,v:=range preset.Agents {if v==nil {out.AgentNullIndexes=append(out.AgentNullIndexes,i)}}
 if preset!=nil {b,err:=protojson.MarshalOptions{UseProtoNames:true,EmitUnpopulated:true}.Marshal(preset);if err!=nil{out.Error=err.Error();return};out.Configuration=b}
 return
}
func main(){scanner:=bufio.NewScanner(os.Stdin);scanner.Buffer(make([]byte,4096),1024*1024);enc:=json.NewEncoder(os.Stdout);for scanner.Scan(){var in Input;if e:=json.Unmarshal(scanner.Bytes(),&in);e!=nil{fmt.Fprintln(os.Stderr,"invalid bounded oracle input");os.Exit(1)};if e:=enc.Encode(one(in));e!=nil{os.Exit(1)}};if scanner.Err()!=nil{os.Exit(1)}}
