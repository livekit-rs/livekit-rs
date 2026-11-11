// Original public static manager invokes original Match/script and proto.Merge.
package main
import("encoding/json";"encoding/hex";"os";"fmt"
 cc "github.com/livekit/livekit-server/pkg/clientconfiguration"
 pb "github.com/livekit/protocol/livekit"
 "google.golang.org/protobuf/proto")
type input struct { ID string `json:"id"`; Browser string `json:"browser"`; Version string `json:"version"`; OS string `json:"os"`; Model string `json:"model"` }
func main(){
 raw,e:=os.ReadFile(os.Getenv("STATIC_CLIENT_CONFIG_CASES"));if e!=nil{panic(e)};var cases []input;if e=json.Unmarshal(raw,&cases);e!=nil{panic(e)}
 f,e:=os.OpenFile(os.Getenv("STATIC_CLIENT_CONFIG_GO_RESULT"),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{panic(e)};defer f.Close();out:=json.NewEncoder(f)
 manager:=cc.NewStaticClientConfigurationManager(cc.StaticConfigurations);seen:=map[string]bool{}
 for _,c:=range cases{if c.ID==""||seen[c.ID]{panic("case inventory")};seen[c.ID]=true
  config:=manager.GetConfiguration(&pb.ClientInfo{Browser:c.Browser,BrowserVersion:c.Version,Os:c.OS,DeviceModel:c.Model})
  var wire []byte;if config!=nil{wire,e=(proto.MarshalOptions{Deterministic:true}).Marshal(config);if e!=nil{panic(e)}}
  if e=out.Encode(map[string]any{"id":c.ID,"input":c,"config_present":config!=nil,"config_wire_hex":hex.EncodeToString(wire)});e!=nil{panic(e)}
 };if e=f.Sync();e!=nil{panic(e)};fmt.Fprintf(os.Stderr,"original static manager rows=%d\n",len(cases))
}
