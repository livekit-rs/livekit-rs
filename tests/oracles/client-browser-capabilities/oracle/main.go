package main
import("encoding/json";"os";"fmt"
 "github.com/livekit/livekit-server/pkg/rtc"
 "github.com/livekit/livekit-server/pkg/rtc/types"
 pb "github.com/livekit/protocol/livekit")
type input struct{ID string `json:"id"`;Browser string `json:"browser"`;Protocol int32 `json:"protocol"`}
func main(){
 b,e:=os.ReadFile(os.Getenv("CLIENT_BROWSER_CASES"));if e!=nil{panic(e)};var cases []input;if e=json.Unmarshal(b,&cases);e!=nil{panic(e)};if len(cases)!=144{panic("source case count must be144")}
 f,e:=os.OpenFile(os.Getenv("CLIENT_BROWSER_GO_RESULT"),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{panic(e)};defer f.Close();out:=json.NewEncoder(f);seen:=map[string]bool{}
 for _,c:=range cases{if seen[c.ID]||c.ID==""{panic("inventory")};seen[c.ID]=true
  client:=rtc.ClientInfo{ClientInfo:&pb.ClientInfo{Browser:c.Browser,Protocol:c.Protocol}};v:=types.ProtocolVersion(c.Protocol)
  // These are real original public getters. ICE is exactly transport.go414's
  // client gate. Sync is participant.go4161's client gate with SyncStreams=true;
  // neither expression pretends to instantiate a native transport/participant.
  row:=map[string]any{"id":c.ID,"input":c,"audio_red":client.SupportsAudioRED(),"ice_lite":v.SupportsICELite()&&client.SupportsPrflxOverRelay(),"sync_stream_id":v.SupportsSyncStreamID()&&client.SupportsPrflxOverRelay()}
  if e=out.Encode(row);e!=nil{panic(e)}
 };if e=f.Sync();e!=nil{panic(e)};fmt.Fprintf(os.Stderr,"original ClientInfo/protocol rows=%d\n",len(cases))
}
