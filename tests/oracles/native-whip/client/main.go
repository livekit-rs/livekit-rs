// Actual fixed Go1.26.0 encoding/json and the exact anonymous struct in
// livekit-stable/pkg/service/whipservice.go:145. No expected error literals.
package main
import("bytes";"encoding/json";"os";"io";"fmt")
type Case struct {ID string `json:"id"`;Input string `json:"input"`}
func main(){b,e:=io.ReadAll(os.Stdin);if e!=nil{panic(e)};var cases []Case;if e=json.Unmarshal(b,&cases);e!=nil{panic(e)};enc:=json.NewEncoder(os.Stdout);for _,c:=range cases{var value struct{ClientIP string `json:"clientIp"`;SubscribedParticipantTrackNames map[string][]string `json:"subscribedParticipantTrackNames"`};err:=json.NewDecoder(bytes.NewReader([]byte(c.Input))).Decode(&value);r:=map[string]any{"id":c.ID,"error":""};if err!=nil{r["error"]=err.Error()}else{r["client_ip"]=value.ClientIP;m:=map[string][]string{};for k,v:=range value.SubscribedParticipantTrackNames{if v==nil{v=[]string{}};m[k]=v};r["subscribed"]=m};if e=enc.Encode(r);e!=nil{panic(fmt.Sprint(e))}}}
