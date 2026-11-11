// Exact HTTP preallocation parser path: webrtc.SessionDescription.Unmarshal,
// not merely sdp.SessionDescription.Unmarshal nor PC.SetRemoteDescription.
package main
import("os";"io";"encoding/json";"github.com/pion/webrtc/v4")
type Case struct{ID string `json:"id"`;SDP string `json:"sdp"`}
func main(){b,e:=io.ReadAll(os.Stdin);if e!=nil{panic(e)};var cases []Case;if e=json.Unmarshal(b,&cases);e!=nil{panic(e)};enc:=json.NewEncoder(os.Stdout);for _,c:=range cases{sd:=&webrtc.SessionDescription{Type:webrtc.SDPTypeOffer,SDP:c.SDP};_,e=sd.Unmarshal();message:="";if e!=nil{message=e.Error()};if e=enc.Encode(map[string]any{"id":c.ID,"accepted":e==nil,"error":message});e!=nil{panic(e)}}}
