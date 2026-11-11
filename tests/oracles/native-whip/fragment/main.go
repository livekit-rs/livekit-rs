// Actual fixed protocol parser/patcher, not a hand-written expected implementation.
package main
import (
 "encoding/json"
 "os"
 "bufio"
 "fmt"
 "errors"
 "github.com/pion/webrtc/v4"
 lksdp "github.com/livekit/protocol/sdp"
 "github.com/pion/sdp/v3"
)
type Case struct {ID string `json:"id"`; Fragment string `json:"fragment"`; SDP string `json:"sdp"`}
type Row struct {ID string `json:"id"`; Error string `json:"error,omitempty"`; Mid string `json:"mid,omitempty"`; Candidates []string `json:"candidates"`; Marshaled string `json:"marshaled,omitempty"`; Ufrag string `json:"ufrag,omitempty"`; Pwd string `json:"pwd,omitempty"`; CredentialError string `json:"credential_error,omitempty"`; CredentialKind string `json:"credential_kind,omitempty"`; PatchError string `json:"patch_error,omitempty"`; Patched string `json:"patched,omitempty"`; Extracted string `json:"extracted,omitempty"`; ExtractError string `json:"extract_error,omitempty"`}
func main(){
 if len(os.Args)!=2 {panic("cases file required")};b,e:=os.ReadFile(os.Args[1]);if e!=nil{panic(e)};var cases []Case;if e=json.Unmarshal(b,&cases);e!=nil{panic(e)}
 out:=bufio.NewWriter(os.Stdout);defer out.Flush();enc:=json.NewEncoder(out)
 for _,c:=range cases{r:=Row{ID:c.ID};f:=&lksdp.SDPFragment{};if e=f.Unmarshal(c.Fragment);e!=nil{r.Error=e.Error()}else{
 r.Mid=f.Mid();r.Candidates=f.Candidates();r.Marshaled,_=f.Marshal();r.Ufrag,r.Pwd,e=f.ExtractICECredential();if e!=nil{r.CredentialError=e.Error();switch {case errors.Is(e,webrtc.ErrSessionDescriptionMissingIceUfrag):r.CredentialKind="missing_ufrag";case errors.Is(e,webrtc.ErrSessionDescriptionMissingIcePwd):r.CredentialKind="missing_pwd";case errors.Is(e,webrtc.ErrSessionDescriptionConflictingIceUfrag):r.CredentialKind="conflicting_ufrag";case errors.Is(e,webrtc.ErrSessionDescriptionConflictingIcePwd):r.CredentialKind="conflicting_pwd";default:r.CredentialKind="unknown"}}
 if c.SDP!=""{doc:=&sdp.SessionDescription{};if e=doc.Unmarshal([]byte(c.SDP));e!=nil{r.PatchError=fmt.Sprintf("input: %s",e)}else if e=f.PatchICECredentialAndCandidatesIntoSDP(doc);e!=nil{r.PatchError=e.Error()}else{wire,_:=doc.Marshal();r.Patched=string(wire);ex,err:=lksdp.ExtractSDPFragment(doc);if err!=nil{r.ExtractError=err.Error()}else{r.Extracted,_=ex.Marshal()}}}
 };if e=enc.Encode(r);e!=nil{panic(e)}}
}
