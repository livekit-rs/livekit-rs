// Independent fixed-Go component oracle. No Rust-generated expected values.
package main
import (
 "encoding/base64"
 "encoding/json"
 "os"
 "github.com/livekit/livekit-server/pkg/rtc"
 "github.com/livekit/livekit-server/pkg/config"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 "google.golang.org/protobuf/proto"
)
type Case struct { ID string `json:"id"`; Left string `json:"left"`; Right string `json:"right"`; BlobUnknown string `json:"blob_unknown"` }
type Result struct { ID string `json:"id"`; LeftValid bool `json:"left_valid"`; LeftInvalidMessage string `json:"left_invalid_message"`; Equal bool `json:"equal"`; Get bool `json:"get"`; LeftText string `json:"left_text"`; RightText string `json:"right_text"`; StoreResponse string `json:"store_response"`; GetResponse string `json:"get_response"` }
func raw(s string) []byte { b,e:=base64.StdEncoding.DecodeString(s);if e!=nil {panic(e)};return b }
func wire(m proto.Message) string {b,e:=proto.Marshal(m);if e!=nil {panic(e)};return base64.StdEncoding.EncodeToString(b)}
func main() {
 if len(os.Args)!=2 {panic("oracle cases.json")};b,e:=os.ReadFile(os.Args[1]);if e!=nil {panic(e)};var cases []Case;if e=json.Unmarshal(b,&cases);e!=nil {panic(e)}
 enc:=json.NewEncoder(os.Stdout)
 for _,c:=range cases {
  a,b:=new(livekit.DataBlobKey),new(livekit.DataBlobKey)
  if e=proto.Unmarshal(raw(c.Left),a);e!=nil {panic(e)};if e=proto.Unmarshal(raw(c.Right),b);e!=nil {panic(e)}
  state:=rtc.NewParticipantDataBlob(rtc.ParticipantDataBlobParams{Logger:logger.GetLogger()})
  blob:=&livekit.DataBlob{Key:a,Contents:[]byte{0,255,1}}
  blob.ProtoReflect().SetUnknown(raw(c.BlobUnknown));state.Add(blob)
  got:=state.Get(b);result:=Result{ID:c.ID,Equal:a.String()==b.String(),Get:got!=nil,LeftText:a.String(),RightText:b.String()}
  limits:=config.LimitConfig{MaxDataBlobKeyLength:256,MaxDataTrackCustomEncodingLength:32}
  result.LeftValid=config.DataBlobKeyLength(a)>0 && limits.CheckDataBlobKeyLength(a) && limits.CheckDataTrackSchemaID(a.GetSchemaId())
  if config.DataBlobKeyLength(a)==0 || !limits.CheckDataBlobKeyLength(a) {result.LeftInvalidMessage="data blob is invalid"} else if !limits.CheckDataTrackSchemaID(a.GetSchemaId()) {result.LeftInvalidMessage="encoding identifier is empty or exceeds the maximum length"}
  result.StoreResponse=wire(&livekit.SignalResponse{Message:&livekit.SignalResponse_StoreDataBlobResponse{StoreDataBlobResponse:&livekit.StoreDataBlobResponse{RequestId:71,Key:a}}})
  if got!=nil {result.GetResponse=wire(&livekit.SignalResponse{Message:&livekit.SignalResponse_GetDataBlobResponse{GetDataBlobResponse:&livekit.GetDataBlobResponse{RequestId:72,Blob:got}}})}
  if e=enc.Encode(result);e!=nil {panic(e)}
 }
}
