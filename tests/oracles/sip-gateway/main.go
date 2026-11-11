// Independent oracle invokes fixed unmodified protocol validators and mapper.
package main
import (
 "encoding/base64"
 "encoding/json"
 "fmt"
 "os"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/rpc"
 "google.golang.org/protobuf/encoding/protojson"
 "google.golang.org/protobuf/proto"
)
type Case struct { ID string `json:"id"`; Op string `json:"op"`; Request json.RawMessage `json:"request"`; Trunk json.RawMessage `json:"trunk,omitempty"` }
type Result struct { ID string `json:"id"`; Error string `json:"error,omitempty"`; Wire string `json:"wire_base64,omitempty"`; Type string `json:"type,omitempty"` }
func main() {
 if len(os.Args)!=2 { panic("usage: oracle cases.json") }
 raw,e:=os.ReadFile(os.Args[1]);if e!=nil {panic(e)}
 var cases []Case;if e=json.Unmarshal(raw,&cases);e!=nil {panic(e)}
 enc:=json.NewEncoder(os.Stdout)
 for _,c:=range cases {
  result:=Result{ID:c.ID};var msg proto.Message;var err error
  switch c.Op {
  case "create":
   req:=new(livekit.CreateSIPParticipantRequest);err=protojson.Unmarshal(c.Request,req)
   if err==nil {req.Upgrade();err=req.ValidateResult().Error();msg=req}
  case "transfer":
   req:=new(livekit.TransferSIPParticipantRequest);err=protojson.Unmarshal(c.Request,req)
   if err==nil {err=req.ValidateResult().Error();msg=req}
  case "map":
   req:=new(livekit.CreateSIPParticipantRequest);err=protojson.Unmarshal(c.Request,req)
   var trunk *livekit.SIPOutboundTrunkInfo
   if err==nil && len(c.Trunk)>0 && string(c.Trunk)!="null" {trunk=new(livekit.SIPOutboundTrunkInfo);err=protojson.Unmarshal(c.Trunk,trunk)}
   if err==nil {out,validation:=rpc.NewCreateSIPParticipantRequestResult("","SCL_fixture","from.fixture","","",req,trunk);err=validation.Error();msg=out}
  default: panic("unknown operation")
  }
  if err!=nil {result.Error=err.Error()} else {wire,e:=proto.Marshal(msg);if e!=nil {panic(e)};result.Wire=base64.StdEncoding.EncodeToString(wire);result.Type=string(msg.ProtoReflect().Descriptor().FullName())}
  if e=enc.Encode(result);e!=nil {panic(fmt.Errorf("emit %s: %w",c.ID,e))}
 }
}
