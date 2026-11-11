// Actual fixed Go PSRPC→xtwirp oracle; this is test tooling, not product runtime.
package main
import (
 "encoding/hex"
 "encoding/json"
 "os"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/utils/xtwirp"
 "github.com/livekit/psrpc"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/types/known/anypb"
)
type Input struct { Code string `json:"code"`; Message string `json:"message"`; Details []*anypb.Any `json:"-"` }
type Detail struct { TypeURL string `json:"type_url"`; ValueHex string `json:"value_hex"` }
type Row struct { ID string `json:"id"`; Code string `json:"rpc_code"`; Message string `json:"rpc_message"`; Details []Detail `json:"details"`; TwirpCode string `json:"twirp_code"`; TwirpMessage string `json:"twirp_message"`; Meta map[string]string `json:"meta"` }
func packed(m proto.Message)*anypb.Any {a,e:=anypb.New(m);if e!=nil{panic(e)};return a}
func main(){
 enc:=json.NewEncoder(os.Stdout)
 emit:=func(id string,in Input){
  err:=psrpc.NewErrorFromResponse(in.Code,in.Message,in.Details...)
  out:=xtwirp.ToError(err)
  r:=Row{ID:id,Code:in.Code,Message:in.Message,Details:[]Detail{},TwirpCode:string(out.Code()),TwirpMessage:out.Msg(),Meta:out.MetaMap()}
  if r.Meta==nil{r.Meta=map[string]string{}}
  for _,a:=range in.Details{r.Details=append(r.Details,Detail{a.TypeUrl,hex.EncodeToString(a.Value)})}
  if e:=enc.Encode(r);e!=nil{panic(e)}
 }
 for _,code:=range []string{"canceled","unknown","invalid_argument","deadline_exceeded","not_found","already_exists","permission_denied","resource_exhausted","failed_precondition","aborted","out_of_range","unimplemented","internal","unavailable","data_loss","unauthenticated","malformed_request","malformed_result","future_code","","upstream_server_error","upstream_client_error","not_acceptable","unprocessable_entity"}{
  emit("code-"+code,Input{Code:code,Message:"synthetic peer failure"})
 }
 status:=&livekit.SIPStatus{Code:livekit.SIPStatusCode(486)}
 text:=&livekit.SIPStatus{Code:livekit.SIPStatusCode(503),Status:"controlled status text"}
 unknownEnum:=&livekit.SIPStatus{Code:livekit.SIPStatusCode(999)}
 transfer:=&livekit.SIPTransferError{TransferId:"public-fixture-transfer",Reason:livekit.SIPTransferReason_STR_REJECTED,SipStatus:status}
 for _,c:=range []struct{id string;details []*anypb.Any}{
  {"sip-status-empty",[]*anypb.Any{packed(status)}},
  {"sip-status-text",[]*anypb.Any{packed(text)}},
  {"sip-status-unknown-enum",[]*anypb.Any{packed(unknownEnum)}},
  {"sip-transfer",[]*anypb.Any{packed(transfer)}},
  {"sip-transfer-empty",[]*anypb.Any{packed(&livekit.SIPTransferError{})}},
  {"sip-transfer-unknown-enum",[]*anypb.Any{packed(&livekit.SIPTransferError{Reason:livekit.SIPTransferReason(999)})}},
  {"multiple-last-wins",[]*anypb.Any{packed(transfer),packed(text)}},
  {"unknown-type",[]*anypb.Any{{TypeUrl:"type.googleapis.com/future.WorkerDetail",Value:[]byte{0xa0,6,0x81,0}}}},
  {"malformed-known",[]*anypb.Any{{TypeUrl:"type.googleapis.com/livekit.SIPStatus",Value:[]byte{0xff}}}},
  {"known-wrong-wire",[]*anypb.Any{{TypeUrl:"type.googleapis.com/livekit.SIPStatus",Value:[]byte{0xa,1,'x'}}}},
  {"short-type-url",[]*anypb.Any{{TypeUrl:"livekit.SIPStatus",Value:packed(status).Value}}},
  {"custom-type-prefix",[]*anypb.Any{{TypeUrl:"controlled.invalid/livekit.SIPStatus",Value:packed(status).Value}}},
  {"malformed-and-valid",[]*anypb.Any{{TypeUrl:"type.googleapis.com/livekit.SIPStatus",Value:[]byte{0xff}},packed(text)}},
 }{emit(c.id,Input{Code:"failed_precondition",Message:"synthetic peer failure",Details:c.details})}
}
