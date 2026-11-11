// Actual fixed Go PSRPC Response + Any unmarshal -> PSRPC error -> xtwirp.
// Module path intentionally permits importing the real fixed psrpc/internal.
package main
import (
 "encoding/hex"
 "encoding/json"
 "os"
 "github.com/livekit/psrpc"
 internal "github.com/livekit/psrpc/internal"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/utils/xtwirp"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/encoding/protowire"
 "google.golang.org/protobuf/types/known/anypb"
)
type AnyRow struct {TypeURL string `json:"type_url"`;ValueHex string `json:"value_hex"`;UnknownHex string `json:"unknown_hex"`;CanonicalWrapperHex string `json:"canonical_wrapper_hex"`}
type Result struct {ID string `json:"id"`;InputHex string `json:"response_wire_hex"`;DecodeOK bool `json:"decode_ok"`;DecodeError string `json:"decode_error,omitempty"`;RequestID string `json:"request_id,omitempty"`;ServerID string `json:"server_id,omitempty"`;Code string `json:"rpc_code,omitempty"`;Message string `json:"rpc_message,omitempty"`;Details []AnyRow `json:"details"`;Legacy *AnyRow `json:"legacy_response,omitempty"`;TwirpCode string `json:"twirp_code,omitempty"`;TwirpMessage string `json:"twirp_message,omitempty"`;Meta map[string]string `json:"meta"`}
func hx(s string)[]byte{b,e:=hex.DecodeString(s);if e!=nil{panic(e)};return b}
func marshal(m proto.Message)[]byte{b,e:=proto.Marshal(m);if e!=nil{panic(e)};return b}
func anyBytes(value proto.Message)[]byte{a,e:=anypb.New(value);if e!=nil{panic(e)};return marshal(a)}
func main(){
 status:=&livekit.SIPStatus{Code:livekit.SIPStatusCode(486)}
 base:=anyBytes(status)
 cases:=[]struct{id string;details [][]byte;legacy [][]byte}{
  {"canonical",[][]byte{base},nil},
  {"unknown-noncanonical-value-order",[][]byte{append(append([]byte{},base...),hx("b00603a00681001007")...)},nil},
  {"unknown-noncanonical-key",[][]byte{append(append([]byte{},base...),hx("a0860007")...)},nil},
  {"unknown-noncanonical-length",[][]byte{append(append([]byte{},base...),hx("b20681007a")...)},nil},
  {"unknown-group",[][]byte{append(append([]byte{},base...),hx("a3060807a406")...)},nil},
  {"known-typeurl-wrong-wire",[][]byte{append(append([]byte{},base...),hx("0807")...)},nil},
  {"known-value-wrong-wire",[][]byte{append(append([]byte{},base...),hx("1007")...)},nil},
  {"duplicate-unknown-fields",[][]byte{append(append([]byte{},base...),hx("a00607a00608")...)},nil},
  {"two-wrapper-cohorts",[][]byte{append(append([]byte{},base...),hx("a00607")...),append(append([]byte{},base...),hx("a00608")...)},nil},
  {"unknown-type",[][]byte{append(marshal(&anypb.Any{TypeUrl:"controlled.invalid/future.Detail",Value:hx("a0068100")}),hx("a00607")...)},nil},
  {"malformed-known-payload-not-wrapper",[][]byte{append(marshal(&anypb.Any{TypeUrl:"type.googleapis.com/livekit.SIPStatus",Value:hx("ff")}),hx("a00607")...)},nil},
  {"empty-wrapper-unknown",[][]byte{hx("a0068100")},nil},
  {"malformed-wrapper",[][]byte{hx("ff")},nil},
  {"invalid-utf8-wrapper",[][]byte{hx("0a01ff")},nil},
  {"legacy-any-clear-type",[][]byte{base},[][]byte{hx("0a036f6e6512020807"),hx("0a00")}},
  {"legacy-any-clear-value",[][]byte{base},[][]byte{hx("0a036f6e6512020807"),hx("1200")}},
  {"legacy-any-clear-both",[][]byte{base},[][]byte{hx("0a036f6e6512020807"),hx("0a001200")}},
  {"legacy-any-merge-then-clear",[][]byte{base},[][]byte{hx("0a036f6e6512020807"),hx("0a00"),hx("0a0374776f"),hx("1200")}},
 }
 enc:=json.NewEncoder(os.Stdout)
 for _,c:=range cases{
  // Construct actual Response frame with each original Any wrapper unchanged.
  body:=marshal(&internal.Response{RequestId:"request-fixture",ServerId:"server-fixture",SentAt:123,Code:"failed_precondition",Error:"synthetic peer failure"})
  for _,a:=range c.details{body=protowire.AppendTag(body,8,protowire.BytesType);body=protowire.AppendBytes(body,a)}
  for _,a:=range c.legacy{body=protowire.AppendTag(body,4,protowire.BytesType);body=protowire.AppendBytes(body,a)}
  r:=Result{ID:c.id,InputHex:hex.EncodeToString(body),Details:[]AnyRow{},Meta:map[string]string{}}
  response:=new(internal.Response)
  if e:=proto.Unmarshal(body,response);e!=nil{r.DecodeError=e.Error()}else{
   r.DecodeOK=true;r.RequestID=response.RequestId;r.ServerID=response.ServerId;r.Code=response.Code;r.Message=response.Error
   if a:=response.Response;a!=nil{r.Legacy=&AnyRow{a.TypeUrl,hex.EncodeToString(a.Value),hex.EncodeToString(a.ProtoReflect().GetUnknown()),hex.EncodeToString(marshal(a))}}
   for _,a:=range response.ErrorDetails{r.Details=append(r.Details,AnyRow{a.TypeUrl,hex.EncodeToString(a.Value),hex.EncodeToString(a.ProtoReflect().GetUnknown()),hex.EncodeToString(marshal(a))})}
   out:=xtwirp.ToError(psrpc.NewErrorFromResponse(response.Code,response.Error,response.ErrorDetails...))
   r.TwirpCode=string(out.Code());r.TwirpMessage=out.Msg();r.Meta=out.MetaMap();if r.Meta==nil{r.Meta=map[string]string{}}
  }
  if e:=enc.Encode(r);e!=nil{panic(e)}
 }
}
