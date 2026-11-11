package main
import("context";"fmt";"time";"encoding/hex"
 pb "github.com/livekit/protocol/livekit"
 "github.com/redis/go-redis/v9"
 "google.golang.org/protobuf/proto")
// Legal protobuf encodings whose exact unknown bytes differ after canonical
// DynamicMessage re-encoding. Expectations stay literal and identical for Go/Rust.
func rawUnknowns(ctx context.Context,c pb.SIP,rc *redis.Client,rows *[]row){
 vectors:=[]struct{name string;raw []byte}{
  {"noncanonical-value",[]byte{0xa0,6,0x81,0}},
  {"noncanonical-key",[]byte{0xa0,0x86,0,7}},
  {"noncanonical-length",[]byte{0xba,6,0x81,0,'X'}},
  {"cross-tag-order-and-repeated-tags",[]byte{0xb0,6,3,0xa0,6,1,0xb0,6,4,0xa8,6,2,0xa0,6,5}},
  {"group-noncanonical-child",[]byte{0xc3,6,0x10,0x81,0,0xc4,6}},
  {"known-field-number-wrong-wire",[]byte{0x10,7}},
 }
 for i,v:=range vectors{
  expected:=v.raw
  // Fixed generated Go fast unmarshal appends canonical tag then the untouched
  // value bytes: internal/impl/decode.go:226-229. It does NOT preserve a
  // noncanonical unknown tag, unlike noncanonical value/length/group bytes.
  if v.name=="noncanonical-key"{expected=[]byte{0xa0,6,7}}
  info:=&pb.SIPInboundTrunkInfo{Numbers:[]string{fmt.Sprintf("+1202555095%d",i)},Media:&pb.SIPMediaConfig{}}
  info.ProtoReflect().SetUnknown(v.raw);info.Media.ProtoReflect().SetUnknown(v.raw)
  created,e:=c.CreateSIPInboundTrunk(ctx,&pb.CreateSIPInboundTrunkRequest{Trunk:info});if e!=nil{if v.name=="known-field-number-wrong-wire"{for _,stage:=range []string{"create","get","patch","disk"}{*rows=append(*rows,row{Case:"raw-"+v.name+"-"+stage,Source:"actual-create-rejection-blocked-later-operations",RootSHA:sha(nil),MediaSHA:sha(nil),RootPreserved:false,MediaPreserved:false})};continue};panicCall("raw unknown Create failed",e)};id:=created.SipTrunkId;defer c.DeleteSIPTrunk(ctx,&pb.DeleteSIPTrunkRequest{SipTrunkId:id})
  observeAny(rows,"raw-"+v.name+"-create","actual-protobuf-response",created,expected,expected)
  get,e:=c.GetSIPInboundTrunk(ctx,&pb.GetSIPInboundTrunkRequest{SipTrunkId:id});if e!=nil{panicCall("raw unknown Get failed",e)};observeAny(rows,"raw-"+v.name+"-get","actual-protobuf-response",get.Trunk,expected,expected)
  patched,e:=c.UpdateSIPInboundTrunk(ctx,&pb.UpdateSIPInboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPInboundTrunkRequest_Update{Update:&pb.SIPInboundTrunkUpdate{Name:text("raw-unknown-patch")}}});if e!=nil{panicCall("raw unknown patch failed",e)};observeAny(rows,"raw-"+v.name+"-patch","actual-protobuf-response",patched,expected,expected)
  raw,e:=rc.HGet(ctx,"sip_inbound_trunk",id).Bytes();if e!=nil{panicCall("raw unknown disk read failed",e)};disk:=new(pb.SIPInboundTrunkInfo);if e=proto.Unmarshal(raw,disk);e!=nil{panic(e)};observeAny(rows,"raw-"+v.name+"-disk","actual-redis",disk,expected,expected)
  if _,e=c.DeleteSIPTrunk(ctx,&pb.DeleteSIPTrunkRequest{SipTrunkId:id});e!=nil{panicCall("raw unknown delete failed",e)}
 }
}

// An earlier direct subtree must be discarded when the *same later singular
// message occurrence* switches individual then back to direct. Parsing the later
// occurrence separately and merging only its final oneof state is incorrect.
func duplicateOneof(ctx context.Context,c pb.SIP,rc *redis.Client,rows *[]row){
 id:=fmt.Sprintf("SDR_unknown_oneof_%d",time.Now().UnixNano())
 defer rc.HDel(context.Background(),"sip_dispatch_rule",id)
 prefix,e:=proto.Marshal(&pb.SIPDispatchRuleInfo{SipDispatchRuleId:id});if e!=nil{panic(e)}
 suffix,e:=hex.DecodeString("12050a03a00607120412000a00");if e!=nil{panic(e)}
 raw:=append(prefix,suffix...)
 // Independent fixed generated Go decoding must itself observe the empty final
 // direct node before any HTTP server is used.
 source:=new(pb.SIPDispatchRuleInfo);if e=proto.Unmarshal(raw,source);e!=nil{panic(e)}
 direct:=source.Rule.GetDispatchRuleDirect();if direct==nil{panic("actual Go final direct missing")}
 observeAny(rows,"duplicate-singular-oneof-go-decode","actual-generated-go-decode",direct,nil,nil)
 if e=rc.HSet(ctx,"sip_dispatch_rule",id,raw).Err();e!=nil{panicCall("duplicate oneof seed failed",e)}
 listed,e:=c.ListSIPDispatchRule(ctx,&pb.ListSIPDispatchRuleRequest{DispatchRuleIds:[]string{id}});if e!=nil||len(listed.Items)!=1{panicCall("duplicate oneof List failed",e)}
 observeAny(rows,"duplicate-singular-oneof-list","actual-protobuf-response",listed.Items[0].Rule.GetDispatchRuleDirect(),nil,nil)
 patched,e:=c.UpdateSIPDispatchRule(ctx,&pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId:id,Action:&pb.UpdateSIPDispatchRuleRequest_Update{Update:&pb.SIPDispatchRuleUpdate{Name:text("same-node-switch")}}});if e!=nil{panicCall("duplicate oneof patch failed",e)}
 observeAny(rows,"duplicate-singular-oneof-patch","actual-protobuf-response",patched.Rule.GetDispatchRuleDirect(),nil,nil)
 stored,e:=rc.HGet(ctx,"sip_dispatch_rule",id).Bytes();if e!=nil{panicCall("duplicate oneof disk failed",e)}
 disk:=new(pb.SIPDispatchRuleInfo);if e=proto.Unmarshal(stored,disk);e!=nil{panic(e)}
 observeAny(rows,"duplicate-singular-oneof-disk","actual-redis",disk.Rule.GetDispatchRuleDirect(),nil,nil)
 if _,e=c.DeleteSIPDispatchRule(ctx,&pb.DeleteSIPDispatchRuleRequest{SipDispatchRuleId:id});e!=nil{panicCall("duplicate oneof delete failed",e)}
}
