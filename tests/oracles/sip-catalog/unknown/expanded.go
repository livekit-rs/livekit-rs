package main

import(
 "context"
 "fmt"
 "time"
 pb "github.com/livekit/protocol/livekit"
 "github.com/redis/go-redis/v9"
 "google.golang.org/protobuf/proto"
)
func observeAny(rows *[]row,name,source string,info proto.Message,wantRoot,wantMedia []byte){
 if info==nil{panic("nil actual observation")};root:=info.ProtoReflect().GetUnknown();var media []byte
 if f:=info.ProtoReflect().Descriptor().Fields().ByName("media");f!=nil&&info.ProtoReflect().Has(f){media=info.ProtoReflect().Get(f).Message().GetUnknown()}
 *rows=append(*rows,row{name,source,sha(root),sha(media),string(root)==string(wantRoot),string(media)==string(wantMedia)})
}
func outbound(ctx context.Context,c pb.SIP,rc *redis.Client,rows *[]row){
 oldRoot,oldMedia:=marker(110,31),marker(111,37)
 info:=&pb.SIPOutboundTrunkInfo{Address:"example.invalid",Numbers:[]string{"+12025550911"},Media:&pb.SIPMediaConfig{}}
 info.ProtoReflect().SetUnknown(oldRoot);info.Media.ProtoReflect().SetUnknown(oldMedia)
 created,e:=c.CreateSIPOutboundTrunk(ctx,&pb.CreateSIPOutboundTrunkRequest{Trunk:info});if e!=nil{panicCall("outbound Create failed",e)};id:=created.SipTrunkId;defer c.DeleteSIPTrunk(ctx,&pb.DeleteSIPTrunkRequest{SipTrunkId:id})
 observeAny(rows,"outbound-create","actual-protobuf-response",created,oldRoot,oldMedia)
 disk:=func(name string,root,media []byte){raw,e:=rc.HGet(ctx,"sip_outbound_trunk",id).Bytes();if e!=nil{panicCall("outbound disk read failed",e)};v:=new(pb.SIPOutboundTrunkInfo);if e=proto.Unmarshal(raw,v);e!=nil{panic(e)};observeAny(rows,name,"actual-redis",v,root,media)}
 get,e:=c.GetSIPOutboundTrunk(ctx,&pb.GetSIPOutboundTrunkRequest{SipTrunkId:id});if e!=nil{panicCall("outbound Get failed",e)};observeAny(rows,"outbound-get","actual-protobuf-response",get.Trunk,oldRoot,oldMedia)
 list,e:=c.ListSIPOutboundTrunk(ctx,&pb.ListSIPOutboundTrunkRequest{TrunkIds:[]string{id}});if e!=nil||len(list.Items)!=1{panicCall("outbound List failed",e)};observeAny(rows,"outbound-list","actual-protobuf-response",list.Items[0],oldRoot,oldMedia)
 for i:=0;i<2;i++{v,e:=c.UpdateSIPOutboundTrunk(ctx,&pb.UpdateSIPOutboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPOutboundTrunkRequest_Update{Update:&pb.SIPOutboundTrunkUpdate{Name:text("")}}});if e!=nil{panicCall("outbound repeated patch failed",e)};observeAny(rows,fmt.Sprintf("outbound-idempotent-patch-%d",i),"actual-protobuf-response",v,oldRoot,oldMedia);disk(fmt.Sprintf("outbound-idempotent-patch-%d",i),oldRoot,oldMedia)}
 replace:=&pb.SIPOutboundTrunkInfo{Address:"example.invalid",Numbers:[]string{"+12025550911"}}
 v,e:=c.UpdateSIPOutboundTrunk(ctx,&pb.UpdateSIPOutboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPOutboundTrunkRequest_Replace{Replace:replace}});if e!=nil{panicCall("outbound empty-unknown Replace failed",e)};observeAny(rows,"outbound-replace-clears-old","actual-protobuf-response",v,nil,nil);disk("outbound-replace-clears-old",nil,nil)
 newRoot,newMedia:=marker(112,41),marker(113,43);replace.Media=&pb.SIPMediaConfig{};replace.ProtoReflect().SetUnknown(newRoot);replace.Media.ProtoReflect().SetUnknown(newMedia)
 v,e=c.UpdateSIPOutboundTrunk(ctx,&pb.UpdateSIPOutboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPOutboundTrunkRequest_Replace{Replace:replace}});if e!=nil{panicCall("outbound own-unknown Replace failed",e)};observeAny(rows,"outbound-replace-owns-new","actual-protobuf-response",v,newRoot,newMedia);disk("outbound-replace-owns-new",newRoot,newMedia)
 nested:=&pb.SIPMediaConfig{};nestedUnknown:=marker(114,47);nested.ProtoReflect().SetUnknown(nestedUnknown)
 v,e=c.UpdateSIPOutboundTrunk(ctx,&pb.UpdateSIPOutboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPOutboundTrunkRequest_Update{Update:&pb.SIPOutboundTrunkUpdate{Media:nested}}});if e!=nil{panicCall("outbound Media patch failed",e)};observeAny(rows,"outbound-explicit-media-replaces-child","actual-protobuf-response",v,newRoot,nestedUnknown);disk("outbound-explicit-media-replaces-child",newRoot,nestedUnknown)
 other,e:=c.CreateSIPOutboundTrunk(ctx,&pb.CreateSIPOutboundTrunkRequest{Trunk:&pb.SIPOutboundTrunkInfo{Address:"example.invalid",Numbers:[]string{"+12025550912"}}});if e!=nil{panicCall("outbound unrelated create failed",e)};defer c.DeleteSIPTrunk(ctx,&pb.DeleteSIPTrunkRequest{SipTrunkId:other.SipTrunkId});observeAny(rows,"outbound-unrelated-request-no-sidecar-leak","actual-protobuf-response",other,nil,nil)
}
func rules(ctx context.Context,c pb.SIP,rc *redis.Client,rows *[]row){
 root,media,ruleUnknown,directUnknown,agent0,agent1:=marker(120,53),marker(121,59),marker(122,61),marker(123,67),marker(124,71),marker(125,73)
 direct:=&pb.SIPDispatchRuleDirect{RoomName:"unknown-proof-room"};direct.ProtoReflect().SetUnknown(directUnknown)
 rule:=&pb.SIPDispatchRule{Rule:&pb.SIPDispatchRule_DispatchRuleDirect{DispatchRuleDirect:direct}};rule.ProtoReflect().SetUnknown(ruleUnknown)
 agents:=[]*pb.RoomAgentDispatch{{AgentName:"one"},{AgentName:"two"}};agents[0].ProtoReflect().SetUnknown(agent0);agents[1].ProtoReflect().SetUnknown(agent1)
 info:=&pb.SIPDispatchRuleInfo{TrunkIds:[]string{fmt.Sprintf("ST_unowned_match_%d",time.Now().UnixNano())},Rule:rule,Media:&pb.SIPMediaConfig{},RoomConfig:&pb.RoomConfiguration{Agents:agents}}
 info.ProtoReflect().SetUnknown(root);info.Media.ProtoReflect().SetUnknown(media)
 created,e:=c.CreateSIPDispatchRule(ctx,&pb.CreateSIPDispatchRuleRequest{DispatchRule:info});if e!=nil{panicCall("rule Create failed",e)};id:=created.SipDispatchRuleId;defer c.DeleteSIPDispatchRule(ctx,&pb.DeleteSIPDispatchRuleRequest{SipDispatchRuleId:id})
 children:=func(name string,v *pb.SIPDispatchRuleInfo){if v.Rule==nil||v.Rule.GetDispatchRuleDirect()==nil||v.RoomConfig==nil||len(v.RoomConfig.Agents)!=2{panic("rule nested known structure lost")};observeAny(rows,name+"-rule","actual-protobuf-node",v.Rule,ruleUnknown,nil);observeAny(rows,name+"-direct","actual-protobuf-node",v.Rule.GetDispatchRuleDirect(),directUnknown,nil);observeAny(rows,name+"-agent0","actual-protobuf-node",v.RoomConfig.Agents[0],agent0,nil);observeAny(rows,name+"-agent1","actual-protobuf-node",v.RoomConfig.Agents[1],agent1,nil)}
 observeAny(rows,"rule-create","actual-protobuf-response",created,root,media);children("rule-create",created)
 listed,e:=c.ListSIPDispatchRule(ctx,&pb.ListSIPDispatchRuleRequest{DispatchRuleIds:[]string{id}});if e!=nil||len(listed.Items)!=1{panicCall("rule List failed",e)};observeAny(rows,"rule-list","actual-protobuf-response",listed.Items[0],root,media);children("rule-list",listed.Items[0])
 for i:=0;i<2;i++{v,e:=c.UpdateSIPDispatchRule(ctx,&pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId:id,Action:&pb.UpdateSIPDispatchRuleRequest_Update{Update:&pb.SIPDispatchRuleUpdate{Name:text("same-patch")}}});if e!=nil{panicCall("rule patch failed",e)};observeAny(rows,fmt.Sprintf("rule-idempotent-patch-%d",i),"actual-protobuf-response",v,root,media);children(fmt.Sprintf("rule-patch-%d",i),v)}
 raw,e:=rc.HGet(ctx,"sip_dispatch_rule",id).Bytes();if e!=nil{panicCall("rule disk read failed",e)};disk:=new(pb.SIPDispatchRuleInfo);if e=proto.Unmarshal(raw,disk);e!=nil{panic(e)};observeAny(rows,"rule-patch-persisted","actual-redis",disk,root,media);children("rule-persisted",disk)
 // Explicit Rule replacement has its own unknowns. Old direct submessage must
 // not survive oneof change; RoomConfig repeated agents remain existing clones.
 newRuleUnknown,newIndividualUnknown:=marker(126,79),marker(127,83)
 individual:=&pb.SIPDispatchRuleIndividual{RoomPrefix:"unknown-prefix"};individual.ProtoReflect().SetUnknown(newIndividualUnknown)
 replacementRule:=&pb.SIPDispatchRule{Rule:&pb.SIPDispatchRule_DispatchRuleIndividual{DispatchRuleIndividual:individual}};replacementRule.ProtoReflect().SetUnknown(newRuleUnknown)
 v,e:=c.UpdateSIPDispatchRule(ctx,&pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId:id,Action:&pb.UpdateSIPDispatchRuleRequest_Update{Update:&pb.SIPDispatchRuleUpdate{Rule:replacementRule}}});if e!=nil{panicCall("rule child replacement failed",e)};observeAny(rows,"rule-patch-explicit-rule","actual-protobuf-response",v,root,media);observeAny(rows,"rule-patch-new-rule-source","actual-protobuf-node",v.Rule,newRuleUnknown,nil);if v.Rule.GetDispatchRuleDirect()!=nil||v.Rule.GetDispatchRuleIndividual()==nil{panic("rule old oneof resurrected")};observeAny(rows,"rule-patch-new-oneof-source","actual-protobuf-node",v.Rule.GetDispatchRuleIndividual(),newIndividualUnknown,nil);observeAny(rows,"rule-patch-keeps-agent0","actual-protobuf-node",v.RoomConfig.Agents[0],agent0,nil)
 replace:=&pb.SIPDispatchRuleInfo{TrunkIds:info.TrunkIds,Rule:&pb.SIPDispatchRule{Rule:&pb.SIPDispatchRule_DispatchRuleDirect{DispatchRuleDirect:&pb.SIPDispatchRuleDirect{RoomName:"replacement-room"}}}}
 v,e=c.UpdateSIPDispatchRule(ctx,&pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId:id,Action:&pb.UpdateSIPDispatchRuleRequest_Replace{Replace:replace}});if e!=nil{panicCall("rule Replace failed",e)};observeAny(rows,"rule-replace-clears-root-and-media","actual-protobuf-response",v,nil,nil);observeAny(rows,"rule-replace-clears-nested-rule","actual-protobuf-node",v.Rule,nil,nil)
 newRoot:=marker(128,89);replace.ProtoReflect().SetUnknown(newRoot);replace.Rule.ProtoReflect().SetUnknown(newRuleUnknown)
 v,e=c.UpdateSIPDispatchRule(ctx,&pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId:id,Action:&pb.UpdateSIPDispatchRuleRequest_Replace{Replace:replace}});if e!=nil{panicCall("rule own-unknown Replace failed",e)};observeAny(rows,"rule-replace-owns-new-root","actual-protobuf-response",v,newRoot,nil);observeAny(rows,"rule-replace-owns-new-child","actual-protobuf-node",v.Rule,newRuleUnknown,nil)
 deleted,e:=c.DeleteSIPDispatchRule(ctx,&pb.DeleteSIPDispatchRuleRequest{SipDispatchRuleId:id});if e!=nil{panicCall("rule Delete failed",e)};observeAny(rows,"rule-delete-returns-unknown","actual-protobuf-response",deleted,newRoot,nil)
}
func legacy(ctx context.Context,c pb.SIP,rc *redis.Client,rows *[]row,modernID string){
 id:=fmt.Sprintf("ST_unknown_legacy_%d",time.Now().UnixNano());defer rc.HDel(context.Background(),"sip_trunk",id);defer rc.HDel(context.Background(),"sip_inbound_trunk",id)
 markerLegacy:=marker(130,97);info:=&pb.SIPTrunkInfo{SipTrunkId:id,OutboundNumber:"+12025550931",OutboundAddress:"example.invalid"};info.ProtoReflect().SetUnknown(markerLegacy)
 data,e:=proto.Marshal(info);if e!=nil{panic(e)};if e=rc.HSet(ctx,"sip_trunk",id,data).Err();e!=nil{panicCall("legacy seed failed",e)}
 in,e:=c.GetSIPInboundTrunk(ctx,&pb.GetSIPInboundTrunkRequest{SipTrunkId:id});if e!=nil{panicCall("legacy inbound Get failed",e)};observeAny(rows,"legacy-to-inbound-drops-source-unknown","actual-protobuf-response",in.Trunk,nil,nil)
 out,e:=c.GetSIPOutboundTrunk(ctx,&pb.GetSIPOutboundTrunkRequest{SipTrunkId:id});if e!=nil{panicCall("legacy outbound Get failed",e)};observeAny(rows,"legacy-to-outbound-drops-source-unknown","actual-protobuf-response",out.Trunk,nil,nil)
 list,e:=c.ListSIPTrunk(ctx,&pb.ListSIPTrunkRequest{});if e!=nil{panicCall("legacy List failed",e)};foundLegacy,foundModern:=false,false
 for _,v:=range list.Items{if v.SipTrunkId==id{foundLegacy=true;observeAny(rows,"stored-legacy-list-preserves-unknown","actual-protobuf-response",v,markerLegacy,nil)};if v.SipTrunkId==modernID{foundModern=true;observeAny(rows,"modern-to-legacy-drops-source-unknown","actual-protobuf-response",v,nil,nil)}}
 if !foundLegacy||!foundModern{panic("legacy list missing owned records")}
 patched,e:=c.UpdateSIPInboundTrunk(ctx,&pb.UpdateSIPInboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPInboundTrunkRequest_Update{Update:&pb.SIPInboundTrunkUpdate{Name:text("legacy-converted-patch")}}});if e!=nil{panicCall("legacy converted patch failed",e)};observeAny(rows,"legacy-converted-patch-has-no-unknown","actual-protobuf-response",patched,nil,nil)
 raw,e:=rc.HGet(ctx,"sip_trunk",id).Bytes();if e!=nil{panicCall("legacy disk read failed",e)};stored:=new(pb.SIPTrunkInfo);if e=proto.Unmarshal(raw,stored);e!=nil{panic(e)};observeAny(rows,"legacy-original-hash-untouched","actual-redis",stored,markerLegacy,nil)
}
