// Additive source public bus observers. Original v2 functions/cases unchanged.
package main
import (
 "context";"encoding/hex";"time"
 "github.com/livekit/psrpc"
 psbus "github.com/livekit/psrpc/pkg/bus"
 "github.com/livekit/protocol/livekit"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/encoding/protowire"
 "google.golang.org/protobuf/reflect/protoreflect"
 "google.golang.org/protobuf/reflect/protoregistry"
)
func globalBoundary()map[string]any{
 ctx:=context.Background();bus:=psrpc.NewLocalMessageBus();a:=psrpc.Channel{Legacy:"global-A",Local:"A"};b:=psrpc.Channel{Legacy:"global-B",Local:"B"}
 ra,e:=bus.SubscribeQueue(ctx,a,1);if e!=nil{panic(e)};rb,e:=bus.SubscribeQueue(ctx,b,1);if e!=nil{panic(e)}
 publish:=func(ch psrpc.Channel,id string){if err:=bus.Publish(ctx,ch,&livekit.AgentDispatch{Id:id});err!=nil{panic(err)}}
 publish(a,"fills-A");started:=make(chan struct{});full:=make(chan struct{});go func(){close(started);publish(a,"blocked-A");close(full)}();<-started
 fullBefore:=false;select{case<-full:fullBefore=true;case<-time.After(100*time.Millisecond):}
 registering:=make(chan struct{});registered:=make(chan psrpc.Reader,1);go func(){close(registering);r,err:=bus.SubscribeQueue(ctx,a,1);if err!=nil{panic(err)};registered<-r}();<-registering
 registerBefore:=false;var replacement psrpc.Reader;select{case replacement=<-registered:registerBefore=true;case<-time.After(100*time.Millisecond):}
 otherStarted:=make(chan struct{});other:=make(chan struct{});go func(){close(otherStarted);publish(b,"cross-topic");close(other)}();<-otherStarted
 otherBefore:=false;select{case<-other:otherBefore=true;case<-time.After(100*time.Millisecond):}
 closeStarted:=make(chan struct{});closedB:=make(chan error,1);go func(){close(closeStarted);closedB<-rb.Close()}();<-closeStarted
 closeBefore:=false;var bErr error;select{case bErr=<-closedB:closeBefore=true;case<-time.After(100*time.Millisecond):}
 if err:=ra.Close();err!=nil{panic(err)}
 select{case<-full:case<-time.After(10*time.Second):panic("source A cancellation did not release full publish")}
 if replacement==nil{select{case replacement=<-registered:case<-time.After(10*time.Second):panic("source registration remained blocked")}};defer replacement.Close()
 select{case<-other:case<-time.After(10*time.Second):panic("source cross-topic publication remained blocked")}
 if !closeBefore{select{case bErr=<-closedB:case<-time.After(10*time.Second):panic("source cross-topic close remained blocked")}}
 if bErr!=nil{panic(bErr)}
 return map[string]any{"id":"transport-global-blocked-membership","full_completed_before_close":fullBefore,"registration_completed_before_close":registerBefore,"other_topic_publish_completed_before_close":otherBefore,"other_topic_close_completed_before_close":closeBefore,"scope":"actual public LocalMessageBus raw readers, cross-topic lock effects; bounded observation not latency benchmark"}
}
func rawResponseAny()map[string]any{
 typ,e:=protoregistry.GlobalTypes.FindMessageByName("internal.Response");if e!=nil{panic(e)}
 base:=typ.New().Interface();fields:=base.ProtoReflect().Descriptor().Fields();base.ProtoReflect().Set(fields.ByName("error"),protoreflect.ValueOfString("actual remote error"));base.ProtoReflect().Set(fields.ByName("code"),protoreflect.ValueOfString("invalid_argument"))
 bytes,e:=proto.Marshal(base);if e!=nil{panic(e)}
 anyWire:=[]byte{0x08,7,0x12,1,'a'};valid:=protowire.AppendTag(append([]byte{},bytes...),8,protowire.BytesType);valid=protowire.AppendBytes(valid,anyWire)
 message:=typ.New().Interface();if err:=proto.Unmarshal(valid,message);err!=nil{panic(err)}
 bus:=psrpc.NewLocalMessageBus();ch:=psrpc.Channel{Legacy:"source-Any",Local:"Response"};sub,e:=psbus.Subscribe[proto.Message](context.Background(),bus,ch,1);if e!=nil{panic(e)};defer sub.Close()
 if err:=bus.Publish(context.Background(),ch,message);err!=nil{panic(err)}
 var received proto.Message;select{case received=<-sub.Channel():case<-time.After(10*time.Second):panic("legal Any unknown did not reach original typed subscription")}
 rm:=received.ProtoReflect();detail:=rm.Get(fields.ByName("error_details")).List().Get(0).Message();df:=detail.Descriptor().Fields()
 bad:=protowire.AppendTag(append([]byte{},bytes...),8,protowire.BytesType);bad=protowire.AppendBytes(bad,[]byte{0x0a,1,0xff})
 envType,e:=protoregistry.GlobalTypes.FindMessageByName("internal.Msg");if e!=nil{panic(e)};envelope:=envType.New();ef:=envelope.Descriptor().Fields();envelope.Set(ef.ByName("type_url"),protoreflect.ValueOfString("type.googleapis.com/internal.Response"));envelope.Set(ef.ByName("value"),protoreflect.ValueOfBytes(bad));wire,e:=proto.Marshal(envelope.Interface());if e!=nil{panic(e)}
 _,invalidErr:=psbus.Deserialize(wire,0);invalidText:="";if invalidErr!=nil{invalidText=invalidErr.Error()}
 return map[string]any{"id":"typed-response-Any-legal-unknown","code":rm.Get(fields.ByName("code")).String(),"error":rm.Get(fields.ByName("error")).String(),"unknown_hex":hex.EncodeToString(detail.GetUnknown()),"value_hex":hex.EncodeToString(detail.Get(df.ByName("value")).Bytes()),"invalid_known_utf8_accepted":invalidErr==nil,"invalid_known_utf8_error":invalidText,"scope":"actual source generated Response/Any Unmarshal, original bus typed forward; malformed isolated via original Deserialize"}
}
