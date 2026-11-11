// Additive real generated-client/localbus observer. No source algorithms copied.
package main
import (
 "context";"fmt";"sync";"time"
 "github.com/livekit/psrpc"
 psbus "github.com/livekit/psrpc/pkg/bus"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/rpc"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/reflect/protoreflect"
)
type injectUnknown struct{psrpc.MessageBus;kind protoreflect.FullName;mu sync.Mutex;seen int}
func(b *injectUnknown)Publish(ctx context.Context,ch psrpc.Channel,m proto.Message)error{
 if m.ProtoReflect().Descriptor().FullName()==b.kind {m=proto.Clone(m);m.ProtoReflect().SetUnknown(append(m.ProtoReflect().GetUnknown(),0x08,7));b.mu.Lock();b.seen++;b.mu.Unlock()}
 return b.MessageBus.Publish(ctx,ch,m)
}
func legalRequestClaims()[]map[string]any{
 var rows []map[string]any
 for _,kind:=range []protoreflect.FullName{"internal.Request","internal.ClaimRequest","internal.ClaimResponse","internal.Response"}{
  bus:=&injectUnknown{MessageBus:psrpc.NewLocalMessageBus(),kind:kind}
  server,e:=rpc.NewAgentDispatchInternalServer[rpc.RoomTopic](&handler{},bus);if e!=nil{panic(e)}
  if e=server.RegisterAllRoomTopics("owned");e!=nil{panic(e)}
  client,e:=rpc.NewAgentDispatchInternalClient[rpc.RoomTopic](bus,psrpc.WithClientTimeout(3*time.Second));if e!=nil{panic(e)}
  request:=&livekit.AgentDispatch{Id:"AD_legal_unknown",Room:"owned",AgentName:"real-generated-handler",Metadata:"unchanged-body"}
  output,err:=client.CreateDispatch(context.Background(),"owned",request)
  text:="";if err!=nil{text=err.Error()};bus.mu.Lock();seen:=bus.seen;bus.mu.Unlock()
  client.Close();server.Shutdown()
  rows=append(rows,map[string]any{"id":"consumer-"+string(kind),"success":err==nil,"equal":proto.Equal(request,output),"injected_count":seen,"error":text,"scope":"actual generated source client/server, legal wrong-wire #1 unknown; echo handler is not a job proof"})
 }
 return rows
}
// Block the actual synchronous Publish on an actual source raw-reader queue.
// The wrapper only supplies a reader/filler and metadata barrier; source Publish
// and all client selection/cancellation code remain untouched.
type blockedPublication struct{psrpc.MessageBus;once sync.Once;entered chan struct{};reader psrpc.Reader;mu sync.Mutex;request proto.Message}
func(b *blockedPublication)Publish(ctx context.Context,ch psrpc.Channel,m proto.Message)error{
 if m.ProtoReflect().Descriptor().FullName()=="internal.Request" {
  b.once.Do(func(){r,e:=b.MessageBus.SubscribeQueue(context.Background(),ch,1);if e!=nil{panic(e)};b.reader=r
   if e=b.MessageBus.Publish(context.Background(),ch,&livekit.AgentDispatch{Id:"actual-filler"});e!=nil{panic(e)}
   b.mu.Lock();b.request=proto.Clone(m);b.mu.Unlock();close(b.entered)})
 }
 return b.MessageBus.Publish(ctx,ch,m)
}
// Bounded source Reader observation: timeout closes the actual owned reader,
// releases blocked Read, and joins that reader goroutine. Never inject a result.
func ownedRead(reader psrpc.Reader,label string)([]byte,bool){
 type result struct{bytes []byte;ok bool};done:=make(chan result,1)
 go func(){b,ok:=reader.Read();done<-result{b,ok}}()
 select{case r:=<-done:return r.bytes,r.ok;case<-time.After(10*time.Second):
  closeDone:=make(chan error,1);go func(){closeDone<-reader.Close()}()
  select{case<-done:case<-time.After(10*time.Second):panic(label+" reader failed to join after Close")}
  select{case e:=<-closeDone:if e!=nil{panic(e)};case<-time.After(10*time.Second):panic(label+" Close failed to join")}
  panic(label+" timed out waiting for actual Reader.Read")
 }
}
func boundedPublication(mode string)map[string]any{
 bus:=&blockedPublication{MessageBus:psrpc.NewLocalMessageBus(),entered:make(chan struct{})}
 client,e:=rpc.NewAgentDispatchInternalClient[rpc.RoomTopic](bus,psrpc.WithClientChannelSize(1),psrpc.WithClientTimeout(3*time.Second));if e!=nil{panic(e)};defer client.Close()
 ctx,cancel:=context.WithCancel(context.Background());defer cancel()
 if mode=="caller-deadline"{ctx,cancel=context.WithTimeout(context.Background(),100*time.Millisecond);defer cancel()}
 done:=make(chan error,1);go func(){var opts []psrpc.RequestOption;if mode=="method-timeout"{opts=append(opts,psrpc.WithRequestTimeout(100*time.Millisecond))};_,e:=client.CreateDispatch(ctx,"owned",&livekit.AgentDispatch{Id:map[string]string{"caller-cancel":"AD_after_cancel","caller-deadline":"AD_after_deadline","method-timeout":"AD_method_timeout"}[mode],Room:"owned"},opts...);done<-e}()
 select{case<-bus.entered:case<-time.After(10*time.Second):panic("actual generated Request Publish did not enter")}
 if mode=="caller-cancel"{cancel()}else if mode=="caller-deadline"{<-ctx.Done()}else{time.Sleep(200*time.Millisecond)}
 completedBefore:=false;var callErr error
 select{case callErr=<-done:completedBefore=true;case<-time.After(100*time.Millisecond):}
 filler,ok:=ownedRead(bus.reader,"filler");if !ok{panic("raw reader closed before release")};fm,e:=psbus.Deserialize(filler,0);if e!=nil{panic(e)};if fm.(*livekit.AgentDispatch).Id!="actual-filler"{panic("wrong raw filler")}
 original,ok:=ownedRead(bus.reader,"original-request");if !ok{panic("actual bounded request publication withdrawn")};m,e:=psbus.Deserialize(original,0);if e!=nil{panic(e)}
 rm:=m.ProtoReflect();fields:=rm.Descriptor().Fields();raw:=rm.Get(fields.ByName("raw_request")).Bytes();actual:=&livekit.AgentDispatch{};if e=proto.Unmarshal(raw,actual);e!=nil{panic(e)}
 deliveredExpired:=rm.Get(fields.ByName("expiry")).Int()<time.Now().UnixNano()
 if !completedBefore{select{case callErr=<-done:case<-time.After(10*time.Second):panic("source call did not finish after real publication release")}}
 if e=bus.reader.Close();e!=nil{panic(e)};text:="";if callErr!=nil{text=callErr.Error()}
 return map[string]any{"id":"blocked-"+mode,"completed_before_reader_release":completedBefore,"delivered_id":actual.Id,"delivered_after_original_expiry":deliveredExpired,"error":text,"error_type":fmt.Sprintf("%T",callErr),"scope":"source generated generic client, no retry middleware; real raw bounded publication, source ctx selection after release"}
}
