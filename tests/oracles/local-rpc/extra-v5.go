// Source-shaped completion observation. Real MessageBus/typed client subscriptions
// persist across requests; pausing an original response Reader is a scheduling
// barrier, never a substituted response or source algorithm replacement.
package main
import(
 "context";"sync";"time"
 "github.com/livekit/psrpc"
 psbus "github.com/livekit/psrpc/pkg/bus"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/rpc"
 "google.golang.org/protobuf/proto"
)
type responseTap struct{psrpc.MessageBus;once sync.Once;entered chan struct{};release chan struct{};mu sync.Mutex;subscriptions int}
type tappedReader struct{psrpc.Reader;tap *responseTap;stop chan struct{};once sync.Once}
func(r *tappedReader)Read()([]byte,bool){
 bytes,ok:=r.Reader.Read();if !ok{return bytes,ok}
 message,e:=psbus.Deserialize(bytes,0);if e!=nil{return bytes,ok}
 if message.ProtoReflect().Descriptor().FullName()=="internal.Response"{r.tap.once.Do(func(){close(r.tap.entered);select{case<-r.tap.release:case<-r.stop:}})}
 return bytes,ok
}
func(r *tappedReader)Close()error{r.once.Do(func(){close(r.stop)});return r.Reader.Close()}
func(b *responseTap)Subscribe(ctx context.Context,ch psrpc.Channel,n int)(psrpc.Reader,error){r,e:=b.MessageBus.Subscribe(ctx,ch,n);if e!=nil{return nil,e};b.mu.Lock();b.subscriptions++;b.mu.Unlock();return &tappedReader{Reader:r,tap:b,stop:make(chan struct{})},nil}
func(b *responseTap)SubscribeQueue(ctx context.Context,ch psrpc.Channel,n int)(psrpc.Reader,error){r,e:=b.MessageBus.SubscribeQueue(ctx,ch,n);if e!=nil{return nil,e};b.mu.Lock();b.subscriptions++;b.mu.Unlock();return &tappedReader{Reader:r,tap:b,stop:make(chan struct{})},nil}
func persistentCompletion()map[string]any{
 base:=psrpc.NewLocalMessageBus();bus:=&responseTap{MessageBus:base,entered:make(chan struct{}),release:make(chan struct{})}
 server,e:=rpc.NewAgentDispatchInternalServer[rpc.RoomTopic](&handler{},bus);if e!=nil{panic(e)};if e=server.RegisterAllRoomTopics("owned");e!=nil{panic(e)}
 client,e:=rpc.NewAgentDispatchInternalClient[rpc.RoomTopic](bus,psrpc.WithClientChannelSize(1),psrpc.WithClientTimeout(3*time.Second));if e!=nil{panic(e)}
 request:=&livekit.AgentDispatch{Id:"AD_queued_completion",Room:"owned"};type result struct{message *livekit.AgentDispatch;err error};done:=make(chan result,1)
 go func(){m,e:=client.CreateDispatch(context.Background(),"owned",request);done<-result{m,e}}()
 select{case<-bus.entered:case<-time.After(10*time.Second):panic("actual source response did not reach Reader barrier")}
 ch:=psrpc.Channel{Legacy:"persistent-completion-held",Local:"held"};reader,e:=base.SubscribeQueue(context.Background(),ch,1);if e!=nil{panic(e)}
 if e=base.Publish(context.Background(),ch,&livekit.AgentDispatch{Id:"filler"});e!=nil{panic(e)}
 publishing:=make(chan error,1);go func(){publishing<-base.Publish(context.Background(),ch,&livekit.AgentDispatch{Id:"blocked"})}()
 select{case e:=<-publishing:panic("full actual publication unexpectedly completed: "+eText(e));case<-time.After(100*time.Millisecond):}
 registered:=make(chan psrpc.Reader,1);go func(){r,e:=base.SubscribeQueue(context.Background(),ch,1);if e!=nil{panic(e)};registered<-r}()
 select{case r:=<-registered:r.Close();panic("source membership unexpectedly completed before full Publish release");case<-time.After(100*time.Millisecond):}
 close(bus.release);completedBefore:=false;var out result
 select{case out=<-done:completedBefore=true;case<-time.After(100*time.Millisecond):}
 // Always release owned global waiters, including negative observations.
 if e=reader.Close();e!=nil{panic(e)}
 select{case e:=<-publishing:if e!=nil{panic(e)};case<-time.After(10*time.Second):panic("source publication cleanup did not finish")}
 var next psrpc.Reader;select{case next=<-registered:case<-time.After(10*time.Second):panic("source registration cleanup did not finish")};if e=next.Close();e!=nil{panic(e)}
 if !completedBefore{select{case out=<-done:case<-time.After(10*time.Second):panic("source completed request never returned")}}
 bus.mu.Lock();before:=bus.subscriptions;bus.mu.Unlock()
 second,e:=client.CreateDispatch(context.Background(),"owned",request);bus.mu.Lock();after:=bus.subscriptions;bus.mu.Unlock()
 client.Close();server.Shutdown()
 return map[string]any{"id":"persistent-client-completion-global-gate","completed_before_global_release":completedBefore,"equal":proto.Equal(out.message,request)&&proto.Equal(second,request),"error":eText(out.err),"second_error":eText(e),"additional_subscriptions_second_call":after-before,"scope":"actual generated source persistent client, original response Reader scheduling barrier and unrelated raw LocalBus capacity/membership locks"}
}
func eText(e error)string{if e==nil{return ""};return e.Error()}
