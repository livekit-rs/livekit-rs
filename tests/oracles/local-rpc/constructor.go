// Additive actual-source constructor observations. Original source12 unchanged.
package main
import (
 "context"; "encoding/json"; "os"; "sync"; "time"
 "github.com/livekit/psrpc"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/protocol/rpc"
 psbus "github.com/livekit/psrpc/pkg/bus"
 "google.golang.org/protobuf/proto"
)
// Close acknowledgements are produced only after the original Reader.Close.
type closeReader struct{psrpc.Reader;once sync.Once;done chan struct{}}
func(r *closeReader)Close()error{e:=r.Reader.Close();r.once.Do(func(){r.done<-struct{}{}});return e}
type constructorBus struct{psrpc.MessageBus;mu sync.Mutex;client bool;sizes []int;closed chan struct{}}
func(b *constructorBus)Subscribe(ctx context.Context,ch psrpc.Channel,n int)(psbus.Reader,error){
 r,e:=b.MessageBus.Subscribe(ctx,ch,n);if e!=nil{return nil,e};b.mu.Lock();client:=b.client;if client{b.sizes=append(b.sizes,n)};b.mu.Unlock()
 if client{return &closeReader{Reader:r,done:b.closed},nil};return r,nil
}
func constructorProfile(buffer int,concurrent bool)map[string]any{
 b:=&constructorBus{MessageBus:psrpc.NewLocalMessageBus(),closed:make(chan struct{},4)}
 server,e:=rpc.NewTypedAgentDispatchInternalServer(&handler{},b);if e!=nil{panic(e)};defer server.Shutdown()
 if e=server.RegisterAllRoomTopics("owned");e!=nil{panic(e)}
 b.mu.Lock();b.client=true;b.mu.Unlock();policy:=rpc.DefaultPSRPCConfig;policy.BufferSize=buffer
 client,e:=rpc.NewTypedAgentDispatchInternalClient(rpc.NewClientParams(policy,b,logger.GetDiscardLogger(),rpc.UnimplementedMetricsObserver{}));if e!=nil{panic(e)}
 count:=1;if concurrent{count=2};type observation struct{id string;equal bool;err string};results:=make(chan observation,count)
 for i:=0;i<count;i++{go func(index int){id:=[]string{"first","second"}[index];request:=&livekit.AgentDispatch{Id:id,Room:"owned",Metadata:"actual-generated-claim-grant-response"};ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();out,e:=client.CreateDispatch(ctx,rpc.RoomTopic("owned"),request);results<-observation{id,proto.Equal(out,request),eText(e)}}(i)}
 observations:=map[string]any{};for i:=0;i<count;i++{select{case x:=<-results:observations[x.id]=map[string]any{"equal":x.equal,"error":x.err};case<-time.After(10*time.Second):panic("actual generated constructor call did not settle")}}
 b.mu.Lock();sizes:=append([]int{},b.sizes...);b.mu.Unlock();client.Close();closed:=0
 for closed<2{select{case<-b.closed:closed++;case<-time.After(10*time.Second):panic("actual original client Reader.Close failed to complete")}}
 return map[string]any{"id":map[int]string{1000:"default1000",0:"explicit-zero",3:"explicit-three"}[buffer]+map[bool]string{true:"-concurrent",false:""}[concurrent],"buffer":buffer,"client_subscription_sizes":sizes,"actual_calls":observations,"actual_closed_readers":closed,"scope":"fixed generated typed constructor and actual localbus Request/Claim/Response; echo is not worker assignment"}
}
func closedBlockedPublication()map[string]any{
 b:=&blockedPublication{MessageBus:psrpc.NewLocalMessageBus(),entered:make(chan struct{})}
 client,e:=rpc.NewAgentDispatchInternalClient[rpc.RoomTopic](b,psrpc.WithClientChannelSize(1),psrpc.WithClientTimeout(3*time.Second));if e!=nil{panic(e)}
 result:=make(chan error,1);go func(){_,e:=client.CreateDispatch(context.Background(),"owned",&livekit.AgentDispatch{Id:"original-after-close",Room:"owned"});result<-e}()
 select{case<-b.entered:case<-time.After(10*time.Second):panic("actual bounded source Publish did not enter")}
 client.Close();before:=false;var callErr error;select{case callErr=<-result:before=true;case<-time.After(100*time.Millisecond):}
 first,ok:=ownedRead(b.reader,"close-filler");if !ok{panic("closed filler")};m,e:=psbus.Deserialize(first,0);if e!=nil{panic(e)};if m.(*livekit.AgentDispatch).Id!="actual-filler"{panic("wrong actual filler")}
 raw,ok:=ownedRead(b.reader,"close-original");if !ok{panic("original request withdrawn")};request,e:=psbus.Deserialize(raw,0);if e!=nil{panic(e)};reflection:=request.ProtoReflect();body:=reflection.Get(reflection.Descriptor().Fields().ByName("raw_request")).Bytes();actual:=&livekit.AgentDispatch{};if e=proto.Unmarshal(body,actual);e!=nil{panic(e)}
 if !before{select{case callErr=<-result:case<-time.After(10*time.Second):panic("source Close call never settled after original publication")}}
 if e=b.reader.Close();e!=nil{panic(e)}
 return map[string]any{"id":"close-during-blocked-publication","completed_before_reader_release":before,"delivered_id":actual.Id,"error":eText(callErr),"scope":"actual synchronous source Publish must settle before closed selection; original request decoded from Reader"}
}
func main(){
 enc:=json.NewEncoder(os.Stdout);ids:=[]string{"default1000","explicit-zero","explicit-three","default1000-concurrent","close-during-blocked-publication"}
 cases:=[]func()map[string]any{func()map[string]any{return constructorProfile(1000,false)},func()map[string]any{return constructorProfile(0,false)},func()map[string]any{return constructorProfile(3,false)},func()map[string]any{return constructorProfile(1000,true)},closedBlockedPublication}
 for i,run:=range cases{row:=run();if row["id"]!=ids[i]{panic("constructor source inventory mismatch")};if e:=enc.Encode(row);e!=nil{panic(e)}}
}
