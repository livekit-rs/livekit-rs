// Fixed-source public bus and generated RPC observers; no expected output rows.
package main
import(
 "context";"encoding/json";"os";"sync";"time"
 "github.com/livekit/psrpc"
 psbus "github.com/livekit/psrpc/pkg/bus"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/protocol/rpc"
 "google.golang.org/protobuf/proto"
)
type row struct{ Queue bool `json:"queue"`; Size int `json:"size"` }
type observed struct{psrpc.MessageBus;mu sync.Mutex;rows []row}
func(b *observed)Subscribe(ctx context.Context,ch psrpc.Channel,n int)(psbus.Reader,error){b.mu.Lock();b.rows=append(b.rows,row{false,n});b.mu.Unlock();return b.MessageBus.Subscribe(ctx,ch,n)}
func(b *observed)SubscribeQueue(ctx context.Context,ch psrpc.Channel,n int)(psbus.Reader,error){b.mu.Lock();b.rows=append(b.rows,row{true,n});b.mu.Unlock();return b.MessageBus.SubscribeQueue(ctx,ch,n)}
type handler struct{}
func(*handler)CreateDispatch(_ context.Context,r *livekit.AgentDispatch)(*livekit.AgentDispatch,error){return proto.Clone(r).(*livekit.AgentDispatch),nil}
func(*handler)DeleteDispatch(context.Context,*livekit.DeleteAgentDispatchRequest)(*livekit.AgentDispatch,error){return &livekit.AgentDispatch{},nil}
func(*handler)ListDispatch(context.Context,*livekit.ListAgentDispatchRequest)(*livekit.ListAgentDispatchResponse,error){return &livekit.ListAgentDispatchResponse{},nil}
func plainAndDefaults()map[string]any{
 bus:=&observed{MessageBus:psrpc.NewLocalMessageBus(psrpc.WithBusCompression(psrpc.CompressionOpts{Quality:0,Threshold:1,MaxDecompressedSize:1}))}
 server,err:=rpc.NewTypedAgentDispatchInternalServer(&handler{},bus);if err!=nil{panic(err)};defer server.Shutdown();if err=server.RegisterAllRoomTopics(rpc.RoomTopic("owned"));err!=nil{panic(err)}
 policy:=rpc.DefaultPSRPCConfig;policy.BufferSize=3
 client,err:=rpc.NewTypedAgentDispatchInternalClient(rpc.NewClientParams(policy,bus,logger.GetDiscardLogger(),rpc.UnimplementedMetricsObserver{}));if err!=nil{panic(err)};defer client.Close()
 request:=&livekit.AgentDispatch{Id:"AD_plain",Room:"owned",AgentName:"actual-generated-handler",Metadata:"NONE request and response larger than cap one"}
 ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();out,err:=client.CreateDispatch(ctx,rpc.RoomTopic("owned"),request)
 text:="";if err!=nil{text=err.Error()};bus.mu.Lock();sizes:=append([]row{},bus.rows...);bus.mu.Unlock()
 return map[string]any{"id":"plain-cap-one-and-independent-channel-defaults","success":out!=nil,"error":text,"equal":proto.Equal(request,out),"request_size":proto.Size(request),"subscriptions":sizes,"scope":"generated internal RPC handler echo, not agent worker job proof"}
}
func queueBoundary()map[string]any{
 ctx:=context.Background();bus:=psrpc.NewLocalMessageBus();ch:=psrpc.Channel{Legacy:"source-queue-boundary",Local:"SourceBoundary"}
 sub,err:=psbus.SubscribeQueue[*livekit.AgentDispatch](ctx,bus,ch,1);if err!=nil{panic(err)}
 publish:=func(id string){if err:=bus.Publish(ctx,ch,&livekit.AgentDispatch{Id:id});err!=nil{panic(err)}}
 first:=make(chan struct{});go func(){for _,id:=range []string{"one","two","three"}{publish(id)};close(first)}()
 select{case<-first:case<-time.After(10*time.Second):panic("first three publications did not reach real two-stage queue")}
 fourthEntered:=make(chan struct{});fourth:=make(chan struct{});go func(){close(fourthEntered);publish("fourth");close(fourth)}();<-fourthEntered
 fourthBefore:=false;select{case<-fourth:fourthBefore=true;case<-time.After(100*time.Millisecond):}
 registered:=make(chan psbus.Subscription[*livekit.AgentDispatch],1);go func(){s,e:=psbus.SubscribeQueue[*livekit.AgentDispatch](ctx,bus,ch,1);if e!=nil{panic(e)};registered<-s}()
 registrationBefore:=false;var next psbus.Subscription[*livekit.AgentDispatch];select{case next=<-registered:registrationBefore=true;case<-time.After(100*time.Millisecond):}
 err=sub.Close();select{case<-fourth:case<-time.After(10*time.Second):panic("close failed to release actual full publish")}
 if next==nil{select{case next=<-registered:case<-time.After(10*time.Second):panic("close failed to release real membership registration")}};defer next.Close()
 publish("only-after-register");var id string;select{case message:=<-next.Channel():id=message.Id;case<-time.After(10*time.Second):panic("registered subscriber received no actual publication")}
 text:="";if err!=nil{text=err.Error()};return map[string]any{"id":"raw-and-decoded-size-one-lock-boundary","first_completed":3,"fourth_completed_before_close":fourthBefore,"registration_completed_before_close":registrationBefore,"close_error":text,"new_subscriber_first_id":id,"scope":"actual local bus generic typed subscriber, no generated API success claim"}
}
func main(){
 expected:=[]string{"plain-cap-one-and-independent-channel-defaults","raw-and-decoded-size-one-lock-boundary","transport-global-blocked-membership","typed-response-Any-legal-unknown","consumer-internal.Request","consumer-internal.ClaimRequest","consumer-internal.ClaimResponse","consumer-internal.Response","blocked-caller-cancel","blocked-caller-deadline","blocked-method-timeout","persistent-client-completion-global-gate"}
 e:=json.NewEncoder(os.Stdout);seen:=map[string]bool{};index:=0
 emit:=func(row map[string]any){id,ok:=row["id"].(string);if !ok||seen[id]||index>=len(expected)||id!=expected[index]{panic("source case ID missing, duplicated, unexpected or out of order")};seen[id]=true;index++;if err:=e.Encode(row);err!=nil{panic(err)}}
 emit(plainAndDefaults());emit(queueBoundary());emit(globalBoundary());emit(rawResponseAny())
 for _,row:=range legalRequestClaims(){emit(row)}
 for _,mode:=range []string{"caller-cancel","caller-deadline","method-timeout"}{emit(boundedPublication(mode))}
 emit(persistentCompletion());if index!=len(expected){panic("source case inventory incomplete")}
}
