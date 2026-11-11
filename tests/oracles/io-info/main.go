// Preparation only: actual pinned Go IOInfo/RedisStore/generated PSRPC client.
// No media worker or fabricated success is implemented by this test program.
package main
import (
 "bufio"; "context"; "crypto/sha256"; "encoding/base64"; "encoding/hex"; "encoding/json"; "errors"; "flag"; "fmt"; "os"; "os/signal"; "sync"; "sort"; "syscall"; "time"
 "github.com/livekit/livekit-server/pkg/service"
 "github.com/livekit/livekit-server/pkg/telemetry"
 pb "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/rpc"
 "github.com/livekit/psrpc"
 "github.com/livekit/psrpc/pkg/bus/redisbus"
 "github.com/livekit/psrpc/pkg/info"
 "github.com/redis/go-redis/v9"
 "google.golang.org/protobuf/proto"; "google.golang.org/protobuf/reflect/protoreflect"; "google.golang.org/protobuf/reflect/protoregistry"
 "google.golang.org/protobuf/encoding/protojson"
 "google.golang.org/protobuf/types/known/emptypb"
)
type input struct { Case string `json:"case"`; Op string `json:"op"`; Message json.RawMessage `json:"message"`; Wire string `json:"wire_base64"`; ID string `json:"id"`; Room string `json:"room"`; Active bool `json:"active"`; TimeoutMS int `json:"timeout_ms"`; ProtoName string `json:"proto_name"` }
type recorder struct { telemetry.NullTelemetryService; mu sync.Mutex; Events []map[string]any; ids map[string]int }
func(r *recorder)event(kind,id string,status int32,started,updated int64){r.mu.Lock();defer r.mu.Unlock();n,ok:=r.ids[id];if !ok{n=len(r.ids);r.ids[id]=n};r.Events=append(r.Events,map[string]any{"event":kind,"entity_ordinal":n,"status":status,"started_at":started,"updated_at":updated})}
func(r *recorder)EgressStarted(_ context.Context,i *pb.EgressInfo){r.event("egress_started",i.EgressId,int32(i.Status),i.StartedAt,i.UpdatedAt)}
func(r *recorder)EgressUpdated(_ context.Context,i *pb.EgressInfo){r.event("egress_updated",i.EgressId,int32(i.Status),i.StartedAt,i.UpdatedAt)}
func(r *recorder)EgressEnded(_ context.Context,i *pb.EgressInfo){r.event("egress_ended",i.EgressId,int32(i.Status),i.StartedAt,i.UpdatedAt)}
func(r *recorder)ingress(kind string,i *pb.IngressInfo){var status int32;var started,updated int64;if i.State!=nil{status=int32(i.State.Status);started=i.State.StartedAt;updated=i.State.UpdatedAt};r.event(kind,i.IngressId,status,started,updated)}
func(r *recorder)IngressCreated(_ context.Context,i *pb.IngressInfo){r.ingress("ingress_created",i)}
func(r *recorder)IngressStarted(_ context.Context,i *pb.IngressInfo){r.ingress("ingress_started",i)}
func(r *recorder)IngressUpdated(_ context.Context,i *pb.IngressInfo){r.ingress("ingress_updated",i)}
func(r *recorder)IngressEnded(_ context.Context,i *pb.IngressInfo){r.ingress("ingress_ended",i)}
func decode(in input,m proto.Message)error{if in.Wire!=""||len(in.Message)==0{b,e:=base64.StdEncoding.DecodeString(in.Wire);if e!=nil{return e};return proto.Unmarshal(b,m)};return protojson.Unmarshal(in.Message,m)}
func response(name string,m proto.Message,e error)map[string]any{out:=map[string]any{"case":name};if e!=nil{out["status"]="ERROR";out["message"]=e.Error();if code,ok:=psrpc.GetErrorCode(e);ok{out["code"]=code};return out};out["status"]="OK";if m!=nil{b,e:=proto.Marshal(m);if e!=nil{panic(e)};hash:=sha256.Sum256(b);out["proto_name"]=string(m.ProtoReflect().Descriptor().FullName());out["wire_base64"]=base64.StdEncoding.EncodeToString(b);out["wire_sha256"]=hex.EncodeToString(hash[:])};return out}
func call(ctx context.Context,c rpc.IOInfoClient,in input)(proto.Message,error){
 switch in.Op{
 case "CreateEgress":m:=new(pb.EgressInfo);if e:=decode(in,m);e!=nil{return nil,e};return c.CreateEgress(ctx,m)
 case "UpdateEgress":m:=new(pb.EgressInfo);if e:=decode(in,m);e!=nil{return nil,e};return c.UpdateEgress(ctx,m)
 case "GetEgress":return c.GetEgress(ctx,&rpc.GetEgressRequest{EgressId:in.ID})
 case "ListEgress":return c.ListEgress(ctx,&pb.ListEgressRequest{RoomName:in.Room,Active:in.Active,EgressId:in.ID})
 case "CreateIngress":m:=new(pb.IngressInfo);if e:=decode(in,m);e!=nil{return nil,e};return c.CreateIngress(ctx,m)
 case "GetIngressInfo":m:=new(rpc.GetIngressInfoRequest);if e:=decode(in,m);e!=nil{return nil,e};return c.GetIngressInfo(ctx,m)
 case "UpdateIngressState":m:=new(rpc.UpdateIngressStateRequest);if e:=decode(in,m);e!=nil{return nil,e};return c.UpdateIngressState(ctx,m)
 case "GetSIPTrunkAuthentication":m:=new(rpc.GetSIPTrunkAuthenticationRequest);if e:=decode(in,m);e!=nil{return nil,e};return c.GetSIPTrunkAuthentication(ctx,m)
 case "EvaluateSIPDispatchRules":m:=new(rpc.EvaluateSIPDispatchRulesRequest);if e:=decode(in,m);e!=nil{return nil,e};return c.EvaluateSIPDispatchRules(ctx,m)
 case "UpdateSIPCallState":m:=new(rpc.UpdateSIPCallStateRequest);if e:=decode(in,m);e!=nil{return nil,e};return c.UpdateSIPCallState(ctx,m)
 case "RecordCallContext":m:=new(rpc.RecordCallContextRequest);if e:=decode(in,m);e!=nil{return nil,e};return c.RecordCallContext(ctx,m)
 default:return nil,errors.New("unsupported fixed operation")
 }
}
func store(ctx context.Context,s *service.RedisStore,in input)(proto.Message,error){
 switch in.Op{
 case "StoreEgress":m:=new(pb.EgressInfo);if e:=decode(in,m);e!=nil{return nil,e};return &emptypb.Empty{},s.StoreEgress(ctx,m)
 case "UpdateEgress":m:=new(pb.EgressInfo);if e:=decode(in,m);e!=nil{return nil,e};return &emptypb.Empty{},s.UpdateEgress(ctx,m)
 case "GetEgress":return s.LoadEgress(ctx,in.ID)
 case "ListEgress":v,e:=s.ListEgress(ctx,pb.RoomName(in.Room),in.Active);return &pb.ListEgressResponse{Items:v},e
 case "CleanEndedEgress":return &emptypb.Empty{},s.CleanEndedEgress()
 case "StoreIngress":m:=new(pb.IngressInfo);if e:=decode(in,m);e!=nil{return nil,e};return &emptypb.Empty{},s.StoreIngress(ctx,m)
 case "UpdateIngress":m:=new(pb.IngressInfo);if e:=decode(in,m);e!=nil{return nil,e};return &emptypb.Empty{},s.UpdateIngress(ctx,m)
 case "GetIngress":return s.LoadIngress(ctx,in.ID)
 case "ListIngress":v,e:=s.ListIngress(ctx,pb.RoomName(in.Room));return &pb.ListIngressResponse{Items:v},e
 case "UpdateIngressState":m:=new(rpc.UpdateIngressStateRequest);if e:=decode(in,m);e!=nil{return nil,e};return &emptypb.Empty{},s.UpdateIngressState(ctx,m.IngressId,m.State)
 case "DeleteIngress":m,e:=s.LoadIngress(ctx,in.ID);if e!=nil{return nil,e};return &emptypb.Empty{},s.DeleteIngress(ctx,m)
 default:return nil,errors.New("unsupported fixed store operation")
 }
}
func subscriptions(ctx context.Context,rc *redis.Client)error{
 methods:=[]string{"CreateEgress","UpdateEgress","GetEgress","ListEgress","UpdateMetrics","CreateIngress","GetIngressInfo","UpdateIngressState","GetSIPTrunkAuthentication","EvaluateSIPDispatchRules","UpdateSIPCallState","RecordCallContext"}
 sd:=&info.ServiceDefinition{Name:"IOInfo"};channels:=[]string{}
 for _,method:=range methods{sd.RegisterMethod(method,false,false,true,true);m:=sd.GetInfo(method,nil);channels=append(channels,m.GetRPCChannel().Legacy,m.GetClaimResponseChannel().Legacy)}
 for{counts,e:=rc.PubSubNumSub(ctx,channels...).Result();if e!=nil{return e};ready:=true;for _,ch:=range channels{if counts[ch]<1{ready=false;break}};if ready{return nil};select{case<-ctx.Done():return ctx.Err();case<-time.After(time.Millisecond*5):}}
}
func main(){mode:=flag.String("mode","call","serve/call/store");url:=flag.String("redis",os.Getenv("IO_REDIS_URL"),"dedicated Redis URL");ready:=flag.String("ready","","ACK readiness file");events:=flag.String("events","","actual callback events file");flag.Parse();if *url==""{panic("dedicated Redis URL required")};opts,e:=redis.ParseURL(*url);if e!=nil{panic("invalid dedicated Redis URL")};rc:=redis.NewClient(opts);defer rc.Close();b:=redisbus.New(rc);s:=service.NewRedisStore(rc)
 if *mode=="serve"{r:=&recorder{ids:map[string]int{}};svc,e:=service.NewIOInfoService(b,s,s,s,r);if e!=nil{panic(e)};var stopOnce sync.Once;stopService:=func(){stopOnce.Do(svc.Stop)};defer stopService();if e=svc.Start();e!=nil{panic(e)};defer s.Stop();ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);e=subscriptions(ctx,rc);cancel();if e!=nil{panic(e)};if e=os.WriteFile(*ready,[]byte("ACK_READY\n"),0600);e!=nil{panic(e)};ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGINT,syscall.SIGTERM);defer stop();<-ctx.Done();stopService();r.mu.Lock();data,e:=json.Marshal(r.Events);r.mu.Unlock();if e!=nil{panic(e)};if e=os.WriteFile(*events,append(data,'\n'),0600);e!=nil{panic(e)};return}
 if *mode=="encode"||*mode=="decode"{scan:=bufio.NewScanner(os.Stdin);scan.Buffer(make([]byte,4096),1<<20);enc:=json.NewEncoder(os.Stdout);for scan.Scan(){var in input;if e=json.Unmarshal(scan.Bytes(),&in);e!=nil{panic(e)};mt,e:=protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(in.ProtoName));if e!=nil{panic(e)};m:=mt.New().Interface();if e=decode(in,m);e!=nil{panic(e)};out:=response(in.Case,m,nil);j,e:=(protojson.MarshalOptions{UseProtoNames:true,EmitUnpopulated:true}).Marshal(m);if e!=nil{panic(e)};var value any;if e=json.Unmarshal(j,&value);e!=nil{panic(e)};out["known_json"]=value;out["unknown_tree"]=unknownTree(m.ProtoReflect(),"$");if e=enc.Encode(out);e!=nil{panic(e)}};if e=scan.Err();e!=nil{panic(e)};return}
 client,e:=rpc.NewIOInfoClient(b);if e!=nil{panic(e)};defer client.Close();scanner:=bufio.NewScanner(os.Stdin);scanner.Buffer(make([]byte,4096),1<<20);enc:=json.NewEncoder(os.Stdout)
 for scanner.Scan(){var in input;if e=json.Unmarshal(scanner.Bytes(),&in);e!=nil{panic(e)};ms:=in.TimeoutMS;if ms==0{ms=3000};if ms<1||ms>30000{panic("request timeout outside fixture bound")};ctx,cancel:=context.WithTimeout(context.Background(),time.Duration(ms)*time.Millisecond);var out proto.Message;if *mode=="store"{out,e=store(ctx,s,in)}else{out,e=call(ctx,client,in)};cancel();if e=enc.Encode(response(in.Case,out,e));e!=nil{panic(e)}};if e=scanner.Err();e!=nil{panic(e)}
}

// This observes actual generated-Go unknown storage after real decoding. It does
// not duplicate protobuf wire parsing or Rust's sidecar algorithm.
func unknownTree(m protoreflect.Message,path string)[]map[string]any{
 out:=[]map[string]any{};if raw:=m.GetUnknown();len(raw)>0{out=append(out,map[string]any{"path":path,"unknown_hex":hex.EncodeToString(raw)})}
 m.Range(func(f protoreflect.FieldDescriptor,v protoreflect.Value)bool{
  if f.IsMap(){if f.MapValue().Message()!=nil{v.Map().Range(func(k protoreflect.MapKey,v protoreflect.Value)bool{out=append(out,unknownTree(v.Message(),fmt.Sprintf("%s.%s[%v]",path,f.Name(),k.Interface()))...);return true})}}else if f.IsList(){if f.Message()!=nil{for i:=0;i<v.List().Len();i++{out=append(out,unknownTree(v.List().Get(i).Message(),fmt.Sprintf("%s.%s[%d]",path,f.Name(),i))...)}}}else if f.Message()!=nil{out=append(out,unknownTree(v.Message(),path+"."+string(f.Name()))...)};return true})
 sort.Slice(out,func(i,j int)bool{return out[i]["path"].(string)<out[j]["path"].(string)});return out
}
