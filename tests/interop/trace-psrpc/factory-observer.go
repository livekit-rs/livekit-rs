// Execute only as an owned standalone process from the fixed server module.
// Actual generated AgentInternal defaults and original ClientParams/helper
// options are observed. These constructor tests are not real worker evidence.
package main

import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "sync"
 "time"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/protocol/rpc"
 "github.com/livekit/psrpc"
 "github.com/livekit/psrpc/pkg/bus/localbus"
 "github.com/livekit/psrpc/pkg/client"
 "github.com/livekit/psrpc/pkg/info"
 "github.com/livekit/psrpc/pkg/middleware/otelpsrpc"
 "github.com/livekit/psrpc/pkg/server"
 "go.opentelemetry.io/otel"
 sdktrace "go.opentelemetry.io/otel/sdk/trace"
 "google.golang.org/protobuf/types/known/wrapperspb"
)

type factoryCapture struct { sync.Mutex; Spans []map[string]any }
func (c *factoryCapture) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
 c.Lock();defer c.Unlock()
 for _,s:=range spans {events:=[]string{};for _,e:=range s.Events(){events=append(events,e.Name)};c.Spans=append(c.Spans,map[string]any{"name":s.Name(),"kind":s.SpanKind().String(),"trace_id":s.SpanContext().TraceID().String(),"span_id":s.SpanContext().SpanID().String(),"parent_span_id":s.Parent().SpanID().String(),"scope_name":s.InstrumentationScope().Name,"scope_version":s.InstrumentationScope().Version,"events":events,"status":s.Status().Code.String()})}
 return nil
}
func (*factoryCapture) Shutdown(context.Context)error{return nil}
func mustFactory(err error){if err!=nil{panic(err)}}
type agentFactory struct{}
func (agentFactory) CheckEnabled(context.Context,*rpc.CheckEnabledRequest)(*rpc.CheckEnabledResponse,error){return &rpc.CheckEnabledResponse{},nil}
func (agentFactory) JobRequest(context.Context,*livekit.Job)(*rpc.JobRequestResponse,error){return &rpc.JobRequestResponse{},nil}
func (agentFactory) JobRequestAffinity(context.Context,*livekit.Job)float32{return 1}
func (agentFactory) JobTerminate(context.Context,*rpc.JobTerminateRequest)(*rpc.JobTerminateResponse,error){return &rpc.JobTerminateResponse{},nil}

func main(){
 rpc.InitPSRPCStats(nil)
 capture:=&factoryCapture{};provider:=sdktrace.NewTracerProvider(sdktrace.WithSyncer(capture));otel.SetTracerProvider(provider);defer provider.Shutdown(context.Background())
 cases:=[]map[string]any{}
 // This is the exact no-option generated factory shape used by Go Agent.
 bus:=localbus.New();agentServer,err:=rpc.NewAgentInternalServer(agentFactory{},bus);mustFactory(err);mustFactory(agentServer.RegisterJobRequestTopic("","room"));mustFactory(agentServer.RegisterJobTerminateTopic("AJ_factory"));agentClient,err:=rpc.NewAgentInternalClient(bus);mustFactory(err)
 ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();_,err=agentClient.JobRequest(ctx,"","room",&livekit.Job{},psrpc.WithSelectionOpts(psrpc.SelectionOpts{AcceptFirstAvailable:true}));mustFactory(err);_,err=agentClient.JobTerminate(ctx,"AJ_factory",&rpc.JobTerminateRequest{});mustFactory(err);agentClient.Close();agentServer.Shutdown()
 if len(capture.Spans)!=0{panic("plain AgentInternal factories unexpectedly instrumented")};cases=append(cases,map[string]any{"name":"generated-AgentInternal-default0","span_count":0})
 run:=func(name string,clientOptions []psrpc.ClientOption,serverOptions []psrpc.ServerOption,wantSent,wantRecv int){
  capture.Lock();start:=len(capture.Spans);capture.Unlock()
  bus:=localbus.New();sd:=&info.ServiceDefinition{Name:name,ID:name};sd.RegisterMethod("Echo",false,false,true,true)
  srv:=server.NewRPCServer(sd,bus,serverOptions...);mustFactory(server.RegisterHandler(srv,"Echo",nil,func(_ context.Context,r *wrapperspb.StringValue)(*wrapperspb.StringValue,error){return r,nil},nil))
  cli,err:=client.NewRPCClient(sd,bus,clientOptions...);mustFactory(err);reply,err:=client.RequestSingle[*wrapperspb.StringValue](ctx,cli,"Echo",nil,wrapperspb.String("actual-echo"));mustFactory(err);if reply.GetValue()!="actual-echo"{panic("actual echo mismatch")};cli.Close();srv.Close(false)
  capture.Lock();spans:=append([]map[string]any(nil),capture.Spans[start:]...);capture.Unlock();sent,recv:=0,0;for _,s:=range spans{if s["kind"]=="client"{sent++};if s["kind"]=="server"{recv++}}
  if sent!=wantSent||recv!=wantRecv{panic(fmt.Sprintf("%s spans Sent/Recv=%d/%d want%d/%d",name,sent,recv,wantSent,wantRecv))}
  cases=append(cases,map[string]any{"name":name,"sent":sent,"recv":recv,"spans":spans})
 }
 // Same ClientParams.Options + explicit wire option, both original functions.
 params:=rpc.NewClientParams(rpc.PSRPCConfig{},localbus.New(),logger.GetLogger(),nil,otelpsrpc.ClientOptions(otelpsrpc.Config{}))
 run("ClientParamsWire",params.Options(),nil,2,0)
 run("RoomManagerManual",[]psrpc.ClientOption{rpc.WithClientLogger(logger.GetLogger())},nil,0,0)
 run("WHIP",nil,[]psrpc.ServerOption{rpc.WithDefaultServerOptions(rpc.PSRPCConfig{},logger.GetLogger())},0,1)
 run("IOInfo",[]psrpc.ClientOption{otelpsrpc.ClientOptions(otelpsrpc.Config{})},[]psrpc.ServerOption{otelpsrpc.ServerOptions(otelpsrpc.Config{})},1,1)
 mustFactory(json.NewEncoder(os.Stdout).Encode(map[string]any{"accepted":true,"scope":"actual original constructor/options functions; real production business remains separate","cases":cases}))
}
