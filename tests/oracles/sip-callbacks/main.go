// Test-only: actual fixed NewIOInfoService + actual RedisStore, not a copied evaluator.
package main

import (
 "context"
 "encoding/base64"
 "encoding/json"
 "flag"
 "fmt"
 "os"
 "time"

 "github.com/livekit/livekit-server/pkg/service"
 pb "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/rpc"
 "github.com/livekit/psrpc"
 "github.com/redis/go-redis/v9"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/encoding/protojson"
)
type fixture struct {
 ID string `json:"id"`
 Operation string `json:"operation"`
 Disconnected bool `json:"disconnected"`
 Trunks []json.RawMessage `json:"trunks"`
 Rules []json.RawMessage `json:"rules"`
 Request json.RawMessage `json:"request"`
}
type result struct { ID string `json:"id"`; Code string `json:"code,omitempty"`; Error string `json:"error,omitempty"`; Wire string `json:"wire_base64,omitempty"`; JSON json.RawMessage `json:"response,omitempty"` }
func main() {
 url:=flag.String("redis","","fresh dedicated Redis URL; never printed")
 input:=flag.String("cases","cases.json","fixture path");flag.Parse()
 if *url=="" {panic("-redis requires a dedicated empty broker")}
 raw,err:=os.ReadFile(*input);must(err);var cases []fixture;must(json.Unmarshal(raw,&cases))
 opts,err:=redis.ParseURL(*url);must(err);client:=redis.NewClient(opts);defer client.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),2*time.Minute);defer cancel()
 keys:=[]string{"sip_trunk","sip_inbound_trunk","sip_outbound_trunk","sip_dispatch_rule"}
 count,err:=client.Exists(ctx,keys...).Result();must(err);if count!=0 {panic("dedicated catalog keys must be empty")}
 store:=service.NewRedisStore(client)
 // Only these preflight-empty keys are task-owned. Other database keys are untouched.
 defer func(){must(client.Del(context.Background(),keys...).Err())}()
 output:=[]result{}
 for _,c:=range cases {
  must(client.Del(ctx,keys...).Err())
  for _,raw:=range c.Trunks {v:=new(pb.SIPInboundTrunkInfo);must(protojson.Unmarshal(raw,v));must(store.StoreSIPInboundTrunk(ctx,v))}
  for _,raw:=range c.Rules {v:=new(pb.SIPDispatchRuleInfo);must(protojson.Unmarshal(raw,v));must(store.StoreSIPDispatchRule(ctx,v))}
  var ss service.SIPStore=store;if c.Disconnected {ss=nil}
  handler,err:=service.NewIOInfoService(nil,nil,nil,ss,nil);must(err)
  var response proto.Message
  switch c.Operation {
  case "authenticate": req:=new(rpc.GetSIPTrunkAuthenticationRequest);must(protojson.Unmarshal(c.Request,req));response,err=handler.GetSIPTrunkAuthentication(ctx,req)
  case "evaluate": req:=new(rpc.EvaluateSIPDispatchRulesRequest);must(protojson.Unmarshal(c.Request,req));response,err=handler.EvaluateSIPDispatchRules(ctx,req)
  default:panic("unknown fixture operation")
  }
  handler.Stop();r:=result{ID:c.ID}
  if err!=nil {code,_:=psrpc.GetErrorCode(err);r.Code=string(code);r.Error=err.Error()} else {b,e:=proto.Marshal(response);must(e);r.Wire=base64.StdEncoding.EncodeToString(b);r.JSON,e=protojson.Marshal(response);must(e)}
  output=append(output,r)
 }
 must(json.NewEncoder(os.Stdout).Encode(output))
}
func must(err error){if err!=nil {panic(fmt.Sprint(err))}}
