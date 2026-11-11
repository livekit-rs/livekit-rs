// Test-only oracle: invokes the real fixed Go RedisStore, never copied storage logic.
package main

import (
 "bufio"
 "context"
 "encoding/base64"
 "encoding/json"
 "flag"
 "fmt"
 "os"
 "time"

 "github.com/livekit/livekit-server/pkg/service"
 pb "github.com/livekit/protocol/livekit"
 "github.com/redis/go-redis/v9"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/encoding/protojson"
)

type command struct { RunID string `json:"run_id"`; Operation string `json:"op"`; ID string `json:"id"`; Protobuf string `json:"protobuf"` }
type result struct { RunID string `json:"run_id"`; Operation string `json:"op"`; Protobuf string `json:"protobuf,omitempty"`; Nil bool `json:"nil,omitempty"`; JSON json.RawMessage `json:"json,omitempty"`; Error string `json:"error,omitempty"` }
func main() {
 url:=flag.String("redis","","isolated Redis URL (never logged)"); run:=flag.String("run-id","","unique invocation identity"); flag.Parse()
 if *url=="" || *run=="" {fmt.Fprintln(os.Stderr,"redis and run-id required");os.Exit(2)}
 opts,err:=redis.ParseURL(*url); if err!=nil {fmt.Fprintln(os.Stderr,"invalid Redis configuration");os.Exit(2)}
 client:=redis.NewClient(opts);defer client.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),60*time.Second);defer cancel()
 if err:=client.Ping(ctx).Err();err!=nil {fmt.Fprintln(os.Stderr,"Redis readiness failed");os.Exit(1)}
 store:=service.NewRedisStore(client)
 scanner:=bufio.NewScanner(os.Stdin);scanner.Buffer(make([]byte,4096),4<<20)
 encoder:=json.NewEncoder(os.Stdout)
 for scanner.Scan() {
  var c command
  if err:=json.Unmarshal(scanner.Bytes(),&c);err!=nil || c.RunID!=*run {fmt.Fprintln(os.Stderr,"invalid command or stale run identity");os.Exit(2)}
  out,err:=execute(ctx,store,client,c)
  r:=result{RunID:*run,Operation:c.Operation,Nil:out==nil}
  if err!=nil {r.Error=err.Error()} else if out!=nil { data,e:=proto.Marshal(out);if e!=nil{fmt.Fprintln(os.Stderr,"encode failed");os.Exit(1)};r.Protobuf=base64.StdEncoding.EncodeToString(data); j,e:=(protojson.MarshalOptions{UseProtoNames:true,EmitUnpopulated:true}).Marshal(out);if e!=nil{fmt.Fprintln(os.Stderr,"JSON encode failed");os.Exit(1)};r.JSON=j }
  if err:=encoder.Encode(r);err!=nil{os.Exit(1)}
 }
 if scanner.Err()!=nil {fmt.Fprintln(os.Stderr,"input exceeds bound or read failed");os.Exit(1)}
}
func execute(ctx context.Context,s *service.RedisStore,client *redis.Client,c command)(proto.Message,error) {
 raw,err:=base64.StdEncoding.DecodeString(c.Protobuf);if err!=nil{return nil,err}
 decode:=func(m proto.Message)error{return proto.Unmarshal(raw,m)}
 switch c.Operation {
 case "seed_raw_inbound": return nil,client.HSet(ctx,"sip_inbound_trunk",c.ID,raw).Err()
 case "encoding_nil_inbound":return &pb.ListSIPInboundTrunkResponse{Items:[]*pb.SIPInboundTrunkInfo{nil,{SipTrunkId:"ST_fixture"}}},nil
 case "encoding_nil_get":return &pb.GetSIPInboundTrunkResponse{Trunk:nil},nil
 case "store_legacy":v:=new(pb.SIPTrunkInfo);if e:=decode(v);e!=nil{return nil,e};return nil,s.StoreSIPTrunk(ctx,v)
 case "store_inbound":v:=new(pb.SIPInboundTrunkInfo);if e:=decode(v);e!=nil{return nil,e};return nil,s.StoreSIPInboundTrunk(ctx,v)
 case "store_outbound":v:=new(pb.SIPOutboundTrunkInfo);if e:=decode(v);e!=nil{return nil,e};return nil,s.StoreSIPOutboundTrunk(ctx,v)
 case "store_rule":v:=new(pb.SIPDispatchRuleInfo);if e:=decode(v);e!=nil{return nil,e};return nil,s.StoreSIPDispatchRule(ctx,v)
 case "load_legacy":return s.LoadSIPTrunk(ctx,c.ID)
 case "load_inbound":v,e:=s.LoadSIPInboundTrunk(ctx,c.ID);if v==nil{return nil,e};return v,e
 case "load_outbound":v,e:=s.LoadSIPOutboundTrunk(ctx,c.ID);if v==nil{return nil,e};return v,e
 case "load_rule":return s.LoadSIPDispatchRule(ctx,c.ID)
 case "list_legacy":v:=new(pb.ListSIPTrunkRequest);if e:=decode(v);e!=nil{return nil,e};return s.ListSIPTrunk(ctx,v)
 case "list_inbound":v:=new(pb.ListSIPInboundTrunkRequest);if e:=decode(v);e!=nil{return nil,e};return s.ListSIPInboundTrunk(ctx,v)
 case "list_outbound":v:=new(pb.ListSIPOutboundTrunkRequest);if e:=decode(v);e!=nil{return nil,e};return s.ListSIPOutboundTrunk(ctx,v)
 case "list_rules":v:=new(pb.ListSIPDispatchRuleRequest);if e:=decode(v);e!=nil{return nil,e};return s.ListSIPDispatchRule(ctx,v)
 case "delete_trunk":return nil,s.DeleteSIPTrunk(ctx,c.ID)
 case "delete_rule":return nil,s.DeleteSIPDispatchRule(ctx,c.ID)
 default:return nil,fmt.Errorf("unsupported oracle operation")
 }
}
