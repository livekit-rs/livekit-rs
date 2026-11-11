// Executes fixed protocol Redis client/driver and actual owned network. No mocks.
package main

import (
 "context"
 "errors"
 "net"
 "os/exec"
 "strings"
 "syscall"
 "time"
 "encoding/hex"
 "encoding/json"
 "os"

 pb "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 lkredis "github.com/livekit/protocol/redis"
 "google.golang.org/protobuf/proto"
)

type input struct {
 ID string `json:"id"`
 EffectiveAddress string `json:"effective_address"`
 DB int `json:"db"`
 Username string `json:"username"`
 SecretKind string `json:"secret_kind"`
 ExpectedRegion string `json:"expected_region"`
}
// Fixture premise only. Never substitutes a Redis response or dial error.
func requireRealRefusal(address string) {
 conn,err:=net.DialTimeout("tcp",address,500*time.Millisecond)
 if err==nil { _=conn.Close();panic("FAILED_SETUP: refusal endpoint accepted connection") }
 if !errors.Is(err,syscall.ECONNREFUSED) {panic("FAILED_SETUP: endpoint did not produce actual ECONNREFUSED")}
 _,port,e:=net.SplitHostPort(address);if e!=nil{panic(e)}
 command:="lsof";if value:=os.Getenv("LSOF");value!=""{command=value}
 ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel()
 output,e:=exec.CommandContext(ctx,command,"-nP","-iTCP:"+port,"-sTCP:LISTEN","-Fpn").Output()
 if len(strings.TrimSpace(string(output)))!=0{panic("FAILED_SETUP: refusal endpoint has LISTEN owner")}
 if e!=nil {var exit *exec.ExitError;if !errors.As(e,&exit)||exit.ExitCode()!=1{panic("FAILED_SETUP: lsof failed")}}
}
func main() {
 if len(os.Args)!=2 { panic("runtime plan path required") }
 data,err:=os.ReadFile(os.Args[1]);if err!=nil {panic(err)}
 var rows []input;if err=json.Unmarshal(data,&rows);err!=nil{panic(err)}
 if len(rows)!=12 {panic("original10 and2 additive rows required")}
 logger.InitFromConfig(&logger.Config{Level:"error"},"redis-address-oracle")
 out:=json.NewEncoder(os.Stdout);seen:=map[string]bool{}
 for _,row:=range rows {
  if row.ID==""||seen[row.ID] {panic("duplicate/missing ID")};seen[row.ID]=true
  password:=os.Getenv("NODE_ADDRESS_REDIS_PASSWORD")
  switch row.SecretKind {case "admin":case "wrong":password="owned-known-wrong-secret";case "empty":password="";default:panic("unknown secret kind")}
  if row.ID=="tcp-refused-real-dial"{requireRealRefusal(row.EffectiveAddress)}
  cfg:=lkredis.RedisConfig{Address:row.EffectiveAddress,Username:row.Username,Password:password,DB:row.DB}
  actual:=map[string]any{"id":row.ID,"accepted":false,"regions":[]string{},"nodes_hex":[]string{}}
  client,err:=lkredis.GetRedisClient(&cfg)
  if err!=nil {actual["error"]=err.Error()} else {
   values,listErr:=client.HVals(context.Background(),"nodes").Result()
   if listErr!=nil {actual["error"]="could not list nodes: "+listErr.Error()} else {
    regions:=[]string{};wire:=[]string{}
    for _,value:=range values {var node pb.Node;if err=proto.Unmarshal([]byte(value),&node);err!=nil {panic(err)};regions=append(regions,node.Region);wire=append(wire,hex.EncodeToString([]byte(value)))}
    actual["accepted"]=true;actual["regions"]=regions;actual["nodes_hex"]=wire
   }
   if err=client.Close();err!=nil {panic(err)}
  }
  if row.ID=="tcp-refused-real-dial"{requireRealRefusal(row.EffectiveAddress)}
  if err=out.Encode(actual);err!=nil {panic(err)}
 }
}
