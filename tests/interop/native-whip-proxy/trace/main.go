// Passive Redis recorder: never registers an RPC handler, claims work or sends a reply.
package main
import (
 "context"
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "errors"
 "flag"
 "fmt"
 "os"
 "os/signal"
 "strings"
 "syscall"
 "time"
 "github.com/livekit/psrpc/internal"
 "github.com/livekit/psrpc/pkg/bus"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/rpc"
 "github.com/redis/go-redis/v9"
 "google.golang.org/protobuf/proto"
)
const maxPayload=1<<20
func hash(s string)string{v:=sha256.Sum256([]byte(s));return hex.EncodeToString(v[:])}
func audio(a *livekit.InputAudioState)any {if a==nil{return nil};return map[string]any{"mime_type":a.MimeType,"average_bitrate":a.AverageBitrate,"channels":a.Channels,"sample_rate":a.SampleRate}}
type Event struct{Kind string `json:"kind"`;T float64 `json:"t_ms"`;Ingress string `json:"ingress_sha256,omitempty"`;Participant string `json:"participant_sha256,omitempty"`;Resource string `json:"resource_sha256,omitempty"`;Closed bool `json:"closed"`;Status int32 `json:"status"`;Started int64 `json:"started_at_ns,omitempty"`;Updated int64 `json:"updated_at_ns,omitempty"`;Ended int64 `json:"ended_at_ns,omitempty"`;Audio any `json:"audio,omitempty"`;ACK bool `json:"ack"`;Replied bool `json:"replied"`;ResponseMS float64 `json:"response_ms,omitempty"`;TokenPresent bool `json:"token_present"`;WsPresent bool `json:"ws_url_present"`;WHIP bool `json:"whip_bypass"`;Code string `json:"code,omitempty"`}
type Record struct{RunID string `json:"run_id"`;Status string `json:"status"`;Ready bool `json:"ready"`;Complete bool `json:"coverage_complete"`;Events []Event `json:"events"`;Errors int `json:"decode_or_transport_errors"`;Overflow int `json:"overflow"`;Pending int `json:"pending"`}
func run()error{
 out:=flag.String("out","","atomic safe JSON output");runID:=flag.String("run-id","","fresh caller chosen run identifier");dsn:=flag.String("redis","","private Redis URL; do not print");flag.Parse();if *out==""||*runID==""||*dsn==""{return errors.New("required flags missing")}
 opts,err:=redis.ParseURL(*dsn);if err!=nil{return errors.New("invalid Redis URL")};client:=redis.NewClient(opts);defer client.Close()
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM);defer stop();ctx,cancel:=context.WithTimeout(ctx,240*time.Second);defer cancel()
 patterns:=[]string{"IngressHandler|WHIPRTCConnectionNotify|*|REQ","IOInfo|UpdateIngressState|REQ","IOInfo|GetIngressInfo|REQ","*|RES"}
 sub:=client.PSubscribe(ctx,patterns...);defer sub.Close();go func(){<-ctx.Done();_ = sub.Close()}();seen:=map[string]bool{}
 // Actual SUBSCRIBE acknowledgement, never a fixed readiness sleep.
 for len(seen)<len(patterns){value,err:=sub.Receive(ctx);if err!=nil{return errors.New("subscription ACK failed")};s,ok:=value.(*redis.Subscription);if !ok||s.Kind!="psubscribe"{return errors.New("unexpected pre-readiness event")};seen[s.Channel]=true}
 start:=time.Now();record:=Record{RunID:*runID,Status:"RECORDING",Ready:true,Complete:true,Events:[]Event{}};pending:=map[string]int{}
 save:=func()error{record.Pending=len(pending);data,err:=json.MarshalIndent(record,"","  ");if err!=nil{return err};tmp:=*out+".next";if err=os.WriteFile(tmp,append(data,'\n'),0600);err!=nil{return err};return os.Rename(tmp,*out)}
 if err=save();err!=nil{return err}
 for {
  message,err:=sub.ReceiveMessage(ctx);if err!=nil{if ctx.Err()!=nil{break};record.Errors++;record.Complete=false;record.Status="INCOMPLETE";_ =save();return errors.New("subscription receive failed")}
  if len(message.Payload)>maxPayload{record.Overflow++;record.Complete=false;_ =save();continue}
  decoded,err:=bus.Deserialize([]byte(message.Payload),maxPayload);if err!=nil{record.Errors++;record.Complete=false;_ =save();continue}
  now:=float64(time.Since(start).Microseconds())/1000
  switch m:=decoded.(type){
  case *internal.Request:
   if _,exists:=pending[m.RequestId];exists{continue}
   if len(record.Events)>=256||len(pending)>=128{record.Overflow++;record.Complete=false;_ =save();continue}
   body:=m.RawRequest;if len(body)==0&&m.Request!=nil{body=m.Request.Value};e:=Event{T:now}
   switch {
   case strings.HasPrefix(message.Channel,"IngressHandler|WHIPRTCConnectionNotify|"):
    var r rpc.WHIPRTCConnectionNotifyRequest;err=proto.Unmarshal(body,&r);e.Kind="notify";e.Participant=hash(r.ParticipantId);e.Closed=r.Closed;e.Audio=audio(r.Audio)
   case message.Channel=="IOInfo|UpdateIngressState|REQ":
    var r rpc.UpdateIngressStateRequest;err=proto.Unmarshal(body,&r);e.Kind="state";e.Ingress=hash(r.IngressId);if s:=r.State;s!=nil{e.Resource=hash(s.ResourceId);e.Status=int32(s.Status);e.Started=s.StartedAt;e.Updated=s.UpdatedAt;e.Ended=s.EndedAt;e.Audio=audio(s.Audio)}
   case message.Channel=="IOInfo|GetIngressInfo|REQ":
    var r rpc.GetIngressInfoRequest;err=proto.Unmarshal(body,&r);e.Kind="get_info" // Never retain stream key or token.
   default:continue
   }
   if err!=nil{record.Errors++;record.Complete=false;_ =save();continue};pending[m.RequestId]=len(record.Events);record.Events=append(record.Events,e)
  case *internal.Response:
   i,ok:=pending[m.RequestId];if !ok{continue};e:=&record.Events[i];e.Replied=true;e.ACK=m.Error==""&&m.Code=="";e.Code=m.Code;e.ResponseMS=now
   if e.Kind=="get_info"&&e.ACK{body:=m.RawResponse;if len(body)==0&&m.Response!=nil{body=m.Response.Value};var r rpc.GetIngressInfoResponse;if err=proto.Unmarshal(body,&r);err!=nil{record.Errors++;record.Complete=false}else{e.TokenPresent=r.Token!="";e.WsPresent=r.WsUrl!="";if r.Info!=nil{e.Ingress=hash(r.Info.IngressId);e.WHIP=r.Info.InputType==livekit.IngressInput_WHIP_INPUT&&r.Info.EnableTranscoding!=nil&&!*r.Info.EnableTranscoding}}};delete(pending,m.RequestId)
  default:continue
  }
  if err=save();err!=nil{return err}
 }
 record.Status="RECORDED";if !record.Complete{record.Status="INCOMPLETE"};return save()
}
func main(){if err:=run();err!=nil{fmt.Fprintln(os.Stderr,"passive trace failed (no private details)");os.Exit(1)}}
