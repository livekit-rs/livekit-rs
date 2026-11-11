// Source-bound test adapters only: no mock success for external worker jobs.
// Nil-state test invokes actual service, generated Twirp, and HTTP Recovery.
package main
import("context";"encoding/json";"errors";"fmt";"net/http";"net/http/httptest";"os";"strings"
 "github.com/livekit/livekit-server/pkg/service";"github.com/livekit/livekit-server/pkg/config";"github.com/livekit/livekit-server/pkg/telemetry"
 "github.com/livekit/protocol/auth";pb "github.com/livekit/protocol/livekit";"github.com/livekit/protocol/rpc";"github.com/livekit/protocol/ingress";"github.com/livekit/protocol/utils/xtwirp";"github.com/livekit/psrpc")
type store struct{service.IngressStore}
func(*store) LoadIngress(context.Context,string)(*pb.IngressInfo,error){return &pb.IngressInfo{IngressId:"IN_controlled",Reusable:true},nil}
type ioFixture struct{service.IOClient; failure error}
func(i ioFixture) CreateIngress(context.Context,*pb.IngressInfo)(*rpc.CreateIngressResponse,error){return nil,i.failure}
func contextWithGrant()context.Context{return service.WithGrants(context.Background(),&auth.ClaimGrants{Video:&auth.VideoGrant{IngressAdmin:true}},"")}
func main(){
 output:=os.Stdout;os.Stdout=os.Stderr;defer func(){os.Stdout=output}()
 conf:=config.DefaultConfig
 bus:=psrpc.NewLocalMessageBus();client,e:=rpc.NewIngressClient(rpc.ClientParams{Bus:bus});if e!=nil{panic(e)};defer client.Close()
 out:=[]map[string]any{}
 for _,operation:=range []string{"UpdateIngress","DeleteIngress"}{
 for _,malformed:=range []string{"nil_state","nil_store"}{
 var sourceStore service.IngressStore;if malformed=="nil_state"{sourceStore=&store{}}
  svc:=service.NewIngressService(&config.IngressConfig{},"ND_controlled",bus,client,sourceStore,ioFixture{},&telemetry.NullTelemetryService{})
  opts:=[]interface{}{};for _,option:=range xtwirp.DefaultServerOptions(){opts=append(opts,option)}
  twirp:=pb.NewIngressServer(svc,opts...)
  handler:=service.NewHTTPHandler(&conf,nil,http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){twirp.ServeHTTP(w,r.WithContext(contextWithGrant()))}),nil)
  r:=httptest.NewRequest("POST","/twirp/livekit.Ingress/"+operation,strings.NewReader(`{"ingressId":"IN_controlled"}`));r.Header.Set("Content-Type","application/json")
  recorder:=httptest.NewRecorder();handler.ServeHTTP(recorder,r)
  first,_,_:=strings.Cut(recorder.Body.String(),"\n")
  out=append(out,map[string]any{"kind":malformed,"operation":operation,"status":recorder.Code,"content_type":recorder.Header().Get("Content-Type"),"first_line":first,"dynamic_stack_omitted":true})
 }
 }
 for _,which:=range []string{"actual_sentinel","remote_reconstruction","ordinary_same_text"}{
  var failure error
  switch which{case "actual_sentinel":failure=ingress.ErrIngressOutOfDate;case "remote_reconstruction":failure=psrpc.NewErrorFromResponse("failed_precondition",ingress.ErrIngressOutOfDate.Error());case "ordinary_same_text":failure=errors.New(ingress.ErrIngressOutOfDate.Error())}
  svc:=service.NewIngressService(&config.IngressConfig{},"ND_controlled",bus,client,&store{},ioFixture{failure:failure},&telemetry.NullTelemetryService{})
  info,e:=svc.CreateIngressWithUrl(contextWithGrant(),"https://controlled.invalid/whip",&pb.CreateIngressRequest{InputType:pb.IngressInput_WHIP_INPUT,ParticipantIdentity:"controlled"})
  row:=map[string]any{"kind":"typed_identity","input":which,"info_present":info!=nil,"error_present":e!=nil}
  if e!=nil{row["error"]=e.Error();if code,ok:=psrpc.GetErrorCode(e);ok{row["code"]=code}}
  out=append(out,row)
 }
 if e:=json.NewEncoder(output).Encode(out);e!=nil{panic(fmt.Errorf("output: %w",e))}
}
