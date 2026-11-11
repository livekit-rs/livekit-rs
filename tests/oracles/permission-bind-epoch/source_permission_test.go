// Source observer only; Apache-2.0 fixed server and original typesfakes are retained.
package rtc
import (
 "encoding/json"
 "os"
 "testing"
 "time"
 "github.com/livekit/livekit-server/pkg/rtc/types"
 "github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
 "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 pu "github.com/livekit/protocol/utils"
)
func TestSourcePermissionRestoreEpoch(t *testing.T) {
 var cases []struct { ID string `json:"id"`; Actions [][]json.RawMessage `json:"actions"` }
 data,e:=os.ReadFile(os.Getenv("GO_PERMISSION_CASES"));if e!=nil{t.Fatal(e)};if e=json.Unmarshal(data,&cases);e!=nil{t.Fatal(e)}
 f,e:=os.Create(os.Getenv("GO_PERMISSION_OUTPUT"));if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 reconcileInterval=3*time.Second;subscriptionTimeout=15*time.Second;notFoundTimeout=time.Minute
 for _,c:=range cases {
  sourceOracleClock=time.Unix(1700000000,0);origin:=sourceOracleClock
  p:=&typesfakes.FakeLocalParticipant{};p.CanSubscribeReturns(true);p.IDReturns("PA_sub");p.IdentityReturns("sub");p.ActiveAtReturns(origin);p.GetTelemetryListenerReturns(&typesfakes.FakeParticipantTelemetryListener{})
  um:=NewUpTrackManager(UpTrackManagerParams{Logger:logger.GetDiscardLogger(),VersionGenerator:pu.NewDefaultTimedVersionGenerator()})
  m:=newTestSubscriptionManager();m.params.Participant=p
  // This is the real publisher's policy store, parsed by UpdateSubscriptionPermission.
  // Original fake transport tracks provide controlled bind/close interface receipts only.
  resolver:=newTestResolver(true,true,"pub","PA_pub")
  m.params.TrackResolver=func(sub types.LocalParticipant,id livekit.TrackID)types.MediaResolverResult {
   res:=resolver.Resolve(sub,id);res.HasPermission=um.HasPermission(id,sub.Identity())
   if res.Track!=nil {
    mt:=res.Track.(*typesfakes.FakeMediaTrack);mt.IDReturns(id)
    mt.RevokeDisallowedSubscribersCalls(func(allowed []livekit.ParticipantIdentity)[]livekit.ParticipantIdentity {
     for _,identity:=range allowed {if identity==p.Identity(){return nil}}
     if s:=m.subscriptions[id];s!=nil && s.getSubscribedTrack()!=nil {setTestSubscribedTrackClosed(t,s.getSubscribedTrack(),false)}
     return []livekit.ParticipantIdentity{p.Identity()}
    })
    um.AddPublishedTrack(mt)
   }
   return res
  }
  errors:=[]map[string]any{}
  m.params.OnSubscriptionError=func(id livekit.TrackID,fatal bool,err error){errors=append(errors,map[string]any{"track":id,"fatal":fatal,"error":err.Error()})}
  for i,a:=range c.Actions {
   var ns int64;var op string;if e=json.Unmarshal(a[0],&ns);e!=nil{t.Fatal(e)};if e=json.Unmarshal(a[1],&op);e!=nil{t.Fatal(e)};sourceOracleClock=origin.Add(time.Duration(ns))
   switch op {
    case "subscribe":m.SubscribeToTrack("TR_audio",false)
    case "bind":s:=m.subscriptions["TR_audio"];if s==nil||s.getSubscribedTrack()==nil{t.Fatal("no actual subscribed track")};setTestSubscribedTrackBound(t,s.getSubscribedTrack())
    case "deny","allow":e=um.UpdateSubscriptionPermission(&livekit.SubscriptionPermission{AllParticipants:op=="allow"},pu.TimedVersion(0),nil);if e!=nil{t.Fatal(e)};m.reconcileSubscriptions()
    case "tick":m.reconcileSubscriptions()
    default:t.Fatal(op)
   }
   for len(m.reconcileCh)>0 {id:=<-m.reconcileCh;if s:=m.subscriptions[id];s!=nil {m.reconcileSubscription(s)}else{m.reconcileSubscriptions()}}
   row:=map[string]any{"id":c.ID,"index":i,"op":op,"at_ns":ns,"publisher_has_permission":um.HasPermission("TR_audio",p.Identity()),"errors":errors}
   if s:=m.subscriptions["TR_audio"];s!=nil {row["desired"]=s.isDesired();row["bound"]=s.isBound();row["needs_bind"]=s.needsBind();row["started_ns"]=s.subStartedAt.Load().Sub(origin).Nanoseconds();row["subscribe_ns"]=s.subscribeAt.Load().Sub(origin).Nanoseconds()}
   if e=out.Encode(row);e!=nil{t.Fatal(e)}
  }
  close(m.closeCh)
 }
}
