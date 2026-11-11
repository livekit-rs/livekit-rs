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
)
func TestSourceMissingSubscription(t *testing.T) {
 var cases []struct { ID string `json:"id"`; Actions [][]json.RawMessage `json:"actions"` }
 data,err:=os.ReadFile(os.Getenv("GO_MISSING_SUBSCRIPTION_CASES"));if err!=nil{t.Fatal(err)}
 if err=json.Unmarshal(data,&cases);err!=nil{t.Fatal(err)}
 f,err:=os.Create(os.Getenv("GO_MISSING_SUBSCRIPTION_OUTPUT"));if err!=nil{t.Fatal(err)};defer f.Close();out:=json.NewEncoder(f)
 reconcileInterval=3*time.Second;notFoundTimeout=time.Minute
 for _,c:=range cases {
  sourceOracleClock=time.Unix(1_700_000_000,0)
  p:=&typesfakes.FakeLocalParticipant{};p.CanSubscribeReturns(true);p.IDReturns("PA_sub");p.IdentityReturns("sub")
  p.GetTelemetryListenerReturns(&typesfakes.FakeParticipantTelemetryListener{})
  callbacks:=[]map[string]any{}
  m:=NewSubscriptionManager(SubscriptionManagerParams{Participant:p,Logger:logger.GetDiscardLogger(),
   TrackResolver:func(types.LocalParticipant,livekit.TrackID) types.MediaResolverResult{return types.MediaResolverResult{}},
   OnSubscriptionError:func(id livekit.TrackID,fatal bool,e error){callbacks=append(callbacks,map[string]any{"track":id,"fatal":fatal,"error":e.Error()})},
  })
  for i,a:=range c.Actions {
   var ns int64;var op string;if err=json.Unmarshal(a[0],&ns);err!=nil{t.Fatal(err)};if err=json.Unmarshal(a[1],&op);err!=nil{t.Fatal(err)}
   sourceOracleClock=time.Unix(1_700_000_000,0).Add(time.Duration(ns))
   switch op {case "subscribe":m.SubscribeToTrack("TR_missing",false);case "unsubscribe":m.UnsubscribeFromTrack("TR_missing");case "deny":p.CanSubscribeReturns(false);case "allow":p.CanSubscribeReturns(true);case "disconnect":p.IsDisconnectedReturns(true);case "connect":p.IsDisconnectedReturns(false);case "tick":m.reconcileSubscriptions();default:t.Fatalf("unknown operation %s",op)}
   // Execute the actual worker queue's original per-track reconcile operation.
   for len(m.reconcileCh)>0 {id:=<-m.reconcileCh;if s:=m.subscriptions[id];s!=nil{m.reconcileSubscription(s)}else{m.reconcileSubscriptions()}}
   desired:=false;attempts:=int32(0);if s:=m.subscriptions["TR_missing"];s!=nil{desired=s.isDesired();attempts=s.getNumAttempts()}
   if err=out.Encode(map[string]any{"id":c.ID,"index":i,"op":op,"at_ns":ns,"desired":desired,"attempts":attempts,"callbacks":callbacks,"reconcile_ns":int64(reconcileInterval),"missing_ns":int64(notFoundTimeout)});err!=nil{t.Fatal(err)}
  }
  // Constructor worker scheduling alone was disabled by the declared overlay;
  // no goroutine is running and all queue actions above were synchronously consumed.
  close(m.closeCh)
 }
}
