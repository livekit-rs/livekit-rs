// Controlled original source manager methods; transport close/bind are original fake interfaces.
package rtc
import (
 "encoding/json"
 "os"
 "runtime"
 "testing"
 "time"
 "github.com/livekit/livekit-server/pkg/rtc/types"
 "github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
 "github.com/livekit/protocol/livekit"
)
func TestSourceSubscriptionSettingsLifetime(t *testing.T){
 var cases []struct{ID string `json:"id"`;OneShot bool `json:"one_shot"`;Actions [][]json.RawMessage `json:"actions"`}
 input,e:=os.ReadFile(os.Getenv("GO_SETTINGS_CASES"));if e!=nil{t.Fatal(e)};if e=json.Unmarshal(input,&cases);e!=nil{t.Fatal(e)}
 f,e:=os.Create(os.Getenv("GO_SETTINGS_OUTPUT"));if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 reconcileInterval=3*time.Second;subscriptionTimeout=15*time.Second;notFoundTimeout=time.Minute
 for _,c:=range cases {
  origin:=time.Unix(1700000000,0);sourceOracleClock=origin
  m:=newTestSubscriptionManager();m.params.UseOneShotSignallingMode=c.OneShot
  participant:=m.params.Participant.(*typesfakes.FakeLocalParticipant);participant.ActiveAtReturns(origin)
  resolver:=newTestResolver(true,true,"publisher","PA_pub")
  allTracks:=[]*typesfakes.FakeSubscribedTrack{};lastRemoved:=0
  m.params.TrackResolver=func(sub types.LocalParticipant,id livekit.TrackID)types.MediaResolverResult{
   res:=resolver.Resolve(sub,id)
   if res.Track!=nil {
    mt:=res.Track.(*typesfakes.FakeMediaTrack);mt.IDReturns(id)
    add:=mt.AddSubscriberStub
    mt.AddSubscriberCalls(func(sub types.LocalParticipant)(types.SubscribedTrack,error){st,err:=add(sub);if st!=nil{allTracks=append(allTracks,st.(*typesfakes.FakeSubscribedTrack))};return st,err})
   }
   return res
  }
  for i,a:=range c.Actions {
   var ns int64;var op string;if e=json.Unmarshal(a[0],&ns);e!=nil{t.Fatal(e)};if e=json.Unmarshal(a[1],&op);e!=nil{t.Fatal(e)};sourceOracleClock=origin.Add(time.Duration(ns))
   switch op {
   case "settings":m.UpdateSubscribedTrackSettings("TR_audio",&livekit.UpdateTrackSettings{Disabled:true})
   case "subscribe":m.SubscribeToTrack("TR_audio",false)
   case "unsubscribe":m.UnsubscribeFromTrack("TR_audio")
   case "close":s:=m.subscriptions["TR_audio"];if s==nil||s.getSubscribedTrack()==nil{t.Fatal("close requires actual previously added track interface")};setTestSubscribedTrackClosed(t,s.getSubscribedTrack(),false);lastRemoved++
   case "bind":s:=m.subscriptions["TR_audio"];if s==nil||s.getSubscribedTrack()==nil{t.Fatal("bind requires original AddSubscriber result")};setTestSubscribedTrackBound(t,s.getSubscribedTrack())
   case "missing":resolver.hasTrack=false
   case "present":resolver.hasTrack=true
   case "disconnect":participant.IsDisconnectedReturns(true)
   case "connect":participant.IsDisconnectedReturns(false)
   case "tick":m.reconcileSubscriptions()
   default:t.Fatal(op)
   }
   for len(m.reconcileCh)>0{id:=<-m.reconcileCh;if s:=m.subscriptions[id];s!=nil{m.reconcileSubscription(s)}else{m.reconcileSubscriptions()}}
   // RemoveSubscriber is original async source work. It returns without producing the
   // controlled OnClose receipt; wait only for that operation to return, not wall-clock policy.
   deadline:=time.Now().Add(2*time.Second);for m.pendingUnsubscribes.Load()!=0 {if time.Now().After(deadline){t.Fatal("source removal interface operation did not return")};runtime.Gosched()}
   row:=map[string]any{"id":c.ID,"index":i,"at_ns":ns,"op":op,"entry":false,"one_shot":c.OneShot,"physical_transport":false,"controlled_close_receipts":lastRemoved}
   if s:=m.subscriptions["TR_audio"];s!=nil{row["entry"]=true;row["desired"]=s.isDesired();row["has_downtrack"]=s.getSubscribedTrack()!=nil;row["kept"]=sourceOracleClock.Before(s.keepUntil);row["keep_until_ns"]=s.keepUntil.Sub(origin).Nanoseconds();row["settings_present"]=s.settings!=nil;if s.settings!=nil{row["disabled"]=s.settings.Disabled}}
   applied:=[]map[string]any{};for j,st:=range allTracks {for k:=0;k<st.UpdateSubscriberSettingsCallCount();k++{settings,immediate:=st.UpdateSubscriberSettingsArgsForCall(k);applied=append(applied,map[string]any{"downtrack":j,"disabled":settings.Disabled,"immediate":immediate})}};row["actual_downtrack_setting_calls"]=applied
   if e=out.Encode(row);e!=nil{t.Fatal(e)}
  }
  close(m.closeCh)
 }
}
