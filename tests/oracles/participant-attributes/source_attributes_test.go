// SPDX-License-Identifier: Apache-2.0
// Added observer; unchanged pinned ParticipantImpl and existing real test factory.
package rtc
import("encoding/json";"os";"testing";pb "github.com/livekit/protocol/livekit";"github.com/livekit/livekit-server/pkg/rtc/types")
func TestActualNonemptyAttributeUpdateObserver(t *testing.T){
 f,e:=os.OpenFile(os.Getenv("GO_ATTRIBUTE_OUTPUT"),os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600);if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 p:=newParticipantForTest("attribute-observer");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false)
 callbacks:=0;p.OnClaimsChanged(func(types.LocalParticipant){callbacks++})
 previous:=p.ToProto().Version
 rows:=[]struct{id string;attrs map[string]string}{
  {"insert",map[string]string{"k":"v"}},
  {"same-value-nonempty",map[string]string{"k":"v"}},
  {"delete-absent-nonempty",map[string]string{"absent":""}},
  {"empty-map",map[string]string{}},
  {"nil-map",nil},
  {"delete-present",map[string]string{"k":""}},
 }
 for _,row:=range rows{before:=callbacks;err:=p.UpdateMetadata(&pb.UpdateParticipantMetadata{Attributes:row.attrs},true);info:=p.ToProto();message:="";if err!=nil{message=err.Error()};if e=out.Encode(map[string]any{"id":row.id,"version_before":previous,"version_after":info.Version,"claims_callbacks":callbacks-before,"attributes":info.Attributes,"error":message});e!=nil{t.Fatal(e)};previous=info.Version}
}
