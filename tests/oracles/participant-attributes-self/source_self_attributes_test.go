// SPDX-License-Identifier: Apache-2.0
package rtc
import("encoding/json";"os";"testing";pb "github.com/livekit/protocol/livekit";"github.com/livekit/livekit-server/pkg/rtc/types")
func TestActualSelfNonemptyAttributesObserver(t *testing.T){
 f,e:=os.OpenFile(os.Getenv("GO_SELF_ATTRIBUTE_OUTPUT"),os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600);if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 for _,allowed:=range []bool{true,false}{
  p:=newParticipantForTest("self-attribute-observer")
  permission:=p.ToProto().Permission;permission.CanUpdateMetadata=allowed;p.SetPermission(permission)
  if e=p.UpdateMetadata(&pb.UpdateParticipantMetadata{Attributes:map[string]string{"k":"v"}},true);e!=nil{t.Fatal(e)}
  callbacks:=0;p.OnClaimsChanged(func(types.LocalParticipant){callbacks++});previous:=p.ToProto().Version
  rows:=[]struct{id string;attrs map[string]string}{{"same-value",map[string]string{"k":"v"}},{"delete-absent",map[string]string{"absent":""}},{"empty",map[string]string{}}}
  for _,row:=range rows{before:=callbacks;err:=p.UpdateMetadata(&pb.UpdateParticipantMetadata{RequestId:81,Attributes:row.attrs},false);info:=p.ToProto();message:="";if err!=nil{message=err.Error()};if e=out.Encode(map[string]any{"id":row.id,"allowed":allowed,"version_before":previous,"version_after":info.Version,"claims_callbacks":callbacks-before,"attributes":info.Attributes,"error":message});e!=nil{t.Fatal(e)};previous=info.Version}
  if e=p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false);e!=nil{t.Fatal(e)}
 }
}
