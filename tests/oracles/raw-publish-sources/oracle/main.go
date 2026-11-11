// Original fixed auth public consumers; no source algorithm reimplementation.
package main
import("encoding/json";"os";"strings";"encoding/base64";"github.com/livekit/protocol/auth";pb "github.com/livekit/protocol/livekit";"google.golang.org/protobuf/proto")
type input struct {ID string `json:"id"`; Sources json.RawMessage `json:"sources"`; CanPublish *bool `json:"can_publish"`}
func object(v any)any{b,e:=json.Marshal(v);if e!=nil{panic(e)};var o any;if e=json.Unmarshal(b,&o);e!=nil{panic(e)};return o}
func projection(p *pb.ParticipantPermission)[]int32{v:=make([]int32,0,len(p.CanPublishSources));for _,s:=range p.CanPublishSources{v=append(v,int32(s))};return v}
func refreshed(v *auth.VideoGrant)any{token,e:=auth.NewAccessToken("owned-key","owned-secret").SetIdentity("owned-participant").SetVideoGrant(v).ToJWT();if e!=nil{panic(e)};parts:=strings.Split(token,".");if len(parts)!=3{panic("JWT parts")};b,e:=base64.RawURLEncoding.DecodeString(parts[1]);if e!=nil{panic(e)};var o map[string]any;if e=json.Unmarshal(b,&o);e!=nil{panic(e)};return o["video"]}
func main(){f,e:=os.Open(os.Args[1]);if e!=nil{panic(e)};defer f.Close();var rows []input;if e=json.NewDecoder(f).Decode(&rows);e!=nil{panic(e)};if len(rows)!=16{panic("exact16 input cases required")};seen:=map[string]bool{};out:=json.NewEncoder(os.Stdout)
for _,row:=range rows{if row.ID==""||seen[row.ID]{panic("missing/duplicate input ID")};seen[row.ID]=true
 b,e:=json.Marshal(map[string]any{"roomJoin":true,"room":"owned","canPublish":row.CanPublish,"canPublishSources":row.Sources});if e!=nil{panic(e)};var v auth.VideoGrant;if e=json.Unmarshal(b,&v);e!=nil{panic(e)}
 p:=v.ToPermission();same:=v.Clone();sameMatches:=same.MatchesPermission(p);if !sameMatches{same.UpdateFromPermission(p)}
 changed:=proto.Clone(p).(*pb.ParticipantPermission);changed.CanManageAgentSession=!p.CanManageAgentSession;for i,j:=0,len(changed.CanPublishSources)-1;i<j;i,j=i+1,j-1{changed.CanPublishSources[i],changed.CanPublishSources[j]=changed.CanPublishSources[j],changed.CanPublishSources[i]}
 updated:=v.Clone();changedMatches:=updated.MatchesPermission(changed);if !changedMatches{updated.UpdateFromPermission(changed)}
 allowed:=make([]bool,0,5);after:=make([]bool,0,5);for _,s:=range []pb.TrackSource{pb.TrackSource_UNKNOWN,pb.TrackSource_CAMERA,pb.TrackSource_MICROPHONE,pb.TrackSource_SCREEN_SHARE,pb.TrackSource_SCREEN_SHARE_AUDIO}{allowed=append(allowed,v.GetCanPublishSource(s));after=append(after,updated.GetCanPublishSource(s))}
 if e=out.Encode(map[string]any{"id":row.ID,"video_before":object(&v),"permission_sources":projection(p),"admission_before":allowed,"same_matches":sameMatches,"same_video":object(same),"changed_matches":changedMatches,"changed_video":object(updated),"admission_after":after,"refresh_video":refreshed(&v),"changed_refresh_video":refreshed(updated),"scope":"actual original auth JSON decode/Clone/ToPermission/MatchesPermission/UpdateFromPermission/GetCanPublishSource and signed JWT; not participant/WS/media"});e!=nil{panic(e)}
}}
