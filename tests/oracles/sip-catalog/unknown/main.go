// Future executable test tool: fixed generated protobuf client + actual Redis.
// No product dependencies or source are changed by preparing this draft.
package main
import("context";"errors";"crypto/sha256";"encoding/hex";"encoding/json";"flag";"fmt";"net/http";"os";"time"
 "github.com/livekit/protocol/auth"
 pb "github.com/livekit/protocol/livekit"
 "github.com/redis/go-redis/v9"
 "github.com/twitchtv/twirp"
 "google.golang.org/protobuf/encoding/protowire"
 "google.golang.org/protobuf/proto")
type row struct{Case string `json:"case"`;Source string `json:"source"`;RootSHA string `json:"root_unknown_sha256"`;MediaSHA string `json:"media_unknown_sha256"`;RootPreserved bool `json:"root_matches_expected"`;MediaPreserved bool `json:"media_matches_expected"`}
func marker(tag protowire.Number,value uint64)[]byte{return protowire.AppendVarint(protowire.AppendTag(nil,tag,protowire.VarintType),value)}
func sha(b []byte)string{v:=sha256.Sum256(b);return hex.EncodeToString(v[:])}
func text(v string)*string{return &v}
func main(){os.Exit(run())}
func run()int{
 base:=flag.String("url","","actual server base URL");redisURL:=flag.String("redis","","its actual isolated Redis DB");key:=flag.String("api-key","","test key");secret:=flag.String("api-secret","","test secret");out:=flag.String("out","","safe report");flag.Parse()
 if *base==""||*redisURL==""||*key==""||*secret==""||*out==""{fmt.Fprintln(os.Stderr,"all flags required");return 2}
 opts,e:=redis.ParseURL(*redisURL);if e!=nil{panic("bad Redis URL")};rc:=redis.NewClient(opts);defer rc.Close()
 token,e:=auth.NewAccessToken(*key,*secret).SetSIPGrant(&auth.SIPGrant{Admin:true}).ToJWT();if e!=nil{panicCall("token generation failed",e)}
 parent,cancel:=context.WithTimeout(context.Background(),40*time.Second);defer cancel();ctx,e:=twirp.WithHTTPRequestHeaders(parent,http.Header{"Authorization":[]string{"Bearer "+token}});if e!=nil{panic(e)}
 client:=pb.NewSIPProtobufClient(*base,&http.Client{Timeout:5*time.Second})
 // Unique task-owned ID, never persisted in the safe report. Direct seeding is
 // deliberate: it represents an already existing/future-written Redis record.
 id:=fmt.Sprintf("ST_unknown_fixture_%d",time.Now().UnixNano())
 defer rc.HDel(context.Background(),"sip_inbound_trunk",id)
 rootOld,mediaOld:=marker(100,7),marker(101,9)
 initial:=&pb.SIPInboundTrunkInfo{SipTrunkId:id,Numbers:[]string{"+12025550901"},Name:"unknown-seeded",Media:&pb.SIPMediaConfig{}}
 initial.ProtoReflect().SetUnknown(rootOld);initial.Media.ProtoReflect().SetUnknown(mediaOld)
 raw,e:=proto.Marshal(initial);if e!=nil{panic(e)};if e=rc.HSet(ctx,"sip_inbound_trunk",id,raw).Err();e!=nil{panicCall("seed failed",e)}
 rows:=[]row{}
 observe:=func(name,source string,info *pb.SIPInboundTrunkInfo,wantRoot,wantMedia []byte){
  if info==nil{panic("nil actual record")};root:=info.ProtoReflect().GetUnknown();var media []byte;if info.Media!=nil{media=info.Media.ProtoReflect().GetUnknown()}
  rows=append(rows,row{name,source,sha(root),sha(media),string(root)==string(wantRoot),string(media)==string(wantMedia)})
 }
 disk:=func(name string,wantRoot,wantMedia []byte){data,e:=rc.HGet(ctx,"sip_inbound_trunk",id).Bytes();if e!=nil{panicCall("read persisted result failed",e)};record:=new(pb.SIPInboundTrunkInfo);if e=proto.Unmarshal(data,record);e!=nil{panic(e)};observe(name,"actual-redis",record,wantRoot,wantMedia)}
 get,e:=client.GetSIPInboundTrunk(ctx,&pb.GetSIPInboundTrunkRequest{SipTrunkId:id});if e!=nil{panicCall("Get failed",e)};observe("seed-get","actual-protobuf-response",get.Trunk,rootOld,mediaOld)
 list,e:=client.ListSIPInboundTrunk(ctx,&pb.ListSIPInboundTrunkRequest{TrunkIds:[]string{id}});if e!=nil||len(list.Items)!=1{panicCall("List failed",e)};observe("seed-list","actual-protobuf-response",list.Items[0],rootOld,mediaOld)
 patched,e:=client.UpdateSIPInboundTrunk(ctx,&pb.UpdateSIPInboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPInboundTrunkRequest_Update{Update:&pb.SIPInboundTrunkUpdate{Name:text("patched")}}});if e!=nil{panicCall("Patch failed",e)};observe("patch-unrelated-field","actual-protobuf-response",patched,rootOld,mediaOld);disk("patch-unrelated-field",rootOld,mediaOld)
 replacement:=&pb.SIPInboundTrunkInfo{Numbers:[]string{"+12025550901"},Name:"replacement-no-unknown"}
 replaced,e:=client.UpdateSIPInboundTrunk(ctx,&pb.UpdateSIPInboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPInboundTrunkRequest_Replace{Replace:replacement}});if e!=nil{panicCall("Replace failed",e)};observe("replace-discards-old-unknown","actual-protobuf-response",replaced,nil,nil);disk("replace-discards-old-unknown",nil,nil)
 rootNew,mediaNew:=marker(102,11),marker(103,13)
 replacement=&pb.SIPInboundTrunkInfo{Numbers:[]string{"+12025550901"},Name:"replacement-own-unknown",Media:&pb.SIPMediaConfig{}}
 replacement.ProtoReflect().SetUnknown(rootNew);replacement.Media.ProtoReflect().SetUnknown(mediaNew)
 replaced,e=client.UpdateSIPInboundTrunk(ctx,&pb.UpdateSIPInboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPInboundTrunkRequest_Replace{Replace:replacement}});if e!=nil{panicCall("Replace own fields failed",e)};observe("replace-keeps-replacement-unknown","actual-protobuf-response",replaced,rootNew,mediaNew);disk("replace-keeps-replacement-unknown",rootNew,mediaNew)
 mediaReplacement:=&pb.SIPMediaConfig{};mediaOverride:=marker(104,17);mediaReplacement.ProtoReflect().SetUnknown(mediaOverride)
 diff:=&pb.SIPInboundTrunkUpdate{Media:mediaReplacement};diff.ProtoReflect().SetUnknown(marker(105,19))
 patched,e=client.UpdateSIPInboundTrunk(ctx,&pb.UpdateSIPInboundTrunkRequest{SipTrunkId:id,Action:&pb.UpdateSIPInboundTrunkRequest_Update{Update:diff}});if e!=nil{panicCall("Patch media failed",e)};observe("patch-replaces-media-not-root-unknown","actual-protobuf-response",patched,rootNew,mediaOverride);disk("patch-replaces-media-not-root-unknown",rootNew,mediaOverride)
 if _,e=client.DeleteSIPTrunk(ctx,&pb.DeleteSIPTrunkRequest{SipTrunkId:id});e!=nil{panicCall("delete failed",e)}
 // Create must keep unknowns attached to the supplied info, not unknowns on the
 // outer Create request. ID is server-generated; only actual ID is cleaned.
 createInfo:=&pb.SIPInboundTrunkInfo{Numbers:[]string{"+12025550901"},Media:&pb.SIPMediaConfig{}}
 createInfo.ProtoReflect().SetUnknown(rootNew);createInfo.Media.ProtoReflect().SetUnknown(mediaNew)
 createRequest:=&pb.CreateSIPInboundTrunkRequest{Trunk:createInfo};createRequest.ProtoReflect().SetUnknown(marker(106,23))
 created,e:=client.CreateSIPInboundTrunk(ctx,createRequest);if e!=nil{panicCall("Create failed",e)};defer client.DeleteSIPTrunk(ctx,&pb.DeleteSIPTrunkRequest{SipTrunkId:created.SipTrunkId});observe("create-keeps-info-not-envelope-unknown","actual-protobuf-response",created,rootNew,mediaNew)
 outbound(ctx,client,rc,&rows);rules(ctx,client,rc,&rows);legacy(ctx,client,rc,&rows,created.SipTrunkId);rawUnknowns(ctx,client,rc,&rows);duplicateOneof(ctx,client,rc,&rows)
 status:="PASS";for _,r:=range rows{if !r.RootPreserved||!r.MediaPreserved{status="FAIL"}}
 report:=struct{Status string `json:"status"`;Scope string `json:"scope"`;Rows []row `json:"rows"`}{status,"actual unknown protobuf preservation; independent future fixture NOT an existing acceptance result",rows}
 data,e:=json.MarshalIndent(report,"","  ");if e!=nil{panic(e)};if e=os.WriteFile(*out,append(data,'\n'),0600);e!=nil{panic(e)};fmt.Printf("%s %d actual unknown-field observations\n",status,len(rows));if status!="PASS"{return 1};return 0
}

func panicCall(label string,err error){
 var typed twirp.Error
 if errors.As(err,&typed){panic(fmt.Sprintf("%s: code=%s message=%s",label,typed.Code(),typed.Msg()))}
 panic(label+": non-Twirp failure or unexpected result cardinality")
}
