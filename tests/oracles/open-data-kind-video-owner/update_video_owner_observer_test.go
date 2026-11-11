// SPDX-License-Identifier: Apache-2.0
// Real original ParticipantImpl -> UpTrackManager -> MediaTrackReceiver;
// interface bridge is not a native media publication or worker success.
package rtc

import (
 "encoding/json"
 "os"
 "testing"
 pb "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/livekit-server/pkg/rtc/types"
 "github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
 "google.golang.org/protobuf/proto"
)
type updateVideoObserverTelemetry struct { types.NullParticipantTelemetryListener; updates int }
func(t *updateVideoObserverTelemetry) OnTrackPublishedUpdate(_ pb.ParticipantID,_ *pb.TrackInfo){t.updates++}
type updateVideoObserverTrack struct { types.MediaTrack; receiver *MediaTrackReceiver }
func(t *updateVideoObserverTrack) ID()pb.TrackID{return t.receiver.ID()}
func(t *updateVideoObserverTrack) ToProto()*pb.TrackInfo{return t.receiver.TrackInfoClone()}
func(t *updateVideoObserverTrack) UpdateVideoTrack(u *pb.UpdateLocalVideoTrack){t.receiver.UpdateVideoTrack(u)}
func(t *updateVideoObserverTrack) AddOnClose(_ func(bool)){}
func TestUpdateVideoOwnerSourceObserver(t *testing.T){
 f,e:=os.OpenFile(os.Getenv("UPDATE_VIDEO_SOURCE_RESULT"),os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 emit:=func(id string,info *pb.TrackInfo,calls,telemetry int,replaced bool,delta uint32,stable *bool,err error){
  row:=map[string]any{"id":id,"kind":nil,"width":nil,"height":nil,"callbacks":calls,"telemetry":telemetry,"track_pointer_replaced":replaced,"participant_version_delta":delta,"track_version_unchanged":stable,"error":""}
  if info!=nil{row["kind"]=int32(info.Type);row["width"]=info.Width;row["height"]=info.Height};if err!=nil{row["error"]=err.Error()};if e=out.Encode(row);e!=nil{t.Fatal(e)}
 }
 for _,kind:=range []pb.TrackType{pb.TrackType_AUDIO,pb.TrackType_VIDEO}{func(){
  p:=newParticipantForTest("update-video-pending");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false)
  p.AddTrack(&pb.AddTrackRequest{Cid:"pending",Type:kind,Source:pb.TrackSource_CAMERA,Width:640,Height:360})
  var info *pb.TrackInfo;p.pendingTracksLock.RLock();for _,pt:=range p.pendingTracks{for _,ti:=range pt.trackInfos{info=ti}};p.pendingTracksLock.RUnlock();if info==nil{t.Fatal("real AddTrack pending missing")}
  listener:=p.params.ParticipantListener.(*typesfakes.FakeLocalParticipantListener)
  for i:=0;i<2;i++{before:=p.ToProto().Version;calls:=listener.OnTrackUpdatedCallCount();old:=proto.Clone(info).(*pb.TrackInfo)
   err:=p.UpdateVideoTrack(&pb.UpdateLocalVideoTrack{TrackSid:info.Sid,Width:1280,Height:720});stable:=proto.Equal(old.Version,info.Version)
   emit("pending-"+kind.String()+"-"+string(rune('0'+i)),info,listener.OnTrackUpdatedCallCount()-calls,0,false,p.ToProto().Version-before,&stable,err)
  }
 }()}
 for _,kind:=range []pb.TrackType{pb.TrackType_AUDIO,pb.TrackType_VIDEO}{func(){
  p:=newParticipantForTest("update-video-published");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false);telemetry:=&updateVideoObserverTelemetry{}
  r:=NewMediaTrackReceiver(MediaTrackReceiverParams{ParticipantID:p.ID,Logger:logger.GetDiscardLogger(),TelemetryListener:func()types.ParticipantTelemetryListener{return telemetry}},&pb.TrackInfo{Sid:"TR_video_observer",Type:kind,Width:640,Height:360,Version:&pb.TimedVersion{}})
  track:=&updateVideoObserverTrack{receiver:r};p.UpTrackManager.AddPublishedTrack(track)
  defer func(){p.UpTrackManager.lock.Lock();delete(p.UpTrackManager.publishedTracks,track.ID());p.UpTrackManager.lock.Unlock()}()
  dims:=[][2]uint32{{1280,720},{1280,720}};if kind==pb.TrackType_VIDEO{dims=append(dims,[2]uint32{0,0})}
  listener:=p.params.ParticipantListener.(*typesfakes.FakeLocalParticipantListener)
  for i,d:=range dims{before:=p.ToProto().Version;calls:=listener.OnTrackUpdatedCallCount();tele:=telemetry.updates;old:=r.TrackInfo()
   err:=p.UpdateVideoTrack(&pb.UpdateLocalVideoTrack{TrackSid:string(track.ID()),Width:d[0],Height:d[1]});stable:=proto.Equal(old.Version,r.TrackInfo().Version)
   emit("published-"+kind.String()+"-"+string(rune('0'+i)),r.TrackInfo(),listener.OnTrackUpdatedCallCount()-calls,telemetry.updates-tele,old!=r.TrackInfo(),p.ToProto().Version-before,&stable,err)
  }
 }()}
 p:=newParticipantForTest("update-video-missing");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false)
 err:=p.UpdateVideoTrack(&pb.UpdateLocalVideoTrack{TrackSid:"TR_missing",Width:1280,Height:720});emit("missing",nil,0,0,false,0,nil,err)
 if e=f.Sync();e!=nil{t.Fatal(e)}
}
