// SPDX-License-Identifier: Apache-2.0
// Source observer, NOT a media/worker success fixture. Install only in an owned
// copy of fixed pkg/rtc. Existing original newParticipantForTest supplies real
// ParticipantImpl/PeerConnections and original recording listener; no rewrites.
package rtc

import (
 "encoding/json"
 "os"
 "testing"

 pb "github.com/livekit/protocol/livekit"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/livekit-server/pkg/rtc/types"
 "github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
)

type updateAudioObserverTelemetry struct { types.NullParticipantTelemetryListener; updates int }
func(t *updateAudioObserverTelemetry) OnTrackPublishedUpdate(_ pb.ParticipantID, _ *pb.TrackInfo) {t.updates++}
// Only bridges the source UpTrackManager interface to the ORIGINAL receiver.
// No receiver method is replaced and no remote media publication is claimed.
type updateAudioObserverTrack struct { types.MediaTrack; receiver *MediaTrackReceiver }
func(t *updateAudioObserverTrack) ID()pb.TrackID{return t.receiver.ID()}
func(t *updateAudioObserverTrack) ToProto()*pb.TrackInfo{return t.receiver.TrackInfoClone()}
func(t *updateAudioObserverTrack) UpdateAudioTrack(u *pb.UpdateLocalAudioTrack){t.receiver.UpdateAudioTrack(u)}
func(t *updateAudioObserverTrack) AddOnClose(_ func(bool)){}

func TestUpdateAudioOwnerSourceObserver(t *testing.T) {
 path:=os.Getenv("UPDATE_AUDIO_SOURCE_RESULT");if path==""{t.Fatal("UPDATE_AUDIO_SOURCE_RESULT required")}
 file,err:=os.OpenFile(path,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if err!=nil{t.Fatal(err)}
 defer file.Close(); out:=json.NewEncoder(file)
 emit:=func(id string, ti *pb.TrackInfo, callback, telemetry int, replaced bool, versionDelta uint32, err error){
  raw:=[]int32{};if ti!=nil{for _,f:=range ti.AudioFeatures{raw=append(raw,int32(f))}}
  text:="";if err!=nil{text=err.Error()}
  row:=map[string]any{"id":id,"features":raw,"callbacks":callback,"telemetry":telemetry,"track_pointer_replaced":replaced,"participant_version_delta":versionDelta,"error":text}
  if ti!=nil{row["kind"]=int32(ti.Type);row["stereo"]=ti.Stereo;row["disable_dtx"]=ti.DisableDtx;row["track_version"]=ti.Version}
  if err:=out.Encode(row);err!=nil{t.Fatal(err)}
 }
 for _,kind:=range []pb.TrackType{pb.TrackType_AUDIO,pb.TrackType_VIDEO}{
  func(){
   p:=newParticipantForTest("update-audio-observer")
   defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false)
   p.AddTrack(&pb.AddTrackRequest{Cid:"pending",Type:kind,Source:pb.TrackSource_MICROPHONE,Stereo:true,DisableDtx:true})
   var ti *pb.TrackInfo
   p.pendingTracksLock.RLock();for _,pending:=range p.pendingTracks{for _,info:=range pending.trackInfos{ti=info}};p.pendingTracksLock.RUnlock()
   if ti==nil{t.Fatal("actual AddTrack did not create pending track")}
   listener:=p.params.ParticipantListener.(*typesfakes.FakeLocalParticipantListener)
   before:=p.ToProto().Version;calls:=listener.OnTrackUpdatedCallCount()
   err:=p.UpdateAudioTrack(&pb.UpdateLocalAudioTrack{TrackSid:ti.Sid,Features:[]pb.AudioTrackFeature{99,-1,4,99}})
   emit("pending-"+kind.String(),ti,listener.OnTrackUpdatedCallCount()-calls,0,false,p.ToProto().Version-before,err)
   err=p.UpdateAudioTrack(&pb.UpdateLocalAudioTrack{TrackSid:ti.Sid,Features:nil})
   emit("pending-empty-"+kind.String(),ti,listener.OnTrackUpdatedCallCount()-calls,0,false,p.ToProto().Version-before,err)
  }()
 }
 for _,kind:=range []pb.TrackType{pb.TrackType_AUDIO,pb.TrackType_VIDEO}{
  func(){
   p:=newParticipantForTest("update-audio-published-observer")
   defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false)
   telemetry:=&updateAudioObserverTelemetry{}
   r:=NewMediaTrackReceiver(MediaTrackReceiverParams{ParticipantID:p.ID,Logger:logger.GetDiscardLogger(),TelemetryListener:func()types.ParticipantTelemetryListener{return telemetry}},&pb.TrackInfo{Sid:"TR_source_observer",Type:kind,Stereo:true,DisableDtx:true,AudioFeatures:[]pb.AudioTrackFeature{0,1},Version:&pb.TimedVersion{}})
   track:=&updateAudioObserverTrack{receiver:r}
   p.UpTrackManager.AddPublishedTrack(track)
   // The test bridge is not a full native track; remove before normal participant
   // close reaches unrelated native interfaces. Actual receiver has no streams.
   defer func(){p.UpTrackManager.lock.Lock();delete(p.UpTrackManager.publishedTracks,track.ID());p.UpTrackManager.lock.Unlock()}()
   listener:=p.params.ParticipantListener.(*typesfakes.FakeLocalParticipantListener)
   for index,features:=range [][]pb.AudioTrackFeature{{99,1,-1,0,99},{99,-1,0,1},{}}{
    before:=p.ToProto().Version;calls:=listener.OnTrackUpdatedCallCount();teleBefore:=telemetry.updates;old:=r.TrackInfo()
    err:=p.UpdateAudioTrack(&pb.UpdateLocalAudioTrack{TrackSid:string(track.ID()),Features:features})
    emit("published-"+kind.String()+"-"+string(rune('0'+index)),r.TrackInfo(),listener.OnTrackUpdatedCallCount()-calls,telemetry.updates-teleBefore,old!=r.TrackInfo(),p.ToProto().Version-before,err)
   }
  }()
 }
 func(){p:=newParticipantForTest("update-audio-missing-observer");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false)
  err:=p.UpdateAudioTrack(&pb.UpdateLocalAudioTrack{TrackSid:"TR_missing",Features:[]pb.AudioTrackFeature{99,99}})
  emit("missing",nil,0,0,false,0,err)
 }()
}
