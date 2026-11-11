// SPDX-License-Identifier: Apache-2.0
// ADDITIVE source observer; original Go11 observer remains byte-for-byte.
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
func TestUpdateAudioCompactTailSourceObserver(t *testing.T) {
 path:=os.Getenv("UPDATE_AUDIO_COMPACT_SOURCE_RESULT");if path==""{t.Fatal("new owned output path required")};f,e:=os.OpenFile(path,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{t.Fatal(e)};defer f.Close();out:=json.NewEncoder(f)
 emit:=func(id string,ti *pb.TrackInfo,callbacks,telemetry int,replaced bool,delta uint32,err error){features:=[]int32{};for _,v:=range ti.AudioFeatures{features=append(features,int32(v))};text:="";if err!=nil{text=err.Error()};if e:=out.Encode(map[string]any{"id":id,"kind":int32(ti.Type),"features":features,"stereo":ti.Stereo,"disable_dtx":ti.DisableDtx,"callbacks":callbacks,"telemetry":telemetry,"track_pointer_replaced":replaced,"participant_version_delta":delta,"error":text,"track_version":ti.Version});e!=nil{t.Fatal(e)}}
 labels:=[]string{"duplicate-no-zero","unique-no-zero","empty-clear"}
 for _,kind:=range []pb.TrackType{pb.TrackType_AUDIO,pb.TrackType_VIDEO}{
  func(){p:=newParticipantForTest("compact-pending");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false);p.AddTrack(&pb.AddTrackRequest{Cid:"compact-pending",Type:kind,Source:pb.TrackSource_MICROPHONE,Stereo:false,DisableDtx:true});var ti *pb.TrackInfo;p.pendingTracksLock.RLock();for _,v:=range p.pendingTracks{for _,x:=range v.trackInfos{ti=x}};p.pendingTracksLock.RUnlock();if ti==nil{t.Fatal("real pending AddTrack premise")};listener:=p.params.ParticipantListener.(*typesfakes.FakeLocalParticipantListener)
   for i,features:=range [][]pb.AudioTrackFeature{{99,-1,4,99},{99,-1,4},{}}{before:=p.ToProto().Version;calls:=listener.OnTrackUpdatedCallCount();err:=p.UpdateAudioTrack(&pb.UpdateLocalAudioTrack{TrackSid:ti.Sid,Features:features});emit("compact-pending-"+kind.String()+"-"+labels[i],ti,listener.OnTrackUpdatedCallCount()-calls,0,false,p.ToProto().Version-before,err)}
  }()
 }
 func(){p:=newParticipantForTest("compact-published");defer p.Close(false,types.ParticipantCloseReasonClientRequestLeave,false);telemetry:=&updateAudioObserverTelemetry{};receiver:=NewMediaTrackReceiver(MediaTrackReceiverParams{ParticipantID:p.ID,Logger:logger.GetDiscardLogger(),TelemetryListener:func()types.ParticipantTelemetryListener{return telemetry}},&pb.TrackInfo{Sid:"TR_compact_source",Type:pb.TrackType_AUDIO,Stereo:false,DisableDtx:true,AudioFeatures:[]pb.AudioTrackFeature{1},Version:&pb.TimedVersion{}});track:=&updateAudioObserverTrack{receiver:receiver};p.UpTrackManager.AddPublishedTrack(track);defer func(){p.UpTrackManager.lock.Lock();delete(p.UpTrackManager.publishedTracks,track.ID());p.UpTrackManager.lock.Unlock()}();listener:=p.params.ParticipantListener.(*typesfakes.FakeLocalParticipantListener)
  for i,features:=range [][]pb.AudioTrackFeature{{99,-1,4,99},{99,-1,4},{}}{before:=p.ToProto().Version;calls:=listener.OnTrackUpdatedCallCount();old:=receiver.TrackInfo();count:=telemetry.updates;err:=p.UpdateAudioTrack(&pb.UpdateLocalAudioTrack{TrackSid:string(track.ID()),Features:features});emit("compact-published-AUDIO-"+labels[i],receiver.TrackInfo(),listener.OnTrackUpdatedCallCount()-calls,telemetry.updates-count,old!=receiver.TrackInfo(),p.ToProto().Version-before,err)}
 }()
}
