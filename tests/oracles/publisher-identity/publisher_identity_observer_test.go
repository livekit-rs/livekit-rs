package rtc

import (
    "encoding/json"
    "testing"
    "time"

    "github.com/livekit/livekit-server/pkg/rtc/types"
    "github.com/livekit/protocol/livekit"
    "github.com/pion/webrtc/v4"
    "github.com/stretchr/testify/require"
    "google.golang.org/protobuf/proto"
)

// Uses the original ParticipantImpl methods and actual Pion ICE state. The
// separate official browser fixture covers physical publication/unpublication.
func TestOriginalPublisherIdentityLifecycle(t *testing.T) {
    p := newParticipantForTest(livekit.ParticipantIdentity("publisher-identity-observer"))
    defer p.Close(true, types.ParticipantCloseReasonClientRequestLeave, false)
    p.params.EnableDataTracks = true // explicit source config, not worker simulation
    rows := make([]map[string]any, 0)
    record := func(name string) {
        pi := p.ToProto()
        t.Logf("ORIGINAL_PUBLISHER_IDENTITY_TSV=%s\t%t\t%d\t%d",name,pi.IsPublisher,len(pi.Tracks),len(pi.DataTracks))
        rows = append(rows, map[string]any{"case":name,"is_publisher":pi.IsPublisher,"media_tracks":len(pi.Tracks),"data_tracks":len(pi.DataTracks),"pc_state":p.TransportManager.publisher.pc.ConnectionState().String(),"established":p.TransportManager.IsPublisherEstablished()})
    }
    record("new")
    require.False(t,p.IsPublisher())
    p.HandlePublishDataTrackRequest(&livekit.PublishDataTrackRequest{PubHandle:1,Name:"original-data"})
    require.Len(t,p.UpDataTrackManager.GetPublishedDataTracks(),1)
    require.True(t,p.IsPublisher())
    record("data-publish")
    permission := proto.Clone(p.grants.Load().Video.ToPermission()).(*livekit.ParticipantPermission)
    permission.CanPublish = false
    require.True(t,p.SetPermission(permission))
    require.False(t,p.IsPublisher())
    require.Len(t,p.UpDataTrackManager.GetPublishedDataTracks(),1)
    record("can-publish-false-data-remains")
    permission = proto.Clone(permission).(*livekit.ParticipantPermission)
    permission.CanPublish = true
    require.True(t,p.SetPermission(permission))
    require.False(t,p.TransportManager.IsPublisherEstablished())
    require.False(t,p.IsPublisher())
    record("can-publish-true-pc-new")
    peer,err := webrtc.NewPeerConnection(webrtc.Configuration{})
    require.NoError(t,err)
    defer peer.Close()
    _,err = peer.CreateDataChannel("original-pc-checking",nil)
    require.NoError(t,err)
    offer,err := peer.CreateOffer(nil)
    require.NoError(t,err)
    gathered := webrtc.GatheringCompletePromise(peer)
    require.NoError(t,peer.SetLocalDescription(offer))
    select {case <-gathered: case <-time.After(5*time.Second):t.Fatal("actual peer ICE gathering deadline")}
    pc := p.TransportManager.publisher.pc
    require.NoError(t,pc.SetRemoteDescription(*peer.LocalDescription()))
    answer,err := pc.CreateAnswer(nil)
    require.NoError(t,err)
    require.NoError(t,pc.SetLocalDescription(answer))
    require.Eventually(t,func()bool{return p.TransportManager.IsPublisherEstablished()},3*time.Second,time.Millisecond)
    require.Equal(t,webrtc.PeerConnectionStateConnecting,pc.ConnectionState(),"actual ICE checking leaves PC New before answer/DTLS reaches peer")
    permission = proto.Clone(permission).(*livekit.ParticipantPermission)
    permission.CanSubscribe = !permission.CanSubscribe
    require.True(t,p.SetPermission(permission))
    require.True(t,p.IsPublisher())
    record("permission-recomputed-with-real-native-checking")
    p.HandleUnpublishDataTrackRequest(&livekit.UnpublishDataTrackRequest{PubHandle:1})
    require.Empty(t,p.UpDataTrackManager.GetPublishedDataTracks())
    require.True(t,p.IsPublisher())
    record("last-data-unpublish-sticky")
    data,err := json.Marshal(rows)
    require.NoError(t,err)
    t.Logf("ORIGINAL_PUBLISHER_IDENTITY_JSON=%s",data)
}
