package rtc
import (
 "encoding/json"
 "fmt"
 "strings"
 "testing"
 "github.com/livekit/protocol/livekit"
 "github.com/pion/webrtc/v4"
)
// Actual original createMediaEngine + original Pion native offer, not copied
// codec registry formula. This does not invoke full PCTransport or NACK writer.
func TestAudioREDRegistrationObserver(t *testing.T) {
 for _,role:=range []string{"publisher","subscriber","single"}{
  for _,browser:=range []string{"chrome","firefox"}{
   info:=ClientInfo{ClientInfo:&livekit.ClientInfo{Browser:browser}}
   direction:=getSubscriberConfig(false);if role=="publisher"{direction=getPublisherConfig(false)}
   me,e:=createMediaEngine([]*livekit.Codec{{Mime:"audio/opus"},{Mime:"audio/red"}},direction,false);if e!=nil{t.Fatal(e)}
   pc,e:=webrtc.NewAPI(webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{});if e!=nil{t.Fatal(e)}
   if _,e=pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio);e!=nil{pc.Close();t.Fatal(e)}
   offer,e:=pc.CreateOffer(nil);if e!=nil{pc.Close();t.Fatal(e)}
   codecs:=[]string{};for _,line:=range strings.Split(offer.SDP,"\r\n"){if strings.HasPrefix(line,"a=rtpmap:"){codecs=append(codecs,line)}}
   row:=map[string]interface{}{"id":role+"-"+browser,"role":role,"browser":browser,"supports_audio_red":info.SupportsAudioRED(),"audio_rtpmap":codecs}
   b,e:=json.Marshal(row);if e!=nil{pc.Close();t.Fatal(e)};fmt.Println(string(b));if e=pc.Close();e!=nil{t.Fatal(e)}
  }
 }
}
