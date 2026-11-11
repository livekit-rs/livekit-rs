package main
import("os";"github.com/pion/webrtc/v4")
func main(){p,e:=webrtc.NewPeerConnection(webrtc.Configuration{});if e!=nil{panic(e)};defer p.Close();for _,k:=range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio,webrtc.RTPCodecTypeVideo}{if _,e=p.AddTransceiverFromKind(k);e!=nil{panic(e)}};s,e:=p.CreateOffer(nil);if e!=nil{panic(e)};if _,e=os.Stdout.WriteString(s.SDP);e!=nil{panic(e)}}
