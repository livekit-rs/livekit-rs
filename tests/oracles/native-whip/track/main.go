// Source-extracted fixed production private method with only receiver substituted.
package main
import (
 "os"
 "strings"
 "encoding/json"
 "encoding/base64"
 "fmt"
 "github.com/pion/sdp/v3"
 "google.golang.org/protobuf/proto"
 "github.com/livekit/livekit-server/pkg/sfu/buffer"
 "github.com/livekit/protocol/utils"
 "github.com/livekit/protocol/livekit"
 protosdp "github.com/livekit/protocol/sdp"
 "github.com/livekit/protocol/utils/guid"
)
type collector struct { Tracks []*livekit.AddTrackRequest }
func(p *collector) AddTrack(req *livekit.AddTrackRequest){p.Tracks=append(p.Tracks,proto.Clone(req).(*livekit.AddTrackRequest))}
func (p *collector) synthesizeAddTrackRequests(parsedOffer *sdp.SessionDescription) error {
	for _, m := range parsedOffer.MediaDescriptions {
		if !strings.EqualFold(m.MediaName.Media, "audio") && !strings.EqualFold(m.MediaName.Media, "video") {
			continue
		}

		cid := protosdp.GetMediaStreamTrack(m)
		if cid == "" {
			cid = guid.New(utils.TrackPrefix)
		}

		rids, ridsOk := protosdp.GetSimulcastRids(m)

		var (
			name        string
			trackSource livekit.TrackSource
			trackType   livekit.TrackType
		)
		if strings.EqualFold(m.MediaName.Media, "audio") {
			name = "synthesized-microphone"
			trackSource = livekit.TrackSource_MICROPHONE
			trackType = livekit.TrackType_AUDIO
		} else {
			name = "synthesized-camera"
			trackSource = livekit.TrackSource_CAMERA
			trackType = livekit.TrackType_VIDEO
		}
		req := &livekit.AddTrackRequest{
			Cid:        cid,
			Name:       name,
			Source:     trackSource,
			Type:       trackType,
			DisableDtx: true,
			Stereo:     false,
			Stream:     "camera",
		}
		if strings.EqualFold(m.MediaName.Media, "video") {
			if ridsOk {
				// add simulcast layers, NOTE: only quality can be set as dimensions/fps is not available
				n := min(len(rids), int(buffer.DefaultMaxLayerSpatial)+1)
				for i := range n {
					// WARN: casting int -> protobuf enum
					req.Layers = append(req.Layers, &livekit.VideoLayer{Quality: livekit.VideoQuality(i)})
				}
			} else {
				// dummy layer to ensure at least one layer is available
				req.Layers = []*livekit.VideoLayer{{}}
			}
		}
		p.AddTrack(req)
	}
	return nil
}

type Case struct {ID string `json:"id"`; SDP string `json:"sdp"`; RandomCID bool `json:"random_cid"`}
func main(){if len(os.Args)!=2{panic("cases required")};b,e:=os.ReadFile(os.Args[1]);if e!=nil{panic(e)};var cases []Case;if e=json.Unmarshal(b,&cases);e!=nil{panic(e)};enc:=json.NewEncoder(os.Stdout);for _,c:=range cases{row:=map[string]any{"id":c.ID};doc:=&sdp.SessionDescription{};if e=doc.Unmarshal([]byte(c.SDP));e!=nil{row["error"]=e.Error()}else{p:=&collector{};if e=p.synthesizeAddTrackRequests(doc);e!=nil{row["error"]=e.Error()}else{wire:=[]string{};for _,track:=range p.Tracks{b,e:=proto.Marshal(track);if e!=nil{panic(fmt.Sprint(e))};wire=append(wire,base64.StdEncoding.EncodeToString(b))};row["tracks"]=wire}};if e=enc.Encode(row);e!=nil{panic(e)}}}
