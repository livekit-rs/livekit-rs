// Source-extracted data mutation from fixed MediaTrackReceiver.UpdateVideoSize.
// Only lifecycle/lock/telemetry wrapper replaced; actual proto.CloneOf/mime retained.
package main
import("encoding/json";"encoding/base64";"os";"io";"google.golang.org/protobuf/proto";"github.com/livekit/protocol/livekit";"github.com/livekit/protocol/codecs/mime";"github.com/livekit/mediatransportutil/pkg/codec")
func update(trackInfo *livekit.TrackInfo,mimeType mime.MimeType,sizes []codec.VideoSize)(*livekit.TrackInfo,bool){var changed bool
	clonedInfo := proto.CloneOf(trackInfo)
	var maxWidth, maxHeight uint32
	for _, size := range sizes {
		if size.Width > maxWidth {
			maxWidth = size.Width
			maxHeight = size.Height
		}
	}

	if clonedInfo.Width != maxWidth || clonedInfo.Height != maxHeight {
		clonedInfo.Width = maxWidth
		clonedInfo.Height = maxHeight
		changed = true
	}

	for _, c := range clonedInfo.Codecs {
		if mime.NormalizeMimeType(c.MimeType) == mimeType {
			for i, l := range c.Layers {
				if i < len(sizes) && (sizes[i].Width != 0 || sizes[i].Height != 0) &&
					(l.Width != sizes[i].Width || l.Height != sizes[i].Height) {
					l.Width = sizes[i].Width
					l.Height = sizes[i].Height
					changed = true
				}
			}
		}
	}

return clonedInfo,changed
}
type Case struct{ID string `json:"id"`; Track string `json:"track"`;Mime string `json:"mime"`;Sizes [][2]uint32 `json:"sizes"`}
func main(){b,e:=io.ReadAll(os.Stdin);if e!=nil{panic(e)};var cases []Case;if e=json.Unmarshal(b,&cases);e!=nil{panic(e)};enc:=json.NewEncoder(os.Stdout);for _,c:=range cases{wire,e:=base64.StdEncoding.DecodeString(c.Track);if e!=nil{panic(e)};track:=&livekit.TrackInfo{};if e=proto.Unmarshal(wire,track);e!=nil{panic(e)};sizes:=[]codec.VideoSize{};for _,s:=range c.Sizes{sizes=append(sizes,codec.VideoSize{Width:s[0],Height:s[1]})};output,changed:=update(track,mime.NormalizeMimeType(c.Mime),sizes);wire,e=proto.Marshal(output);if e!=nil{panic(e)};if e=enc.Encode(map[string]any{"id":c.ID,"changed":changed,"track":base64.StdEncoding.EncodeToString(wire)});e!=nil{panic(e)}}}
