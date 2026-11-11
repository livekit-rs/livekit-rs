package rtc

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"testing"
)

func TestOriginalLiveKitCodecRegressionPolicy(t *testing.T) {
	for _, policy := range []livekit.BackupCodecPolicy{livekit.BackupCodecPolicy_PREFER_REGRESSION, livekit.BackupCodecPolicy_SIMULCAST, livekit.BackupCodecPolicy_REGRESSION} {
		for _, allowed := range []bool{false, true} {
			track := NewMediaTrack(MediaTrackParams{Logger: logger.GetLogger(), ShouldRegressCodec: func() bool { return allowed }}, &livekit.TrackInfo{
				Sid: "TR_source_codec", Type: livekit.TrackType_VIDEO, BackupCodecPolicy: policy,
				Codecs: []*livekit.SimulcastCodecInfo{{MimeType: "video/vp9", Cid: "primary"}, {MimeType: "video/vp8", Cid: "backup"}}})
			row := struct {
				Kind    string `json:"kind"`
				Policy  int32  `json:"policy"`
				Allows  bool   `json:"allows"`
				Enabled bool   `json:"enabled"`
				Target  string `json:"target"`
			}{"policy", int32(policy), allowed, track.enableRegression(), track.regressionTargetCodec.String()}
			wire, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("SOURCE_CODEC_GATE %s\n", wire)
			track.Close(false)
		}
	}
}
