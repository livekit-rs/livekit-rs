// Test-only fixed original DownTrack/Pion counterexample; no Go algorithm edits.
package sfu_test

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	mtucodec "github.com/livekit/mediatransportutil/pkg/codec"
	"github.com/pion/rtp/codecs"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOriginalHighOnlyIndependentSpatialPacket(t *testing.T) {
	sink := newSourceSvcDownTrack(t, 2)
	// L=1/F=1/B=1/E=1, SID=2, TID=0, D=0: no lower spatial
	// dependency. The fixed codec keyframe consumer reads the actual bitstream.
	payload := []byte{0x3c, 4, 0x82, 0}
	descriptor := &codecs.VP9Packet{}
	_, err := descriptor.Unmarshal(payload)
	require.NoError(t, err)
	keyframe := mtucodec.IsVP9KeyFrame(descriptor, payload)
	require.True(t, keyframe)
	ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: 1000, Timestamp: 3000, SSRC: 101, PayloadType: 98, PayloadSize: len(payload), Marker: true, IsKeyFrame: keyframe, VideoLayer: buffer.VideoLayer{Spatial: 2, Temporal: 0}, ArrivalTime: time.Now()})
	require.NoError(t, err)
	ep.Packet.Payload = payload
	ep.VP9 = descriptor
	writes := sink.dt.WriteRTP(ep, 2)
	require.EqualValues(t, 1, writes)
	require.Eventually(t, func() bool { return sink.capture.count() > 0 }, 10*time.Second, 20*time.Millisecond)
	actual := sink.capture.all()[0]
	require.Equal(t, payload, actual.Payload)
	row := map[string]any{"spatial": descriptor.SID, "temporal": descriptor.TID, "dependency": descriptor.D, "keyframe": keyframe, "writes": writes, "physical_packets": sink.capture.count(), "payload": actual.Payload}
	raw, err := json.Marshal(row)
	require.NoError(t, err)
	fmt.Println("SOURCE_HIGH_ONLY_SVC " + string(raw))
}

func TestOriginalVp9ProfileKeyframeConsumer(t *testing.T) {
	for _, value := range []byte{0x80, 0x82, 0x84, 0x88, 0x90, 0x92, 0x94, 0xa0, 0xa2, 0xa4, 0xb0, 0xb2, 0xb4, 0xb6} {
		payload := []byte{0x0c, value, 0}
		result := mtucodec.IsVP9KeyFrame(nil, payload)
		raw, err := json.Marshal(map[string]any{"byte": value, "profile": (value >> 4) & 3, "keyframe": result})
		require.NoError(t, err)
		fmt.Println("SOURCE_VP9_KEYFRAME " + string(raw))
	}
}
