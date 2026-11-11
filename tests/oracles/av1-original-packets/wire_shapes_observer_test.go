// Copyright 2026 livekit-rs contributors. Apache-2.0 fixed original DD selector/serializer calls.
package videolayerselector

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	"github.com/livekit/protocol/logger"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOriginalDdBareAndRepeatedAttachedContinuationWireShape(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		structure := &dd.FrameDependencyStructure{StructureId: 0, NumDecodeTargets: 1, Templates: []*dd.FrameDependencyTemplate{{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch}}}}
		selector := NewDependencyDescriptor(logger.GetLogger())
		selector.SetMax(buffer.VideoLayer{Spatial: 0, Temporal: 0})
		selector.SetTarget(buffer.VideoLayer{Spatial: 0, Temporal: 0})
		for part := 0; part < 2; part++ {
			descriptor := &dd.DependencyDescriptor{FirstPacketInFrame: part == 0, LastPacketInFrame: part == 1, FrameNumber: 10, FrameDependencies: structure.Templates[0].Clone()}
			active := uint32(1)
			if part == 0 || repeated {
				descriptor.AttachedStructure = structure
			}
			if part == 0 {
				descriptor.ActiveDecodeTargetsBitmask = &active
			}
			ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: 1000 + uint16(part), Timestamp: 3000, SSRC: 101, PayloadType: 98, PayloadSize: 5, Marker: part == 1, IsKeyFrame: true, VideoLayer: buffer.VideoLayer{Spatial: 0, Temporal: 0}, ArrivalTime: time.Now()})
			require.NoError(t, err)
			ep.DependencyDescriptor = &buffer.ExtDependencyDescriptor{Descriptor: descriptor, DecodeTargets: buffer.ProcessFrameDependencyStructure(structure), StructureUpdated: part == 0, ActiveDecodeTargetsUpdated: part == 0, ActiveDecodeTargetsMask: 1, Integrity: part == 1, ExtFrameNum: 10, ExtKeyFrameNum: 10}
			result := selector.Select(ep, 0)
			require.True(t, result.IsSelected)
			raw := result.DDBytes[:result.DDBytesLen]
			if result.DDBytesSpill != nil {
				raw = result.DDBytesSpill[:result.DDBytesLen]
			}
			decoded := &dd.DependencyDescriptor{}
			extension := dd.DependencyDescriptorExtension{Descriptor: decoded, Structure: structure}
			_, err = extension.Unmarshal(raw)
			require.NoError(t, err)
			require.Equal(t, part == 0 || repeated, decoded.AttachedStructure != nil, "original selector does not strip a nonnil structure")
			if part == 1 && !repeated {
				require.Equal(t, 4, len(raw))
				require.NotNil(t, decoded.ActiveDecodeTargetsBitmask)
				require.EqualValues(t, 1, *decoded.ActiveDecodeTargetsBitmask)
			}
			data, err := json.Marshal(map[string]interface{}{"repeated_input_attached": repeated, "part": part, "first": decoded.FirstPacketInFrame, "last": decoded.LastPacketInFrame, "input_attached": descriptor.AttachedStructure != nil, "output_attached": decoded.AttachedStructure != nil, "length": len(raw), "active_present": decoded.ActiveDecodeTargetsBitmask != nil})
			require.NoError(t, err)
			fmt.Println("SOURCE_DD_CONTINUATION " + string(data))
		}
	}
}
