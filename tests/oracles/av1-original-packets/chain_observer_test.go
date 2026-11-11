// Copyright LiveKit contributors. Apache-2.0. Test-only fixed original calls.
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

func TestOriginalProtectedChainDropRecoveryAndUnknownReference(t *testing.T) {
	structure := &dd.FrameDependencyStructure{StructureId: 0, NumDecodeTargets: 1, NumChains: 1, DecodeTargetProtectedByChain: []int{0}, Templates: []*dd.FrameDependencyTemplate{
		{DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch}, ChainDiffs: []int{0}},
		{DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired}, ChainDiffs: []int{1}},
		{DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent}, ChainDiffs: []int{1}},
	}}
	selector := NewDependencyDescriptor(logger.GetLogger())
	selector.SetMax(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	selector.SetTarget(buffer.VideoLayer{Spatial: 0, Temporal: 0})
	inputs := []struct {
		frame    uint16
		dti      dd.DecodeTargetIndication
		chain    int
		diffs    []int
		selected bool
	}{
		{10, dd.DecodeTargetSwitch, 0, nil, true},
		{11, dd.DecodeTargetNotPresent, 1, nil, false},
		{12, dd.DecodeTargetRequired, 1, nil, false},
		{13, dd.DecodeTargetSwitch, 0, nil, true},
		{14, dd.DecodeTargetRequired, 1, nil, true},
		{16, dd.DecodeTargetRequired, 1, []int{1}, true},
		{15, dd.DecodeTargetRequired, 1, []int{1}, true},
		{17, dd.DecodeTargetRequired, 1, []int{1}, true},
	}
	for i, in := range inputs {
		active := uint32(1)
		descriptor := &dd.DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: in.frame, FrameDependencies: &dd.FrameDependencyTemplate{DecodeTargetIndications: []dd.DecodeTargetIndication{in.dti}, FrameDiffs: in.diffs, ChainDiffs: []int{in.chain}}}
		if i == 0 {
			descriptor.AttachedStructure = structure
			descriptor.ActiveDecodeTargetsBitmask = &active
		}
		ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: 1000 + uint16(i), Timestamp: uint32(in.frame) * 3000, SSRC: 101, PayloadType: 98, PayloadSize: 5, Marker: true, IsKeyFrame: i == 0, VideoLayer: buffer.VideoLayer{Spatial: 0, Temporal: 0}, ArrivalTime: time.Now()})
		require.NoError(t, err)
		ep.DependencyDescriptor = &buffer.ExtDependencyDescriptor{Descriptor: descriptor, DecodeTargets: buffer.ProcessFrameDependencyStructure(structure), StructureUpdated: i == 0, ActiveDecodeTargetsUpdated: i == 0, ActiveDecodeTargetsMask: 1, Integrity: true, ExtFrameNum: uint64(in.frame), ExtKeyFrameNum: 10}
		result := selector.Select(ep, 0)
		require.Equal(t, in.selected, result.IsSelected, "fixed original protected chain/frame %d", in.frame)
		row, err := json.Marshal(map[string]any{"frame": in.frame, "dti": int(in.dti), "chain_diff": in.chain, "frame_diffs": in.diffs, "selected": result.IsSelected})
		require.NoError(t, err)
		fmt.Println("SOURCE_AV1_CHAIN " + string(row))
	}
}
