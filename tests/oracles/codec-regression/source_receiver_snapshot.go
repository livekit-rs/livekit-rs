//go:build source_codec_observer

package sfu

import "github.com/livekit/livekit-server/pkg/sfu/connectionquality"

// SourceCodecSnapshot exposes only the original receiver's snapshot action.
func SourceCodecSnapshot(receiver *WebRTCReceiver) {
	connectionquality.SourceCodecSnapshot(receiver.connectionStats)
}
