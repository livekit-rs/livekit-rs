// SPDX-License-Identifier: Apache-2.0
// Same real collector initialization as fixed original pkg/rtc/room_test.go:44.
package rtc
import (
 "github.com/livekit/livekit-server/pkg/telemetry/prometheus"
 "github.com/livekit/protocol/livekit"
)
func init(){ if err:=prometheus.Init("test",livekit.NodeType_SERVER);err!=nil{panic(err)} }
