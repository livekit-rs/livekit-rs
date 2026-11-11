module livekit-rs-test/warp-sped-peer

go 1.24.0

require (
 github.com/pion/webrtc/v4 v4.2.18
 github.com/pion/ice/v4 v4.4.0
)

replace github.com/pion/webrtc/v4 => github.com/livekit/webrtc-pion/v4 v4.2.18-warp.6
replace github.com/pion/dtls/v3 => github.com/livekit/dtls/v3 v3.1.5-warp.3
replace github.com/pion/ice/v4 => github.com/livekit/ice/v4 v4.4.0-warp.3
