module livekit-rs-tests/pionrtppaddingoracle

go 1.24.0

// The exact github.com/pion/rtp revision that the fixed livekit-server resolves
// through github.com/livekit/webrtc-pion/v4 v4.2.18-warp.6. go.sum is committed
// so `-mod=readonly` with GOPROXY=off resolves offline and fails loudly on drift.
require github.com/pion/rtp v1.10.5

require github.com/pion/randutil v0.1.0 // indirect
