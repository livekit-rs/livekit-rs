#!/bin/bash
set -euo pipefail
: "${SOURCE_ROOT:?verified extracted current Rust + fixed source graph}"
: "${FIXED_GO_BINARY:?absolute fresh output}"
[[ $(uname -m) == aarch64 && $(id -u) -ne 0 && ! -e $FIXED_GO_BINARY ]]
[[ $SOURCE_ROOT == /home/fixture/fixture/* && $FIXED_GO_BINARY == /home/fixture/fixture/* ]]
[[ -w $(dirname "$FIXED_GO_BINARY") ]]
cd "$SOURCE_ROOT/references/livekit-stable"
# Original module graph unchanged. Network/toolchain downloads occur BEFORE nft lock.
export GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 CGO_ENABLED=0 GOOS=linux GOARCH=arm64
[[ $(go version) == 'go version go1.26.3 linux/arm64' ]]
go build -mod=readonly -p 1 -o "$FIXED_GO_BINARY" ./cmd/server
sha256sum "$FIXED_GO_BINARY"
