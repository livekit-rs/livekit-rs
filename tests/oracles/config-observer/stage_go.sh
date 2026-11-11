#!/bin/sh
# Stage a COPY of references/livekit-stable with the observer test in cmd/server
# and build the test binary. references/ is never modified.
# Usage: stage_go.sh <repo-root> <work-dir>   -> <work-dir>/config-observer.test
set -eu
REPO=$(cd "$1" && pwd)
WORK=$2
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$WORK"
WORK=$(cd "$WORK" && pwd)
rsync -a --delete --exclude .git "$REPO/references/livekit-stable/" "$WORK/src/"
cp "$HERE/go/config_observer_test.go" "$WORK/src/cmd/server/config_observer_test.go"
cat >>"$WORK/src/go.mod" <<EOF

replace github.com/livekit/protocol => "$REPO/references/protocol-stable"
replace github.com/livekit/mediatransportutil => "$REPO/references/go-modules/mediatransportutil-f234b534b095"
EOF
cd "$WORK/src"
GOTOOLCHAIN=go1.26.3 GOFLAGS=-mod=mod go test -c -o "$WORK/config-observer.test" ./cmd/server
echo "$WORK/config-observer.test"
