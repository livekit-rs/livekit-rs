#!/bin/bash
# Coordinator runs only in owned Linux guest after transfer/extraction + hash verification.
set -euo pipefail
: "${SOURCE_ROOT:?guest source root}"
: "${RUSTUP_HOME:?owned guest Rust installation}"
: "${CARGO_HOME:?owned guest Cargo installation}"
export PATH="$HOME/fixture/toolchain/go/bin:$HOME/fixture/toolchain/rust/bin:$HOME/fixture/toolchain/node/bin:$CARGO_HOME/bin:$HOME/fixture/bin:$PATH"
export GOTOOLCHAIN=local CGO_ENABLED=1 CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
[[ "$(go version)" == 'go version go1.26.8 linux/arm64' ]]
[[ "$(rustc --version)" == 'rustc 1.99.0 '* ]]
[[ "$(cat "$SOURCE_ROOT/references/egress/.go-version")" == 1.26.8 ]]
[[ "$(cat "$SOURCE_ROOT/rust-toolchain.toml")" == *'1.99.0'* ]]
# Actual upstream template build required by go:embed, same source as Dockerfile.
(cd "$SOURCE_ROOT/references/egress/template-default" && npm exec --yes --package=pnpm@10.18.3 -- pnpm install --frozen-lockfile && npm exec --yes --package=pnpm@10.18.3 -- pnpm build)
mkdir -p "$SOURCE_ROOT/references/egress/cmd/server/templates"
cp -R "$SOURCE_ROOT/references/egress/template-default/build/." "$SOURCE_ROOT/references/egress/cmd/server/templates/"
(cd "$SOURCE_ROOT/references/egress" && go build -mod=readonly -p 1 -o "$HOME/fixture/bin/egress" ./cmd/server)
(cd "$SOURCE_ROOT" && cargo build --locked -j 1 -p livecloud --bin livecloud)
cp "$SOURCE_ROOT/target/debug/livecloud" "$HOME/fixture/bin/livecloud"
sha256sum "$HOME/fixture/bin/egress" "$HOME/fixture/bin/livecloud" > "$HOME/fixture/binary-sha256.txt"
# Installs unchanged platform-specific Linux FFI dependency from committed lock.
(cd "$SOURCE_ROOT/tests/interop" && npm ci)
(cd "$SOURCE_ROOT/tests/agent-sdk" && npm ci)
