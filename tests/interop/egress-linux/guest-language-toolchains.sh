#!/bin/bash
# Coordinator executes INSIDE owned guest only. Frozen exact HTTPS artifacts/SHA.
set -euo pipefail
[[ "$(uname -m)" == aarch64 ]]
base="$HOME/fixture/toolchain"
mkdir -p "$base" "$HOME/fixture/cargo-home" "$HOME/fixture/rustup-home"
cd "$base"
curl --fail --location --proto '=https' https://static.rust-lang.org/dist/rust-1.99.0-aarch64-unknown-linux-gnu.tar.xz -o rust.tar.xz
printf '%s\n' '5a30ce742be0835d9b23fc862db5cbbbc71464e1c9b1fca22917e10e4ba32a92  rust.tar.xz' > rust.sha256
sha256sum -c rust.sha256
tar -xJf rust.tar.xz
./rust-1.99.0-aarch64-unknown-linux-gnu/install.sh --prefix="$base/rust" --components=rustc,cargo,rust-std-aarch64-unknown-linux-gnu --disable-ldconfig
curl --fail --location --proto '=https' https://nodejs.org/dist/v24.22.1/node-v24.22.1-linux-arm64.tar.xz -o node.tar.xz
printf '%s\n' 'dbabf7a1cbf947e6059ffad2c27b80041db97dea1e553b62afdee7f13fca336e  node.tar.xz' > node.sha256
sha256sum -c node.sha256
tar -xJf node.tar.xz
mv node-v24.22.1-linux-arm64 node
"$base/rust/bin/rustc" --version
"$base/node/bin/node" --version
# Exact source toolchains; no rustup/latest resolution or host-global installation.
