#!/bin/bash
# Root executes INSIDE DISPOSABLE GUEST ONLY, after SSH host-key verification.
set -euo pipefail
[[ "$(uname -m)" == aarch64 ]]
[[ -f /etc/cloud/cloud.cfg ]]
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates curl nftables ffmpeg redis-server libglib2.0-dev libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev libgstreamer-plugins-bad1.0-dev gstreamer1.0-tools gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav
mkdir -p "$HOME/fixture/toolchain" "$HOME/fixture/bin" "$HOME/fixture/output"
curl --fail --location --proto '=https' https://go.dev/dl/go1.26.8.linux-arm64.tar.gz -o "$HOME/fixture/toolchain/go.tar.gz"
printf '%s\n' '211ffced9dcb9633a55eac6364816ec0ddd951389a740e88fa8b3337971bdda0  go.tar.gz' > "$HOME/fixture/toolchain/go.sha256"
(cd "$HOME/fixture/toolchain" && sha256sum -c go.sha256 && tar -xzf go.tar.gz)
sudo mkdir -p /home/egress/tmp
sudo chown -R "$(id -u):$(id -g)" /home/egress
for element in appsrc queue rtpopusdepay opusdec opusenc audioconvert audioresample oggmux filesink; do gst-inspect-1.0 "$element" >/dev/null; done
dpkg-query -W > "$HOME/fixture/guest-package-versions.txt"
# Original image /usr/share/doc/**/copyright remains the package license source.
"$HOME/fixture/toolchain/go/bin/go" version
gst-inspect-1.0 --version
