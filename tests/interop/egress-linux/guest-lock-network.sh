#!/bin/bash
# Runtime-only full-stack-loopback boundary AFTER all package/toolchain downloads.
set -euo pipefail
# No global host network change. Owned disposable guest table only.
sudo nft list table inet livekit_fixture >/dev/null 2>&1 && { echo 'refusing preexisting fixture table' >&2; exit 2; }
sudo nft -f - <<'NFT'
table inet livekit_fixture {
 chain output {
  type filter hook output priority 0; policy drop;
  oifname "lo" accept
  ct state established,related accept
 }
}
NFT
# SSH stays reachable by established response flow, but no guest new Internet,
# host-LAN or 10.0.2.2 connections. Entire media/bus/control stack is localhost.
