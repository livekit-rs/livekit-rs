# Actual SCTP closed-write fixed-source oracle

Test-only driver, not a production dependency. From the repository root, choose a new private staging directory (the destination must not exist):

```sh
/usr/bin/python3 livekit-rs-server/tests/oracles/prepare-locked-server-oracle.py \
  --repository "$PWD" \
  --oracle "$PWD/livekit-rs-server/tests/oracles/closed-data-channel" \
  --out /tmp/livekit-closed-data-channel-replay
cd /tmp/livekit-closed-data-channel-replay
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly .
```

The preparer verifies fixed server/protocol checkout identities, tracked source cleanliness and this manifest's input hashes. It copies the fixed server go.mod/go.sum and adds local replacements without changing references. This oracle has no stdin cases. Network access or an existing Go module/toolchain cache is needed; it creates a real local UDP ICE/DTLS/SCTP pair, not an external server. The original main.go is unchanged. Do not run a stale developer-local module from this source directory; no go.mod/go.sum is needed here.

Required identities are server v1.13.10 / e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 and protocol 9bb708cdb81918a6c63dd25e915b4644310b45b6. Module versions and replacement forks come from that exact server lock: Pion SCTP1.12.0, datachannel1.6.3 and WebRTC4.2.18 replaced by LiveKit WebRTC4.2.18-warp.6. Preserve both source checkouts; the selected licenses retained here are not a complete source snapshot or dependency cache.

Both reliable/lossy cases establish genuine ICE/DTLS/SCTP, deliver a marker, reset the peer channel and attempt a later write on the retained original DataChannelWriter. Output records the actual typed closed-write result rather than inferring it from a callback. The driver fails handshake/read/write prerequisites but deliberately reports the observed terminal classification for comparison rather than assuming it.

Historical F93 executed both paths. This packaging revision has NOT_RUN status until the coordinator stages and replays it. Full official SDK session renewal remains OPEN; this finite oracle does not certify it. Stop owned processes and remove the private staging directory only after persisting actual evidence. The preparer now quotes local module paths as Go strings; the current checkout was parsed by actual Go. Alternate-path runtime replay remains a separate packaging gate.
