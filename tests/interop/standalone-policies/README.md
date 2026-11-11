# Owned standalone original Go/Rust policies

Use qualified immutable Go v1.13.10 and Rust binaries. This macOS runner builds nothing, installs nothing and starts four independent fresh no-Redis servers: each RPC/SendData profile against Go and Rust. It uses the original pinned SDK project and requires its existing package-lock SHA. Default browser channel is installed Chrome; use --browser-channel chromium for the pinned bundled Chromium.

```
python3 tests/interop/standalone-policies/run-owned.py \
 --go /absolute/immutable/go --go-sha256 EXACT_SHA \
 --rust /absolute/immutable/rust --rust-sha256 EXACT_SHA \
 --interop-project /absolute/pinned-sdk-project \
 --interop-lock-sha256 EXACT_PACKAGE_LOCK_SHA \
 --output /fresh/absolute/output
```

`--fixtures /absolute/server-inputs/tests/interop` allows executing these script-only overlays without modifying an immutable production tree. `--profile` and `--side` select diagnostic scopes; a subset receipt cannot certify all four campaigns.

Every probe retains its existing business/RTT/nonce/data assertions. Chromium is created with launchServer and connected through its owned endpoint. The shared owned-browser-exit helper observes actual child exit, closes all contexts before the server and persists owned stderr bytes/hash; receipts require actual code 0, no signal and zero contexts. Node/server PID birth time, UID, mapped executable identity, executable hashes and exact owned socket PIDs are checked. The child Node receipt must match the tracked Node PID/birth. Server binaries, SDK lock and fixture/helper inputs are checked before and after execution.

Only a fresh allowlist environment reaches servers/probes, excluding inherited LIVEKIT_*, Redis, Node flags, loader or proxy settings. Public dev fixture credentials are the only configured keys; no Redis is configured. Each owned config uses fresh HTTP/TCP/UDP ports, is hashed and persisted as public YAML, then removed. The runner signals only tracked PIDs whose birth/UID remain unchanged; emergency Chromium cleanup additionally checks the recorded mapped executable and SHA. Probe/server exits must both be 0, Chromium exit must actually be 0, and all three server ports must be released. Emergency kills fail acceptance. No unrelated process, shared build output or reference checkout is touched.

RPC genuine nonzero subscribed-media RR remains mandatory; failure on loopback is retained and must be diagnosed with real media timing, not weakened to ICE/TWCC/signaling RTT. LocalRoom SDK delivery is a positive consumer profile, not joint forced-congestion/caller-cancel acceptance. Server/probe/browser logs and original failures remain under the selected output. They contain only task public dev credentials; coordinator persists necessary reproducible evidence and handles final delivery/cleanup.
