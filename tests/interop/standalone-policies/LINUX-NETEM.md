# Deterministic actual Linux wire RTT controls

This companion runs the same unmodified-SDK standalone RPC/SendData business probes against four fresh independent Go/Rust servers on the owned Linux cloud VM. It builds/installs nothing. The coordinating agent must have finished every other VM campaign before execution; `--exclusive-owned-vm-loopback` explicitly identifies that scope.

```
python3 /absolute/overlay/tests/interop/standalone-policies/run-owned-linux-netem.py \
 --source-root /absolute/guest/repository-root \
 --go /absolute/qualified/go --go-sha256 EXACT_GO_SHA \
 --rust /absolute/qualified/rust --rust-sha256 EXACT_RUST_SHA \
 --interop-project /absolute/installed/pinned/interop \
 --interop-lock-sha256 EXACT_EXISTING_PACKAGE_LOCK_SHA \
 --exclusive-owned-vm-loopback \
 --browser-executable /absolute/installed/chromium-or-chrome \
 --output /fresh/absolute/output
```

`--fixtures` optionally selects this exact overlay's tests/interop directory.
Without `--browser-executable`, original Playwright channel selection remains
available (`--browser-channel chromium` or installed chrome). A diagnostic
`--profile`/`--side` subset is explicitly scoped and cannot certify all4.
Existing official SDK versions remain livekit-client2.23.0 and server-sdk2.19.2;
package/lock/script/helper/binary identities are retained. The shared
owned-browser-exit helper is unchanged. Probe changes are limited to portable
Linux /proc start-tick and actual browser executable identity, plus optional
browser executable selection. All RTP/positive RR/budget/retry/nonce/data/
replacement/cleanup assertions remain unchanged.

Before any case, the runner locks its owned VM-lo fixture and reads the actual
`tc -j qdisc show dev lo`. It accepts **only one default root noqueue0:**; it
refuses preexisting netem/prio/other configuration. With passwordless owned-VM
`sudo -n tc`, it adds only root `401: netem delay20ms`. Actual JSON and textual
qdisc identity/delay are checked before and after every profile. This affects
real loopback packets, including the actual authenticated RTP/SR/RR path; no SDP
rewrite, RTT field injection, fake clock, ICE/TWCC substitution, or longer wait
replaces the original positive-RR assertion. The resulting physical roundtrip
has deterministic nonzero latency instead of relying on incidental loopback
scheduler jitter.

In every exit path, only the still-matching owned401:netem may be deleted.
The complete noqueue baseline must restore exactly, tc binary SHA must remain
stable, and the fixture lock inode/UID must match before its removal. Failure
or contested qdisc identity stays FAIL; it never removes somebody else's qdisc.
No other device/namespace/network configuration is touched. Never run this on
a shared host or alongside other VM campaigns.

Process ownership uses existing Linux OwnedProcessTree plus actual /proc UID,
start ticks, mapped executable/SHA and socket fd inodes. HTTP/TCP/UDP listeners
must all belong to the tracked server PID. Node and Chromium start ticks from
the actual launched child must match the tracked process tree. All original
Node/server exit0, actual Chromium child exit0, contexts0, original stderr hash,
no surviving owned descendants, config removal and all ports released remain
required. Emergency kill/reclaimed descendants fail acceptance. Only tracked
PID/start/UID identities are signaled; no process names or shared outputs are
cleaned. Input hashes are checked before/after and original failures persisted.

Source preparation is NOT execution credit. Root must execute all4 on the exact
qualified immutable binaries, preserve genuine Go controls alongside Rust, and
bind the successful receipts to the final combined source regression.
