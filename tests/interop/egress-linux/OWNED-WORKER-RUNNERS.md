# Original worker Linux orchestration

SOURCE PREPARED, NOT RUN by the author. These entry points launch actual binaries
only in the coordinator-owned Linux ARM64 cloud-image guest as fixture, never on
macOS or another user's VM. They do not compile, install, change nft rules or
patch original workers/SDKs. The coordinator completes dependency installation
and runtime loopback network isolation first.

The shared helper loads the existing run-owned-stack.py process observer by
file path. Transfer all four Python files together: run-owned-stack.py,
owned_worker_stack.py, run-owned-ingress.py, run-owned-sip.py. Helper identity is
read dynamically and frozen per campaign; no historical helper SHA is hardcoded.
Root's EGRESS_AUTO_DIR policy remains unchanged.

```sh
python3 tests/interop/egress-linux/run-owned-ingress.py \
 --source-root /home/fixture/fixture/candidate08-source \
 --go /home/fixture/fixture/bin/livekit-official-candidate08-control \
 --rust /home/fixture/fixture/bin/livecloud \
 --worker /home/fixture/fixture/bin/ingress \
 --output /home/fixture/fixture/output/ingress-NEW

python3 tests/interop/egress-linux/run-owned-sip.py \
 --source-root /home/fixture/fixture/candidate08-source \
 --go /home/fixture/fixture/bin/livekit-official-candidate08-control \
 --rust /home/fixture/fixture/bin/livecloud \
 --worker /home/fixture/fixture/bin/livekit-sip \
 --output /home/fixture/fixture/output/sip-NEW
```

Ingress defaults to all four independent URL/RTMP × Go/Rust stacks. Optional
`--profile url` or `--profile rtmp` is a scoped two-case diagnosis. Every case
gets fresh Redis data and original worker, actual HTTP health200, existing exact
probe, and a new output file. URL requires original souphttpsrc fetch, two seconds
of actually decoded 440Hz PCM, normal EOS and independent active Delete. RTMP
requires actual ffmpeg AAC→original ingress child→SFU→native SDK decoded PCM,
PUBLISHING/resource/timestamps and actual handler/publisher teardown on Delete.
A startup/health result or STARTING state cannot satisfy either probe.

The original SIP probe uses real digest-authenticated local UDP SIP peers, actual
PCMU 440Hz→RTC audio, actual RTC 880Hz→PCMU audio, real answered target
INVITE/ACK and REFER/NOTIFY, original BYE and actual RTC participant removal.
It uses the unchanged installed Playwright Chromium (`channel=chromium`) inside
the guest, not an assumed macOS Chrome path. `pw:browser` process diagnostics must
report exactly one actual browser exit0/signal=null, independently of probe PASS.
This remains a narrow local PCMU/Opus profile, not PSTN, TLS, DTMF or all SIP codecs.

Ports are checked free before and after every case: Redis17842, main17880,
RTC TCP17881/UDP17882; Ingress health18090, relay19090, RTMP19355; SIP
TCP/UDP15060, health15061, RTP UDP15100–15199. The main/clients use loopback.
Original worker health/relay listeners may bind wildcard because their public
config has no bind-address option; the already isolated guest network is the
boundary. No invented worker bind field or host firewall modification is used.

The runner clears ambient LIVEKIT overrides, explicitly supplies the public fake
fixture credentials, pins the actual worker directory first on PATH and validates
its original executable basename. It records binary/runner/probe/dependency/lock
and Redis/Node/ffmpeg identities before/after execution. Actual main/worker/Redis
exit0 and port release are mandatory. Its Linux /proc observer tracks only owned
PID/start-time descendants: survivors are failed release gates before narrowly
reclaiming those exact processes, never by names or arbitrary PID matches.

All old failures stay distinct. No Rust sources, timeouts, audio thresholds,
original worker admission costs, SDK versions or success states are weakened.
The source snapshot and binaries must be associated with the real execution
receipt; a source/build success is not media acceptance.
