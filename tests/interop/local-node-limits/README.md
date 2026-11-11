# Single-node allocation limits through actual official SDK publication

Status: implementation/tests/harness source, not yet executed or accepted.

Fixed Go `service/roomallocator.go:132-176` calls `GetNodeForRoom` and applies
`selector.LimitsReached` even with the in-process LocalRouter, whose
`GetNodeForRoom` always returns its actual current node. `limit.num_tracks`
compares the sum of inbound/outbound tracks; `limit.bytes_per_sec` compares the
first actual sampled rate's inbound/outbound bytes. Exact equality is full;
nonpositive limits disable their own check. This is an admission behavior,
not a performance optimization. An existing room is also subject to admission.

The candidate shares the existing ConfiguredSelector and actual LocalNode
snapshot through an explicitly installed single-node policy. Public CreateRoom
and AgentDispatch auto-create share the real local creation boundary. WHIP
checks after syntax/auth validation and before StartSession. RTC uses the same
pre-upgrade guard; the executable installs the selector only without Redis.
Redis routing retains its real selected-owner checks. An invalid selector is
therefore rejected in both modes. CPU/sysload selector policies do not invent a
new hard limit on an already assigned local node; the fixed source only applies
the two capacity limits at that point. A stale local record falls through the
same original selector over LocalRouter's single actual node.

The three library tests deliberately use a controlled NodeStatsProvider to
isolate the consumer's boundary/ordering and positive RoomService/AgentDispatch
create/delete semantics. They are **not** evidence that real media counters or
sampling worked. Actual process/SDK acceptance uses the independent runner:

```sh
python3 tests/interop/local-node-limits/run.py \
  --go /absolute/path/to/fixed-livekit-server \
  --rust /absolute/path/to/current-livecloud \
  --output /absolute/path/to/new-evidence-directory
```

No Redis process is started. The runner launches only owned servers and browser
processes and invokes no build, Cargo or npm command. Each server is exercised
with invalid-selector startup, track limit, and byte-rate limit: six cases.
`--only go-num-tracks` supports honest baseline calibration and is explicitly
recorded as an incomplete matrix.

The unchanged installed official `livekit-client` 2.23.0 connects with its normal
dual-PC setting and publishes a real 440Hz browser AudioContext track. Its public
`getRTCStatsReport()` must show over 4096 actual outbound bytes and 50 packets.
There is no manually injected server NodeStats in this campaign. The probe waits
for **actual** CreateRoom responses to switch to the fixed capacity error as the
node's real sampler advances. It requires HTTP500 for both a new room and the
existing media room. A second unchanged official SDK must fail its RTC join,
with an independent authenticated RFC6455 Upgrade request proving actual
HTTP503 and the exact `node has exceeded its configured limit` body for the same room/identity. Passive browser
CDP handshake events remain diagnostic: Chrome can omit the response event on
failed upgrades, so a missing event cannot stand in for a wire-status verdict.

The publisher really unpublishes and leaves. Recovery is based on actual public
CreateRoom success, not a fixed sleep; a subsequent actual official SDK join
must then succeed. All created rooms are removed, the browser is explicitly
closed, the server must exit cleanly and its HTTP/RTC ports must be released.
A surviving owned browser or forced server kill is a failure gate.

Each output records executable and SDK input identities, actual sender stats,
public request outcomes, upgrade status, recovery, exits and cleanup. The
invalid-selector cases establish startup rejection, not byte-identical startup
log wording; full logs remain available for that separate diagnostic verdict.
No test substitutes an error-only campaign for positive publication/recovery.

RTC admission follows fixed `pkg/service/utils.go:ValidateConnectRequest`: the prior node `LimitsReached` check returns HTTP503 / `rtc.ErrLimitExceeded` before `startConnection`. Public CreateRoom and WHIP allocation keep their independent HTTP500 mapping.
