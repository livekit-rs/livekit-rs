# Fixed-source ICE nomination acceptance waits

Candidate source only: not compiled or executed by the implementing child.

The four official `rtc.{host,srflx,prflx,relay}_acceptance_min_wait` leaves are
parsed previously, but before this patch were absent from runtime configuration
and native ICE selection. They now reach the actual controlling agent's pair
nomination and USE-CANDIDATE emission. This is a functional selection delay,
not a delay in publishing a connected event.

## Fixed Go contract

- `mediatransportutil-f234b534b095/pkg/rtcconfig/webrtc_config.go:206-218`
  sets each corresponding SettingEngine duration only if strictly positive.
- Fixed `livekit/ice/v4@v4.4.0-warp.3/agent_config.go:29-42,257-278`
  inherits host 0, srflx 500ms, prflx 1s, relay 2s. The unexposed relay-only
  CandidateTypes special case is not the server's default all-kind agent.
- `selection.go:controllingSelector.Start/isNominatable/ContactCandidates`
  chooses the best valid pair first and requires both its local and remote
  candidate kinds to be old enough. A weaker eligible pair does not bypass it.
  Controlled USE-CANDIDATE responses are not subject to these waits.
- `agent.go:startConnectivityChecks/setSelector` starts selection when checks
  start and restarts it after role selection. In the Rust sans-I/O model the
  first authenticated connectivity input / timeout after remote credentials
  starts the clock; constructing an idle transport cannot spend the wait.
  Role conflict uses its actual authenticated request/response time; ordinary
  packets do not change the separate STUN Ta pacing clock.

Rust's valid local mapped candidate may be peer-reflexive even when the
original checklist candidate was host. Eligibility uses that actual successful
check's mapped kind; no route is synthesized or changed for this policy.

The generic `is` and `str0m` libraries retain their existing zero-wait defaults.
Only the application injects the fixed Go defaults and strictly positive
configuration overrides. Nonpositive official values inherit defaults.

## Coordinator commands (must require nonzero tests)

From the new combined server source, using the coordinating agent's owned
resource gate and target directory:

```sh
cargo test -p lk-config --lib ice_acceptance_projection_tests -- --nocapture
cargo test --manifest-path vendor/is/Cargo.toml acceptance_ -- --nocapture
```

The native tests drive real serialized/HMAC-authenticated STUN checks and
responses, inspect actual USE-CANDIDATE datagrams, cover all 16 kind pairs,
exact nanosecond boundary, mapped local prflx, controlled nomination, generic
zero policy, best-pair ordering and role/restart clocks, and pre-credentials queued role-conflict requests. They also require the
pending nomination timer to wake without waiting for the previous STUN RTO.
Full native/workspace/SDK regression is still required on the combined input.

Original-source observer (8 observations, direct fixed toolchain; private
staging only, original ICE bytes checked before/after):

```sh
python3 tests/interop/ice-acceptance/run-go-observer.py \
  --fixed-ice-source /Users/zoujiaqing/go/pkg/mod/github.com/livekit/ice/v4@v4.4.0-warp.3 \
  --go-binary /path/to/fixed/go1.26.3/bin/go \
  --evidence-dir /new/task-owned/evidence/ice-acceptance-original-go-attempt01
```

The observer appends one private `_test.go` to an exact staged source copy and
calls original `AgentConfig.initWithDefaults`, original candidate constructors,
`controllingSelector.isNominatable` and `Start`. It does not duplicate their
algorithms. Its wall-clock eligibility observations use a 100ms scheduling
margin; exact nanosecond boundary is covered by the Rust virtual clock tests.
It does not claim original Go wire/media interoperability by itself.

## Integration boundary

The branch is based on immutable candidate 27. Native str0m patch chain 1..38
is retained byte-for-byte; the ICE builder patch is a separately named final
patch. The coordinator must append it after the latest sibling native 39/40
rather than replacing their SOURCE.json. The independent `is` source chain
adds one final patch and its original package reconstruction remains exact.
No parent leaf/family/compatibility completion status is upgraded here.
