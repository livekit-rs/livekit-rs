# Fixed source network consumers (public replay inputs)

These source-only observers retain the original frozen inputs; no actual output or expected candidates are embedded. `mux/oracle/owner-lifetime/main.go` is the exact Go13 source observer (including its immediate same-ufrag replacement EOF race); `loopback/oracle/loopback/main.go` is the exact Go8 Validate→NewWebRTCConfig→real PC/Agent gather observer. Originals are immutable, with the fixed server module/sum and source identities guarded by their retained manifests. Parent root has executed both; replay here remains explicit, not an assumed runtime PASS.

From the repository root, restore separate new owned stages:

```sh
python3 livekit-rs-server/tests/oracles/network-interfaces/mux/prepare_oracle.py --root "$PWD" --out NEW_MUX_STAGE
python3 livekit-rs-server/tests/oracles/network-interfaces/loopback/prepare.py --root "$PWD" --out NEW_LOOPBACK_STAGE
```

Then from NEW_MUX_STAGE, using the unmodified fixed server MVS:

```sh
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./owner-lifetime
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./full-agent-existing udp-existing
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./full-agent-existing tcp-wildcard
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./multiport
```

From NEW_LOOPBACK_STAGE:

```sh
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./loopback
```

The source-only all-interface predicate/actual-mux observer is `go run -mod=readonly . cases.json`; mapped IPv6 edge observer is `go run -mod=readonly ./edges`. The original Linux alias-only `./full-agent` profile is retained unchanged; a missing required127.0.0.2/.3 bind is a setup failure, not a skipped PASS, and the historical Darwin failure must remain. Existing-interface profiles avoid adding aliases and preserve actual Host rewrite/dedup and CPU/port round-robin rules.

Keep exact output privately until safe hash/count/relationship extraction. Raw rows may contain private interface addresses, ports and ICE credentials. Source Go13 same-ufrag immediate GetConn can wrap the closed underlying mux before its asynchronous removal and yield EOF; do not turn that row into timeout or claim it proves a fully retired replacement. Runtime successful-generation tests are a different boundary. Source Go8 false-loopback-only intentionally has zero candidates; that is successful source behavior. Explicit NodeIP127 alias from a physical non-loopback base is valid and must not be rejected by an alias==base assumption.

The original component README/SOURCE files remain copied byte-for-byte as historical provenance. Their old /tmp command examples describe original staging; use the public paths above for current replay. Go is a test reference only, with Pion MIT and LiveKit Apache source notices retained beside this README. No private mock Net replaces actual sockets or Agent gathering.

Current Rust loopback consumer uses the **actual** full test selector
`interfaces::configured_port_tests::normalized_development_does_not_override_explicit_loopback_interface_admission`
with `cargo test -p livecloud --lib ... -- --exact --nocapture`.
The frozen component README's old `interfaces::tests` spelling is historical;
zero selected tests are a setup failure, never acceptance.
