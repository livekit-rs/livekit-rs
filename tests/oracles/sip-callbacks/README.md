# Fixed SIP callback oracle

This executable invokes the **unmodified pinned Go** `service.NewIOInfoService` with the real `service.RedisStore`, then calls `GetSIPTrunkAuthentication` and `EvaluateSIPDispatchRules`. It does not copy the SIP match/evaluation algorithms or substitute an in-memory catalog. Its 48 fixtures include source IP/host/CIDR and phone filtering, ambiguous trunks, explicit-ID lookup, default/specific dispatch, exact legacy versus modern call fields, PIN prompt/accept/reject, privacy attributes, deterministic caller/callee rooms, media/header/feature and RoomConfiguration inheritance, missing rules and disconnected storage.

The module/sums reuse the existing fixed server oracle dependency graph. No new Rust or production Go dependency is introduced. These files do not assert that a Go run has occurred; the coordinator must persist actual execution identities and results.

Run with a **fresh dedicated broker** for Go and a separate fresh broker for Rust:

```sh
cd livekit-rs-server/tests/oracles/sip-callbacks
GOMAXPROCS=2 go run . -redis "$SIP_CALLBACK_GO_REDIS_URL" > "$SIP_CALLBACK_ORACLE_RESULTS"
cd ../../..
SIP_CALLBACK_REDIS_URL="$SIP_CALLBACK_RUST_REDIS_URL" \
SIP_CALLBACK_ORACLE_RESULTS="$SIP_CALLBACK_ORACLE_RESULTS" \
cargo test --locked -p lk-compat workers::sip_callbacks::oracle_tests -- --include-ignored
```

URLs are supplied by the coordinator and must not be logged. The Go executable refuses pre-existing SIP catalog keys and touches only the four known catalog keys after that preflight; its deferred cleanup clears those task-owned keys. The Rust test independently preflights the same four keys in its separate broker and removes them before asserting each actual comparison. No `FLUSHDB`, external SIP endpoint or credential from deployment configuration is used. All credentials in `cases.json` are static public test strings.

Outputs preserve exact actual PSRPC code/error strings and actual Go protobuf bytes. Successful Rust results are decoded into the same response types and compared field-for-field, including optional message presence; map ordering and JSON rendering are not used to waive differences. Ordinary Go Twirp errors are observed through `psrpc.GetErrorCode` rather than assumed to be native PSRPC typed errors. Randomized room IDs have a separate source-bound unit regression; oracle rooms are deterministic, so no SID normalization weakens these comparisons.

The oracle exercises actual storage and callback evaluation directly. It is not a SIP media worker, external carrier call or registered PSRPC round-trip acceptance test. Raw future-field cloning is additionally covered by production-helper unit tests; the JSON fixtures alone do not establish unknown-byte retention.
