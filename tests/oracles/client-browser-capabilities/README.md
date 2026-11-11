# Original browser capability observer144

Run `oracle/prepare.py --fixed-source-root ABS_REPO --out FRESH_DIRECTORY` with Python. It verifies the exact pinned original source and public observer inputs, materializes an owned Go module, and never edits references. Build there with Go1.26.3, `GOMAXPROCS=2 go build -mod=readonly -o observer .`, then execute with CLIENT_BROWSER_CASES pointing at its cases.json and CLIENT_BROWSER_GO_RESULT pointing at a fresh output. Compare actual144 rows using CLIENT_BROWSER_GO_RESULTS and the ignored Rust test old_public_browser_capabilities_actual_source_matrix. The test requires all144 complete input/outcome rows, not a subset.

The original public getters and protocol composition are scoped capability evidence, not original ParticipantImpl construction or native packet proof. Native SDP/AV/DC coverage is in tests/interop/client-browser-capabilities. Unicode EqualFold differs from the separately accepted static configuration ToLower rule. Actual audio NACK policy and RED/FEC packets remain independently open.

Accepted execution, original REDs, fixture corrections and source/build identities are recorded in livekit-rs-docs/evidence/2026-10-28/functional-browser-capabilities-final. Complete authored source packages are archived there; their historical apply scripts are not required by this public runner.
