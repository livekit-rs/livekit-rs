# Actual authenticated whole-RTCP admission

NOT_COMPILED / NOT_RUN. This independent test workspace enables the pre-existing str0m `_internal_test_exports` feature only for tests. Production feature selection remains unchanged. Root owns compilation, lock/feature inspection and cleanup.

`_protect_rtcp` uses the **actual connected peer's DTLS-derived native SRTP send context**. It exports no keys and implements no alternative cipher or RTCP parser. The test first creates genuine ICE/DTLS peers and exchanges actual encrypted VP8. It sends a real protected valid REMB+NACK+TWCC compound, then the same compound with an invalid trailing byte, then bad authentication, then another valid report. In Remote / independent Pion / explicit Native configurations, rejected authenticated reports must produce no native RTCP raw events, receiver estimate, repeated-NACK mutation or external observer report. Authentication failure must never reach the policy. Valid reports and subsequent media must continue. A separate no-hook encrypted control must expose the old partial-prefix behavior; if this control cannot expose it, this mechanism test has not proven the claimed defect.

The actual test fixture configures the public native hook directly. The real Engine factory installs it for every direction/mode in production; its selection is separately inspected/tested in ordinary library regressions. This test does not claim browser or SDK acceptance.

After coordinator adoption and independent review, from `livekit-rs-server/`:

```sh
cargo test --manifest-path tests/conformance/pion-rtcp-admission/Cargo.toml --release --test atomic -- --nocapture
```

A separate workspace requires its own generated lock and pure-Rust dependency/feature audit before acceptance. It must select the same locked patched dimpl/prost-derive/prost-reflect versions; do not silently upgrade the main graph or enable a vendor crypto backend. Clean its task-owned `target` at final delivery. Root may share the main target directory if it records the exact features and still runs the normal production graph gates independently.

Before the new hook is adopted, the test-only old-vs-new mechanism can be reproduced by changing only the hook-install call in the test or replacing only the Session admission conditional in an isolated mutant, retaining all physical assertions. Never weaken the invalid-prefix assertion or force an event into the receiver.
