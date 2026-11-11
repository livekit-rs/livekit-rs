# Actual fixed source DataTrack writer observer

Source manifest retains its original pre-execution state; actual Go131 execution and strict Rust consumer PASS are recorded in SPEC0039 and evidence/2026-10-27. No expected output is populated. Fixed LiveKit source files datachannel_writer.go and bitrate.go are copied unchanged except both clock APIs replaced with one explicit test clock, using the exact fixed server module/sums/protocol replacement. No source buffer/branch/arithmetic is changed. Driver controls genuine writer IO seam (BufferedAmountGetter and raw ReadWriteCloserDeadliner) and invokes actual Write, including deadline errors/partial timeout/closed source panic. This component observer is not a physical SCTP/SDK result.

Coordinator:

```
/usr/bin/python3 livekit-rs-server/crates/rtc-media/tests/oracles/datachannel-writer/prepare_oracle.py --root "$PWD" --out /tmp/livekit-rs-writer-source-fresh
cd /tmp/livekit-rs-writer-source-fresh
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly . cases.jsonl > actual.jsonl
```

15 cases / 131 operations: source zero bypass, negative nil-rate panic observation (not silently clamped), exact minimum/target strict greater-than, real tracked drain/busy time vs fixed64KiB, warmup/idle/DCEP/partial-write/deadline/overflow and fixed seeded buffer sequences. The expected row count is 131 (each operation emits once even if source panic). Retain all results including source panic; it does not justify crashing the Rust process or inventing upstream valid-negative behavior.

Related SCTP config source: mediatransportutil rtcconfig.webrtc_config.go sets max message65535; minCwnd/fastRtxWnd/cwndCaStep only positive values and uint32 conversion. They are distinct actual association consumers, not satisfied by projecting scalars or changing writer buffer thresholds. This follow-up must inventory native sctp-proto APIs before choosing any source-faithful implementation; currently OPEN.

License: copied source is fixed LiveKit Apache-2.0; pinned references/livekit-stable/LICENSE is hash-guarded, and final public source fixture will retain its notice/license. Driver is local test code. No new dependency or production source has been introduced.

The original `SOURCE.json` is an immutable source/observer identity, not a live acceptance flag. Actual execution receipts are separate; physical SCTP, unchanged SDK and whole-workspace results are bound to SPEC0039's final binary. Follow-up SCTP knobs/reliability work is tracked by SPEC0040, independently of this accepted writer scope.
