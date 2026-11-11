# Fixed SCTP source observers (separate from unfinished production draft)

**Oracle inputs frozen / NOT_RUN by author. Production payload is an unsealed draft: DO NOT APPLY.** Source target is fixed server-selected github.com/pion/sctp v1.12.0, github.com/livekit/webrtc-pion/v4 v4.2.18-warp.6, and retained mediatransportutil-f234b534b095.

`pion-source-inputs.json` records the actual cached module archive SHA256, module zip Go h1,127 original file hashes and a public proxy URL. The coordinator must retain/verify the original reference archive under the source lock before public reconstruction; no module-cache absolute path is part of the preparer contract. The original Rust sctp-proto0.10.7 package is separately exact-archive verified (registry SHA bba218fe...,71files,source commit a2318497...) under original/, not a version update.

Coordinator, once the Pion source snapshot has been retained:

```
/usr/bin/python3 /tmp/f136-sctp-association-knobs/prepare_oracle.py --root /Users/zoujiaqing/projects/livekit-rs --sctp-source /Users/zoujiaqing/projects/livekit-rs/references/go-modules/pion-sctp-v1.12.0 --out /tmp/livekit-rs-f136-sctp-source-stage
cd /tmp/livekit-rs-f136-sctp-source-stage
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./settings cases.jsonl > settings-actual.jsonl
GO_SCTP_KNOB_INPUTS=$PWD/cases.jsonl GO_SCTP_KNOB_OUTPUT=$PWD/association-actual.jsonl GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go test -mod=readonly ./source -run '^TestSourceKnobObserver$' -count=1 -v
/usr/bin/python3 /tmp/f136-sctp-association-knobs/oracle/to_tsv.py --cases cases.jsonl --actual association-actual.jsonl --out association-actual.tsv
```

10 rows per observer, no prefilled expected values. Settings observer calls actual fixed NewWebRTCConfig with ephemeral range settings and reflects exact numeric settings; no listener/gathering is started. Association observer adds a test in an exact original algorithm copy, invoking actual constructor/min-window setter/congestion avoidance/fast retransmit packet serializer. It uses a standard net.Pipe only to satisfy the original constructor's connection ownership; no SCTP handshake is claimed. The actual emitted packet count/sizes are source observations; the Rust adapter losslessly consumes all columns and does not insert tolerances or normalize default/overflow values. The original module package logic is unmodified.

Source policy: signed >0 is tested before uint32 conversion; nonpositive remains default; 2^32 wraps to0 and is subsequently the native default. MinCwnd floors every source cwnd mutation; cwndCAStep uses max(MTU,step); FastRtxWnd is a burst budget with separately MTU-bounded packets, not a larger MTU.

65535 is the local advertised capability and the LiveKit detached-reader buffer. Outbound association max is the **remote** capability, so200KiB to a256KiB peer must remain valid. An inbound65536 message from a deliberately noncompliant peer produces short-buffer and closes its reader/channel; it must not be implemented as an association-wide65535 reassembly failure. The production draft includes an encrypted negative/independent-channel-survival test and an outbound200KiB positive, both NOT_RUN.

Both Pion upstream LICENSE/MIT and Rust package MIT OR Apache-2.0 must accompany any final port/reconstruction. This source observer and unfinished Rust draft are not whole-protocol compatibility or official SDK evidence. No source versions are upgraded.
