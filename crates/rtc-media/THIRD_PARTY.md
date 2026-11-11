# Third-party attribution

The AV1 dependency descriptor reader in `src/av1/descriptor.rs` follows the wire
reader and bitstream layout in LiveKit Server v1.13.10, commit
`e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`:
https://github.com/livekit/livekit/tree/e5655ac20267fa5005dccaf3f0e3bfc3ea688c07/pkg/sfu/rtpextension/dependencydescriptor

The adapted reader retains this upstream attribution:

```text
Copyright 2023 LiveKit, Inc.
Licensed under the Apache License, Version 2.0.
```

The complete, unmodified upstream license is included in
[third-party/livekit/LICENSE](third-party/livekit/LICENSE). `AV1-SOURCE.json`
records the exact source commit and reference file hashes. The local adaptation
uses bounded Rust parsing and transactional structure replacement; its output
encoder, reference-number remapping, OBU selection and str0m integration are
local code. These notices preserve upstream provenance and do not change the
workspace's license policy.

The source-identifier frame-rate calculators in `src/temporal_rate.rs` adapt `pkg/sfu/buffer/fps.go` at the same locked LiveKit commit. Copyright 2023 LiveKit, Inc.; Apache License 2.0 as retained above. The original packet parser/consumer observer is independently staged from fixed upstream sources; execution evidence must bind the current candidate.

Original-packet AV1 source lifetime/selection, frame-integrity ring, GOP-wide frame-number offset and RTP dropped-slot range translation in `src/av1/{packet_receiver,packet_selector,integrity,sequence}.rs` adapt locked LiveKit `dependencydescriptorparser.go`, `dependencydescriptor.go`, `frameintegrity.go`, `framenumberwrapper.go` and `rtpmunger.go` (Copyright 2023 LiveKit, Inc.; Apache License 2.0 retained above). Native raw send primitives preserve real source RTP packet boundaries; controlled original Go consumers provide independent execution inputs. No decoded-pixel claim follows from synthetic RTP payload receipts.
