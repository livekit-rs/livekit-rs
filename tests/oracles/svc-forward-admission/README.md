# Original SVC successful packet admission observer

Stage only: `python3 prepare.py --root /absolute/livekit-rs --out /fresh/stage`.
Root runs inside stage/source: `go test -count=1 -run '^TestOriginalSvcPacketSuccessfulAdmissionObserver$' -v ./pkg/sfu`.
The observer reuses unchanged original test helpers, constructs original DownTrack, negotiates original Pion DTLS/SRTP peers on in-process vnet, and calls original WriteRTP/pacer.Enqueue. Existing ForceForwardLayerForTest establishes selected targets (allocation itself is outside this observer); no original algorithm is replaced. Every phase waits for actually accepted SRTP to arrive before closing the high target.

24 SOURCE_SVC_ADMISSION rows cover low-only, mixed low/high, real OOO input, temporal T2 rejected by T0 targets, and retired high target. sink_writes/writes are actual original return values; eligibility repeats the pinned receiver_base.go consumer predicate and is not a substitute for the native physical SRTP tests. Root then sets GO_SVC_FORWARD_RESULTS to the actual log and runs `regression::svc_forward_timing::original_go_per_packet_successful_writes_match_native_admission_union --ignored --exact` in rtc-media. No rows/no test execution cannot pass.

Native true per-packet SID/TID is verified through authenticated SRTP depacketization, not inferred from final frame metadata. Rust still assembles/repacketizes codec frames; this work fixes which original packet observations have a successful target, and does not claim independent physical RTP forwarding or pixel decoder certification.

High-only independent source counterexample: selected `^TestOriginalHighOnlyIndependentSpatialPacket$` calls original codec classifier, actual DownTrack and authenticated Pion peer. Rust test-only beforefix failed Some(0)/Some(1), original Go actually returned1 and physically delivered. Do not reuse old receipts for modified source.

Selected `^TestOriginalVp9ProfileKeyframeConsumer$` emits14 original codec profile/header rows. Native reader `packet::vp9::test::original_go_profile_keyframe_rows_match_actual_packetizer_flags` (use actual test --list spelling) additionally checks serialized RTP P bits; env GO_VP9_KEYFRAME_RESULTS binds actual original output. This is header classification/packetizer acceptance, not full pixel decoder coverage.


## Independent AV1 decode targets (source after sparse39)

`TestOriginalAv1IndependentDecodeTargetSelection` calls the unchanged fixed Go
DD selector and `ProcessFrameDependencyStructure`. It emits four
`SOURCE_AV1_TARGET` rows: a high-only independent S2 input is selected; in a full
independent S0/S1/S2 picture target2 selects only S2. The separate
`TestOriginalAv1IndependentTargetsActualDownTrackSrtp` uses actual original
DownTrack/Pion DTLS+SRTP, asserts each real WriteRTP return and the exact single
physical high-component payload, then closes the downtrack. No codec/selector
algorithm is replaced. Run the two named tests in pkg/sfu without `-docker`.

Rust's ignored `av1::tests::original_go_dti_target_rows_match_actual_parsed_frame_ids`
reads `GO_AV1_TARGET_RESULTS` from the actual original log and compares production
DD parsing and retained source frame IDs. The encrypted runtime fixture is
`regression::av1_target_timing::encrypted_av1_independent_targets_retain_exact_components_and_timing_union`;
it also checks mixed low/high sinks, unused out-of-order middle packet, closed
high transport, unpublished source, exact real subscriber payload and cohort
counts. `encrypted_av1_independent_high_target_without_timing_still_forwards`
keeps the C21 collector disabled and requires physical SRTP media reception.
These small coded-body fixtures prove protocol/bytes/admission, **not decoded
video pixels**.

This fix covers sparse independent components and actual DTI/active-target
selection; it does not certify the entire Go DD selector. The Rust frame bridge
still conservatively rejects unknown external references, and Go chain-integrity,
reorder/late repair, stream-restart and multi-structure transitions require their
own original-consumer traces and real decoder campaign. Do not close the parent
AV1/SVC capability based on these tests alone.


## VP9 lower-spatial / higher-temporal admission correction

Original Go27 actual24-row reader failed at mixed/phase0/SN1003: current(S2,T0)
returned one real DownTrack/pacer write for incoming(S0,T2), while low(S0,T0)
returned zero. The fixed original VP9.Select final VideoLayer.GreaterThan check
is spatial-first lexicographic, not independent spatial and temporal ceilings.

The production VP9 path now computes an actually retained spatial prefix: when
incoming TID exceeds the current temporal ceiling, retain only spatial IDs below
the current selected spatial layer; otherwise retain its inclusive prefix.
A lower-only incoming picture can therefore reach an established high target.
The successful original-packet timing predicate uses the same VP9-specific
ordering, while AV1's exact selected-DD-frame union remains unchanged. The reader
uses that real production predicate rather than increasing a fixture's cap.

The new encrypted_lower_spatial_higher_temporal_packet_follows_original_vp9_order
regression sends genuine SRTP(S0,T2) between full spatial pictures. It requires
actual high-sink coded bytes and no low-sink receipt, sample union3+1, unchanged
low-only admission after high retirement, and source/transport release. Existing
spatial/OOO/high-only fixtures and assertions remain. Full source native/SRTP and
unchanged SDK decoded-video regression must pass on the final new input before
acceptance. Native frame repacketization is not being certified as byte-identical
to original per-packet payload descriptors by this correction.


## Real decoded VP9 temporal failure and source correction

Original Go v05 official SDK VP9/L3T3_KEY single truly decoded about3.75fps at
requested5 then15.04fps at requested15. Rust28 truly decoded14.915fps and15.02fps,
failing the unchanged functional ratio. Root preserves those original receipts.
Rust's former VP9 temporal cap only changed on a codec keyframe, whereas fixed
Go downs at actual E and ups at actual U+B. Native output also rewrote all
spatial/temporal components into SID0/TID0 with a final marker; forwarding legal
lower-only high-TID components could therefore create extra displayed pictures.

The correction retains actual U+B/E flags through native metadata independently
of C21. It packets retained coded spatial components separately, preserves actual
SID/TID/D and per-component U, uses one shared picture ID, increments TL0 only
forT0, describes the selected spatial topology, and withholds the final picture
marker when only lower spatial components were retained. Real lower component
bytes are still enqueued/counted under the pinned lexicographic rule; nothing is
changed into padding, fabricated statistics or whole-picture drop to get FPS green.
Ordinary native packetization without this forwarding hint retains old behavior.

The lowerSID/T2 fixture now requests FPS1 and sends five genuine encrypted T0
source pictures before comparing a T0 target. It asserts actual source cadence
and actual downstream caps resolveT0, then isolates the later samples with the
retained real priming baseline. The original counter failure after high-close
was defaultFPS0 legitimately leaving T2 enabled, not a reason to weaken release.
Now the high target must receive authenticated raw S0T2 bytes with non-final RTP
marker, low must not, and these lower-only bytes must not be mistaken for a
completed displayed picture. Existing full spatial receipt is strengthened to
require all separately described coded components and actual three-layer/T0
metadata rather than a flattened superframe-index artifact.

Run the new native forwarding_metadata_tests producer/parser/marker test, all
native tests (including Event size bound) with the production pure Rust provider,
all rtc-media and whole workspace tests, actual Go24 reader, unchanged decoded
SDK original/Rust profiles (both codecs, single/dual,C21on/off) on the same final
source. Source41 byte reconstruction alone is not runtime acceptance.


## Original VP9 prediction references after actual temporal selection

Root's decoded Rust30 VP9 ratio-only PASS was not parity: its low rate was only
0.3296fps/source-marker stride46 (keyframes), original Go was3.75178fps/stride4.
The original receipts and v05 result are preserved; v06 now requires actual Go
cadence and marker-stride bounds, not just a high/low ratio.

Native30 output replaced the original source flexible-reference descriptors by
non-flexible G0 metadata while generating a new picture-ID domain. Native now
retains actual source I/F/P, source picture-ID width and per-component P_DIFF
values and emits true flexible references when present. A bounded per-output
picture map translates each source reference into that target's actually emitted
native picture domain: low target skipping source1001..1003 maps source1004's
reference4 to outgoing reference1; high target forwarding those actual lower
components preserves the corresponding larger difference. Source U/B/E/SID/TID/D,
selected topology and non-final marker behavior from source41 remain. Parsed
reference arrays are cleared per packet so a prior multi-reference tail cannot
leak into a subsequent single-reference frame.

No missing retained reference is fabricated. The packetizer rejects references
that cannot map to a real emitted picture or valid1..127 flexible difference;
ordinary packetization and no-source-ID fixtures retain their existing path.
The actual first source reference profile is available through a debug event
`VP9 original reference profile` (source flexible flag/picture width/PID only,
no media body or credentials). Source without flexible reference information
still uses its existing non-flexible branch; acceptance must inspect actual
codec-mode receipts rather than assume the browser produced a given format.

Run native original_reference_translation_tests and entire native pure-Rust
suite/heapbound, complete workspace/Go24 observer, then unchanged v06 decoded
Go/Rust VP9 and AV1 single/dual/C21on/off with the exact original reference path.
Only real decoded cadence/marker receipts can accept this correction. Named
native patch and SOURCE.fragment.json are supplied separately so the coordinator
can compose its concurrent nominated-transport/RTT patch without SOURCE overwrite.
