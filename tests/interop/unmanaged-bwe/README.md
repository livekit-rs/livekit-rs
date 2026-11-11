# Unmanaged-track BWE actual consumer — candidate20 scratch

Status: source candidate only. No Cargo, Go, server or browser execution by this agent. Coordinator must execute and retain failures. This does not certify C01/C02/C09 or all congestion-control leaves.

Base: immutable candidate18 source archive (BASE.json), not current main/19 media edits. Integrate only the listed diff with three-way review; never replace a whole main file from this scratch.

## Fixed original contract

`pkg/sfu/streamallocator/streamallocator.go:580` calls actual `Track.IsManaged` only when `DisableEstimationUnmanagedTracks`; unknown tracks remain enabled. `track.go:112` means screen-share is unmanaged only when not multilayered. `streamallocator.go:1170-1190` grants unmanaged optimum but deducts it from available capacity only when the flag is false. Original expected bandwidth and NACK aggregations still include all tracks (1279 and1308). `downtrack.go:824-879` clears ABS/TWCC for excluded downtracks; `pacer/base.go:98-133` only registers send-side BWE when the TWCC extension ID is nonzero, while physical probe observations remain.

Official YAML typo `disable_etimation_unmanaged_tracks` remains intact, normalizing into the existing internal field. No schema/default changes or invented alias required. Audio, unknown, camera, SVC/simulcast share remain in BWE. Disabling this estimation never suppresses media or physical statistics. Existing overall probe expected-usage accounting stays source-compatible.

## Real candidate chain

Config -> actual MediaEngine::allocate_transport -> known source/multilayer classification -> per-StreamTx gate. The real SRTP sender omits stale upstream ABS/TWCC, does not consume the transport sequence nor register the packet with native BWE, and preserves payload/SSRC/primary sequence. Shared-transport managed streams remain enabled. The actual allocator keeps unmanaged optimal output but releases its budget to managed tracks when configured.

## Required execution by coordinator

From merged source workspace with isolated target:

```sh
cargo test -p rtc-media --lib unmanaged -- --nocapture
cargo test --manifest-path vendor/str0m/Cargo.toml --lib per_stream_estimation_gate -- --nocapture
```

Require nonzero executed tests: allocator boundary plus native/Feedback engine tests (3 in rtc-media) and wire sender gate (1 vendor). Native and Feedback tests negotiate actual SDP, pump ICE/DTLS/SRTP in memory and require authenticated received RTP, correct per-stream extensions and managed admission differences. These are controlled policy/RTP consumers, not decoded-video or network-capacity measurements. Existing relevant default behavior tests and complete workspace regression must also pass.

Original Go observer calls original `StreamAllocator.IsBWEEnabled` and `Track.IsManaged`, not a copied algorithm. Its six source cases cover default/disabled unmanaged, camera, multilayered share, unknown and microphone. Capacity deduction is covered in Rust actual engine and allocator tests; this observer does not claim an original-Go capacity lifecycle oracle.

```sh
/usr/bin/python3 tests/interop/unmanaged-bwe/run-go-observer.py --go-binary /ABS/GO1.26.3/bin/go --fixed-server-source /Users/zoujiaqing/projects/livekit-rs/references/livekit-stable --evidence-dir /ABS/NEW/OWNED/EVIDENCE
```

Runner privately stages original server, grants write only to owned copied directories, inserts one test observer, uses the original module graph and offline toolchain, hashes source before/after and requires exact six observations/nonzero test count/exit0. Never edits references. Root owns execution and lifecycle cleanup.
