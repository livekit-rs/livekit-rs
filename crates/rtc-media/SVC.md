# VP9 and AV1 SVC implementation and verification

Status: **implemented, with an explicit verified profile**. This is not a claim of
complete support for all codecs, scalability modes, clients, or LiveKit behavior.

## Verified interoperability profile

The recorded browser experiment uses the unmodified LiveKit JavaScript SDK
**2.23.0**, Chrome **155.0.8059.85**, single PeerConnection mode, VP9 profile 0,
and sender scalability mode **`L3T3_KEY`**. The harness verifies the actual
outbound codec and scalability mode rather than inferring them from requested
options. Both endpoints receive decoded video and switch from **240×136** at LOW
to **960×544** at HIGH. Eight successive dimension observations per endpoint
confirm each requested size. JavaScript negotiation rejection collections are
empty.

Evidence (paths relative to the server workspace):

- `tests/interop/output/rust-svc-selected-single-video-vp9.json`: Rust PASS.
- `tests/interop/output/official-svc-single-video-vp9.json`: official Go server
  baseline PASS with the same input and decoded dimensions.
- `tests/interop/output/rust-svc-temporal-fixed-single-video-vp9.json`: Rust
  PASS with default congestion control enabled, spatial selection and actual
  temporal FPS measurements after the source-cadence fix.
- `tests/interop/output/official-svc-temporal-single-video-vp9.json`: official
  Go temporal FPS comparison.

The initial `rust-svc-selected` evidence disables congestion control to isolate
codec negotiation, SVC selection, repacketization and decoding. It establishes
actual decoded interoperability, not just a unit-test result.

The later `rust-svc-temporal-fixed` experiment keeps default congestion control
**enabled**. At a 5 FPS request, each endpoint decodes 22 frames in six seconds;
at 15 FPS, each decodes 90 frames in six seconds. This exceeds the independent
high-rate threshold of 66 decoded frames per endpoint. Spatial selection also
passes; under the active allocator the recorded HIGH observations are 960×544
and 480×272 on the two endpoints, so requesting HIGH must not be described as an
unconditional promise of full resolution under congestion control.

These experiments do **not** establish combined SVC congestion recovery under
imposed bandwidth limits, TURN/TLS, packet-loss resilience, or behavior on other
browsers. Those scenarios need their own evidence.

## Codec selection is part of the implementation

The SDK's publisher offer may still list VP8 before VP9 even when AddTrack asks
for VP9. The official server puts the agreed primary publishing codec first in
its answer. str0m's default answer instead preserves its global codec registration
order, which put VP8 first and resulted in a real VP8/L1T2 sender. This was an
observed interoperability failure, preserved in
`tests/interop/output/rust-svc-trace-single-video-vp9.json`.

`signaling.rs` now obtains the primary codec from the bound or expected
`PublishedTrackSpec` for the publishing MID. `sdp_munge.rs` promotes that codec's
negotiated payload types and associated RTX in the answer. It only permutes
existing payload types; it does not add codecs, change payload identifiers or
modify negotiated format parameters. The regression
`add_track_vp9_primary_selects_vp9_even_when_browser_offers_vp8_first` exercises real
str0m offer/answer objects, checks the primary codec, and accepts the answer on the
client endpoint.

## Frame-mode forwarding

The production engine remains on str0m **0.24.1**'s `MediaData` frame API. No str0m
fork, native codec library, C/C++ media worker, or dependency on a checkout in
`references/` is required by this implementation.

The source API used is `Vp9CodecExtra`: `layers_scheme` supplies cumulative byte
boundaries of up to three depayloaded spatial components, and `tid` supplies their
temporal layer. The frozen source is in
`../references/crates/str0m-0.24.1/src/packet/vp9.rs` relative to the server
workspace. The struct's own SVC example
requires keeping all spatial and temporal dependencies below the selected target.
The upstream frame writer repacketizes media, so it cannot simply forward the
original VP9 RTP layer descriptors.

The local bridge performs the following operations:

1. `svc.rs` validates strictly increasing, gap-free spatial boundaries bounded by
   the frame length and a temporal index in 0–2. Malformed metadata is dropped and
   triggers a throttled keyframe request.
2. `downtrack.rs` acquires or changes a spatial target on a keyframe. Until then,
   an already acquired target continues forwarding. Resume requires a keyframe.
3. For a selected spatial target, the bridge keeps that component **and every
   lower component**. It constructs a VP9 superframe index with correctly sized
   little-endian component lengths. A single spatial component is forwarded as
   its original elementary frame. Concatenating spatial components without an
   index would discard the boundaries needed by a decoder.
4. The resulting frame is passed through str0m's regular VP9 frame writer and
   RTP/SRTP pipeline. Only up to three spatial prefixes are materialized for each
   incoming picture; subscribers selecting the same prefix share an `Arc`.
5. `uptrack.rs` measures cumulative spatial bitrate: S1 costs S0 + S1, not just
   the incremental S1 bytes. Availability follows the observed components.
   All layers share the upstream stream: PLI requests use its RID-less SSRC and
   one throttle bucket, and a stream pause clears all component availability.

The **verified** superframe profile is VP9 profile 0 / `L3T3_KEY` described above.
Other VP9 profiles and scalability modes have not passed the browser gate and are
not covered by this claim. The bridge does not preserve original RTP dependency
metadata as a packet forwarding implementation would.

## Temporal selection

The code estimates the base temporal frame rate from the median of the last nine
clean T0 intervals measured in source RTP timestamps. At least three samples are
required. Keyframes and their adjacent intervals are excluded because PLI can
insert a T0 keyframe or reset the temporal cadence. Stable history survives
consecutive keyframes; duplicates and reordered timestamps add no samples. A
clock discontinuity requires fresh samples.
A nonzero subscriber FPS request selects a T0/T1/T2 ceiling relative to that
estimate; zero means no FPS cap. Changes request a keyframe and take effect at
that keyframe, after which frames above the ceiling are dropped. Before a usable
base-rate estimate exists, the full temporal range is retained.

Temporal filtering and keyframe-gated transitions have executable state-machine
coverage. The first browser FPS comparison found a defect in the earlier instantaneous-FPS
EWMA estimator: in six seconds the official baseline decoded 22/22 frames at a
5 FPS request and 90/82 at 15 FPS, while Rust decoded 24/23 and 47/47. PLI-induced
short T0 intervals could raise the estimated base rate and incorrectly retain a
lower temporal ceiling after the 15 FPS request. The median estimator above has
regressions covering consecutive keyframes and real source-rate changes from
30 to 15 and back to 30 FPS. The subsequent real-browser rerun with default
congestion control enabled passes: 22/22 decoded frames at 5 FPS and 90/90 at
15 FPS over six seconds, including the absolute high-rate threshold of at least
66 frames at each endpoint. This verifies temporal FPS selection for the stated
profile; it does not extend the claim to other codecs or encoder modes.

## Executable coverage

`cargo test -p rtc-media --locked` includes these focused regressions:

- VP9 spatial prefixes preserve lower dependencies and component boundaries.
- Superframe size fields use the required little-endian magnitude.
- Malformed, truncated, non-monotonic, sparse, and out-of-range metadata fails
  closed.
- Acquisition, spatial and temporal target changes are keyframe-gated, including
  pause behavior.
- Consecutive keyframes do not inflate source cadence; actual source FPS changes
  converge in both directions, and cold/resumed streams require clean samples.
- Cumulative layer cost, source-clock temporal ceiling, shared-stream PLI and
  shared-stream pause behavior.
- Codec preference changes only the negotiated payload permutation.
- A real str0m negotiation retains the AddTrack-selected VP9 primary codec.

A separate local FFmpeg experiment accepted an indexed pair of independently
encoded VP9 frames. That only checks index syntax and is **not** evidence for
inter-picture SVC dependencies; the browser result above is the meaningful
interoperability check.

## AV1 dependency descriptor bridge

AV1 SVC now has an implemented, separately verified path. The initial real-browser
combination uses the unmodified JS SDK **2.23.0**, Chrome **156.0.8078.47**, AV1
**`L3T3_KEY`**, single PC and native **WSS + HTTPS RoomService**. The offer and
answer both negotiate the dependency descriptor URI; the harness asserts actual
outbound codec/mode rather than requested options alone. Both endpoints decode
LOW **240×136** then HIGH **960×544**. Over six seconds, 5 FPS requests produce
**22/22** decoded frames; 15 FPS produces **90/85**, above the absolute 66-frame
minimum per endpoint. Audio and reliable/lossy data checks also pass, with no
JavaScript negotiation rejections.

Raw evidence is `tests/interop/output/rust-native-av1-initial-single-video-av1.json`;
the official media/DD comparison is
`tests/interop/output/official-av1-dd-retry-single-video-av1.json`.
[AV1-VALIDATION.json](AV1-VALIDATION.json) preserves their hashes, measured results,
versions and time windows even after ignored raw outputs are cleaned.

This is the **initial combination gate**: subsequent TLS trust-root coverage and
the missing-DD error-path hardening were not in that first binary. The main task
must rerun the full scenario on its final unified binary; this evidence must not
be relabeled as that final source-complete release gate.

### Source and protocol boundaries

- `av1/descriptor.rs` parses mandatory fields, attached template structures,
  frame/chain differences, decode-target indications/active masks, and resolutions
  with bounded counts. Structures replace cached state only after a complete
  valid picture. The parser follows the fixed LiveKit source with attribution in
  [THIRD_PARTY.md](THIRD_PARTY.md) and hashes in `AV1-SOURCE.json`.
- str0m **0.24.1** is minimally patched under `vendor/str0m`. The upstream
  `Depacketized.meta` already contained every packet's metadata, but the ordinary
  frame extension merge lost first/intermediate descriptors. The patch preserves
  ordered nonempty custom extension sets and supplies actual output fragment
  first/last context. It changes no ICE, DTLS, SRTP, codec, pacing or congestion
  implementation. See `vendor/str0m/PATCHES.md`, `SOURCE.json` and the reproducible
  patch file; production never links to `references/`.
- A custom extension serializer negotiates/parses the DD URI. Per-publisher
  receivers collect complete coded-frame descriptors and match their S/T layers
  to explicitly sized AV1 OBUs. Malformed, incomplete or contradictory metadata
  requests a throttled keyframe instead of guessing dependencies.
- Selection retains the target spatial component and its lower OBU dependencies.
  Existing keyframe-gated spatial/temporal selection and robust source-clock FPS
  estimation are reused. Bounded per-subscriber reference history maps input frame
  numbers to outgoing picture numbers. A required external reference that was
  dropped/unknown rejects the picture; a new independent picture can recover.
- Output DD describes one flattened decode target for the selected complete
  picture, remaps its actual external reference distances, attaches a fresh
  structure on an independent picture, and rewrites F/L flags using **actual
  repacketization**. Input packet flags are never blindly copied after fragmentation.
- A clear AV1 track already identified as SVC, or explicitly advertised with
  multiple layers, drops/request-PLI on a missing-DD picture. Non-SVC single-layer
  AV1 that has not established DD remains eligible for ordinary forwarding.

The public-API regression
`av1_fragmented_dd_metadata_survives_public_webrtc_frame_pipeline` uses real
ICE/DTLS/SRTP and a fragmented 4 KiB OBU picture. It checks first/intermediate/last
DD retention and packet flags, injects a missing-DD frame and a persistently
missing first RTP fragment (including blackholed RTX repair), then verifies
independent-picture recovery. These synthetic codec bytes test transport and
metadata, **not pixel decoding**; Chrome supplies the separate decoder gate.
Parser tests cover truncated structures, broad malformed input, custom reference
differences, missing input references, frame-number wrap and OBU size bounds.
The two private-module vendor tests remain **NOT_RUN**; no upstream test-only
native crypto graph was built to claim those gates passed.

### Reproduction

From the server workspace, generate the ignored development certificates using
`npm run tls:fixture` in `tests/interop`, build the current release and run:

```sh
target/release/livecloud --dev --config tests/interop/native-https-config.yaml
```

In a separate terminal at `tests/interop`:

```sh
NODE_EXTRA_CA_CERTS=output/tls/root.pem \
TLS_SPKI="$(cat output/tls/spki-sha256.txt)" \
LIVEKIT_URL=wss://localhost:8443 VIDEO=true VIDEO_CODEC=av1 \
SCALABILITY_MODE=L3T3_KEY SVC_SWITCH=true SVC_TEMPORAL=true \
DURATION_MS=10000 RUNS=1 SERVER_LABEL=rust-native-av1 npm test
```

The CA and SPKI settings above are for generated local fixtures only. The
application still validates actual TLS, API, SDK, media and DD behavior. Unit
and public API regressions use the normal locked production graph:

```sh
CARGO_INCREMENTAL=0 cargo test -p rtc-media av1 --locked
CARGO_INCREMENTAL=0 cargo clippy -p rtc-media --all-targets --no-deps --locked -- -D warnings
```

## Remaining scope

- Successful AV1 and VP9 profiles are explicitly **`L3T3_KEY` on the measured
  JS/Chrome combinations**. Other AV1/VP9 profiles, scalability modes and SDK
  platforms have not passed these browser gates.
- Mixed RID simulcast plus per-RID SVC is outside these bridges. Encrypted-payload
  filtering does not use the clear-payload OBU bridge and is not established here.
- Input reference/structure histories are bounded; missing references or metadata
  can require a new independent picture. No unlimited loss recovery, zero-PLI
  switching, or seamless arbitrary stream migration is promised.
- Repeated spatial/temporal switching under sustained packet loss, multi-region
  paths, and combined bandwidth-impairment/TURN/TLS gates need their own evidence.
