# Original VideoLayer open-quality consumer observer (1056 cases)

Fixed-Go source-observer paired differential for ledger leaf
`SIGNAL:VideoLayer.OpenQualityConsumers` [1109]. The observer calls the REAL exported
original `pkg/sfu/buffer` consumers on a constructed codec-less `TrackInfo` premise and
emits their ACTUAL internal outcomes; the ignored Rust test compares every row against
the `crates/rtc-media/src/layers.rs` mirrors over the same inventory.

## Observed consumers (Go export -> Rust mirror)

| Go (`pkg/sfu/buffer/videolayerutils.go`) | Rust (`rtc-media/src/layers.rs`) |
| --- | --- |
| `LayerPresenceFromTrackInfo` | `presence` |
| `VideoQualityToSpatialLayer` | `quality_to_spatial` |
| `GetSpatialLayerForVideoQuality` | `spatial_for_quality` |
| `SpatialLayerToVideoQuality` | `spatial_to_quality` |
| `SpatialLayerToRid` | `spatial_to_rid` |
| `VideoQualityToRid` | `quality_to_rid` |
| `GetSpatialLayerForRid` | `spatial_for_rid` |
| `GetVideoQualityForSpatialLayer` | `quality_for_spatial` |

## Run

1. Regenerate the inventory (optional; committed copies are byte-identical):
   `/usr/bin/python3 gen_cases.py` writes `cases.json` and the Rust `include_str!`
   copy `crates/rtc-media/src/video_quality_cases.json`.
2. Stage an owned module (never edits references):
   `/usr/bin/python3 oracle/prepare.py --fixed-source-root ABS_REPO --out FRESH_DIRECTORY`.
   It verifies the pinned `PROVENANCE.json` source + asset hashes, copies `main.go` +
   `cases.json`, renames the module and adds local `replace` directives.
3. Build there with Go 1.26.3: `GOMAXPROCS=2 go build -mod=readonly -o observer .`
4. Execute: `VIDEO_QUALITY_CASES=<stage>/cases.json VIDEO_QUALITY_GO_RESULT=<fresh>.jsonl ./observer`
   (output opened `O_CREATE|O_EXCL`, 0600; refuses to overwrite).
5. Compare all 1056 complete input/outcome rows with the ignored Rust test
   `rtc-media layers::video_quality_source_differential::open_quality_consumers_actual_source_matrix`,
   run with `VIDEO_QUALITY_GO_RESULTS=<fresh>.jsonl`. The test requires the full
   inventory (count, unique ids, exact key set, input echo), not a subset.

## Inventory domain

12 layer premises (empty; single LOW/MEDIUM/HIGH; {L,M}/{L,H}/{M,H}/{L,M,H};
{H,unknown77}; non-canonical signalled spatial; single OFF-quality; {L,M,H} without
rids) x 4 rid spaces (`q/h/f`, `2/1/0`, partial `q/h/""`, empty) x three focused query
families sweeping `query_quality` {0,1,2,3,4,77,-1,2147483647}, `query_spatial`
{-1,0,1,2,3,77}, `query_rid` {"","q","h","f","2","1","0","x"}. Each row emits all eight
outputs, so the fixed representative queries of the non-focused families are compared
too. Layer qualities are NON-NEGATIVE: fixed Go `LayerPresenceFromTrackInfo` indexes
`layerPresence[layer.Quality]` when `Quality<=HIGH`, so a negative layer quality panics
upstream — a documented boundary the Rust mirror safely excludes (already covered by
the M04 component test `undeclared_quality_uses_go_default_but_empty_single_layer_stays_zero`).
Requested query quality may be any int32 (the switch default returns
`InvalidLayerSpatial`, never indexes).

## Scope limit (honest residuals — official tier attests ONLY the eight consumers above)

- This is source-level mapping evidence, NOT real layered SRTP forwarding/keyframe
  resume (component M04 `open_quality_settings_reach_three_layer_srtp_forwarding_and_keyframe_resume`),
  NOT the assembled main server (main tier NOT_RUN), NOT genuine OPEN publisher dynacast
  demand-notification ordering/transition parity.
- `TrackInfo` is always non-nil and codec-less, so `GetVideoLayersForMimeType` falls
  back to `ti.Layers`. The nil-`TrackInfo` guards (`GetSpatialLayerForRid`/
  `GetSpatialLayerForVideoQuality` -> -1, `GetVideoQualityForSpatialLayer` -> OFF) and
  codec-section selection are NOT exercised here; the Rust helpers take already-resolved
  layers, so those guards live above this abstraction.
- The published-metadata numeric sort producer leg (`participant.go` `cloneLayers`
  `slices.SortFunc(int(i.Quality)-int(j.Quality))` in `AddTrack`, and `mediaTrackReceived`
  rewrite mirrored by Rust `rewrite_layers`) is NOT source-observed here (it is an
  `AddTrack` closure needing typesfakes); it stays component M04 evidence.
- `NormalizeVideoLayersRid` / Rust `rid_space_from_sdp` and `RidToSpatialLayer` are NOT
  observed (rid-space normalization is a separate concern; `rid_space` is passed as a
  given input to the rid-mapping consumers). Scoping note: Go `NormalizeVideoLayersRid`
  overwrites the front slots and leaves the tail of the original array, while Rust
  `rid_space_from_sdp` rebuilds a fresh `""`-padded array — these agree on contiguous
  SDP-realistic rid sets but diverge on middle-gap input (e.g. `{h,"",f}` -> Go `{h,f,f}`
  vs Rust `{h,f,""}`). That boundary is recorded here as an unattested residual for a
  future batch, NOT hidden and NOT part of this official-tier attestation.

Accepted execution, identities and falsifiability positive control are recorded in
livekit-docs/server/evidence under the C47 slot.