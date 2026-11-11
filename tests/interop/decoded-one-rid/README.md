# Official SDK decoded ONE/RID

This campaign uses unmodified livekit-client 2.23.0 to publish VP9 or AV1 with the public `simulcast: true` and `scalabilityMode: 'L1T3'` options. The exact Go v1.13.10 control runs first. Its passing receipt is required for each matching Rust codec and single/dual-PC run.

`LocalParticipant.ts` sets `SimulcastCodec.videoLayerMode=ONE_SPATIAL_LAYER_PER_STREAM` for this public option combination, provided the server version exceeds 1.13.6 and the browser supports modern SVC simulcast encodings. Each campaign verifies the acknowledged public TrackInfo, actual local q/h/f SDP, browser sender parameters and negotiated DD. It then requires positive outbound RTP frames/packets for all three RIDs and three distinct SSRCs. The marker pixels and frame hashes must match the source on a high/low/high series. The temporal cadence gate uses the same actual decoder measurements as decoded-svc v07, including the exact Go receipt. The campaign also checks resume, fresh Track SID, continued low media after high subscriber retirement, true browser process exit, and room release.

The browser reports DD negotiation and actual encoded-frame metadata. It does not expose encrypted RTP extension bytes. The separate physical SRTP fixture is the boundary for literal DD bytes, original fragment ordering, gap padding and reference-clock evidence. This browser profile does not exercise INCOMPLETE_RTCP_SR or no-RID explicit ONE. The installed SDK's normal VP9/AV1 publication defaults to SVC in the absence of the public simulcast option and does not expose those two explicit modes.

Run using the exact immutable server binary and its verified SHA-256. Keep the unchanged SDK dependency directory from tests/interop. Run each codec and each PC topology separately with a fresh absolute output directory.

```sh
python3 /absolute/decoded-one-rid/run-owned-server.py \
  --binary /absolute/original-go/livekit-server --sha256 "$GO_BINARY_SHA256" \
  --harness /absolute/decoded-one-rid --dependencies /absolute/tests/interop \
  --output /absolute/receipts/go-vp9-single --codec vp9 --mode L1T3 \
  --single true --c21 original

python3 /absolute/decoded-one-rid/run-owned-server.py \
  --binary /absolute/rust/livekit-server --sha256 "$RUST_BINARY_SHA256" \
  --harness /absolute/decoded-one-rid --dependencies /absolute/tests/interop \
  --output /absolute/receipts/rust-vp9-single-on --codec vp9 --mode L1T3 \
  --single true --c21 enabled \
  --original-go-receipt /absolute/receipts/go-vp9-single/sdk/evidence.json
```

Use `--c21 disabled` for the separate disabled Rust config. Use the same exact browser executable across matching Go/Rust runs. The browser and server outputs distinguish observed failures and scoped capability blocks. Missing outbound RID or mode data cannot pass as equivalent video.
