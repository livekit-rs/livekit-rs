# Native WHIP functional acceptance

The coordinator owns compilation, servers, browsers, commit/push and cleanup.
Scope and actual delivered identities belong to SPEC-0024/0025 and their dated
evidence. This document is the reproducible procedure, not a runtime PASS.

1. From the server workspace run
   `python3 tests/oracles/native-whip/run.py --output /tmp/NEW_PRIVATE_DIRECTORY`.
   The directory must be new. It executes the actual fixed-Go fragment29,
   track14, ClientInfo33, SDPgrammar45, optionalSDP13, videoMutation9 and
   RTPStats6 methods, then the six scoped WHIP Rust comparisons and receiver
   comparison. The toolchain defaults to the actually used Go1.26.3. Preserved
   references and source locks must match; no precomputed Go results are inputs.
2. Run full release workspace tests, strict Clippy/fmt, release build, source
   reconstruction, protocol-schema and dependency/security/license checks.
   Run from the server workspace so its ARMv8 RustCrypto configuration applies.
   Bind the actual final binary hash and source-input manifest before browser
   gates. Never combine partial passes from different candidates.
3. Run `http-oracle.mjs` against pinned official Go and that Rust binary using
   the same profile; `compare-http.mjs GO_JSON RUST_JSON REPORT_JSON` compares
   every one of45 rows. Retain malformed-SDP diagnostics, methods, authentication,
   CORS, ETag/error ordering and missing real owner behavior; no exclusions.
4. Run `run.mjs` against both actual servers with the installed unchanged SDK.
   Require real440/880 decoded content,75packet/8000sample thresholds,20-second
   windows, real candidate PATCH, same-PC ICE restart/newETag/stale-tag failure,
   unchanged participant/publications and actual DELETE/retained SDK observer.
   `WHIP_MEDIA_OUTPUT` selects an owned temporary output path.
5. The native video fixture runs VP8/VP9/AV1 actual640×360→960×540 with40+30
   decoded frames and original20-second windows, same publication and DELETE.
   Both servers must explicitly set
   `api.enable_psrpc_for_get_list_participants:true`. Independently capture the
   fixed Go metadata, then provide it with `WHIP_VIDEO_REFERENCE_RESULTS` to
   Rust; full TrackInfo metadata must match exactly. Correct ONE-mode API base
   dimensions do not permit cropping the actual decoded stream to that base.
6. Run participant-store-view and participant-owner-negative fixtures in both
   actual cached/default and explicit live profiles. Match exact independently
   captured source bodies/statuses; reading must not refresh cached state. Local
   owner waiting cannot stand in for real distributed owner RPC/storage.
7. Run the original Ingress worker's real WHIP proxy fixture in
   `../native-whip-proxy/`: returned worker URL/stream key, native media into
   unchanged SDK, actual nonzero RTC statistics, acknowledged alive>=10s and
   Closed/state callbacks of the same generation. It is an independent leaf;
   direct native media or existing RTMP/URL media cannot substitute for it.
8. Run unchanged SDK extended/dualPC/VP9/AV1 and relevant nativeWSS/TURN-TLS,
   certificate, HTTPSWebhook regressions. Performance campaigns keep their exact
   original gates and historical results; this functional batch does not claim
   a new performance baseline or complete bandwidth-estimator parity.
9. Persist only safe source/binary-bound evidence. Update only genuinely tested
   TODO leaves, keeping nonUTF8SDP, VP9ONE-SVC and remote-owner families separate
   until their own actual acceptance. Commit/push and verify remote identity;
   stop owned processes, remove task private artifacts and finally cargo clean.
