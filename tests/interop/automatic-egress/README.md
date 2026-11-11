# F105 real automatic Egress worker acceptance — source-only, NOT_RUN

Fixed unchanged worker: references/egress v1.15.0, source identities in SOURCE.json. No alternate recorder, canned media or fabricated IOInfo callback is permitted. Original F96 participant Ogg script remains unchanged as a source-incompatible proposed positive; no historical failure is relabeled.

## Actual source branches

* pipeline.go:230–243 RoomComposite selects SDKSource when audio_only=true AND layout/custom_base_url are empty (ShouldUseSDKSource:565). Thus this narrow room-audio profile does not launch Chrome. A nonempty layout or custom URL changes that conclusion; do not silently set either.
* source/source.go:37–44 dispatches actual SDKSource. SDKSource joins real LiveKit using existing worker credentials and receives genuine tracks. Participant: pipeline.go:337–369 also always SDKSource; Track:414–433 SDKSource + passthrough.
* Worker build is still unchanged cmd/server with go:embed templates (main.go:45), so actual upstream pnpm frozen-lock template build remains a build prerequisite even though these jobs do not consume the WebSource. Default template HTTP listener is not evidence of Chrome usage. Existing guest-build.sh implements pnpm + copies actual generated build.
* Participant sets AudioEnabled AND VideoEnabled true (pipeline.go:349–352). Encoded Ogg contains only Opus codec support (types.go:146); output validation:879–891 intersects required video codecs and rejects it. The old proposed participant Ogg positive is invalid before SDK joining. New dedicated participant AV profile uses actual MP4 H264+AAC; it is separately named and never counted as Ogg evidence.
* Native full worker writes /home/egress/tmp (config/base.go:31); disposable Ubuntu guest is required, no host /home mutation. Worker starts child by egress run-handler; retain egress on owned PATH.

## Coordinator-owned prerequisites and order

1. Use committed tests/interop/egress-linux fetch/boot/build scripts and their recorded upstream/image hashes, with task-owned VM. Root already validated a new image: don't infer VM/compile readiness from this draft. All SDK/main/Redis/worker/fixture execute inside same guest loopback so ICE is real. No host127.0.0.1-as-guest candidate shortcut.
2. Unchanged Egress Go1.26.8 + GStreamer actual installed versions/plugins, official pnpm template build, genuine Linux Rust binary, unchanged ServerSDK2.19.2 and rtc-node1.2.1/FFI0.13.1 native bindings. Bindings declarations retained from immutable npm package; SHA/SRI verified, no library executed or modified here.
3. Dedicated Redis, real main17880 and worker19080. Main keys must be configured and global webhook.urls must include http://127.0.0.1:$EGRESS_WEBHOOK_PORT/egress BEFORE start. Worker ws_url must target actual main. Wait actual bus/worker readiness; fixed sleeps are not readiness. Each run uses fresh output/report path and zero existing worker children.
4. First positive layer explicit-api: run-auto-audio.mjs with AUTO_RECORDING_MODE=room and AUTO_CONFIG_SOURCE=explicit-api; then mode=track. Neither script calls public StartEgress: only actual CreateRoom egress config or real publication can trigger recording. Use same fixture against fixed Go then candidate Rust; don't splice successes.
5. Run-auto-participant-av.mjs uses AUTO_RECORDING_MODE=participant, AUTO_CONFIG_SOURCE=explicit-api, fresh .mp4. Real 640x360 RGBA moving gradient via unchanged VideoSource.captureFrame + real 48k mono sine AudioSource. Video source uses actual monotonic timestamp, real outbound stream.kind=video packet/byte + framesEncoded must progress; submitted buffers alone do not prove RTP. MP4 source codec defaults must actually be H264/AAC or case fails.
6. Only after explicit layer passes, repeat each family separately for preset-api, preset-jwt and inline-jwt using original 45s waits. Main's actual source protoyaml accepts snake_case. Preset must be configured before startup and match output/family; JWT signed RoomConfiguration must not be pre-created via API. Explicit-empty suppression, signed refresh/rejoin, generation deletion and malformed policy ordering are separate OPEN cases, never silently counted by these positives.

Entry command (root inside owned guest, not executed here):

    INTEROP_PROJECT=$SOURCE_ROOT/livekit-rs-server/tests/interop \
    AGENT_SDK_ROOT=$SOURCE_ROOT/livekit-rs-server/tests/agent-sdk \
    LIVEKIT_URL=http://127.0.0.1:17880 LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret \
    EGRESS_WORKER_PID=$OWNED_EGRESS_PID EGRESS_WEBHOOK_PORT=17981 \
    EGRESS_OUTPUT_FILE=/home/fixture/fixture/output/fresh-room.ogg \
    EGRESS_MEDIA_REPORT=/home/fixture/fixture/output/fresh-room-report.json \
    AUTO_RECORDING_MODE=room AUTO_CONFIG_SOURCE=explicit-api \
    node /home/fixture/fixture/f105/fixture/run-auto-audio.mjs

Same for track with fresh path. Participant uses fresh .mp4 and run-auto-participant-av.mjs. No CPU cost override: source default Participant cost2 (service.go:36), so 2vCPU may have insufficient headroom; real worker no-response/resource refusal must remain failure. Root may provision a properly recorded larger VM before acceptance, not fake acceptance or disable admission.

## Unchanged media/lifecycle standards

Room/Track: original ACTIVE wait45s, actual child, real RTP, continue8s, StopEgress, COMPLETE45s, actual FileInfo positive size/duration/ended>started, child exit45s, file-size equality/hash; Ogg exactly one Opus48000 stream, >=5s, >=240000 decoded mono samples, RMS>100, actual440Hz power>.5, real signed started/ended callbacks.

Participant legal AV profile: same original8s/45s lifecycle, FileInfo and >=5s/240000sample/RMS>100/440Hz>.5 audio thresholds; actual MP4 H264+AAC (different legal container/codecs explicitly scoped). Additionally actual video outbound packet/byte/encodedframe progress, ffmpeg decodes >=1 real frame, nonconstant gradient, no substitute black file or callback. All reports bind actual input/binary/worker hashes outside fixture and retain first failure. No runtime PASS claimed.

Cleanup owns only generated room/Egress/audio+video handles/Webhook listener; rtc.dispose after all sources; root waits worker/main/broker/QEMU exits and persists safe hashes/stats then removes task output/target and cargo clean. Existing frozen references and committed public fixtures are inputs, not temporary output.
