# Actual external-worker HTTP URL pull functional fixture

ACTUAL SCOPED PASS against pinned official Go and final Rust candidate, with
unchanged original external worker and official SDK. See F09 delivery evidence.
All historical failures retained; full Ingress and native WHIP remain open.
Coordinator owns builds, processes, evidence, submission and cleanup.

## Source-driven format and lifecycle

Fixed unchanged Ingress v1.5.0 commit
363f6090d572db8eef5b60c273c0970826fb7ca6. `pkg/media/urlpull/source.go`
creates real `souphttpsrc` for HTTP; caps allow `audio/ogg`, NOT WAV/audio/x-wav.
Thus positive fixture generates actual OGG/Opus, not an assumed WAV positive.
Real path: public CreateIngress URL_INPUT2 -> IngressInternal.StartIngress ->
Service.StartIngress/HandleURLPublishRequest -> original run-handler child ->
real GStreamer decode -> SDK RTC publish -> real SFU -> native PCM subscriber.
No stub StartIngress server, fake callback or codec implementation is used.
`pkg/service/handler.go` final successful nonreusable URL sends ENDPOINT_COMPLETE4
(not reusable INACTIVE0). Actual error remains ERROR3 and fails fixture.

## Run prerequisites

Same native worker/Basename-PATH/GStreamer requirements as
../ingress-rtmp/README.md. Parent worker binary MUST be called ingress
on owned PATH because original source exec.Command("ingress",...) runs cwd `/`.
Use dedicated worker with zero pre-existing handler children. Shared real Redis
and Rust main/IOInfo are already root-owned. Old unchanged psrpc0.7.1 worker
was exercised with default adapter + global compression quality0; positive gzip
compatibility remains OPEN. Native WHIP is unrelated and not exercised here.

Set INTEROP_PROJECT (locked unchanged ServerSDK2.19.2 project), AGENT_SDK_ROOT
(locked unchanged rtc-node1.2.1 installed project), LIVEKIT_URL, private API
credentials, actual INGRESS_WORKER_PID, and new INGRESS_URL_OUTPUT. Optional
FFMPEG_BIN defaults ffmpeg. Root executes `node tests/interop/ingress-url/run-url.mjs`.
Actual Go compiler/worker builds and services remain exclusively root-owned.

Fixture itself creates bounded temporary OGG files through actual installed
ffmpeg libopus (15s EOS and60s active Delete), each <=8MiB. It starts a real
owned local HTTP server bound only127.0.0.1 ephemeral port, supports correct
HEAD/GET/single byte-range/416 for GStreamer, counts real accesses, serves exact
bytes and retains no HTTP bodies in committed evidence. It joins actual RTC
subscriber before starting URL jobs. No synthetic PSRPC input is emitted.

Each separate URL job must be nonreusable and must launch an actual handler
child of actual worker. Real List state must be PUBLISHING2 with nonempty,
distinct resource generation and positive callback timestamps. Real HTTP fetch
must occur; unchanged AudioStream must decode >=96000 actual mono48k samples,
RMS>100 and440Hz projected tone power ratio>0.5. These predeclared functional
checks do not change any existing SDK/network/accuracy/performance gate.

EOS phase waits for actual COMPLETE4 callback, same resource generation and
positive endedAt. It then requires actual handler exit and participant gone,
and deletes the completed catalog through public API. Active phase checks
PUBLISHING immediately before actual public Delete, then requires actual child
exit and publisher gone within15s. In both cases source List-by-ID MUST throw
exact not_found after deletion; empty list is not substituted. Cleanup does not
count as the positive deletion/exit proof.

Root should execute same script first with fixed official server + same exact
unchanged worker, then Rust, each with dedicated state. Save separate actual
campaigns; do not concatenate phases across runs or suppress failures.

## Evidence / ownership

Safe output has booleans/counts/tone statistics/local fixture hashes only, no
stream keys, JWTs, DSNs, addresses, raw child args, generated IDs, PCM or log
bodies. Failure text is SHA256 for privacy; root diagnoses private logs before
safe aggregation. Worker logs may expose streamKey and cannot be committed raw.
Only exact owned public ingress/room are deleted, no FLUSH. Finally closes
native readers/room, public rtc.dispose(), owned HTTP sockets/server, and removes
owned temporary files. It does not stop root-owned worker/main/broker. Root must
wait owned external service exits, hash stable logs and preserve safe proof.

NOT_RUN: actual native start/plugin compatibility, HTTP real pull, PCM decode,
worker callback/generation, EOS and active Delete. Not claimed: URL/SRT arbitrary
codec support, video, WHIP, RTMP (separate fixture), callback race corpus,
security network policy, overload/performance or unknown-wire management matrix.

## Functional decoded-content readiness correction

The original RTMP candidate has real retained module/content failures. In the
coordinator's separate10s diagnostic (NOT acceptance), initial2s content failed
the original tone threshold while a later full content window was good. Neither
old FAIL nor that diagnostic becomes a passing campaign for this revision.

Current candidate imports ../ingress-tone-window.mjs. After actual PUBLISHING,
within the original wait default45000ms, it requires a full contiguous96000
actually decoded mono48k samples with the SAME original RMS>100 and440Hz tone
power ratio>0.5. It retains EVERY incoming decoded sample in a fixed2s FIFO; no
silence skip, PCM filtering, sample repair, synthetic data, codec/SDK changes or
new sleep. Aging samples out at the two-second boundary is the explicit rolling
content-readiness definition, not a claim about startup SLA/performance. Final
assertions and all existing transport, lifecycle and delete timeouts stay intact.

Report includes exact qualifying sample_start_inclusive/end_exclusive, first
96000-sample metrics unchanged, latest window, whole-stream metrics and total
decoded samples/frames. Original first2s never disappears when a later window
passes. The helper uses bounded96000 Int16 PCM slots plus bounded precomputed
phase arrays; it does not retain PCM in evidence. New independent tests include
silence, wrong tone, mixed-tone transient startup, exact sample-count readiness,
long chunked bounded storage and independent direct-signal metric comparison.
Seven independent helper tests and actual Go/Rust media executions passed; safe results retain the first-window and whole-stream metrics.
Existing congestion, SDK画质/恢复 and accuracy standards remain untouched.

## HTTP pacing and retained reference failure

Instant local-object reference attempt produced app-sink EOS before the original
worker SDK transport became ready. That failure remains in evidence. The passed
functional subprofile serves every original object byte in1024-byte chunks with
125ms intervals (8192bytes/s), including correct Range semantics. It changes no
SDK, worker, codec, PCM sample, content threshold or observation/shutdown timeout.
Normal finite-object EOS and an independent actually active Delete both pass.

## State callback diagnostics

`state_observations` retains each phase's CreateIngress projection and every
changed public ListIngress state (presence, status, nanosecond state timestamps,
hashed ingress/resource identity and error), with elapsed time, actual lookup
count and lookup error codes. Repeated identical projections are collapsed. This
observer preserves the 45-second PUBLISHING deadline and active Delete assertion;
real decoded audio alone never satisfies either state gate. It does not attribute
a missing PUBLISHING observation to the catalog without callback or stored-state
evidence. No raw identity, URL, error text or media payload is added to receipts.
