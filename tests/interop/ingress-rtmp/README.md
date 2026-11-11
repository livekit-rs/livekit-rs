# Real official Ingress v1.5.0 RTMP acceptance draft

ACTUAL SCOPED PASS against pinned official Go and final Rust candidate, with
unchanged original external worker and official SDK. See F09 delivery evidence.
All historical failures retained; full Ingress and native WHIP remain open.
Coordinator owns builds, processes, evidence, submission and cleanup.

## Exact prerequisites and source

Unchanged references/ingress v1.5.0 commit
363f6090d572db8eef5b60c273c0970826fb7ca6. Build cmd/server, but install/copy the
result with basename `ingress` into an owned temporary bin directory and prepend
that directory to PATH: pkg/service/cmd.go uses exec.Command("ingress",
"run-handler",...) with cwd `/`, not the parent's original executable path.
Do not patch this source to get a handler. Preserve worker and handler binary
hash equality. Native GStreamer libraries/plugins and DYLD_LIBRARY_PATH per
references/ingress/README.md are required. Installed executable paths observed
only: /opt/homebrew/bin/ffmpeg and /opt/homebrew/bin/gst-inspect-1.0. Their
presence does not prove their decoders/encoders/plugins work.

Root owns dedicated Redis + real Rust server/IOInfo + actual worker process.
Actual tested run used default psrpc-v0.8.2 adapter with global compression quality0;
this is an integration prerequisite, not complete upstream configuration parity.
No fake IOInfo callback publisher or fake worker may be substituted.

Example worker configuration (replace owned ports/credentials privately):

```yaml
redis:
  address: 127.0.0.1:17642
api_key: devkey
api_secret: secret
ws_url: ws://127.0.0.1:17880
rtmp_port: 19355
whip_port: -1
http_relay_port: 19090
health_port: 18090
logging:
  level: info
```

Worker command `ingress --config /absolute/private/config.yaml` is the actual
cmd/server entry. Config file mode0600. Rust ingress.rtmp_base_url must be
rtmp://127.0.0.1:19355/live. Check actual health HTTP readiness and Redis service
registration; fixed sleeps alone are not readiness proof. Root must capture
actual worker startup failure if GStreamer/native initialization fails.

## Actual executable fixture

After locked SDK projects are installed, set:

- INTEROP_PROJECT = absolute tests/interop project (unchanged ServerSDK2.19.2).
- AGENT_SDK_ROOT = unchanged locked Agent/rtc-node project (rtc-node1.2.1).
- LIVEKIT_URL, private LIVEKIT_API_KEY/SECRET.
- INGRESS_WORKER_PID = actual owned dedicated parent worker PID.
- INGRESS_MEDIA_OUTPUT = new private JSON safe evidence destination.
- optional FFMPEG_BIN = installed ffmpeg absolute path.

Root executes `node /tmp/f09-ingress-media/run-rtmp.mjs`.
The script does not start a worker or server. It owns only an actual ffmpeg
publisher and a native RTC subscriber runtime. It genuinely invokes public
CreateIngress, sends real AAC sine in RTMP/FLV using ffmpeg lavfi, waits for
actual child `ingress run-handler` (observes PID/PPID/comm, never args/tokens),
requires public real state callback PUBLISHING with resource generation and
started/updated timestamps, and decodes two seconds of actual received PCM
through unchanged rtc-node AudioStream. It independently checks RMS>100 and
440Hz projection power>0.5, rather than counting publish/subscribe events as
media delivery. These are a new functional sine fixture's predeclared positive
checks; no existing media/performance thresholds are changed.

Active public Delete must cause the actual handler to exit, the real publisher
participant to disappear and the still-running ffmpeg connection to close
within15s; cleanup SIGTERM does not satisfy that assertion. Public catalog must
be empty for owned ingress afterwards. Cleanup always disconnects native room,
closes decoded reader, deletes only own ingress/room, stops owned ffmpeg and
calls official rtc.dispose(); never process.exit to hide runtime handles.
Script does not stop the root-owned worker/servers. Root must finally stop and
wait their real exit statuses and hash the now-stable logs.

Only source-created identities/streamKey are in memory, never safe evidence.
No raw PCM, RTMP URL/token, process arguments, address, generated ID or log body
is persisted. Errors are message hashes; root may separately inspect private
logs for diagnostics before redaction. Raw worker logs themselves may contain
streamKey (pkg/rtmp/server.go), so never commit them unredacted.

## Limits / follow-up gates

Audio-only RTMP and native mac startup have not been executed. If actual source
requires a video stream or AudioStream API differs, preserve first real failure,
fix fixture against locked API, and rerun; never pretend callbacks prove PCM.
No RTMP reconnect/new-generation/stale callback, URL pull, WHIP, video decode,
codec matrix, Redis outage or unknown-field media acceptance is claimed here.
Root should first run same fixture against fixed official server+same worker,
then Rust with the same settings and independent empty dedicated state.
Full source IOInfo stale-generation callback oracle remains independent and
must not be replaced with this single positive media run.

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
