# Official SDK real decoded SVC campaign

Source-ready, **not executed**. No production server or SDK source is changed.
This app uses the exact pinned livekit-client2.23.0 and server-sdk2.19.2 from an
existing installed `tests/interop` dependency directory. Standard browser PC and
WebSocket observation records public objects. The SDK's exported-but-`@internal`
LocalTrack.sender getter is deliberately not used. Sender lookup is exclusively
`RTCPeerConnection.getSenders().track.id`; transforms use the standard public
RTCRtpSender.transform and RTCEncodedVideoFrame.getMetadata APIs.

## One case (coordinator starts/stops servers)

Set explicit absolute inputs and run:

```
INTEROP_DEPENDENCY_ROOT=/absolute/installed/tests/interop \
INTEROP_OUTPUT_DIR=/fresh/absolute/decoded-receipt \
LIVEKIT_PEER_URLS='["ws://127.0.0.1:7880","ws://127.0.0.1:7880"]' \
VIDEO_CODEC=vp9 SCALABILITY_MODE=S3T3 SINGLE_PC=true \
C21_STATE=disabled SERVER_LABEL=rust-final \
SERVER_BINARY=/absolute/frozen/server SERVER_CONFIG_FILE=/absolute/server.yaml \
node /absolute/decoded-sdk/run.mjs
```

Use API key/secret environment only; evidence does not serialize tokens or secrets.
The output directory must not exist. Keep each original failure receipt. Browser
and HTTP app server are task-owned and closed in finally; room and participants
must release. Exit0 is only full PASS, exit1 is failure, exit2 is explicitly
BLOCKED_SCOPED. Unsupported cases cannot become green by successful process
execution. A closed browser connection/static server and zero participants are
necessary completion gates, not a substitute for decoded pixels.

The requested C21 label is recorded honestly as an external owned-server setup.
The runner cannot infer live C21 configuration from that label. Supply binary and
configuration paths so their SHA are persisted and use the actual server fixture
with forward_stats three durations enabled or disabled. Run both configurations;
never assert a label alone proves a configuration consumer.

## Full paired matrix

`run-campaign.py --targets /absolute/targets.json --out /fresh/absolute/matrix
--dependencies /absolute/installed/tests/interop` executes both codecs, both
single/dual PC, modes S3T3 and L3T3_KEY, and every explicit target. Example targets:

```
[
 {"label":"original-go","url":"ws://127.0.0.1:7880","c21":"original","binary":"/absolute/fixed-go","config":"/absolute/go.yaml"},
 {"label":"rust-c21-on","url":"ws://127.0.0.1:7881","c21":"enabled","binary":"/absolute/final-rust","config":"/absolute/on.yaml"},
 {"label":"rust-c21-off","url":"ws://127.0.0.1:7882","c21":"disabled","binary":"/absolute/final-rust","config":"/absolute/off.yaml"}
]
```

Optional `entries` gives distinct owner/remote entry URLs for a real cluster;
entries are not treated as cluster proof without root's owner-routing receipts.
Targets can provide API/TLS environment in `env`. Targets must include original,
enabled and disabled C21 setups. Each child uses a fresh output directory, records
its actual exit, and never starts or alters a LiveKit service. Any failed case
makes matrix exit1; otherwise any blocked case makes exit2. Every mode attempts the high-only phase when actual metadata is available.
No L/S label makes a frame independent or inapplicable: actual dependency IDs
decide. The worker drops lower frames, rejects high frames whose references
include an intentionally dropped ID, then marks the rejected high ID dropped so
its descendants cannot slip through. Dependent keyframes in L3T3_KEY may be
rejected while independently decodable high deltas continue from warm-up; the
actual metadata/rejection/pixel receipts define what was covered. A timeout
remains a real failure for original-Go calibration; it is not automatically
waived to NA by the mode name. Missing browser metadata remains scoped blocked.
The raw Go receipt is needed to distinguish a source-inapplicable dependent-only
interval from a server decoder failure before any narrowly justified case change.

## Actual functional gates

Three official SDK clients join: source, changing high/low subscriber, and a
separate live low subscriber. Source canvas encodes a24-bit epoch/frame marker in
large black/white tiles. Receiver draws real rVFC video pixels, extracts marker
and tile values, stores a reduced pixel hash, decoded dimensions/mediaTime and
presentedFrames. It requires marker values that actually existed at source,
multiple hashes and actual inbound framesDecoded progression. Outbound and inbound
codec are read from actual WebRTC stats; the exact requested scalabilityMode must
be reported active. Publish options alone do not certify an encoder mode.

Stages cover high warm-up, low/high/low/high (actual240x136/960x544 for requested
three-spatial encoders), temporal1/15fps using actual median decoded intervals,
and return to high. A strict functional ratio gate catches a temporal stream
that never resumes; the raw intervals/markers are retained for comparison with
unchanged original Go. These are not throughput claims.

For every requested mode the standard transform records actual spatial/temporal
indices, frameId and dependencies. After warm-up it drops lower spatial encoded
frames, refuses high frames depending on intentionally dropped IDs, and requires
new decoded source epoch pixels at the high resolution. Missing metadata or
unsupported transform is explicitly scoped blocked; actual declared independent
frames depending on dropped data are a failure. No codec fallback, SDK patch,
SDP rewrite or fabricated encoded frame is used. Full source restores and low/high
must recover after the sparse interval.

Then real SDK data and an actual signaling reconnect must retain participant SID
and continue new decoded epochs. Source unpublish settles and must stop decoder
progress; re-publish needs a fresh actual track SID and new-source decoded epoch.
Finally the high subscriber closes while the independent low subscriber must keep
showing fresh source markers. Close cancels video callbacks, disconnects SDK rooms,
stops source capture and workers, closes browser contexts/process, verifies server
participants release and removes the owned room.

## Honest remaining scope

These are moving pixels, unlike the small non-decodable native/SRTP byte fixtures.
They still do not by themselves prove every source DD chain, arbitrary late repair,
restart generation, E2EE transform, packet-level latency or full global platform
operation. Unsupported browser capabilities leave those exact cases open. Preserve
the original Go result alongside final Rust; don't loosen a failing gate because
one browser mode cannot be exercised.


## v03 actual owned browser termination

v01/v02's Browser.isConnected() only established Playwright disconnection, not
actual child termination, so they cannot certify cleanup. v03 copies the existing
proven project `owned-browser-exit.mjs` unchanged, launches a BrowserServer,
connects a public Browser client, and records actual Chromium child PID, OS UID,
birth time, Linux start ticks where applicable, executable path/hash and the
coordinator Node identity. It captures this exact child's bounded stderr with
full-byte SHA, persists private stderr/ownership receipts, closes every public
context before the server and client, and observes the actual child exit event
or real child exit state. Exit0 requires code0, no terminating signal, zero
remaining contexts and no cleanup failures; client disconnection alone is never
accepted. The underlying fixed Go/Rust LiveKit service remains externally owned.


## v04 SDK-mode classification and actual PC topology

Fixed Go v03 VP9/S3T3/single actually failed in the unmodified SDK constructor
with `invalid scalability mode` before media. SDK2.23.0's pinned publishUtils.ts
accepts only /^L(\d)T(\d)(h|_KEY|_KEY_SHIFT){0,1}$/. v04 records that exact installed
source SHA and only classifies BLOCKED_SDK_MODE when both the requested mode is
rejected by that source pattern and the actual SDK stack contains its specific
constructor/publishUtils error. It does not pre-skip the real publish attempt,
modify SDK code, substitute a mode or turn arbitrary server errors into blocked.
S3T3 consequently remains an open unchanged-SDK capability rather than green.

SDK singlePeerConnection is an attempt and can fall back. v04 records each actual
standard captured PC's connected/ICE/signaling state, public sender/receiver and
transceiver mids/directions, actual outbound/inbound video SSRC/packet stats.
The video source must have exactly1 connected PC for single and2 for dual, plus
an actually sending PC; both real subscribers require an actually receiving PC.
Subscriber-only clients may legitimately have only their receiving PC connected
in dual mode: the source's two active PCs establish the dual transport topology.
A source silently falling back from single to dual fails the topology assertion
and cannot be reported as single-PC acceptance.


## v05 pre-encoder standard transform attachment

Actual fixed Go v04 L3T3_KEY single decoded nine stages, but worker total0 while
video flowed means the test transform was not processing the actual sender; it
cannot establish missing browser metadata. v05 installs at public browser
addTrack/addTransceiver/replaceTrack before the source publish begins encoding,
identifies the source by actual public track ID, requires a unique nonclosed PC
sender and records native transform identity. It never calls the SDK internal
LocalTrack.sender getter. The worker records actual onrtctransform startup and
processed frame totals, reports worker runtime/message errors, and a zero
pipeline with encoded frames is FAIL_HARNESS_TRANSFORM, not unsupported-browser
BLOCKED. Only processed frames whose actual metadata lacks required fields can
justify the narrower metadata BLOCKED state. The official SDK source explicitly
says ScriptTransform does not need the legacy encodedInsertableStreams flag;
no such flag workaround was added.


## v06 source-bound cadence parity, not ratio-only green

Rust30 v05 VP9 was previously marked PASS while low decoded FPS was only0.3296
and source markers advanced46 each display, versus original Go3.75fps/stride4.
A high/low improvement ratio alone therefore cannot certify temporal behavior.
v05 and all raw PASS receipts remain unchanged, with this limitation recorded.

v06 Rust cases require ORIGINAL_GO_RECEIPT=/absolute/actual-go/evidence.json for
the same codec, exact mode, PC profile and identical official SDK bytes. The
reference must be actual PASS with childexit0/serverrelease; its immutable bytes
are SHA-bound in the new receipt. Actual low and high decoded cadence must each
be within0.65..1.35 of the original actual cadence AND median source-frame stride
within0.75..1.25 of original. The latter distinguishes true temporal selection
(stride4 for observed L3T3_KEY low) from keyframe-only progression (stride46).
Raw original source marker sequences, all intervals and gate values are retained;
no Go result, harness ratio assertion, requested FPS or server algorithm is
modified. The existing faster-than-low assertion also remains. An original Go
control can run without a reference and produces the reference for its profile.

Matrix Rust targets can provide a `references` mapping keyed by
`vp9-L3T3_KEY-single`, `vp9-L3T3_KEY-dual`, `av1-L3T3_KEY-single`, etc. Values are
absolute actual original evidence.json paths; alternatively pass an explicit
ORIGINAL_GO_RECEIPT per one-profile run. No fake or merely scoped old reference
can certify a new Rust profile.

V07 requests1fps for the stable lowest temporal target. It retains the same original-reference FPS/stride gates and all cleanup/decoded assertions. Request5 ceil behavior is independently required by the original public RTC consumer observer; this harness change does not certify that consumer or the current median-rate estimator as exact. Go control must be rerun with v07; v06 receipts use a different request.
