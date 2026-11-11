# Local patch: frame custom-extension metadata

Upstream: str0m **0.24.1**, commit
`8c4c5284cb11b1e3c82962639cda5b3e95af73e8`.
The original package identity, source files and upstream MIT/Apache license text
are retained. This is a patched copy of str0m, not a newly authored protocol stack.
`SOURCE.json` records hashes of the original files. Apply
`frame-extension-metadata.patch` to the frozen published source to reproduce the
local source changes.

The application uses a root Cargo `[patch.crates-io]` pointing here. It never
links against the ignored `references/` checkout. The patch is the explicit
metadata extension exception recorded by SPEC-0006, not a silent dependency fork.

## Why the frame API needs this extension

`Depacketized.meta` already retains all incoming packet headers, but `ext_vals()`
merged only the last header with selected values from the first. An AV1 picture
can contain multiple coded spatial frames with different dependency descriptors,
and its first packet can attach the template structure. Keeping just the merged
value loses information required for dependency-safe SVC selection.

The frame payloader also lacked a custom-extension view of the actual output
fragment boundaries. Reusing input descriptor first/last flags after codec
repacketization is incorrect.

## Exact changes

- `FrameUserExtensions` retains ordered **nonempty** custom-extension value sets
  in the existing `MediaData.ext_vals.user_values` container. There is no new
  required field in `MediaData` or application packet structures. The list is
  bounded by the existing depacketization packet limit.
- `FramePacketization { first, last }` exposes the output sample's true fragment
  boundaries to custom serializers. It is inserted only when a sample has custom
  user values. Plain media remains on its original path.
- The two types are publicly re-exported and `UserExtensionValues::is_empty()`
  supports the conditional behavior.
- Focused internal regressions verify retention of intermediate packet values
  and fragment-boundary context across actual packetization.

No ICE, DTLS, SRTP, codec implementation, pacing, or congestion algorithm is
changed. The existing AV1 codec packetizer/depacketizer remain upstream code.

## Regression commands

The application-level regression traverses actual public str0m frame APIs,
ICE/DTLS/SRTP, incoming per-packet custom metadata and outgoing first/middle/last
packet flags, using the existing workspace target/cache and production graph:

```sh
CARGO_INCREMENTAL=0 cargo test -p rtc-media av1_fragmented --locked
CARGO_INCREMENTAL=0 cargo test -p rtc-media av1 --locked
```

The application also performs real unmodified-LiveKit-SDK browser decoding;
internal metadata tests alone are not an AV1 interoperability claim.

The two focused private-module upstream tests are retained in this patch but are
**NOT_RUN** in this phase. The vendor package is intentionally excluded from the
application workspace; running its own original development graph would compile
additional test-only crypto/native dependencies. No passing status is inferred
for those unexecuted private tests from the application integration result.

## Independent patch: read-only BWE observations

`bwe-observations.patch` applies after the frame metadata patch. It adds
`Rtc::bwe().snapshot()` with raw native estimate, acknowledged throughput,
current delay/loss limiting cause, ALR state, actual matched TWCC receipt time,
latest scheduled native cluster and latest feedback-validated probe result,
including the earliest local send time actually included in the measurement.
Late feedback processing does not turn an old measurement into new proof.

`can_probe` remains a transport capability flag. Neither periodic estimates nor
cached RTT imply newly received feedback. Snapshot reads and local timeouts do
not advance `last_twcc_feedback_at`; it advances only when a real TWCC report
supplies matched send records. A BWE reset clears the observations together with
the existing estimator state. Scheduled probe rate is explicitly separate from
measured probe bitrate and must never be allocated as proven capacity.

This patch does not alter native GCC, ALR, probe scheduling, estimator arithmetic,
packet pacing or SDP. Native probe target/result debug records aid correlation
with the application's per-transport allocation log. Application allocation
policy is separately implemented and tested outside this vendor directory.

Public-API regression (production provider and actual ICE/DTLS/SRTP/TWCC):

```sh
CARGO_INCREMENTAL=0 cargo test --release --locked -p rtc-media --test bwe_snapshot
```

The bootstrap test checks absent feedback before receiving any peer data. The
connected test checks positive feedback/probe observations, then isolates the
sender for longer than the application's stale interval and verifies local
periodic processing cannot create fresh feedback. Reset must clear stale proof.
Upstream license and attribution remain unchanged. The intended exit is an
upstream observation API providing equivalent receipt/probe/cause semantics,
after which this observation patch can be removed independently of the AV1 patch.

The batch quality is explicitly an instantaneous batch observation. It is not a
rolling loss window or a counter of every report since the consumer's previous
poll. Consumers sampling snapshots infrequently must not infer that an unseen
batch had no loss. The application retains its separately tested real-feedback
RTCP-window safety policy; this additional batch record is diagnostic.
Empty batches do not replace the latest record; reset clears it. The public API
regressions additionally cover matching receipt/quality timestamps, lossless
real-link counts, retained quality during silence, reset and empty/partial/
all-lost loss-fraction arithmetic.

## Independent algorithm integration patch: classic GoogCC ALR / ACK exit

Apply `googcc-alr-ack.patch` **after** `bwe-observations.patch`. This patch is
separate from the read-only observations and changes exactly these integration
semantics:

- Every actually sent tracked RTP payload, including native padding and probe
  packets, is charged to the existing ALR budget. No desired rate or requested
  padding amount is charged before transmission. Existing payload-byte units
  are preserved; this is not a claim of bit-exact Google wire accounting.
- On actual feedback, a transition from previously observed ALR to non-ALR arms
  the ACK estimator with that feedback's local time. Empty feedback cannot arm
  it. Acknowledgments for packets sent before or at that time cannot trigger it.
- The first acknowledged packet sent **after** the exit raises the existing
  Bayesian variance by 200 exactly once. It never assigns an estimate to an
  application request, scheduled probe or guessed capacity. A later distinct
  ALR exit may arm another adaptation.

The source is Google WebRTC classic GoogCC at fixed commit
`202acee882062f7ca632472e4810f25938828f6a`, especially
`acknowledged_bitrate_estimator.cc`, `bitrate_estimator.cc`,
`goog_cc_network_control.cc` and the traced RTP sent-packet path. Original BSD
attribution and patent grant are retained in `GOOGLE-WEBRTC-LICENSE` and
`GOOGLE-WEBRTC-PATENTS`. The source lock and reference clone are documented in
`SOURCE.json`; no C++ code is linked into production.

Current Google defaults to `RobustThroughputEstimator`. This patch completes
only the classic ALR-exit wiring missing from str0m's existing Bayesian path; it
is not a port of the current default robust algorithm. The estimator windows,
Bayesian formula, native GCC, probe arithmetic, pacing rates and network/browser
acceptance thresholds otherwise remain unchanged.

The application public test exchanges real ICE/DTLS/SRTP/TWCC, establishes an
actual audio-only ALR period, then proves **native transmitted padding** can end
ALR and reach the ACK gate before video starts, followed by actual VP8 frame
receipt. Direct source tests compile the exact vendored ACK module using the
same `str0m-proto` unit types; they exercise queued pre-exit feedback, strict
send-time ordering, exactly-once variance adaptation, a later independent exit
and the original estimator window vectors without enabling upstream dev crypto:

```sh
CARGO_INCREMENTAL=0 cargo test --release --locked -p rtc-media --test native_ack_source --test bwe_snapshot
```

`ack_fast_rate_changes` is a read-only wiring counter, not probe success or proof
of increased capacity. End-to-end congestion baseline, pause and recovery remain
separate unchanged browser gates. The intended patch exit is an upstream release
with equivalent classical ALR integration or a separately reviewed complete
replacement of this acknowledged-throughput estimator.

## Google default robust throughput (fourth sequential patch)

Apply `googcc-robust-throughput.patch` after the three patches above. It
replaces the production Bayesian ACK estimator with a Rust port of Google's
current-default `RobustThroughputEstimator`, fixed at commit
`202acee882062f7ca632472e4810f25938828f6a`. The classic module and its five
vectors remain historical audit inputs; they no longer drive production.
`ack_fast_rate_changes` remains API compatible and is always zero on this path.
Actual transmitted RTP, including padding, still feeds native ALR accounting.
Robust ALR notifications are no-ops, matching the reference.

The port uses the default 20/500-packet, 1/5-second window, ten required
acknowledgments, one-second severe-reordering limit, batch insertion before
trimming, receive-gap correction and minimum of measured send/receive rates.
It consumes only actual TWCC acknowledgments after native deduplication. Lost
records and requested rates cannot create throughput. All relevant RTP is
TWCC-tracked, so reference `prior_unacked_data` is zero. Other field-trial
settings are outside this port. Existing GCC delay/loss, pacing and probing
formulas remain unchanged. Existing str0m nanosecond/floating-point units are
retained; Google uses microseconds/integer bps, so outputs are not bit-exact.

The original BSD license and patent grant remain in `GOOGLE-WEBRTC-LICENSE`
and `GOOGLE-WEBRTC-PATENTS`. Package metadata declares the combined obligations
as `(MIT OR Apache-2.0) AND BSD-3-Clause`; the original manifest license and
SHA-256 remain recorded separately in `SOURCE.json`. No C++ dependency is added.
All four patches reconstruct fourteen changed/new files exactly from the
published str0m 0.24.1 inputs. Source hashes and reconstruction scope are recorded
in `SOURCE.json`.

```sh
CARGO_INCREMENTAL=0 cargo test --release --locked -p rtc-media --test native_robust_source --test native_ack_source --test bwe_snapshot
CARGO_INCREMENTAL=0 cargo test --release --locked -p rtc-media --lib
```

The first command passes 13 synthetic vectors compiled from the exact production
Robust module, five historical classic vectors and four actual WebRTC API tests.
The second passes all 111 media unit/regression tests. The public tests verify
actual authenticated TWCC replay deduplication, feedback freshness, native
padding exiting audio ALR, measured robust throughput and more than fifty VP8
frames received. Synthetic vectors include measured 48 kbps audio, 4 Mbps probe
and 1.8 Mbps video steps, loss, reordering, severe clock jumps, batching and
silence. They are explicitly synthetic, not recorded browser packet replays.
Unmodified-SDK browser baseline, bounded pause and recovery remain separate
acceptance gates with their original thresholds.

## Probe timing and feedback accuracy (fifth sequential patch)

`probe-timing-accuracy.patch` applies after the default Robust ACK patch. It adds
read-only packet-train input diagnostics and aligns the explicit Google pacing
safety guards at the same fixed source commit:

- Cancel an actually transmitting train strictly more than 10 ms beyond its next
  probe deadline. Never mark an unsent queued request late by assuming it sent.
- Expire queued cluster requests older than five seconds. Match Google's
  pre-insertion `size > 5` trimming: one active and up to five pending clusters
  can remain. Every removal has an end event; draining end events cleans
  generated padding from both regular RTX and fallback probe sources, preserving
  media, retransmissions, packet cache and SRTP sequence continuity.
- Revalidate congestion and the current demand ceiling before dispatching the
  next element of a deferred controller vector. This adapter returns one config
  per timeout rather than Google's whole vector, so conditions can change before
  the next element dispatches. The controller vector itself contains at most two
  configs; the separate six-entry history limit applies at the pacer queue.
- Generate server TWCC arrival deltas by rounding against the previously encoded
  timestamp and advancing by the encoded delta. This retains sub-tick remainder,
  handles negative reordered arrivals and checks signed tick/Instant bounds.

Primary sources and hashes are recorded in `SOURCE.json`. The original BSD
notice and patent grant already retained for the preceding Google ports also
cover these adaptations. Five sequential patches reconstruct 21 source/manifest
files exactly from the published str0m 0.24.1 inputs.

The direct-source `native_probe_source` target compiles the actual identifier,
clock, cluster and controller modules and their vectors. The separate
`native_twcc_time_source` compiles the actual production quantizer. Private
pacer/stream tests remain distinct from these harnesses; they must not be called
executed merely because the direct-source targets pass. Actual public WebRTC
regressions and unchanged browser acceptance are separate recorded gates.

The diagnostics expose achieved short-train send/receive rates; they do not turn
a short train into sustained path capacity. The late guard fixes a confirmed
reference protection gap, not a proven explanation of a historical 18.3312 Mbps
sample. Server-generated TWCC quantization fixes uplink feedback and cannot
explain Chrome-generated downlink TWCC samples. No physical-link constant,
requested-rate capacity assignment or weaker browser assertion is introduced.

## Bounded sustained validation (sixth sequential patch)

`bounded-sustained-validation.patch` applies after the timing patch. This is an
original server validation extension, not a modification of Google's short-probe
estimator or a claim that a requested rate is measured capacity. The triggering
failure is preserved in `accuracy-candidate-native-grants.json`: legitimate
short trains around 400 kbps temporarily granted 210/232 kbps budgets on a path
refilling at 120 kbps per client. The original allocation-horizon gate rejected
the combined budgets; its threshold remains unchanged.

The new `Bwe::start_validation(target, now)` returns an identity only for a ready
transport and permitted congestion state. Only one validation runs at a time;
repeating the request cannot renew its immutable three-second deadline. A
separate `Validation` cluster reuses the native media-plus-padding pacer at the
required target, with no extra factor of two. Ordinary probe dispatch pauses
while preserving its controller state. The virtual packet deadline retains at
most two milliseconds of pacing credit, independently of padding-request burst
spacing. A 50 ms or one-second scheduling stall cannot replay the entire elapsed
budget. Cancellation removes only validation/probe padding, preserving queued
media, real retransmissions and SRTP sequence state. Reset clears old probe
queues before starting a new estimator.

Actual protected RTP datagram sizes from `protected.len()` drive validation
pacing and measurement. They include RTP/SRTP overhead, exclude unknown
IP/UDP/TURN overhead and therefore represent a wire lower bound. Existing
Google short probes and GCC keep their original payload-byte accounting. Unique
matching TWCC records from this request are accumulated over separate local-send
and remote-receive clocks. The last sent and first received packet are excluded
from the corresponding numerators. No receive gap is removed. Both actual spans
must cover at least one second; their minimum raw rate must meet the requested
target. Matching reported loss rejects the train before the batch can publish a
pass. Historical loss-controller state does not count as new matching loss.
Timeouts and duplicate/old feedback cannot manufacture evidence or extend a
request. A pass describes the observed window and does not guarantee future
packets or loss-free in-flight traffic.

`last_validation` is read-only and retains terminal outcomes. Each terminal
transition logs its identity, actual counts/bytes/spans and measured rate once,
without media payload or credentials. The state bounds duplicate tracking at
8192 records and fails closed if exceeded; high-rate profiles with very small
packets require separate coverage. Pacing vectors at 20 Mbps do not imply that
all 20 Mbps end-to-end validation profiles were accepted.

Verification targets include `native_validation_source` for the exact production
state machine, `native_probe_source` for actual pacing/queue/controller timing,
and `bwe_validation` for real ICE/DTLS/SRTP/TWCC validation at 1/2 Mbps, fixed
deadline/reset/cancel and resumed ordinary probing. The coordinating agent owns
their execution and unchanged browser accuracy/recovery acceptance. Six patches
reconstruct 23 source/manifest files exactly from published str0m inputs;
`SOURCE.json` records each patch's independent identity and base hashes.

## Coarse-timer validation correction (seventh sequential patch)

`validation-pacing-correction.patch` applies after the unchanged sixth patch.
The sixth patch remains the identity of the failed 2 Mbps actual-WebRTC input;
this correction records a separate hypothesis and implementation. Its observed
failure sent 2329 packets / 499744 protected bytes over 2.328 seconds, yielding
1.717333 Mbps without loss: roughly 1000 packets per second under the original
one-millisecond outer driver. The native pacer formerly required a fresh
one-microsecond timer after every packet, making small-packet supply depend on
operating-system timer resolution.

Validation now returns an already-due packet deadline when bounded virtual
credit permits another packet. Production already drains due-now work with a
16-round limit; the actual-WebRTC helper follows that same contract while
retaining the original one-millisecond outer clock. Padding creation still has
its separate two-millisecond burst spacing. Only earned two-millisecond credit
may be drained; a scheduling stall cannot replay unlimited old debt. Ordinary
Google probe scheduling is unchanged. The sending train has an explicit
request-plus-two-second wall-clock end, leaving feedback the existing immutable
three-second deadline. Neither the one-second actual spans nor measured-rate
acceptance is weakened.

Estimator resets additionally retain the session's monotonically increasing
cluster counter. Old TWCC records survive a reset; even records sent at the
same logical instant must not match a newly allocated validation identity.
The real-WebRTC reset regression delivers old encrypted packets' feedback after
a same-instant reset and requires the new validation to remain empty/Pending.

Seven sequential patches reconstruct the same 23 source/manifest files exactly.
`SOURCE.json` records independent old/new patch hashes and the sixth-patch base
hashes. Reconstruction is verified independently of runtime test acceptance;
the coordinating agent records the actual coarse-timer, full-workspace and
unchanged official-SDK browser results.

## Irregular-wake scheduling credit (eighth sequential patch)

`validation-scheduling-credit.patch` preserves all first-seven patch identities.
The seventh patch's one-millisecond-driver success did not establish production
scheduler tolerance: official-SDK recovery failed with five full, zero-loss
validation windows achieving 95.71–99.40% of the required send/receive rate.
A deterministic actual-protocol regression with mixed Opus and irregular 2–10 ms
wakes reproduced 300000 bps requested versus 214276.799 bps measured before the
unchanged three-second deadline. The failed input and log hashes remain recorded.

The validation packet ledger now retains up to the existing native 10 ms maximum
probe scheduling delay, separately from the two-millisecond minimum padding
creation interval. When all real queues (including media and retransmissions)
are empty, a padding request covers earned ledger deficit (capped at 10 ms) plus
one future creation interval. Queued bytes are supply, not sending permission:
paced validation packets must still satisfy that same virtual deadline.
Existing unpaced audio can bypass packet admission, but its actual protected
bytes still advance the ledger and reduce padding permission. For validation
packets admitted by the probe deadline over interval T, the bound is
target*(T+10ms) plus one actual packet; this is not a bound on arbitrary
unpaced media bursts. Long stalls discard older debt, and the driver's 16-round fairness
limit remains unchanged. This changes scheduling allowance, not the target
bandwidth or required measured proof. Ordinary Google short probes are unchanged.

New coverage includes fixed 5/10 ms and irregular wakes, pure padding and mixed
Opus, at 300/600 kbps and 2 Mbps, with the same actual one-second spans, measured
rate requirement and fixed three-second feedback deadline. Source vectors check
padding supply without implicit send authorization and interval envelopes across
50 ms/one-second stalls. Source reconstruction is a distinct gate; the coordinating
agent records runtime and original official-SDK acceptance separately.

## Explicit periodic ALR demand policy (ninth sequential patch)

`periodic-alr-demand-policy.patch` adds the explicit native API
`Bwe::set_periodic_alr_probing(bool)`, enabled by default. It lets an application
suspend periodic exploration when current budget already covers all demand.
This is allocation policy, not a new bandwidth observation: bootstrap remains
bootstrap, real congestion is still processed, and no capacity is added.
Initial/exponential/allocation/large-drop recovery probes and sustained validation
remain independently available. Original default-periodic tests retain their
meaning and are not weakened.

The flag gates periodic creation, deferred dispatch and timeout calculation.
Changing to false removes only `PeriodicAlr` entries from the native pacer and
probe controller. Cancellation returns those exact IDs for estimator cleanup;
it does not drain unrelated completion events. Generated padding is cleared only
when the active periodic train is removed, preserving queued real media and RTX.
Re-enabling starts a normal five-second interval. Repeated identical settings
cannot perpetually postpone that interval. Estimator resets retain the policy.

Exact-source regressions cover queued/deferred kind isolation, absence of a
past-deadline polling loop and re-enable timing. The actual WebRTC regression
waits for default periodic scheduling, cancels its already-generated padding,
checks no remaining RTP leak, resets BWE with the policy retained, proves a
one-megabit sustained train still passes, and checks disabled/resumed periodic
behavior. The fixture's independent exponential branch was corrected statically
before execution; that edit is not described as an executed red test.

This patch responds to repeated original-SDK startup failures after successful
sustained validations. The observed real delay/queue/loss signals remain valid;
short-probe timing correlation alone does not establish a false-positive GCC
cause. Nine-patch byte reconstruction and actual source/API tests are separate
from the unchanged original-SDK browser acceptance gate.

## Reset-safe periodic re-enable cooldown (tenth sequential patch)

`periodic-alr-reset-cooldown.patch` preserves the ninth patch's independent
identity and actual passing workspace/API run. An independent lifecycle review
found that preserving only the policy boolean across estimator reset lost a
future re-enable deadline when enabled matched the new controller's default.
The complete `(enabled, absolute_not_before)` policy now survives the reset.
Its deadline is neither recomputed against a sentinel clock nor renewed.
An exact-source regression checks rejection one nanosecond before the original
deadline, eligibility at that deadline, repeated true settings, and restoration
of a disabled policy without a deadline. This correctness fix is not presented
as a demonstrated cause of the original-SDK startup failure.

All ten patches reconstruct 23 source/manifest files byte-for-byte. The ninth
patch's 504 passing tests are historical evidence for that input; the final
tenth-patch tests and browser acceptance require their own recorded executions.

## Product low-latency normal-media pacing (eleventh sequential patch)

`normal-media-low-latency-pacing.patch` changes the normal-media scheduling
interval from40ms to5ms. This is an original product policy, not a missing
upstream correction: Google's pinned202acee `PacerConfig::kDefaultTimeInterval`
is40ms. Its5ms padding duration is a different setting. The production rate
factor remains1.1; debt/queue policies, short-probe timing, sustained validation
proof, byte accounting and SDK acceptance remain unchanged.

The workspace `native_pacer_source` target directly includes the actual pacer
module tree and its original private tests, using only an already-locked dev RNG
and diagnostic/time/type adapters. The new deterministic scenario drives real
queued-packet timestamps and actual native `register_send` at1ms with the same
16-round work limit. It models1200B payload+24B RTP/extensions+16B SRTP+28B
IPv4/UDP, a4Mbps4500B bucket,5ms flush at phases0/4ms, an84KB keyframe-sized
queue and7.2KB interframe-sized queues every33.333ms. It is explicitly a
synthetic wire model, not actual SRTP, decoding or browser evidence.

The40ms red run actually sent424 packets/508800 payload bytes and forwarded424;
modeled queue peaked at8876B/17ms, with262ms maximum media queue age. The new
latency subscenario requires at least400KB throughput and at most5ms modeled
network delay; it supplements the unchanged official-SDK gate. Packet-envelope,
low/high-rate and unpaced-audio/fairness regressions remain separate.

The initial5ms full-source run passed37 tests and failed four old fixtures.
Their debt, queue draining, padding and probe-deadline assertions are retained.
Fixtures now drive the correct5ms debt-clearance times, preserve the2s queue
limit and explicitly test multiple future maintenance wakes before the actual
probe deadline, still within the original10ms bound. The abstract queue-unit
force-drain load scales1200to9600 to retain its old allowance under the shorter
interval; this is not a serialized network packet. The unchanged63000B burst-cap
test now uses200Mbps to exercise that cap under5ms. No failures are ignored.
All previous patch identities and failed-run evidence remain independent.

The uncommitted eleventh-patch candidate initially passed40 source tests and
failed the queue-debt fixture at99ms. Its patch remains archived separately as
`normal-pacing-pre-quantized.patch` in the dated evidence directory. Native
`str0m-proto` rounds each rate-times-duration drain to whole bytes: remaining
16B minus a58ms drain rounds14.5B to15B, leaving1B, legitimately below the
1.25B new burst allowance. The fixture now checks actual state explicitly:
95ms leaves2B at2kbps and remains blocked;103ms drains exactly2B to zero and
allows the next packet. This correction preserves production arithmetic and
all original queue-order/drain assertions. The historical failure is not
relabeled a runtime bug or a passed test.

The final native source target passed41/41 tests, and the final workspace passed
553 tests with0 failures and6 existing ignored tests. The synthetic-wire scenario
forwarded the identical424 packets/508800 payload bytes under both policies;
network delay decreased17ms to4ms, while maximum local media-queue age increased
262ms to297ms. This is a measured tradeoff in the isolated model, not proof of
improved end-to-end latency. Original-SDK browser acceptance remains independent.

## Explicit application-managed discovery (twelfth sequential patch)

`application-managed-probing.patch` adds default-enabled
`Bwe::set_automatic_probing(bool)`. A disabled policy suspends every ordinary
short-probe kind without disabling native congestion estimation, real feedback,
physical probing capability, media/RTX or explicit sustained validation. It gates
creation/deferred dispatch/timers, removes only ordinary pacer entries, reports
only their IDs for cleanup, and clears generated padding only when an active
ordinary train is removed. Active validation and unrelated completion events
survive. Repeated identical settings are no-ops; reset retains the policy and
the periodic re-enable absolute deadline.

The explicit application policy supports validating actual unmet demand directly
without requiring a preliminary short-train result. It does not itself assign
budgets, change measured-proof thresholds or feed targets into native estimates.
The read-only observation, `configured_pacing_rate()`, reads the normal-media
base pacing setting; it excludes temporary probe/queue-drain rates and is not
measured capacity.

Actual-protocol regression requirements include managed-off before the first
poll, reset32kbps, real1/2Mbps sustained proofs and continued paced video. The
media test submits200 synthetic VP8 frames at the goal payload rate over4s and
requires all actual encrypted RTP frames to assemble after a1s drain, at most
10 pending frames, and protected bytes covering all submitted payload. It logs
actual raw estimates and configured normal pacing before/after media. This is
frame assembly, not successful video decoding or official-SDK acceptance.

The prior eleventh-patch browser campaign failed startup before round1 despite
passing all native source/model tests. That failure is retained separately;
the twelfth patch is a new discovery-policy counterfactual, not evidence that
real queue/delay/loss events can be ignored.

The initial twelfth candidate passed five real API cases but failed the stronger
2Mbps paced-video supply bound: actual protected proof2.01844Mbps left native
raw990kbps/normal1.0879Mbps, resulting in11 pending frames versus the unchanged
limit10. Its patch is archived independently with the failing run identity.
The corrective candidate feeds only a newly Passed real measurement into native
rate control, using **payload bytes from the identical unique TWCC packet set**,
with the same send-last/receive-first endpoint exclusions and independent clock
spans. Protected proof and payload measurement remain different units. A complete
feedback batch is processed first; current Overuse rejects proof and any current
batch loss prevents rebasing loss control. The Passed edge is consumed once,
including a blocked result, so cached proof cannot undo subsequent congestion.
Existing probe-result clamps are retained; no requested target is inserted as
measurement, ACK output is unchanged, and no supply headroom is added. Exact
vectors cover protected80kbps/payload72kbps, one-shot, loss and Overuse precedence.
This extends reconstruction to24 files, including delay/control.rs.


Twelfth corrective candidate actual verification: root full workspace84347
completed with570 passed, zero failures and six existing ignored tests across
35 suites. The original managed2Mbps case now assembles200/200 paced VP8 frames
with maximum2 pending frames (unchanged limit10), versus the retained red11.
The additional lifecycle case drives actual encrypted RTP through increasing
receive delay until nativeOveruse and an actual rate decrease both occur,
drains **every submitted precongestion frame** before negotiating inactivity,
observes1000ms genuine RTP silence, performs the existing manual32kbps reset,
and requires a new actual2Mbps proof and200/200 recovered frames/maxPending2.
This test does not claim application automatic reset or official-SDK recovery.
The initial lifecycle fixture cleared a half-sent frame through SDP inactivity;
its receiver sequence gap waited for the default30-frame reorder buffer. That
fixture failure and compiler errors remain distinct from production behavior.
Full browser acceptance remains pending; component/source tests do not replace it.


## Thirteenth patch: bounded incoming TWCC metadata

`incoming-twcc-metadata.patch` adds read-only diagnostics for server-generated
TWCC about incoming RTP. It changes no quantization, serialized report bytes,
feedback schedule, GCC, pacing, validation or admission threshold. Collection
uses explicit per-Rtc `set_incoming_twcc_observations(bool)`, defaultfalse.
Repeating a value is a no-op; disabling clears diagnostic history only, retaining
all actual receive/report progress. The flag gates receive-gap statistics and
all cloning/linear MTU-bounded reconstruction, with no diagnostic allocations
when disabled. Engine creation opts in only when application DEBUG is enabled;
collection correctness does not depend on ambient tracing callsite state.

A32-report ring retains report identity/SSRC, actual/encoded timestamp bounds,
maximum point error, real received/missing counts, build and serialization-poll
times. The last timestamp is **not** kernel/network send time. Gap and age maxima
cover the current enabled collection epoch; a lifetime maximum alone cannot date
a current stall. Each timestamp reconstruction must be within125us; a span's two
endpoint errors can sum to250us. Application correlation uses the existing
one-second periodic tick, DEBUGguarded, with transport IDs including publisher-only
transports. Native anomalies log at most once/second/registration. No payload,
SDP, tokens or credentials are captured; no extra async task is introduced.

Exact-source tests load production TWCC/header/fmt code with only surrounding
trait/type/16-bit extension glue. Opt-in off/on must produce identical actual
serialized bytes for identical inputs. Tests include100us receipt spacing, a193ms
gap, true missingseq70, duplicateseq80, flag lifecycle and historybound32. Actual
duplex ICE/DTLS/SRTP/TWCC first establishes real receipts for200ms, holds actual
RTP over193ms, then explicitly starts a fresh bounded1Mbps validation with all
original proof constraints. It reports29 feedback progressions and a measured
212ms receive gap. The earlier unanchored fixture held the first-ever RTP, which
cannot establish an interarrival gap. The earlier tracing-gated collector was
also absent under full-suite parallel execution despite successful proof; its
generated patch and actual failing-log identity remain archived. Explicit opt-in
removes that ambient logging dependency.

Root98501 actual full workspace:611 passed, zero failed, six existing ignored,
37 suites. Native report suite33/33, history2/2, real validation8/8 passed. This
instrumentation establishes observability and wire invariance, not the cause or
resolution of the fifth official-SDK campaign failure.
Root build and Clippy subsequently passed. A test-only scoped allowance for
existing upstream style warnings changed no native code or assertions; the
exact33-source-test rerun also passed. Official-SDK diagnostic acceptance
remains pending.

## 14. Actual discarded-credit measurement boundaries

`validation-credit-measurement-segments.patch` follows the first thirteen
patches. This is a project validation-design correction implementing
SPEC-0008 §5.6, not an attributed Google pacing change.

A real transmitted validation packet can cause the unchanged pacing ledger to
cancel positive old local credit. That synchronous event, with actual TWCC
sequence identity, starts a new raw measurement segment. Polling, requested
bandwidth, remote jitter, receive gaps and feedback arrival cannot create a
segment. The packet still pays exactly the existing protected-byte pacing debt;
no cancelled credit is restored and no sending rate or burst bound is raised.

The immutable request retains its full matched-feedback cumulative facts,
unique-packet set, loss, 8192-packet resource bounds, two-second sending limit
and three-second deadline. A later segment cannot pass until every actual send
in earlier segments has matching feedback; an old-segment loss fails the whole
request. A same-wake boundary also uses the real TWCC sequence so equal send
clock timestamps cannot conceal earlier packets. The current segment retains
all of its raw send/receive gaps and original endpoint byte deductions. Its
same unique packets supply both protected proof and payload-rate native input.
As before, proof does not predict unknown future loss in the current in-flight
tail. `request_totals` describes full-request matched feedback, not unacknowledged
wire traffic.

Actual red evidence retained by SHA: a 215010bps train with one 50ms local
no-drive interval measured 211885bps across its full request, despite a complete
contiguous 1.739s healthy suffix measuring 216216.216bps. The diagnostic suffix
search was never used as the production algorithm. With the actual credit-loss
boundary, the strict public API test measured 216936.660bps over 1.042s, while
the retained full-request rate remained 210130.031bps; segment count2 and actual
cancelled credit38.661ms. The target, two raw one-second minimum spans, zero
reported loss, original deadline and sending envelope remain unchanged.

Root's selected workspace run93144 passed109 tests: real validation9,
exact pacing43, exact probe40, exact validation15 and independent real network
negative cases2. New vectors cover old unresolved sends and delayed loss,
equal-clock sequence identity, old good feedback/duplicates, repeated or invalid
clipping, network gaps, insufficient rate, resource bounds and one-shot native
payload input. Root3588 subsequently completed the full workspace:623passed,0failed,6existing
ignored in38suites; Node checks passed30/30. Clippy53868 and releasebuild6034
subsequently passed; binary SHA256 is
`a7504518ce5cfff30cd3d22baf92812c084975bcf658ed9cc44344b38472d06c`.
Official SDK five-round plus accuracy acceptance remains pending; its server
uses the default info logging with diagnostic proxy tracing disabled. Source and patch SHA values are in
`SOURCE.json`; the offline reconstruction checks all25 modified native files
from the original published package and all14 sequential patches.

## 15. Matched-feedback propagation RTT observation

`matched-feedback-propagation-rtt.patch` follows the fourteen earlier patches.
The formula is adapted from fixed Google WebRTC commit
`202acee882062f7ca632472e4810f25938828f6a`,
`modules/congestion_controller/goog_cc/goog_cc_network_control.cc:425–449`.
Attribution for this adaptation: Copyright (c) 2018 The WebRTC project authors.
All Rights Reserved. The BSD-style terms and additional patent grant are
retained in `GOOGLE-WEBRTC-LICENSE` and `GOOGLE-WEBRTC-PATENTS`; the existing
combined vendored license declaration remains unchanged.

This adds an allocation-free, two-pass observation over the already-built
actual received/matched `AckedPacket` collection. Maximum feedback RTT remains
unchanged as a diagnostic. The new minimum subtracts only receiver waiting
proved by timestamps inside that same report. Remote timestamps are compared
only with other remote timestamps. Lost records do not form the maximum remote
receipt. Empty received sets and any negative/future-send candidate yield
whole-batch unknown. This explicit conservative `Duration` behavior differs
from Google's signed `TimeDelta`; no negative is clamped to zero. Unknown
waiting after the last received packet remains, especially in singleton
reports, so this is not a reconstructed pure physical RTT.

Native GCC, ACK, loss/delay algorithms, packet bytes, reporting cadence and
pacing are unchanged. Application safety consumes only the identity-matched
new field, without falling back to `max_rtt`, cached Stats RTT or zero. Existing
queue/clean fixtures explicitly populate their intended real propagation
sample; their assertions and original thresholds remain unchanged.

The old max-only application logic was actually red:107 passed,1 failed in
root's exact-source quick diagnostic. A constant20ms round trip with160ms
report span was incorrectly treated as a queue; the paired actual-return-queue
170ms sample still passed the original safety cut. The actual production
helper's six direct-source vectors subsequently passed. Formal release
workspace and real encrypted-protocol tests are still pending at this
checkpoint, as are Clippy, release build and official SDK acceptance. The
public encrypted test uses the unchanged real50ms reporting cadence, actual
RTP/SRTCP and forward10ms/return10ms versus160ms queues; it does not fabricate
one160ms report by merely withholding output polling.

Root76480 subsequently completed the formal release workspace:648passed,
0failed,6originalignored across39suites. The actual encrypted test observed
12normal-return reports with minimum propagation20ms and9delayed-return reports
with minimum170ms; receiver report waiting remained visible in maximum RTT.
The new controller negative and genuine queue positive also passed. Dependency
graph, cargo-deny and15patch/26file reconstruction checks passed. Clippy is
running; release binary and official SDK acceptance remain pending. B0 is not
accepted or frozen on these native/component results alone.

The same frozen candidate subsequently passed Clippy, the release build,
explicit OpenSSL expanded/handshake checks and official-CLI opt-in execution.
Their actual log hashes and scope are in SOURCE.json. Release SHA256 is
`5c605c0400147b979204d18e546a602dc0e5b1624dbc09c06d10688dd4eaf752`.
The original info-logging five-round SDK plus accuracy campaign is running;
these component/check successes do not establish SDK acceptance or freeze B0.

## 16. Explicit validation proof floor and pacing experiment

`validation-proof-floor-and-pacing.patch` applies after patch15, without
rewriting any preceding patch or failure checkpoint. This is a new, explicitly
opted-in product experiment, not a claimed Google algorithm correction.
`start_validation(P, now)` retains its original P pacing and segment-only proof
qualification. `start_validation_with_pacing(P, Q, now)` rejects invalid,
nonfinite or unrepresentable values before allocating an identity or changing
an active request, and requires `0 < P <= Q <= 1.2P`.

The existing protected-byte ledger uses Q, never an added Q on top of media.
The original10ms credit envelope,2s wall-clock sending cap,3s immutable request
deadline,8192 packet bound, actual request-wide loss handling and unresolved
old-sent closure remain. Current overuse and same-batch loss take precedence.
There is no change to ordinary Google probe math, GCC, loss, ACK or pacing
formulas. Current segment proof remains preferred; only the new dual request
can fall back to the complete unique matched received collection, retaining
all internal gaps and original endpoint byte deductions. Both actual send and
receive spans must be at least1s and the actual minimum rate must meet P.

`BweValidationObservation.pacing_bitrate` records Q;
`selected_proof: Option<BweValidationProof>` identifies the selected collection
as `CurrentSegment` or `WholeRequest` and retains its actual first send, two
spans, protected endpoint bytes, unique count and protected/payload rates.
Current-segment fields and cumulative request facts are not rewritten to
imitate the selected collection. Native measured-rate integration consumes
only the selected collection's independently measured payload rate once.
Neither P nor Q is inserted as an ACK or measured native rate. The application
explicitly chooses Q=1.2P, then admits at most P/actual proof/current demand/hard
cap, preserving its pre-existing P admission cap.

No existing test assertion expected value changed. The existing controller
proof fixture adds equivalent selected CurrentSegment facts and Q=P; its
original999ms negative cases mirror those invalid spans into selected facts.
New source vectors cover dual-only complete-prefix proof, insufficient actual
prefix despite Q, same-set payload, unresolved old loss, overuse, invalid
P/Q/one-ULP ratio excess, and an independent Q sender envelope. Independent
public encrypted-protocol tests are coordinated separately. Source-only
reconstruction verifies16 sequential patches against26 exact native files;
formal component and official SDK acceptance are pending at this checkpoint.

## 17. Independent RemoteBWE padding and outgoing-only TWCC gate

`independent-remote-padding.patch` applies after patch16; previous patch identities
remain unchanged. This adds an execution primitive, not another estimator and
not a change to Google's ACK/probe/pacer formulas. `DirectApi::request_padding`
requires an existing negotiated sending RTX stream, DTLS/SRTP keys, no native
BWE, a protected-byte budget512B..16MiB, rate1..100000000bps and duration(0,5s].
It returns one monotonically unique identity; busy/invalid requests cannot replace
existing work. Cancellation is identity-scoped and idempotent. One bounded status
snapshot records actual locally produced protected bytes/packets, never receipt
or validated capacity. Less than512B budget remainder is deliberately unused.

Ordinary media and requested retransmissions keep their existing NullPacer path
and priority. Only after that poll produces nothing does the executor use an
existing RTX source to generate one blank RTP-padding packet, through normal
RTP header/RTX sequence/SRTP serialization. It neither consumes queued media nor
requested RTX, uses no SSRC0 fallback, and enables no GCC to obtain padding. No
padding packet queue is retained. Each actual packet advances an independent
no-credit deadline; a stalled scheduler cannot catch up with a burst. A packet
exceeding512 protected bytes is not emitted and terminates the request; its
outgoing transport sequence is restored rather than creating a fabricated TWCC
send record. Expiry/disconnect/source loss also stop authorization.

`set_send_transport_cc(false)` independently suppresses outgoing transport
sequence insertion/registration, while leaving negotiated SDP, incoming RTP,
and receiver-side TWCC registration/feedback untouched. Default true preserves
the native path; ABS generation remains based on the negotiated mapping. This
supports the consolidated single-PC RemoteBWE boundary without globally removing
the publisher's feedback negotiation.

Nine new real encrypted-protocol public API cases and three exact-state cases
are in `crates/rtc-media/tests/remote_padding.rs`: RTX identity/actual protected
budget, missing feedback, cancellation/old identities, media survival, expiry,
source absence, no catch-up, invalid/busy and native rejection, and actual
outgoing ABS/no-TWCC with incoming TWCC retained. These are IMPLEMENTED_NOT_RUN
until the coordinator compiles and executes them. Byte-exact reconstruction of
17 patches across28 source files is source provenance only, not runtime PASS.

Repeated-NACK observation remains a separate explicit gap: existing `nacks` is
an RTCP message count. No such count is relabeled as repeated packets. Fixed
LiveKit counts after sequencer timing/count filtering and successful source
lookup; this patch does not alter native retransmission eligibility to imitate it.


Patch17 first compilation actually failed with20 missing_docs errors (exit101).
The failure log/input hashes are retained in SOURCE.json. Public variants and
fields are now documented without lint exemptions. Observation also records
`first_output_at` and `last_output_at` only from actual local datagram production;
never substitute request/deadline times. Cancel/expiry without output leaves both
absent. Ordinary media emitted during an independent request is a separate
observation gap, not silently included in these padding-only counters. The new
inherited-RtpWrite test supplies an old transport sequence and asserts both local
and actual decrypted receiver headers clear it under the outgoing gate. Revised
runtime checks remain pending the coordinator's next execution.


An actual encrypted-wire counterexample then confirmed two original metadata
assertions failed while all wire assertions passed:16/16 packets had P=true
and the negotiated RTX SSRC, but0/16 RawTx metadata headers had P=true; the
gated case had5/5 wireP/ABS and0 TWCC, but0 metadataP. The failure log and input
hashes remain in SOURCE.json. Only the new independent Blank branch now sets
`header.has_padding` before serialization, matching the P bit that the existing
padding writer already emitted. Native padding and final wire behavior are
unchanged; every original metadata and new actual-wire assertion is retained.
Full runtime verification after this correction remains pending.


## Patch18: exact installed receive SRTP authentication overhead

`receive-srtp-auth-tag.patch` adds the read-only Rtc → Session → actual SrtpContext getter. It returns None before DTLS keying material installation and returns the concrete cipher's existing constants (10 for SHA1_80,16 for eitherGCM). No ciphertext-length inference, secondary profile state, default cipher guess, or packet/crypto/GCC behavior change is introduced. The native regression protects/decrypts both padded and unpadded real RTP under all three actual profiles and checks the observed overhead.

The application consumes this per-transport fact for authenticated one-shot ingress RTP accounting; all-padded real media no longer waits indefinitely for an unpadded bootstrap packet. The fixed Go six-case byte oracle remains unchanged. Current reconstruction chain has18 patches and29 files, retaining all previous checkpoint records. Runtime and full SDK acceptance remain independent; coordinator records actual results after execution.


## Patch19: external feedback boundaries and authenticated compound admission

`external-feedback-boundaries.patch` applies after patch18; all original18
patch bytes and historical checkpoint objects remain unchanged. Current exact
reconstruction covers19 sequential patches and33 native/manifest files. This
patch captures the installed default-off actual RTP/RTCP observations, bounded
RemoteBWE packet/repeated-NACK metering, complete REMB SSRC lists, independent
padding output lifetimes, per-audio/video extension applicability and optional
authenticated whole-datagram RTCP admission hook. The estimator and source-typed
RTCP validator are application-owned Rust modules; no Go runtime/dependency or
Pion estimator is inserted into native GCC. Default hook None preserves original
native parsing, and explicit native-mode algorithm arithmetic stays unchanged.

Send observations are emitted only after actual protection/size checks and keep
RTP header/payload sizes separate from padding/SRTP overhead. The512-item/256KiB
observer makes overflow an explicit terminal history discontinuity. Receiver
estimates preserve full SSRC lists in a bounded256-report queue. Repeated NACK
counts require prior sent history and actual cache-hit/timing/count eligibility;
no RTCP-message counter is relabeled as packet loss. SDP applicability persists
per media kind through remapping and final encoded headers, preserving publisher
negotiation and configured native behavior.

The optional synchronous admission function runs after successful SRTCP
authentication/decryption and before any native handler/stat/raw/observer effect.
The application uses its locked Pion RTCP typed decoder to reject an entire
malformed compound; later valid-parsed adapter processing errors remain the
application's source-ordered callback concern, not a native rollback. The
existing internal-test-exports feature adds a connected-context SRTCP helper;
no production key material is exposed.

Root's `streams/send.rs` documentation repair is captured: primary SSRC rustdoc
remains attached to `ssrc()`, separate from the private output observer comment.
The stale historical patch18 `src/rtp/srtp.rs` hash remains visible; exact original
18-patch replay/current bytes agree, and the latest33-file table records their
true hash. No SRTP implementation is changed in patch19.

Pion interceptor0.1.49 and RTCP1.2.19 source archives and complete module-file
hashes are retained by SPEC0018. Their original2026 Pion community MIT texts
are preserved in `LICENSES/pion-interceptor-0.1.49-MIT.txt` and
`LICENSES/pion-rtcp-1.2.19-MIT.txt`; NOTICE attributes the application ports.
Source reconstruction only proves byte identity. Real source-oracle, encrypted
wire, full Cargo and original SDK mode gates remain separate actual receipts;
this documentation does not predict their outcome or close all feedback profiles.

## DataTrack source writer available budget (proposed patch20)

`data-track-writer-budget.patch` adds an explicit opt-in application available-byte budget. Normal Channel::write and its default128KiB aggregate buffer remain unchanged; caller-supplied unbounded admission is used only by the application's source-default nonblocking DataTrack writer, whose actual buffered/rate/latency gate replaces a fixed64KiB heuristic. Stream state/message-size/SCTP encryption semantics are unchanged. Corresponding source policy comes from fixed LiveKit datachannel_writer.go and bitrate.go (Apache-2.0, retained alongside policy). This native API integration is local str0m code under the existing native license. Source-only metadata is not runtime acceptance. Association knob/max-message work remains a separate declared cut.

## Proposed patch21: SCTP source options and local advertised capability

Adds default-preserving optional association congestion controls and local SDP advertisement, consumed by the application fixed source configuration. All original20 patches retained byte-for-byte. Local advertised65535 does not alter negotiated remote outbound maximum or impose association-wide65535 reassembly failure. SNAP local INIT transport agrees with selected options. Exact file table/reconstruction in SOURCE is source identity, not executed behavior.

22. `independent-source-sctp-policy.patch`: carry explicit application source-PR policy independently of the fast retransmission window. Library default remains false. Source reconstruction only; encrypted loss recovery remains an independent functional gate.

## Patch 23: source SCTP wire alignment

`source-sctp-wire-alignment.patch` aligns source-enabled SCTP DATA payload to four bytes before fragmenting, preserving the existing transport MTU. This fixes encrypted first-record loss recovery when padding otherwise exceeds the inferred retransmission MTU. Native library false/fast-zero defaults remain unchanged. See SPEC-0040 old public API RED/GREEN evidence.

## Patch 24: SNAP answer requires local opt-in

`snap-answer-requires-local-optin.patch` makes the reciprocal `a=sctp-init` in an answer conditional on the local `set_snap_enabled` opt-in. Upstream str0m reciprocates whenever the remote offered the attribute; upstream LiveKit/pion (`references/deps/livekit--webrtc-pion/peerconnection.go:3014`) only puts `localSctpInit` in an answer when its own `EnableSctpSnap` is set, so a peer that did not enable WARP never negotiates SNAP. The default stays off: with `set_snap_enabled(false)` the answer carries no INIT chunk and the peer performs the regular SCTP handshake (covered by SPEC-0074 C20 `rtc-media/src/warp.rs`).

## Patch 25: keyframe skip-ahead after a gap

`keyframe-skip-ahead-after-gap.patch` lets the depacketizing buffer recover on the first VP8/VP9 keyframe after an upstream outage. Upstream str0m holds every frame behind a sequence gap until `hold_back` (30 video frames) or `reordering_timeout` (2 s video) expires, so after a 3 s publisher blackout the subscriber waited 0.6–2.2 s for video. Fixed LiveKit v1.13.10 forwards packets without such a hold-back (the RTP munger keeps `SN - snOffset` gaps), and pion has no reordering buffer on the SFU path; Go recovered in 100–180 ms. When the head of the buffer is blocked (missing packets, incomplete head frame or an expired sequence), the buffer now drops everything before the first queued segment that starts a keyframe and emits it at once; a keyframe never waits for contiguity. Inter frames after a gap still wait for retransmission, and other codecs are unchanged. Covered by `keyframe_after_gap_is_emitted_without_waiting_for_hold_back_or_timeout`, `keyframe_after_incomplete_head_skips_the_incomplete_frame` and `inter_frame_after_gap_still_waits_for_retransmission`; real SDK evidence in `livekit-rs-docs/evidence/2026-11-07/spec0077-m06-media-recovery/`.

Patch 25 boundary validation (2026-11-07): `keyframe_skip_boundaries` in `buffer_rx.rs` covers an incomplete keyframe (missing tail or middle packet is neither a skip target nor emitted), out-of-order keyframe packets, late retransmissions from before the skip point (discarded, stream stays contiguous), 16-bit sequence wraparound, inter frames never being skip targets, a VP9 SVC keyframe picture emitted with all spatial layers, a VP9 SID>0 intra layer without its base, and Opus and H.264 keeping the upstream wait. Real SDK decode/packet evidence: `livekit-rs-docs/evidence/2026-11-07/spec0077-m06-media-packets/`.

## Patch 26: RTCP feedback intersection

`rtcp-feedback-intersection.patch` limits NACK/PLI/FIR/REMB of each codec to what the first remote description also lists (`fb_negotiated`, not compared). Pion (`livekit/webrtc-pion v4.2.18-warp.6` `mediaengine.go:670,685`) negotiates feedback as `rtcpFeedbackIntersection(local, remote)`. Upstream str0m kept the local flags, so a single-PC answer advertised Opus `nack` that Chrome never offered, Chrome then NACKed audio and the server retransmitted it, which fixed Go never does. TWCC keeps the existing first-m-line rule. Unit test `rtcp_feedback_is_the_intersection_with_the_first_remote_description`.

## Patch 27: opt-in answer codec order follows the offer

`answer-in-offer-order.patch` adds `RtcConfig::set_answer_in_offer_order` (default `false`, so upstream `test_rtp_payload_priority` is unchanged). When enabled, an answer to a remote-created m-line lists negotiated codecs in the offerer's order, stable for the rest. Pion appends negotiated codecs in remote order, so Go answered Chrome's H.264 `packetization-mode=1` while str0m answered the locally first-registered mode 0 (same registration order as Go's `VideoCodecsParameters`). Unit test `answer_in_offer_order_lists_the_offerers_preference_first`.

Patch 25 file correction (2026-11-07): the committed `keyframe-skip-ahead-after-gap.patch` was the original diff with the boundary-test diff appended using post-patch line numbers, so `git apply` rejected it and source reconstruction could not pass. It is now one diff from the patch-24 state to the vendored `buffer_rx.rs` (content and the recorded source/base file hashes are unchanged). `scripts/check-str0m-reconstruction.py` passes for all 60 sequential patches.

## Patch 28: NACK throttle until keyframe

`nack-throttle-until-keyframe.patch` adds `StreamTx::set_nack_throttled` / `nack_throttled` (default `false`). While throttled, `handle_nack` ignores incoming NACKs. This is the mechanism for fixed LiveKit v1.13.10 `FlagStopRTXOnPLI` (`pkg/sfu/downtrack.go`): a subscriber PLI sets `isNACKThrottled` and the next forwarded keyframe clears it, so the SFU stops retransmitting packets the subscriber will skip once it decodes the keyframe. The application sets and clears the flag; upstream behaviour is unchanged otherwise. Unit test `nack_throttle_ignores_new_nacks_until_cleared`.

## Patch 29: media packet arrival counter

`media-packet-arrival-counter.patch` adds the read-only `StreamRx::media_packets_received`, counting RTP packets with a non-empty payload as they arrive, before depacketization. Fixed Go `streamtracker_packet.go` observes each packet on arrival; str0m only surfaces whole frames after the depacketizing buffer, which hides stopped or resumed layers for up to the hold-back time. The application's Go stream-tracker port reads this counter.

## Patch 30: LiveKit FIR media SSRC admission

`livekit-fir-media-ssrc.patch` retains the common FIR media SSRC when parsing RTCP. Native routing still uses each FCI target. LiveKit mode requires the common media SSRC to appear in the FCI, then dispatches once to that stream. Fixed Go's Pion SRTCP layer routes the FIR to its distinct FCI targets before `DownTrack.handleRTCP` checks the common media SSRC. The authenticated regression covers zero and mismatched media SSRCs, multiple targets, repeated FCI entries, and a compound PLI. The SFU enables this mode on its transport.

## Patch 31: authenticated RTP packet events for frame tracking

`frame-tracker-packet-events.patch` adds the default-off
`Rtc::set_all_video_packet_events` opt-in. With packet events enabled, this
also delivers non-duplicate authenticated video RTP for VP8, H.264 and other
negotiated video codecs ahead of depacketization. The SFU opts in only for an
explicit frame-tracker configuration, which uses marker and RTP timestamp
even when no complete frame reaches the decoder.

## Patch 32: publisher transport-wide feedback

`livekit-publisher-twcc-feedback.patch` adds an opt-in profile for LiveKit
publisher TWCC. Authenticated video RTP with negotiated transport-cc and
video RTX with the negotiated extension trigger a report on arrival when
more than 20 new receipts accumulate and 100 ms have elapsed, when more
than 20 receipts accumulate and a marker arrives after 50 ms, or after
more than 100 receipts regardless of elapsed time. The report uses the
triggering RTP source and preserves missing sequence statuses across
report boundaries. Authenticated non-media SSRC 0 probes participate
when an active receiving video line negotiated transport-cc. Generic
str0m receivers keep periodic feedback.
The regression tests cover the packet, marker and elapsed-time boundaries,
silence, duplicate receipts, target SSRC, sequence wrap, long gaps and
an authenticated SSRC 0 probe that creates no media.

## Patch 33: publisher TWCC receipt history

`livekit-publisher-twcc-receipt-history.patch` gives LiveKit's publisher
feedback the fixed Pion arrival map semantics. Unwrap against the last
arriving sequence and rewind the report cursor before checking for
duplicates. Preserve first arrival timestamps and evict only the aged
prefix, including holes, after the previous report cursor has caught up.
Keep at most 32768 sequence positions. This leaves native periodic TWCC
history intact. Cap RTCP run-length chunks at the maximum representable
13-bit value in both modes.

The high-rate test exercises the 500 ms boundary, the exact sequence
range boundary, duplicate timing and rewind, reordering across wrap,
and serialized long missing runs. Rust keeps the RTCP datagrams within
its MTU even when Go combines equivalent status coverage in a larger report.

## Patch 34: immediate Opus resume after a qualified outage

`opus-outage-resume.patch` adds a default-off frame mode policy. The
LiveKit engine opts in for newly authenticated, nonempty primary Opus.
After enough missing RTP slots to exceed the normal repair window, the
same SSRC can resume without another wait if both receive time and
source timestamp advance past the window and agree within the configured
repair deadline. The frame retains its sequence gap. Ordinary reordering,
SSRC changes, mismatched clocks, padding, RED and RTX keep existing
behavior. A detected source restart resets the existing per-RID
depacketizer history.

## Frame-assembled VP9 logical RTP size

`frame-vp9-logical-rtp-bytes.patch` carries the authenticated logical RTP
size through VP9 depacketization and sums the packets retained for the
emitted frame. Synthetic sequence placeholders contribute zero. The
metadata stays in the frame and does not change wire extensions or
depacketization behavior.

## Local ICE preference hook

`ice-local-preference-hook.patch` exposes the existing ICE agent policy
through Direct API. Generic str0m retains its default preferences. The
LiveKit engine supplies fixed Pion's preference for UDP host candidates
while delegating other candidates to the existing policy.
