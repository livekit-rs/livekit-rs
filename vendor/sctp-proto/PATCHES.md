# Native SCTP functional compatibility patch

Published sctp-proto 0.10.7 remains the original input. `SOURCE.json` and the pure reconstruction checker bind all original and modified files. The same-version path patch is Rust only.

The application source policy enables message PR independently of fast retransmission window, source min-CWND/CA/fast-window knobs, ACK/RACK timing, DATA PTO/TLR and coalesced writer wake, plus graceful shutdown with carried cumulative ACK and final endpoint release. Published defaults are preserved. Source control fixtures and public endpoint/encrypted regressions live in SPEC-0040 evidence. A passed scoped test does not establish all SCTP or LiveKit compatibility.

Heartbeat private-handler and authenticated-restart drafts are not part of this patch.

Ordinary data writer readiness: generation-local pending-DATA latch follows actual enqueue and source notifyBlockWritable selection; Channel getter exposes it without conflating buffered bytes or remote ACK. New candidate scope NOT_RUN.


## Authenticated restart and fractional RTO append (implementation; NOT_RUN)

Apply `authenticated-restart-fractional-rto.patch` after the original patch,
without changing that original input. `SOURCE.json` binds both hashes and all
84 reconstructed native files. `scripts/check-sctp-proto-reconstruction.py`
replays both patches without Cargo or network access.

The append retains the existing association and application-open streams while
an authenticated COOKIE-ECHO commits new verification tags, TSNs, queues and
recovery timers. It rejects stale/forged/competing cookies, port changes and new
addresses; one-minute cookie age uses a monotonic TCB-local epoch. The endpoint
routes restart INIT/COOKIE-ECHO to its unambiguous existing remote transport and
updates verification-tag routing after commit. Plain cookie bytes are opaque;
byte identity with Pion's random/HMAC wall-clock cookie is not claimed.

Actual send-to-SACK fractional samples now remain floating milliseconds through
EWMA and timer backoff, converting to nanoseconds at physical scheduling. Test
source exercises encoded DATA/SACK, T3 expiry/retransmission, Karn exclusion and
same-handle restart. It has not yet been compiled or executed for this candidate;
original Go differential and same-final-source global acceptance remain required.
Reflected ABORT flags and broader I-DATA/interleaving are separate open domains.
