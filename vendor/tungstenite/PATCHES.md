# Source-bound LiveKit close reply policy

Published tungstenite0.29.0 is the existing compatible Axum0.8.9 transport.
The archive checksum and complete39-file source are pinned in SOURCE.json.
Two modified files add one opt-in feature and adjust only the queued reply
for a valid received Close in server role. Original received code/reason is
returned unchanged to the application. Client roles, feature-off defaults and
invalid-code error generation retain upstream behavior. There is no new socket
stack or HTTP server; Tokio+Axum remains the server boundary.

Fixed LiveKit v1.13.10 uses Gorilla1.5.3's default SetCloseHandler, which replies
with the received code and an empty reason. Actual unchanged Worker20 detected
this difference; tests must not normalize reason or alter peer input to hide it.
The compatibility layer explicitly activates this narrow feature. It does not
prove all invalid RFC6455 control errors match Go. Upstream license files remain
byte-identical. Original upstream deprecation warnings under Rust1.99 are not
suppressed or expanded into unrelated source changes.

Run scripts/check-tungstenite-reconstruction.py from the server workspace to
verify the real original package, exact patch and complete resulting file tree.


## Sequential second patch: header data-message read budget (F98)

`livekit-header-read-budget.patch` applies AFTER the preserved close-reply patch.
It adds default-off `livekit-server-header-read-budget`, enabled by lk-compat.
Only server-role complete parsed headers use it; the original generic FrameCodec
callers and client roles retain their previous path. Current incomplete-message
bytes plus declared data payload are checked before reserve/read of that payload.
Control frames retain the independent125-byte RFC bound and consume no data
budget, even when fragmented-message budget is zero. Complete invalid headers
retain protocol classification ahead of capacity. Gorilla1.5.3 advanceFrame
source/individual BSD-2-Clause attribution is retained in SOURCE and LICENSE-GORILLA.

This is not a forced TCP reset or a promise of exact close transport outcomes.
The original24 RTC vectors and additional header-only/raw-close vectors must
run against actual fixed Go/Rust; single1025 clean-vs-reset remains a separate
OPEN cause. Compression,32bit allocations and all malformed-header split timings
are not certified by component tests. No frame/message bound is increased here.

## RTC reserved bits

The RTC upgrade opts into Gorilla v1.5.3 reserved-bit handling. A negotiated
RSV1 on continuation/control frames does not reset or start decompression.
Rejected RSV bits are carried as a typed compact mask so the RTC consumer can
send Close1002 with the source-ordered reason. Default and client-role paths
remain strict. The decoder uses pinned pure Rust zlib-rs with the Rust allocator
so distances beyond the fresh dictionary are rejected. Wire and decoded bounds and valid Close reply rules are unchanged.

`livekit-rtc-frame-header.patch` extends the RTC opt-in to Gorilla v1.5.3's two-byte header validation. It combines reserved-bit, opcode, fragmentation and masking errors in wire order before awaiting an extended length, masking key or payload. Native clients and non-RTC server defaults retain their validation.

`livekit-rtc-close-payload.patch` validates received RTC Close status before UTF-8 and preserves the Gorilla wire error reasons. A short status-less Close gets the original empty acknowledgement.

## Sequential seventh patch: RTC signed length range (W04)

`livekit-rtc-signed-length.patch` follows the RTC Close-payload patch.
Gorilla v1.5.3 rejects a 64-bit length with the sign bit set after reading the
extended length and before reading the mask. Its signed aggregate counter
also rejects overflow before applying the positive configured message limit.
The RTC opt-in reports the typed length error so the LiveKit consumer writes
normal Close 1000, while ordinary configured wire overflow still writes 1009.
The same opt-in grows its frame buffer as data arrives. Non-minimal valid
length encodings remain accepted. Default and client behavior is unchanged.
