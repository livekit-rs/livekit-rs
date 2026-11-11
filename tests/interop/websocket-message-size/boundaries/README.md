# F95 additional header-budget / close-boundary diagnostics

NOT_RUN. Original F94 24 vectors are unchanged. This independent driver uses the
same real authenticated /rtc and RoomService cleanup, no mock server/browser.

Six cap1024 cases × immediate close-ACK or deliberately withheld close-ACK ×
three fresh repeats × official/Rust. `WS_CAP_REPEATS=1..5` and
`WS_CAP_SIDES=official` allow actual Go self-repeat first; both is strict pair.
Ten-second observed close bound; server must actually close the raw transport,
not pass via the client's five-second forced socket destroy. Stable comparisons
retain write error, actual close code/reason/clean and transport error. Timing
traces are captured independently, never canonicalized into a matching value.
A self-repeat mismatch is reported, not erased as OS variability.

Cases: single1025 full masked header only/no payload; whole1025 coalesced;
header then body as separate writes; fragmented aggregate1024 plus continuation
header declared1/no payload; and same fragmented tail coalesced or separate.
Actual no-payload aggregate is the exact semantic counterexample: Gorilla checks
readLength1025 at the second header, before waiting for that payload. Current
Tungstenite aggregate check occurs only after the tail payload is read.

```sh
WS_CAP_INTEROP_ROOT="$REPO/livekit-rs-server/tests/interop" WS_CAP_LIMIT=1024 \
WS_CAP_OFFICIAL_URL=ws://127.0.0.1:17880 WS_CAP_RUST_URL=ws://127.0.0.1:17980 \
WS_CAP_SIDES=official WS_CAP_REPEATS=3 \
LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret \
WS_CAP_OUTPUT="$OWNED_OUT/go-header-close-repeat.json" \
node /tmp/f95-ws-cap-close-boundaries/run.mjs
```

Both servers must use declared limit1024. Repeat identical driver with
WS_CAP_SIDES=official,rust to capture current Rust RED, and again after reviewed
vendor changes. Parent owns execution. No unconditional reset, linger flag,
OS-error normalization or changed original assertion is supplied by this draft.
Compression is not negotiated; it remains an independent OPEN RTC capability.
