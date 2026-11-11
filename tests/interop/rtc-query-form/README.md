# RTC first-value query semantics

The exact Go baseline consumes the first decoded value for each query key
via `net/http.Request.FormValue`. A syntactically invalid percent-escape
or an unescaped semicolon invalidates that pair. An explicitly empty first
value remains first. The authentication middleware, RTC handshake, validation
and initial wrapped join must interpret the same raw query.

The owned fixture uses the original locked LiveKit browser and server SDKs and
a real Chromium process. At the browser's native WebSocket constructor it
adds explicit duplicate parameters to SDK-generated URLs without modifying
the SDK. It verifies each successful handshake via the actual participant SID
and each expected rejection via absence of the participant. It also uses
the real original SDK-generated v1 wrapped JoinRequest in public HTTP
validation on both routes. The receipt retains HTTP status, body and CORS,
plus actual owned browser exit and server port reuse. No JWT is persisted.

Use a fresh output directory for each engine. Run the same fixture for pinned
Go, the exact pre-fix Rust executable and the repaired executable.

```sh
python3 tests/interop/rtc-query-form/run-owned.py --engine go \
  --binary "$FIXED_GO" --browser "$CHROMIUM" --interop tests/interop \
  --probe tests/interop/rtc-query-form/probe.mjs --output "$GO_OUTPUT"
```

Use `--engine rust-before` and `--engine rust-after` for the other two runs.
The baseline is expected to exit nonzero on the original Axum map behavior.
It must still pass ordinary SDK connections and release its resources.
