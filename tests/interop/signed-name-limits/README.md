# Actual configured name-limit profiles — NOT_RUN

Parent starts owned fixed Go/Rust servers with exactly the same one of the nine
`profiles/*.yaml` limit overlays. Retain listener/keys/network from the declared
controlled local server profile. The fixture does not start servers or assume
an HTTP error is an implemented media feature.

For each actual server:

    LIVEKIT_URL=ws://127.0.0.1:OWNED_PORT SIGNED_LIMIT_PROFILE=room-negative SIGNED_LIMIT_OUTPUT=OWNED_JSON node run.mjs

Then:

    node compare.mjs ACTUAL_GO_JSON ACTUAL_RUST_JSON OWNED_COMPARISON_JSON

Run all9 independently. Use the same declared profile ID and actual YAML for each
pair; report binary/config/source hashes outside the fixture. Different runtime
service URLs are not compared, but no observable code/body/name/metadata value is
normalized. This tests HTTP/configured signaling; no browser/RTC performance or
public SDK Blob/DataTrack API claim. Auth uses real locally signed JWTs against
both genuine servers; there is no fake responder.

The only imported reader is copied byte-for-byte from committed
`tests/interop/signal-json/raw-protocol.mjs`; it reads the actual binary Join and
RequestResponse oneofs. It does not change SDK code. All file, socket and room
cleanup failures turn the report FAIL. Existing rooms are checked absent before
CreateRoom; only successfully created owned rooms are deleted.

## Reproducible fresh-process campaign

`run-paired.py --go /absolute/pinned-livekit-server --rust /absolute/livecloud --output /absolute/new-owned-output` runs all nine configured profiles using the committed YAML files. It rejects occupied HTTP ports, checks actual readiness, retains process identities/exit codes, gives each fresh server its own output, and requires all81 paired observations. It stops only its own servers and treats runner, cleanup and process failures as failures. The coordinator must persist safe evidence then remove raw output; output can include participant metadata and must not be committed indiscriminately.

These are real HTTP/authenticated raw-signaling consumers. They do not substitute for unchanged official SDK media, actual DataChannel fatal writes, distributed owner API validation or a full compatibility claim.
