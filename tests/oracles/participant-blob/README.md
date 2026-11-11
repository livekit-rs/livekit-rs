# Participant Blob reference oracle

This test program imports the pinned reference server's actual participant Blob
implementation. It does not implement a reference server or generate expected
values from Rust. Restore `references/livekit-stable` and `protocol-stable` from
the repository reference lock first. Go is a test dependency only.

From this directory:

```sh
GOTOOLCHAIN=go1.26.3 go run -mod=readonly . "$PWD/cases.json" > /tmp/participant-blob-go.jsonl
```

Then from the server workspace:

```sh
BLOB_GO_CASES="$PWD/tests/oracles/participant-blob/cases.json" \
BLOB_GO_RESULTS=/tmp/participant-blob-go.jsonl \
cargo test --release --workspace --locked \
  actual_go_blob_key_identity_and_unknown_response_provenance_match_all_26_cases -- --ignored
```

The opt-in test requires all 26 actual Go rows. Unknown wire provenance and
Go key-identity equivalence are separate assertions. See SPEC-0026 and the
authenticated browser/raw-WebSocket fixture for actual server acceptance;
component-oracle success alone does not establish network functionality.
