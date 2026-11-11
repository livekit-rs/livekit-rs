# Signed webhook retry timing

The probe calls CreateRoom on the selected real daemon with the official
server SDK and verifies every receiver invocation with WebhookReceiver.
It records the event ID, raw body digest and token digest, and requires those
values to be stable across retries. The disconnect control closes the actual
connection after verification and before writing any response.

Run each daemon with the same fixture and a separate unused output directory.

```sh
node run.mjs "$repository" "$fixed_go_binary" "$go_output"
node run.mjs "$repository" "$rust_binary" "$rust_output"
python3 compare.py "$go_output/execution.json" "$rust_output/execution.json" --expect fixed
```

The base profile uses loopback HTTP. Set WEBHOOK_TEST_TLS=1 for TLS 1.3 when
both daemons trust the repository fixture root and can verify its leaf.
The comparator verifies each daemon's transport as recorded. It requires
actual child exit on normal runs and records the forced retirement of a
baseline held by the 300-second delay after the short observation window.

Set `WEBHOOK_TEST_LONG_RETRY=1` on both runs to also measure a real 302-second
retry. The comparator distinguishes the previous 300-second cap from the
explicit upstream delay and still verifies both requests with the official SDK.
Set `WEBHOOK_TEST_PENDING_SHUTDOWN=1` instead to terminate the server after
the receiver verifies the first request and returns a 302-second retry delay.
Compare a binary with the 300-second cap and previous shutdown behavior
using `--expect capped-rust`.

Run original-go-backoff.go from the pinned Go source module with its locked
dependencies to reproduce signed delta, overflow, date and first-header
observations without waiting through large durations.
