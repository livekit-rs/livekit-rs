# Actual standalone Room.SendData consumer

Run separately against immutable fixed Go v1.13.10 and the immutable Rust candidate, with no Redis configuration. The coordinator owns server startup, binary/source identity and cleanup. This runner starts only its owned local browser fixture and unchanged SDK peers.

```
INTEROP_PROJECT=/absolute/pinned-interop-project \
LIVEKIT_URL=ws://127.0.0.1:7880 \
SENDDATA_DEPLOYMENT=standalone \
SENDDATA_OUTPUT=/fresh/absolute/receipt.json \
node tests/interop/local-room-senddata/run.mjs
```

Required installed versions are livekit-client 2.23.0, livekit-server-sdk 2.19.2 and Playwright/Chromium from the project's pinned public fixtures. No install or server launch is performed. API credentials default to the public development fixture; override LIVEKIT_API_KEY/LIVEKIT_API_SECRET for an owned server.

The receipt requires actual reliable data delivery to SDK consumers with binary payload integrity: identity-only targeting, SID-only targeting, 60KB broadcast, repeated 128-bit nonce deduplication and same-room-name replacement with a new participant SID. A successful public API response alone is insufficient. Cleanup errors fail the receipt. The output starts RUNNING and records failure rather than manufacturing PASS on interruption.

This browser profile does not force an SCTP congestion window, and does not prove an accepted write survived an HTTP disconnect. Source-owned component tests separately exercise the exact accepted-operation owner while blocked and the real typed LocalMessageBus 3s/5s retry attempts preserving payload, nonce and destinations. Existing native SCTP runtime backpressure tests remain required alongside this profile; these scopes must not be combined into a claim of end-to-end forced-backpressure acceptance.
