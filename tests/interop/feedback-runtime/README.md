# Functional feedback consumers

Pinned official JS SDK2.23.0 and original `../client.html` are used unchanged.
Each configuration must establish real audio/video and reliable data before
checking actual native PC topology and negotiated feedback extensions. These
are functional consumers, not bandwidth/performance acceptance campaigns.

From repository root, start the built server with one configuration, then run:

```sh
livekit-rs-server/target/release/livecloud --dev --config livekit-rs-server/tests/interop/feedback-runtime/remote.yaml
LIVEKIT_URL=ws://127.0.0.1:18880 FEEDBACK_MODE=remote FEEDBACK_OUTPUT=/tmp/owned-feedback.json node livekit-rs-server/tests/interop/functional-feedback-runtime.mjs
```

The other configurations use ports18881/18882/18883 and expected modes
interceptor/native/native respectively. `native-both.yaml` verifies native
precedence when both source flags are enabled. The runner executes Single and
dual profiles and cleans its browser contexts and rooms. Stop the owned server
afterward; do not reuse an earlier binary's evidence for changed production code.

CLI/ENV consumers start `remote.yaml` with either
`--rtc.congestion_control.use_send_side_bwe_interceptor`, or environment
`LIVEKIT_RTC_CONGESTION_CONTROL_USE_SEND_SIDE_BWE_INTERCEPTOR=true`; expect
interceptor mode at the same18880 URL. Adding
`--rtc.congestion_control.use_send_side_bwe` verifies native precedence.
A wrong-configuration control runs unchanged `remote.yaml` while expecting
interceptor. It must establish actual media/data, pass Single union, then fail
the dual subscriber-feedback assertion; arbitrary startup failures do not count.

The YAML files contain public development credentials only. Raw reports include
media identifiers and must remain local; archive only sanitized observations,
exact input/config/binary identities and actual process exits.
