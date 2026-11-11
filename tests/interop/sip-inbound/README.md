# F08 real inbound SIP acceptance fixture (scoped PASS)

Actual fixed Go and final Rust candidates passed this complete profile with
unmodified official SIP worker and SDKs. See SPEC0020 and F07 delivery for
source/binary identities and exact execution evidence. Future candidates must
rerun the profile; this result is not whole-SIP or whole-server certification.

## Required real path

Unchanged official `livekit-server-sdk` creates an authenticated SIPInboundTrunk plus a direct SIPDispatchRule → local real SIP UAC sends UDP INVITE without credentials → unmodified official `livekit/sip v1.21.0` worker calls actual Rust IOInfo authentication callback and issues actual407 → UAC sends actual Proxy-Authorization → worker invokes actual Rust dispatch callback → real SIP RTC participant joins the rule's room → real browser `livekit-client` and worker exchange media.

The UAC emits no PSRPC, no worker callback message and no synthetic participant. Only the official SIP process handles these internal methods. Its config contains service API credentials and actual dedicated Redis details, but no local trunk passwords or direct room override; those come from actual Rust callbacks following Server SDK configuration. The browser remains the unchanged official RTC SDK.

## Assertions

1. A wrong-password actual network INVITE must receive real407, send Digest, be rejected, and create no SIP RTC participant. No unauthorized call success is tolerated.
2. The valid UAC must receive actual407, authenticate, receive real200 SDP supporting PCMU/8000 and ACK. A call accepted without Digest is a failure. Challenge nonce/realm/credentials and raw SIP packets are not written to evidence.
3. RoomService API must report the actual inbound participant with attributes matching both the created `sip.trunkID` and `sip.ruleID`, plus a real `sip.callID`. Matching is checked without persisting those identifiers in safe evidence.
4. Independently generated PCMU440Hz must be decoded from actual RTC audio in the browser. Independently generated browser880Hz must be decoded from actual received PCMU at the UAC. The inherited F07 thresholds stay the same:≥75 real sent/received packets, rms>.02, target amplitude>.025, and reverse880 amplitude>five times440 reference. Packet count by itself cannot pass, and echo is not reverse-path proof.
5. UAC must send real in-dialog BYE and receive actual200. The actual SIP RTC participant must leave while the browser observer remains. The test never calls RemoveParticipant or deletes the room to satisfy this assertion.
6. All resources created by this fixture (trunk, rule, browser, room, UAC sockets and local HTML host) are cleaned. Existing external SIP worker/server/Redis are never stopped by the runner. A failed cleanup makes the run fail. Coordinator still owns process lifecycle and final repository cleanup.

## Runner and source inputs

`run.mjs` uses the existing locked `livekit-rs-server/tests/interop/package.json` through `createRequire`. Set `INTEROP_PACKAGE` if moving the draft. Set `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, `SIP_WORKER_PORT` (default15060), `SIP_INBOUND_OUTPUT`. Bindings are strictly loopback UDP; no provider/PSTN destination can be dialed by this fixture. `worker-config.yaml.in` is the F07 fixed-source worker config template; the coordinator renders actual private credentials/ports and runs its already fixed worker binary. Do not commit rendered config or keys.

The relative import `../sip-media/peer.mjs` reuses the actual shared F07 local standards-facing UA parser/G711/tone/RTP machinery without maintaining a second copy. `uac.mjs` adds real INVITE transactions, Proxy-Digest retry, actual SDP answer, retransmission handling, ACK and BYE. It implements no SIP registrar or external worker replacement. `peer.test.mjs` retains pure parser/G711/tone tests and adds Digest challenge parser checks; these tests have passed actual coordinator execution. Real Digest acceptance is proved only by the official worker run, not by local hash self-comparison.

Coordinator execution sequence: run pure Node tests; run the fixture against the current Rust server plus source-pinned official SIP worker; repeat the same fixture against fixed official Go server with independently rendered credentials/DB if cross-server baseline is needed. Capture actual exit statuses and safe evidence hashes. A component fixture or source lock is insufficient for a PASS claim.

## Limits kept explicit

This covers authenticated inbound UDP/PCMU and direct room dispatch, bidirectional content, and UAC BYE ownership. It does not cover inbound PIN, individual/callee rules, hidden ports, TLS/SRTP, redirects, routed Record-Route dialogs, SIP registration, DTMF/hold/re-INVITE, telephone networks or provider interoperability. Those remain separate functional leaves, not silently declared supported by this test. Pure local UA implementation errors must be fixed without relaxing worker/RTC/media assertions or substituting internal callback messages.

## Actual service identity detail

The worker's service helper accepts the narrow `IOInfoSIPClient` Go interface, but the actual unmodified executable `references/sip/cmd/livekit-sip/main.go:84` constructs `rpc.NewIOInfoClient`. It therefore sends the legacy IOInfo service channels implemented by the current Rust service. The fixture does not manufacture an IOInfoSIP alias or route based solely on a Go interface name. `pkg/service/psrpc.go` maps real auth/dispatch RPC replies into worker decisions; `pkg/sip/inbound.go:237-319` issues/verifies the actual Proxy-Digest challenge.

## Normal SDK consumer activation correction

The browser now follows the corrected shared `tests/interop/sip-media/client.html`: actual RemoteAudioTrack.attach(), autoplay, volume0, append to document and play() before the unchanged WebAudio measurement. This activates Chromium's normal playout consumer rather than assuming a graph-only connection starts decoding. Read-only receiver getStats diagnostics distinguish RTP arrival from totalSamplesReceived, concealment and decoder activity; no RTP/SDP or clock values are modified. Every attached media element, analyser, stream and timer is owned and explicitly detached/disconnected on cleanup. The440/880 content thresholds and20s allowance are unchanged. The earlier isolated graph-only draft is superseded; this integrated fixture passed actual coordinator execution.

## Coordinator commands and port contract

From `livekit-rs-server/`, first run `node --test tests/interop/sip-inbound/peer.test.mjs`. With an actual server and the unmodified official SIP worker already running, run `SIP_WORKER_PORT=15060 SIP_INBOUND_OUTPUT=tests/interop/output/sip-inbound.json node tests/interop/sip-inbound/run.mjs`. The runner reads existing LIVEKIT_URL/API_KEY/API_SECRET environment values; it never launches or reconfigures a worker. Defaults only target local developer setup. INTEROP_PACKAGE defaults to the adjacent locked package.json via a relative path.

The included source-bound worker template uses SIP UDP15060, health15061 and RTP15100–15199 on127.0.0.1, with actual private API/Redis substitutions supplied by coordinator. HTML and UAC SIP/RTP/RTCP sockets use owned ephemeral loopback ports. Do not run a second worker on the same fixed ports; either reuse the same source-pinned worker sequentially or render an independent template and set matching SIP_WORKER_PORT. No proxy/network rate, media timeout or assertion is changed by moving this fixture into the repository.
