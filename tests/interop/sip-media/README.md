# F07 local SIP media acceptance

This fixture is integrated. The fixed official server and unmodified SIP worker
have passed the full outbound/transfer profile after enabling actual browser
track playback. The first candidate run exposed a real queue selection defect;
the corrected final candidate passed the complete profile. Safe execution evidence
and exact acceptance state are maintained in SPEC-0019 and the F07 delivery
record; this README does not certify a new binary automatically.

## Real topology and content proof

Unmodified official server SDK `SipClient` → actual candidate HTTP SIP gateway →
actual Redis PSRPC → **unmodified references/sip v1.21.0 worker** → local UDP SIP
peer. Worker also joins the actual candidate RTC room. An unmodified locked
livekit-client browser publishes and subscribes to the real RTC media paths.

`peer.mjs` is a standards-facing local telephony UA, **not a mock PSRPC worker**.
It requires SIP Digest MD5 challenge/response before accepting the initial call,
negotiates only real PCMU/8000 RTP via SDP, and streams independently generated
440Hz PCM encoded with G.711 mu-law. The browser publishes a real 880Hz audio
track, receives actual SIP participant media and measures decoded 440Hz content.
The peer independently decodes received RTP PCMU and measures 880Hz content,
checking energy and rejecting a 440Hz echo as the reverse-path proof. Packet
counts alone cannot pass the gate.

Transfer is not a canned successful NOTIFY: upon receiving an actual REFER, the
local UA issues a genuine new INVITE to a second local SIP target, waits for its
200, sends ACK, and only then sends final 200 sipfrag NOTIFY. The test requires
that target's INVITE and ACK, both NOTIFY response acknowledgements, the SIP
worker's actual BYE on the original dialog, and actual departure of the SIP
participant from the RTC room. The new target call is signaling-only; onward
media to the transfer target is not claimed. After proof, fixture cleanup ends
that target dialog with a real BYE.

## Source contract

Pinned SIP source: commit ee19da1a7ef178593e232a27365de7b4ba8f8829, tag v1.21.0.
- `pkg/config/config.go`: Redis/key/secret/ws_url required; local SIP/RTP/advertised
  addresses/port names match the worker's actual configuration schema.
- `pkg/sip/outbound.go:823-875`: actual transfer waits REFER outcome; successful
  outcome schedules closure of the old SIP/RTC call after one second if the
  remote has not already hung up. The fixture therefore expects worker BYE.
- `pkg/sip/protocol.go`: REFER requires matching Event refer;id=CSeq, actual final
  sipfrag and subscription state. A 202 alone is not transfer completion.
- Public calls use unchanged locked livekit-server-sdk 2.19.2 methods. Browser
  uses unchanged livekit-client 2.23.0 and Playwright 1.64.1 from the existing
  tests/interop package lock. No paid PSTN/provider or external network needed.

No real credentials, JWTs, raw SIP Authorization headers, SDP or packet payloads
are written to output. Only bounded counters, decoded-content statistics,
versions, step names and redacted error messages are recorded. Cache64, error16,
audio64000sample bounds are explicit. Overflow is a test failure.

## Coordinator execution prerequisites

1. Complete, compile and connect the genuine CreateSIPParticipant and
   TransferSIPParticipant gateway, room owner, PSRPC and IO callbacks. A typed
   boundary returning not_connected cannot pass this profile. This prerequisite
   is not yet known to be integrated; F06 IO batch acceptance is independent.
2. Dedicated actual Redis broker/database shared by the tested server and SIP
   worker; do not share production or another campaign's broker.
3. Actual server running with Redis/IO routes enabled, real key/secret, RTC UDP
   ports open on loopback. The worker must reach the candidate ws URL directly.
4. Already built unmodified worker `/tmp/livekit-rs-f07-official-sip` from the
   pinned checkout, built by coordinator with Go1.26.3. Build success is not a
   runtime acceptance claim. Installed GStreamer, opusfile and soxr are worker
   native prerequisites. This fixture uses browser RTC and G.711 generation, so
   missing agent-sdk node_modules/rtc-node is not a dependency for this profile.
5. The existing interop npm lock installed unchanged and Playwright Chromium
   present. The fixture uses createRequire on the actual interop package.json;
   override `INTEROP_PACKAGE` if moved. No SDK source modification or new npm
   dependency is required.
6. Render worker-config.yaml.in into a private temporary config. Set actual
   key/secret/ws_url/Redis address/db, reserve SIP15060, health15061, RTP15100-15199.
   Do not print config or process environment into committed evidence. Start the
   official worker externally with SIP_CONFIG_FILE (its normal supported flag).
   Check real readiness/worker registration before launching the fixture.

Run the fixture only when all prerequisites are ready:

```
INTEROP_PACKAGE=/absolute/repo/livekit-rs-server/tests/interop/package.json \
LIVEKIT_URL=ws://127.0.0.1:7880 \
LIVEKIT_API_KEY=<task-key> LIVEKIT_API_SECRET=<task-secret> \
SIP_MEDIA_OUTPUT=/private/task/sip-media.json node livekit-rs-server/tests/interop/sip-media/run.mjs
```

The fixture creates a uniquely named room and one actual outbound trunk with
loopback Digest credentials, and deletes only those owned resources. It starts
only its own UDP peers, local static HTTP page and browser; stops them in finally.
It never stops external Redis/LiveKit/SIP processes. Coordinator must stop those
after acceptance and persist safe output/source/binary/lock identities before
removing task files. Cleanup failure marks FAIL rather than hiding a leak.

## Required execution outcomes and remaining coverage

Expected actual proof: SDK Create response + matching SIP participant callID;
401 challenge + verified authentication; real INVITE/SDP/ACK; at least75 actual
RTP packets each direction; RTC decoded440Hz and peer decoded880Hz thresholds;
REFER to real answered target, final NOTIFY200 accepted, original worker BYE and
real RTC participant departure. Any failure is retained and must be fixed or
explicitly left OPEN, never ignored.

This local test covers UDP/PCMU outbound creation, Digest, RTC transcoding and
successful REFER lifecycle. It does not prove inbound dispatch/PIN, SIP TLS,
SRTP, Opus-on-SIP, DTMF, call forwarding, hold/re-INVITE, packet-loss behavior,
provider billing/PSTN, onward target media, all codecs or all SIP features.
Digest algorithm coverage is MD5/auth only, source worker default challenge
compatibility to be observed. Negative auth and target rejection profiles should
follow after the positive actual path runs; they are not silently claimed here.
