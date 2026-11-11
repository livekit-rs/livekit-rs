# Original Ingress WHIP proxy functional fixture

This was prepared as an isolated draft and then actually compiled/executed by the coordinator. Current scoped PASS and source/binary identities are in SPEC0024 and f14-functional-delivery evidence.
Do not substitute a direct POST to the SFU and call that external-worker coverage.
C1/C2/C3 frozen staged work and all existing native WHIP assertions are untouched.

## Real path and precise scope

1. Original, unmodified Ingress v1.5.0 is configured `whip_proxy_enabled: true`.
2. Actual ServerSDK2.19.2 `IngressClient.createIngress(WHIP_INPUT=1,
   enableTranscoding:false)` creates a real Redis catalog entry. Its returned URL
   must be `http://127.0.0.1:WORKER_PORT/whip`, distinct from the SFU origin.
3. Chromium generates actual full gathered ICE SDP and sends it to that returned
   URL using its returned streamKey as Bearer. Original worker itself calls real
   IOInfo.GetIngressInfo (fixed OSS returns Info only), derives a real INGRESS token using its original Params fallback and configured URL, then HTTP POSTs `/whip/v1`
   on the real SFU with `X-Livekit-Ingress:true`.
4. Audio is actual oscillator440 -> encrypted RTP -> native SFU -> unchanged SDK
   observer/client.html. Existing native observer also really publishes880, but
   original Ingress tokens intentionally have CanSubscribe=false; no reverse880
   subscription is claimed. The older native bidirectional440/880, trickle204,
   restart/newETag and deletion test remains its separate, unchanged requirement.
5. Passive Redis recorder sees real notification requests AND their successful
   responses. It never registers/claims/replies. Actual same-participant alive
   notifications must span >=10s, contain actual nonzero audio statistics, and
   decoded audio sample/packet counters must independently advance over >=10s.
6. Real public RemoveParticipant closes native resource. Same-participant actual
   `Closed:true` acknowledgement and original worker IOInfo INACTIVE state with
   same resource generation and EndedAt must arrive. Actual public DeleteIngress
   then removes the real entry; ListByID must return source `not_found`.
7. No GStreamer run-handler child is required: original no-transcode proxy runs in
   the unchanged parent process. Parent worker liveness is checked before/after.

Source quirks are tested as they exist, not corrected in the fixture:
- POST CORS exposes Location only. Browser ETag visibility is diagnostic; actual
  Playwright HTTP wire headers must include ETag and Location.
- Worker proxy accepts upstream200/201 only. Ordinary native trickle204 is not
  transparently accepted by that old proxy. Full ICE gathering avoids the need
  for initial PATCH. This does NOT pass/replace the separate native trickle gate.
- Params.SetInputAudioState preserves existing persisted AverageBitrate. Thus
  nonzero bitrate is asserted on real Notify data + browser receive bytes,
  not invented in persisted state. Persisted generation/status/timestamps and
  actual state callbacks remain independently required.

## Root-owned prerequisites / exact launch order

Use actual already-built unchanged `references/ingress` v1.5.0 binary. It still
initializes GStreamer even in bypass mode; use the actual F09 native environment
and plugin paths. No replacement worker, callback stub or fake SDK is acceptable.
Use isolated dedicated Redis (same broker/database in server, worker, recorder).
Current adapter v0.8.2 -> old worker v0.7.1 compatibility profile explicitly uses
server-global `psrpc.compression.quality: 0`. This is a declared integration
prerequisite, not automatic config rewriting or proof of old-worker positive gzip.
The server must run real IOInfo and real native WHIP Notify callbacks.

Coordinator selects unused ports and private credentials, then sets:

```
export LIVEKIT_URL=ws://127.0.0.1:17880
export LIVEKIT_API_KEY=... LIVEKIT_API_SECRET=...
export REDIS_ADDRESS=127.0.0.1:17642
export WHIP_PROXY_PORT=18080 WHIP_PROXY_RTC_PORT=17885
export INGRESS_RELAY_PORT=19090 INGRESS_HEALTH_PORT=18081
python3 tests/interop/native-whip-proxy/configure.py --out /tmp/ROOT_OWNED_FRESH_PRIVATE_DIR
```

This writes0600 configs with O_EXCL, never logs secrets. Merge the exact
`server-overlay.yaml` subtree into the root-owned proven SFU config (preserve its
Redis, keys, HTTP and RTC config), then start root-owned server. Start original
worker `ingress --config /tmp/ROOT_OWNED_FRESH_PRIVATE_DIR/worker.yaml` with F09
native plugin environment. Require actual `/health` HTTP200; do not call a sleep
readiness. `rtc_config.udp_port` is a real unused native worker socket even though
this session uses the proxy bypass. Root records binary/config/source hashes.

Compile the passive trace with root-controlled Go toolchain in `trace/`. Its
module prefix intentionally permits actual psrpc/internal import. go.mod uses
repository-relative reference paths and actual fixed server's
locked graph. Relocating requires replacing only these root paths, not versions.

```
TRACE_RUN_ID=<fresh random non-entity label>
<actual-trace-binary> --redis redis://127.0.0.1:17642/0 \
 --out /tmp/OWNED_SAFE_TRACE.json --run-id "$TRACE_RUN_ID"
```

Keep recorder alive. Require actual JSON `ready:true`, same run_id,
`coverage_complete:true`, empty events before the browser starts. This readiness
comes from all four real Redis PSUBSCRIBE acknowledgements, never fixed sleep.
Its data is atomic/bounded256 events,128 pending,1MiB payload; overflow or decode/
transport error makes coverage invalid. Max lifetime240s, SIGTERM exits naturally
and closes its own subscription/client. It is observation only: no callback
server stealing a production reply, no Redis MONITOR/credential dump.

Root launches the fixture (only after actual worker and observer readiness):

```
INTEROP_PROJECT=/Users/zoujiaqing/projects/livekit-rs/livekit-rs-server/tests/interop \
LIVEKIT_URL="$LIVEKIT_URL" LIVEKIT_API_KEY="$LIVEKIT_API_KEY" \
LIVEKIT_API_SECRET="$LIVEKIT_API_SECRET" \
WORKER_ORIGIN=http://127.0.0.1:18080 INGRESS_WORKER_PID=<actual-original-pid> \
WHIP_TRACE_FILE=/tmp/OWNED_SAFE_TRACE.json WHIP_TRACE_RUN_ID="$TRACE_RUN_ID" \
WHIP_PROXY_OUTPUT=/tmp/OWNED_F20_RESULT.json \
node tests/interop/native-whip-proxy/run.mjs
```

Fixture owns only its browsers/HTTP fixture server/created room and ingress; it
finally stops/deletes these, preserving failure and cleanup errors. Parent owns
recorder/worker/SFU/Redis lifetimes. Do not SIGKILL a still-running fixture and
claim natural cleanup. Do not persist configs, SDP, tokens, stream keys, URLs,
addresses, participant IDs or raw broker messages. Persist only bounded safe
result, hashes, actual process exits and necessary source locks. No content error
is ignored; assertion failure is nonzero exit with failure-message hash.

## Acceptance and limitations

Unchanged numerical browser media criteria: actual440 tone RMS>.02, amplitude
>.025; actual sender packets>=75, receiver packets>=75, decoded samples>=8000,
20s media-readiness window. Actual callback/state windows are bounded45s (Closed
and end15s), same real generation throughout. No media/performance/congestion
standards were weakened. Existing old SDK/native gates are separate requirements.

A PASS here proves original-worker proxy control/media/lifecycle, not native WHIP
trickle-through-old-proxy support, video, transcoding, distributed WHIP owner
routing, worker positive gzip, or complete Ingress feature coverage. The final declared profile actually passed. Other profiles and complete Ingress remain open.
