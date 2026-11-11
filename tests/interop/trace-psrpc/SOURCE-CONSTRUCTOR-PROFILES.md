# Fixed source tracing is an explicit constructor option

Target: LiveKit server e5655ac20267fa5005dccaf3f0e3bfc3ea688c07, protocol9bb708cdb81918a6c63dd25e915b4644310b45b6, psrpc e9701ee1b5a981cfa3c7c69503d4775ddff13729.

| Actual factory | Go constructor chain | OTel unary client/server layers |
| --- | --- | --- |
| AgentInternal | agent/client.go94 NewAgentInternalClient(bus), service/agenthandler.go154 NewAgentInternalServer(h,bus) |0/0|
| RoomManager | routing/roommanager.go38 manually passes channel/metrics/logger, omitting ClientParams.Options; roommanager.go162 server logger/metrics/channel only |0/0|
| Room/Participant/AgentDispatchInternal | wire_gen.go91/95/103 typed clients→protocol typed_api.go240/252/276 Params.Args→Options; corresponding owner servers pass no OTel options |2/0|
| WHIP | whipservice.go64 ClientParams.Args; roommanager.go174 WithDefaultServerOptions→protocol typed_api.go139 WithServerObservability |2/1|
| WHIPParticipant | typed client Params.Args; roommanager.go584 plain typed server |2/0|
| Egress/Ingress/SIP RPC clients | actual wire ClientParams→Options plus explicit wire OTel option |2/client factory only|
| IOInfo backend | ioservice.go59 explicit otelpsrpc.ServerOptions |server1|
| Original SIP worker IOInfo caller | SIP cmd/livekit-sip/main.go84 explicit otelpsrpc.ClientOptions |client1|

The two client layers are intentional fixed-source behavior: protocol typed_api.go113 appends its built-in OTel option, then116 appends p.ClientOptions containing wire_gen343's additional OTel option. Actual Go20 receipts show nested duplicate Sent names, not duplicate business calls. A generic global provider is not an interceptor option. Pub/sub and stream RPCs remain outside unary middleware; multi clients remain uninstrumented.

Independent source patch defaults plain constructors to no interceptor, preserves explicit one-layer middleware fixtures, and installs the actual0/1/2 profiles at host production factories. RoomControl's combined façade excludes its manually-created RoomManager client while retaining the typed-client profile for other methods. The implementation creates nested actual spans and preserves inner injection, end/status/events order; it never synthesizes a parent ID.

## Tests authored, not run

- Existing six isolated middleware tests stay explicit1-layer tests, not blanket production-constructor evidence.
- Plain factory with a genuinely recorded ambient parent emits zero RPC spans and preserves outgoing header bytes.
- Real bus requests exercise typed client2 and RoomManager0, nested lifetimes/events/status and no server trace.
- Actual IOInfo.register factory produces one Recv with the real not-connected error when Redis is absent; this is negative constructor evidence, not positive worker functionality.
- Actual WHIP front/owner constructors produce2Sent→1Recv for a real rejected owner-validation request, with no shadow room. Decoded media remains the real multi-process campaign.
- factory-observer.go invokes original generated AgentInternal defaults and original Params/helper options in an owned Go process; it does not replace original packages or claim its echo handler is a media worker.

## Strong production campaigns

1. Agent four real business pairs remain mandatory (availability, signed assignment, low-load claim, running dispatch, remote terminate, deletion and exit0). Trace profile requires no AgentInternal spans in either language and actual two-layer upper AgentDispatch client exports. Old20failure receipts remain unchanged; OLD20-TRACE-SOURCE-DIAGNOSIS.json is read-only reinterpretation, never a new run.
2. Existing native-whip-cluster runner adds optional --trace. Every original decoded440/880Hz, ICE restart, actual owner/no shadow room, participant delete, browser0 and process0 gate remains unchanged. Trace additionally requires actual A outerSent→innerSent→B Recv.WHIP.Create, exact scope/kind/status/events. Plain WHIPParticipant must not produce Recv spans.
3. Linux original SIP runner adds optional --trace-ioinfo. Actual Go SIP worker is side a/kind go-sip-worker; actual backend is side b. Every existing Digest/PCMU↔Opus tone/REFER/NOTIFY/status/browser/exit/resource gate remains. IOInfo.UpdateSIPCallState must have a real source-workerSent→backendRecv parent chain. Fixed OSS callback is explicitly a placeholder (ioservice.go186), so this is a real tracing transport/constructor test, not durable SIP state acceptance. Positive SIP media is proven by the unchanged independent probe. No third probe is disguised as server A.

Source SIP dependency psrpc@v0.8.2 still declares version.Version=v0.8.0 in version/version.go, as does the server's fixed psrpc source; the expected scope is exactly github.com/livekit/psrpc v0.8.0.

## Coordinator commands

Apply this23-based delta by three-way merge into the next committed24 input; never overwrite whole rpc.rs/worker files over other fixes. Use fresh target and record exact source identity.

```sh
cargo test -p lk-compat --lib service_bus::telemetry::tests:: -- --nocapture
cargo test -p lk-compat --lib actual_whip_factory_clientparams_two_sent_and_default_server_one_recv -- --nocapture
```

Require original6+new3 telemetry tests and new1 WHIP constructor test, nonzero count/exit0, then full relevant regression. From fixed references/livekit-stable cwd:

```sh
GOTOOLCHAIN=local GOPROXY=off /ABS/GO1.26.3/bin/go run -mod=readonly /ABS/next/server-inputs/tests/interop/trace-psrpc/factory-observer.go > /NEW/evidence/original-factory-options.json
```

Actual full production commands (binaries/probe must be source-bound; each output directory new):

```sh
python3 tests/interop/trace-psrpc/run-paired.py --go-server /IMMUTABLE/Go --rust-server /IMMUTABLE/next-Rust --probe /IMMUTABLE/actual-agent-probe --config-mode yaml --evidence-dir /NEW/agent-absence-four
python3 tests/interop/native-whip-cluster/run-paired.py --go-server /IMMUTABLE/Go --rust-server /IMMUTABLE/next-Rust --trace --trace-config-mode yaml --evidence-dir /NEW/whip-trace-four
```

Repeat CLI and ENV configuration modes in new directories. In existing guest using original SIP binary and loopback nft policy (no broadened network):

```sh
python3 /home/fixture/fixture/repo/livekit-rs-server/tests/interop/egress-linux/run-owned-sip.py --source-root /home/fixture/fixture/repo --go /ABS/guest/bin/Go --rust /ABS/guest/bin/next-Rust --worker /ABS/guest/bin/original-SIP --trace-ioinfo --trace-config-mode yaml --output /home/fixture/fixture/output/NEW-sip-ioinfo-trace
```

These are execution entries, not claimed results. No implementer ran Cargo, Go, servers, browsers or VM services. Exact exported resource enrichment/duration log format/HTTPS trust gaps remain separately open; this patch does not certify allC10 or815 leaves.
