# Fixed-source RTC development / loopback observer

**FROZEN INPUTS / NOT_RUN.** This is a separate source observer; it does not alter F123, F124/F125 production candidates, their original authored tests, or the actual 13-row owner receipt. No expected candidates or admission results are prefilled.

Eight real source runs cover development false/true × explicit loopback false/true × all interfaces / only the actual UP loopback interface. Each uses the original `RTCConfig.Validate` followed by original `NewWebRTCConfig`, then a genuine unmodified Pion PeerConnection/ICE Agent gather. An explicit `NodeIP{V4:"127.0.0.1"}` and `UseExternalIP:false` avoid discovery/STUN, while retaining the real Host alias rewrite. The requested UDP port is an independently acquired ephemeral port; source setup bind failure is reported, never silently retried into a PASS. No TCP is configured in this source-selection scope. Source configurations using TCP and real public SDK peers remain separate required controls.

Each row reports the actual validated flag, NodeIP-auto flag, bound mux addresses, real NAT mapping list, callback candidate strings, and delegated GetConn physical addresses. The observer replaces only the same SettingEngine UDP mux with a delegate implementing its original interface; it does not replace filtering, gathering, candidate construction, configuration validation, socket behavior or source arithmetic. One port per interface avoids changing multiport choice while reading addresses for observation. It adds no private package hook, copied source predicate, expected row, network alias, fake candidate or live application listener.

The existing accepted C3 official profile (dev + explicit NodeIP127 + no explicit loopback) is not invalidated by the fact that public alias and bound physical base may differ. This observer must identify the source's actual distinction. Its UDP-only gather is not proof of full SDK reachability, TCP alias handling, native identity admission or payload delivery.

Coordinator only:

```sh
python3 /tmp/f126-network-loopback-source/prepare.py --root /Users/zoujiaqing/projects/livekit-rs --out NEW_OWNED_STAGE
cd NEW_OWNED_STAGE
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go list -mod=readonly -m github.com/livekit/mediatransportutil github.com/pion/ice/v4 github.com/pion/webrtc/v4
GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly ./loopback > actual.jsonl
```

Keep stdout actual rows and stderr source diagnostics separately; preserve failed setup/execution rather than normalizing them away. Check exactly eight distinct rows, genuine completed gather, actual nonempty candidates for source-eligible cases, and actual registered/bound address provenance. Missing physical prerequisites are NOT_RUN/FAIL, never source acceptance. Only after actual output may a source-correct minimal selector correction and its old-API test-only RED be frozen; the old development-forces-loopback authored assertion must remain in historical F123/F125, with its discrepancy recorded explicitly.

Sources are the fixed server's unmodified MVS: LiveKit server v1.13.10 plus its recorded module replace to Pion ICE v4.4.0-warp.3 / WebRTC v4.2.18-warp.6 and MTU f234b534b095. Reference source hashes and licenses are recorded in SOURCE; Go does not execute from a private absolute GOMODCACHE path. `prepare.py` uses the restored fixed server module reference and original locked sums.
