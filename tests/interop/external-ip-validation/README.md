# External-IP validation and interface rewrite consumer

This source checkpoint is not compiled or executed by the implementing agent.
The coordinator owns actual acceptance and final cleanup. No reference checkout,
service, Cargo target or shared production input was modified.

Fixed source: `mediatransportutil-f234b534b095/pkg/rtcconfig/ip.go` original
`getExternalIP/findExternalIP/validateExternalIP` and `webrtc_config.go`
`getNAT1to1IPsForConf/nat1To1AddressRewriteRules`.

The previously parsed `rtc.skip_external_ip_validation` now reaches normalized
RTC and the real per-interface STUN consumer. A successful STUN response is not
accepted as validated reachability: the STUN socket closes, its specified local
address/port is rebound, and the original magic is sent from a separate socket
to the external IP at the newly bound port. The local receiver must actually
receive that magic. `skip=true` skips only this validation; it does not bypass
STUN, family matching or configured interface filters.

Mapping discovery now runs for `use_external_ip` even when `external_ip_only`
is false. It retains external/local pairs and source-keyed deduplication,
including source's self-fallback collision behavior. Discovery uses the original
five-second window, reset to one second after the first success, shared socket
cancellation and scoped joins; it does not detach blocked probe threads.
`external_ip_only` independently filters unresolved IPv4 while retaining IPv6.
No discovered mappings still preserves the original configured node-IP fallback;
validation failure is not invented as a whole-server startup rejection.

`advertise_internal_ip` now appends internal candidates on the actual NetRouter
socket consumer. Each physical socket is bound once, external and internal
candidates share a real base, and routing/retirement owns one descriptor. Existing
NetRouter constructors retain their old default; the server passes its explicit
configured append policy through `start_with_address_rewrites`.

## Candidate17 source-collection correction

`getNAT1to1IPsForConf` calls original `GetLocalIPAddresses`, not Pion's physical
ICE bind enumerator. NAT discovery ignores interface UP/carrier, keeps eligible
duplicate addresses, applies exact name/IP filter precedence, excludes only
link-local unicast/multicast, appends loopbacks last when enabled and uses them
as fallback when no eligible non-loopback exists. Candidate17 now retains this
separate address collection. The physical candidate filters stay unchanged.

The original actual-STUN test mistakenly expected ::1 to survive the pinned ICE
IPv6 filter. Original ice/v4@v4.4.0-warp.3/net.go:24-35,100-107 rejects ::/96,
including ::1. The corrected fixture uses admissible unresolved 2001:db8::1 and
keeps len2, exact external mapping and explicit IPv6 bind/self-mapping assertions.
It does not weaken the production filter. New enumeration tests explicitly
assert that ::1 can appear in the NAT probe set but not physical ICE candidates.

## Coordinator tests

From the integrated next `livekit-rs-server/`, require exit0 and nonzero counts:

```
cargo test -p lk-config --lib netdetect::bound_validation_tests::
cargo test -p lk-config --lib nat1to1::tests::parallel_discovery_passes_validation_policy_and_preserves_distinct_mappings_and_self_fallback
cargo test -p lk-config --lib normalize::tests::skip_external_validation_projects_real_yaml_cli_env_and_cli_false_precedence
cargo test -p livecloud --lib interfaces::tests::nat_probe_
cargo test -p livecloud --lib interfaces::tests::actual_stun_rewrites_each_bound_interface_even_when_external_ip_only_is_false
cargo test -p livecloud --lib interfaces::tests::distinct_external_mappings_and_unresolved_local_self_mapping_are_real_candidate_rewrites
cargo test -p rtc-runtime --lib net::tests::internal_rewrite_append_uses_one_real_socket_for_external_and_internal_candidates
```

Expected new tests from the original patch **3+1+1+1+1+1 = 8**, plus **2** discovery-address enumeration tests in the candidate17 revision (10 total). The first3 use actual task-owned loopback
UDP STUN and hairpin sockets, including an unreachable loopback address, explicit
skip, cancellation and same-source-port rebind release. The interfaces test uses
actual STUN and the real candidate-selection consumer, not only a mapping helper.
The runtime test sends an actual STUN routing datagram to the advertised internal
candidate, observes the original physical base, then requires bounded port release.
No user network interface or host route is changed.

Run the unchanged original Go function observer:

```
python3 tests/interop/external-ip-validation/run-go-observer.py \
  --go-binary /direct/fixed-go1.26.3/bin/go \
  --fixed-server-source /repo/references/livekit-stable \
  --mtu-source /repo/references/go-modules/mediatransportutil-f234b534b095 \
  --evidence-dir /new-evidence/original-external-ip
```

The runner creates a task-owned copy, verifies every original MTU file hash,
adds only `_test.go`, and builds using the fixed server module graph via a private
modfile. It uses the direct fixed Go1.26.3 executable, `GOTOOLCHAIN=local`, and `GOPROXY=off`; no loader download is attempted. It mutates no reference, and removes its staged copy
only after preserving source hashes, exact command/stdout/stderr, one real test
pass and five original-function observations. No substitute map or validation
algorithm is used as the Go oracle. Reachable/negative/skip calls and actual
interface mappings are observed. The fake STUN endpoint is a controlled network
fixture; validation itself still requires actual loopback delivery.

Further final-binary official-SDK campaigns must observe both mapped public and
internal candidates and actual ICE/media over each admitted base. The source
unit consumers do not certify every NAT/router/OS deployment. DNS resolution
uses the existing synchronous OS ToSocketAddrs operation. Fixed Go also calls
net.Dialer.Dial rather than DialContext before observing cancellation, so neither
path promises force-cancellation during resolver/dial; do not invent a
Rust-specific gap or a stronger upstream guarantee. HTTPS/trace is unrelated.

Root ledger leaves include YAML and generated flags for use_external_ip,
skip_external_ip_validation, external_ip_only, advertise_internal_ip, and
CONFIG_ENABLED_CAPABILITY:rtc.external_ip_only. No parent is closed by this
source-only patch.
