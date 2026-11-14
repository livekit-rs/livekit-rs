#!/usr/bin/env python3
"""SPEC-0077 C34 — launched-MAIN rtc.use_ice_lite paired Go/Rust differential.

One ledger leaf is named: YAML:rtc.use_ice_lite. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate
are each launched as the REAL main server, four times, by the self-contained
rtc-use-ice-lite observer (tests/interop/rtc-use-ice-lite/observe.go). The
observer writes use_ice_lite plus the gating media-port keys into the daemon
YAML, reserves the configured media ports in a LOW band (40000-40999, below the
macOS ephemeral range 49152-65535), releases the reservations so the daemon can
bind, drives an authenticated MEDIA-FREE signal join over WebSocket /rtc with a
modern client (protocol>5), and records whether the daemon's subscriber SDP
OFFER carries the session-level `a=ice-lite` attribute. This harness wraps that
observer per side per profile and compares the normalized ICE-lite selection
semantics cross-side.

Upstream consumer (locked Go, pinned mediatransportutil-f234b534b095):
    rtcconfig/webrtc_config.go:223-224  if rtcConf.UseICELite { s.SetLite(true) }
    rtcconfig/webrtc_config.go:225-235  else if (NodeIP empty/auto) && !UseExternalIP
                                          { add STUN servers }   // lite OFF branch
    pkg/service/roommanager.go:446-447  if pi.DisableICELite { SettingEngine.SetLite(false) }
    pkg/rtc/transport.go:414-422        if !ProtocolVersion.SupportsICELite()
                                          || !ClientInfo.SupportsPrflxOverRelay()
                                          { se.SetLite(false) }  // Firefox-only disable
    pkg/service/rtcservice.go:240       ParseClientInfo(r)        // legacy /rtc query
    pkg/service/rtcservice.go:253       pi.DisableICELite = boolValue(r.FormValue("disable_ice_lite"))
    So use_ice_lite turns on the ICE-lite SDP attribute, gated off by the
    disable_ice_lite join param, by an old protocol (<=5), and by Firefox.

Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
    config parse:       lk-config/src/official.rs:303   use_ice_lite: bool = false.
    config projection:  lk-config/src/normalize.rs:587  use_ice_lite passthrough.
    policy:             rtc-core/src/config.rs:109      policy.use_ice_lite = cfg.rtc.use_ice_lite.
    join consumer:      rtc-core/src/room/join.rs:72-73 ice_lite = policy.use_ice_lite
                            && !params.disable_ice_lite && params.capabilities.ice_lite.
    transport spec:     rtc-core/src/room/join.rs:183   -> TransportSpec.ice_lite.
    capability gate:    lk-compat/src/profile.rs:41     capabilities.ice_lite = protocol>5 && !firefox.
    SDP emit:           rtc-media/src/engine.rs:321     .set_ice_lite(spec.ice_lite) (str0m a=ice-lite).
    srflx gate:         rtc-media/src/engine.rs:233     gather_srflx = gather_srflx && !ice_lite.
    join param parse:   lk-compat/src/signal/params.rs:151 disable_ice_lite, :164 protocol,
                        :180 browser, :359 augment only fills an EMPTY browser (explicit
                        query browser preserved on both sides).

WHY NOT WHIP (load-bearing): both Go (whipservice.go:196 DisableICELite:true) and
Rust (whip/mod.rs:374, whip/runtime.rs:142 disable_ice_lite=true) FORCE ICE-lite off
on the WHIP ingress path, so a WHIP offer/answer never carries a=ice-lite on either
side regardless of rtc.use_ice_lite. WHIP is correct parity but NON-discriminating
for this leaf. The media-free signal join is the real launched-MAIN consumer surface.

Discriminator choice (empirically grounded on BOTH sides, load-bearing):
ice_lite_present = (`a=ice-lite` session attribute in the daemon subscriber offer
SDP, exact trimmed-line match). The subscriber offer is sent BEFORE trickle ICE
gathering, so the host-candidate catalog (types/transports/ports/IPs) is EMPTY on
both sides and carries no signal; a=ice-lite is the clean session-level observable.
With node_ip=127.0.0.1 + use_external_ip=false the Go NAT1To1 catch-all
(webrtc_config.go:111-116) masks candidate IPs to 127.0.0.1 but does NOT touch the
session-level a=ice-lite attribute. An accept-but-ignore mutant that ignores
use_ice_lite never sets lite, so a=ice-lite is absent even when enabled:
  - ice-lite-on:               use_ice_lite=true  + Chrome  + disable=false -> PRESENT;
  - ice-lite-off:              use_ice_lite=false + Chrome  + disable=false -> ABSENT;
  - ice-lite-on-firefox:       use_ice_lite=true  + Firefox + disable=false -> ABSENT
                               (capability gate: profile.rs:41 !firefox / Go transport.go:414);
  - ice-lite-on-disable-param: use_ice_lite=true  + Chrome  + disable=true  -> ABSENT
                               (param gate: join.rs:72 !disable_ice_lite / Go roommanager.go:446).
The three gating profiles carry NO a=ice-lite on either side, so they SURVIVE an
accept-but-ignore mutant while ice-lite-on FAILS — specific, not indiscriminate.

Normalization (scope limit): the offer SDP foundation/priority/ice-ufrag/ice-pwd/
session-id and the (empty) candidate catalog are nondeterministic or non-signal per
launch, so we do NOT compare the SDP byte-equal cross-side. Instead we compare
DERIVED, deterministic semantics: ice_lite_present (the discriminator),
join_received, offer_received, signal_upgrade_status, refused, the daemon exit
code, port release and binary immutability. The raw SDP hash, the candidate catalog
and the configured ports are recorded per side for transparency but are NOT
cross-compared as byte-equal. No comparison field is deleted; these are explicit
scope bounds. We do NOT claim the daemon's full ICE surface is identical — only the
use_ice_lite ICE-lite selection consumer this harness drives. The eight observer
invocations run sequentially (shared low media-port band + loopback host).

Privacy normalization (commit boundary): the manifest embeds raw daemon server_log
provenance. Any RFC1918 private IPv4 literal is rewritten to RFC 5737 TEST-NET-1
(192.0.2.<last-octet>) before writing, preserving the last octet for correlation
while removing the real network identity. The redaction is pattern-based (no real
literal is embedded in this source). The loopback-only fixture config normally
emits no private literal, so this is a defensive no-op guard, not a data transform.

Out of scope (honest residual): WHIP forces ICE-lite off on both sides (correct
parity, non-discriminating); the srflx/relay candidate face, public NAT / NAT1To1
rewrite profiles, other platforms, ICE restart, the full ICE surface and the
generated --rtc.use_ice_lite flag remain open. This harness proves the launched-MAIN
use_ice_lite ICE-lite SDP selection consumer surface only (a=ice-lite added when
enabled, gated off when disabled / Firefox / disable_ice_lite param). Every original
observer exit code is preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, re, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.use_ice_lite']
PROFILES = ['ice-lite-on', 'ice-lite-off', 'ice-lite-on-firefox', 'ice-lite-on-disable-param']

# Per-profile observer stimulus. protocol=15 (>5) on every profile so the
# capability gate is exercised by browser alone; use_ice_lite is false only for
# ice-lite-off; disable_ice_lite is true only for ice-lite-on-disable-param;
# browser is Firefox only for ice-lite-on-firefox.
STIMULUS = {
    'ice-lite-on':               {'use_ice_lite': True,  'browser': 'Chrome',  'disable_ice_lite': False, 'protocol': 15},
    'ice-lite-off':              {'use_ice_lite': False, 'browser': 'Chrome',  'disable_ice_lite': False, 'protocol': 15},
    'ice-lite-on-firefox':       {'use_ice_lite': True,  'browser': 'Firefox', 'disable_ice_lite': False, 'protocol': 15},
    'ice-lite-on-disable-param': {'use_ice_lite': True,  'browser': 'Chrome',  'disable_ice_lite': True,  'protocol': 15},
}

# Expected DERIVED observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'ice-lite-on':               {'refused': False, 'server_exit': 0, 'signal_upgrade_status': 101, 'join_received': True, 'offer_received': True, 'ice_lite_present': True},
    'ice-lite-off':              {'refused': False, 'server_exit': 0, 'signal_upgrade_status': 101, 'join_received': True, 'offer_received': True, 'ice_lite_present': False},
    'ice-lite-on-firefox':       {'refused': False, 'server_exit': 0, 'signal_upgrade_status': 101, 'join_received': True, 'offer_received': True, 'ice_lite_present': False},
    'ice-lite-on-disable-param': {'refused': False, 'server_exit': 0, 'signal_upgrade_status': 101, 'join_received': True, 'offer_received': True, 'ice_lite_present': False},
}
# Cross-side comparison uses ONLY deterministic derived semantics + lifecycle. The
# nondeterministic SDP hash, the empty candidate catalog and the raw per-side
# reserved ports are recorded in the observables for transparency but NOT in COMPARED.
COMPARED = ['refused', 'server_exit', 'signal_upgrade_status', 'join_received',
            'offer_received', 'ice_lite_present', 'ports_released', 'binary_unchanged']
TRANSPARENT = ['candidate_types', 'candidate_transports', 'host_candidate_count',
               'udp_host_ports', 'tcp_host_ports', 'host_candidate_ips',
               'offer_sdp_sha256', 'ice_lite_lines', 'cfg_range', 'cfg_tcp']

# RFC1918 private IPv4 -> RFC 5737 TEST-NET-1, preserving the last octet.
_PRIVATE_IP = re.compile(
    r'\b(?:192\.168|10|172\.(?:1[6-9]|2[0-9]|3[01]))\.\d{1,3}\.(\d{1,3})\b')


def redact_private_ips(text):
    return _PRIVATE_IP.sub(lambda m: '192.0.2.' + m.group(1), text)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalize(r):
    return {
        'refused': r.get('refused') is True,
        'server_exit': r.get('server_exit'),
        'signal_upgrade_status': r.get('signal_upgrade_status'),
        'join_received': r.get('join_received') is True,
        'offer_received': r.get('offer_received') is True,
        'ice_lite_present': r.get('ice_lite_present') is True,
        'ports_released': r.get('server_ports_released') is True,
        'binary_unchanged': r.get('binary_unchanged') is True,
        'candidate_types': sorted(r.get('candidate_types', []) or []),
        'candidate_transports': sorted(r.get('candidate_transports', []) or []),
        'host_candidate_count': r.get('host_candidate_count', 0) or 0,
        'udp_host_ports': sorted(r.get('udp_host_ports', []) or []),
        'tcp_host_ports': sorted(r.get('tcp_host_ports', []) or []),
        'host_candidate_ips': sorted(r.get('host_candidate_ips', []) or []),
        'offer_sdp_sha256': r.get('offer_sdp_sha256'),
        'ice_lite_lines': r.get('ice_lite_lines', []) or [],
        'cfg_range': [r.get('configured_port_range_start', 0) or 0,
                      r.get('configured_port_range_end', 0) or 0],
        'cfg_tcp': r.get('configured_tcp_port', 0) or 0,
    }


def contract_ok(n, profile):
    if any(n[k] != v for k, v in CONTRACT[profile].items()):
        return False
    return n['ports_released'] and n['binary_unchanged']


def cross_equal(g, r):
    return all(g[k] == r[k] for k in COMPARED)


p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--observer', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
a = p.parse_args()
for name in ('go', 'rust', 'observer'):
    path = getattr(a, name)
    if not path.is_absolute():
        raise SystemExit(f'--{name} must be an absolute path: {path}')
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)

binaries = {'go': {'path': str(a.go), 'sha256': sha(a.go)},
            'rust': {'path': str(a.rust), 'sha256': sha(a.rust)}}
observer_sha = sha(a.observer)

NORM = {}
RAW = {}
for profile in PROFILES:
    NORM[profile] = {}
    RAW[profile] = {}
    stim = STIMULUS[profile]
    for side in ('go', 'rust'):
        binary = a.go if side == 'go' else a.rust
        out = a.output / f'{side}-{profile}'
        cmd = [str(a.observer), '--binary', str(binary), '--output', str(out),
               '--profile', profile,
               '--use-ice-lite=' + ('true' if stim['use_ice_lite'] else 'false'),
               '--browser', stim['browser'],
               '--disable-ice-lite=' + ('true' if stim['disable_ice_lite'] else 'false'),
               '--protocol', str(stim['protocol']),
               '--enable-loopback=true']
        proc = subprocess.run(cmd, capture_output=True, text=True)
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        n = normalize(result)
        n['observer_exit'] = proc.returncode
        # The observer exits 0 iff the profile ran cleanly (alive shutdown); a
        # nonzero exit is an infrastructure integrity fail.
        n['exit_integrity'] = (proc.returncode == 0) and n['binary_unchanged'] and n['ports_released']
        NORM[profile][side] = n
        RAW[profile][side] = {'exit': proc.returncode, 'result': result,
                              'stderr': proc.stderr[-2000:]}

P = {}
for profile in PROFILES:
    g, r = NORM[profile]['go'], NORM[profile]['rust']
    go_ok = contract_ok(g, profile) and g['exit_integrity']
    rust_ok = contract_ok(r, profile) and r['exit_integrity']
    equal = cross_equal(g, r)
    P[profile] = {
        'match': go_ok and rust_ok and equal,
        'go_contract_ok': go_ok,
        'rust_contract_ok': rust_ok,
        'cross_side_equal': equal,
        'go_observables': {k: g[k] for k in COMPARED + TRANSPARENT},
        'rust_observables': {k: r[k] for k in COMPARED + TRANSPARENT},
    }

controls = {}
for side in ('go', 'rust'):
    n = {profile: NORM[profile][side] for profile in PROFILES}
    # POSITIVE control: the discriminator CAN be True (not vacuously False).
    controls[f'ice_lite_present_when_enabled_{side}'] = n['ice-lite-on']['ice_lite_present'] is True
    controls[f'ice_lite_absent_when_disabled_{side}'] = n['ice-lite-off']['ice_lite_present'] is False
    controls[f'ice_lite_capability_gated_{side}'] = n['ice-lite-on-firefox']['ice_lite_present'] is False
    controls[f'ice_lite_param_gated_{side}'] = n['ice-lite-on-disable-param']['ice_lite_present'] is False
    controls[f'join_offer_upgrade_integrity_{side}'] = all(
        n[profile]['join_received'] and n[profile]['offer_received']
        and n[profile]['signal_upgrade_status'] == 101 and n[profile]['server_exit'] == 0
        and n[profile]['ports_released'] and n[profile]['binary_unchanged']
        and n[profile]['exit_integrity'] for profile in PROFILES)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_USE_ICE_LITE' if all_match else 'FAIL',
    'case_count': len(PROFILES),
    'items': COVERS,
    'profiles': P,
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': binaries['go']['sha256'] == FIXED_GO_SHA,
    'harness_sha256': sha(pathlib.Path(__file__).resolve()),
    'privacy_normalization': 'RFC1918 private IPv4 literals in raw server_log rewritten to RFC5737 TEST-NET-1 (last octet preserved); pattern-based, no real literal embedded.',
    'raw': RAW,
}
(a.output / 'execution.json').write_text(
    redact_private_ips(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n'))

batch = {
    'status': manifest['status'],
    'case_count': len(PROFILES),
    'items': COVERS,
    'profiles': {name: {'match': e['match'],
                        'go_contract_ok': e['go_contract_ok'], 'rust_contract_ok': e['rust_contract_ok'],
                        'cross_side_equal': e['cross_side_equal'],
                        'go_observables': e['go_observables'], 'rust_observables': e['rust_observables']}
                 for name, e in P.items()},
    'cross_profile_controls': controls,
    'failed_profiles': failed_profiles,
    'failed_controls': failed_controls,
    'binaries': binaries,
    'observer_sha256': observer_sha,
    'input_identity_stable': manifest['input_identity_stable'],
    'harness_sha256': manifest['harness_sha256'],
}
(a.output / 'BATCH.json').write_text(
    redact_private_ips(json.dumps(batch, indent=2, ensure_ascii=False) + '\n'))
print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
                  'input_identity_stable': manifest['input_identity_stable']}))
raise SystemExit(0 if all_match else 1)