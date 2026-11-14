#!/usr/bin/env python3
"""SPEC-0077 C33 — launched-MAIN rtc.use_stun_port_as_ice paired Go/Rust differential.

One ledger leaf is named: YAML:rtc.use_stun_port_as_ice. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate
are each launched as the REAL main server, four times, by the self-contained
rtc-stun-port-as-ice observer (tests/interop/rtc-stun-port-as-ice/observe.go). The
observer writes use_stun_port_as_ice plus the gating media-port keys into the
daemon YAML, reserves the configured mux port in a LOW band (40000-40999, below
the macOS ephemeral range 49152-65535), verifies the fixed STUN port 3478 is free,
releases the reservations so the daemon can bind, drives an authenticated WHIP
Create then DELETE with a filter-less Pion offer, and extracts the HOST ICE
candidate PORTS per transport from the daemon's SDP answer. This harness wraps that
observer per side per profile and compares the normalized port-selection semantics
cross-side.

Upstream consumer (locked Go, pinned mediatransportutil-f234b534b095):
    rtcconfig/webrtc_config.go:123   if !rtcConf.ForceTCP {            // UDP guard
    rtcconfig/webrtc_config.go:127     if ICEPortRangeStart/End != 0 { range }
    rtcconfig/webrtc_config.go:131     else if rtcConf.UDPPort.Valid() {  // mux
    rtcconfig/webrtc_config.go:162-167   if rtcConf.UseStunPortAsICE {
                                           CreateUDPMuxesFromPorts([]int{3478})
                                         }
    rtcconfig/webrtc_config.go:169       NewMultiPortsUDPMux(muxes, standalonePortMuxes)
    The standalone STUN mux is created ONLY inside !ForceTCP AND mux mode
    (UDPPort.Valid(), i.e. not the dedicated-range branch), so use_stun_port_as_ice
    adds a 3478 host candidate alongside the configured mux port, and is gated off
    in range mode and under force_tcp.

Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
    config projection: lk-config/src/normalize.rs:589 use_stun_port_as_ice passthrough.
    standalone STUN mux: livecloud/src/interfaces.rs:274-277 standalone_stun_mux_port
        = (!force_tcp && use_stun_port_as_ice && UdpMode::Mux).then_some(3478)
        — the SAME !force_tcp + mux gating as Go :123/:131/:162.
    server assembly: livecloud/src/server.rs:339 standalone_stun_mux_port arg ->
        :336 start_with_configured_ports(net_cfg, startup_mux_ports, standalone_port, ..).
    socket binds: rtc-runtime/src/net/mod.rs:365 two mux lists
        [(ports,false),(standalone_port,true)]; :384-390 the standalone 3478 bind is
        pushed into shared_candidates; :477 shared_candidates advertised via
        open_transport. So the 3478 candidate is gathered exactly like Go's
        standalonePortMuxes.

Candidate provenance (load-bearing): the observer's Pion driver configures NO port
preference and NO interface filter of its own (webrtc.NewPeerConnection(Configuration{})),
so every recorded host candidate PORT originates from the DAEMON's bound media
sockets and depends on use_stun_port_as_ice being consumed. With node_ip=127.0.0.1 +
use_external_ip=false the Go NAT1To1 catch-all (webrtc_config.go:111-116) masks
candidate IPs to 127.0.0.1 but NOT ports, so the PORT is the surviving discriminator.

Discriminator choice (empirically grounded on BOTH sides, load-bearing): the fixed
STUN port 3478 is NOT reserved by the observer (the daemon binds it); the configured
mux port is reserved in the LOW band 40000-40999. An accept-but-ignore mutant that
ignores use_stun_port_as_ice never binds 3478, so the 3478 host candidate is absent:
  - stun-on-mux:       udp_port=U (mux) + use_stun_port_as_ice=true  -> UDP host ports
                       contain BOTH U and 3478 (stun_3478_present=True);
  - stun-off-mux:      udp_port=U (mux) + use_stun_port_as_ice=false -> UDP host ports
                       == {U}, NO 3478 (stun_3478_present=False);
  - stun-on-range:     port_range=[A,B] + use_stun_port_as_ice=true  -> NO 3478 (range,
                       not mux; Go :127 before :131, Rust UdpMode::Range gating);
  - stun-on-force-tcp: force_tcp + use_stun_port_as_ice=true         -> ZERO UDP host
                       ports, NO 3478 (Go :123 guard, Rust !force_tcp gating).
The three gating profiles carry NO 3478 on either side, so they SURVIVE an
accept-but-ignore mutant while stun-on-mux FAILS — specific, not indiscriminate.

Normalization (scope limit): the dedicated-range ports and the host-candidate COUNT
are nondeterministic per launch, so we do NOT compare them byte-equal cross-side.
Instead we compare DERIVED, deterministic port-selection semantics: stun_3478_present
(3478 in the UDP host ports), mux_port_present (the configured mux port in the UDP
host ports, or None when no mux port is set), udp_in_configured_range, udp_empty,
tcp_port_honored, plus the transport catalog, the candidate-type catalog, the WHIP
statuses, the daemon exit code, port release (including 3478) and binary immutability.
The raw configured values and observed port lists are recorded per side for
transparency but are NOT cross-compared as byte-equal. No comparison field is deleted;
these are explicit scope bounds. We do NOT claim the daemon's full ICE surface is
identical — only the use_stun_port_as_ice standalone-mux consumer this harness drives.
3478 is a shared fixed port, so the eight observer invocations run sequentially.

Out of scope (honest residual): the srflx/relay candidate surface, multi-port mux
rotation and CPU-count truncation, public NAT / NAT1To1 rewrite profiles, other
platforms, ICE restart, and the generated --rtc.use_stun_port_as_ice flag remain open.
This harness proves the launched-MAIN use_stun_port_as_ice standalone-STUN-mux
selection consumer surface only (3478 added in mux mode, gated off when disabled / in
range mode / under force_tcp). Every original observer exit code is preserved; no pipe
filters a result.
"""
import argparse, hashlib, json, pathlib, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.use_stun_port_as_ice']
PROFILES = ['stun-on-mux', 'stun-off-mux', 'stun-on-range', 'stun-on-force-tcp']
STUN_PORT = 3478
LOW_BAND_END = 40999     # configured band 40000-40999; macOS ephemeral floor 49152

# Per-profile observer stimulus. tcp_mode=on reserves an explicit low TCP port T
# (so the shared-host default 7881 is never touched). force_tcp is set only for
# stun-on-force-tcp; use_stun_port_as_ice is false only for stun-off-mux.
STIMULUS = {
    'stun-on-mux':       {'udp_mode': 'mux',       'tcp_mode': 'on', 'force_tcp': False, 'stun': True},
    'stun-off-mux':      {'udp_mode': 'mux',       'tcp_mode': 'on', 'force_tcp': False, 'stun': False},
    'stun-on-range':     {'udp_mode': 'range',     'tcp_mode': 'on', 'force_tcp': False, 'stun': True},
    'stun-on-force-tcp': {'udp_mode': 'ephemeral', 'tcp_mode': 'on', 'force_tcp': True,  'stun': True},
}

# Expected DERIVED observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'stun-on-mux':       {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp', 'udp'], 'candidate_types': ['host'], 'stun_3478_present': True,  'mux_port_present': True, 'udp_in_configured_range': False, 'udp_empty': False, 'tcp_port_honored': True},
    'stun-off-mux':      {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp', 'udp'], 'candidate_types': ['host'], 'stun_3478_present': False, 'mux_port_present': True, 'udp_in_configured_range': False, 'udp_empty': False, 'tcp_port_honored': True},
    'stun-on-range':     {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp', 'udp'], 'candidate_types': ['host'], 'stun_3478_present': False, 'mux_port_present': None, 'udp_in_configured_range': True,  'udp_empty': False, 'tcp_port_honored': True},
    'stun-on-force-tcp': {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp'],        'candidate_types': ['host'], 'stun_3478_present': False, 'mux_port_present': None, 'udp_in_configured_range': False, 'udp_empty': True,  'tcp_port_honored': True},
}
# Cross-side comparison uses ONLY deterministic derived semantics + catalogs +
# lifecycle. Raw per-side reserved values and nondeterministic port lists are
# recorded in the observables for transparency but are NOT in COMPARED.
COMPARED = ['refused', 'server_exit', 'whip', 'transports', 'candidate_types',
            'ports_released', 'binary_unchanged', 'stun_3478_present',
            'mux_port_present', 'udp_in_configured_range', 'udp_empty', 'tcp_port_honored']
TRANSPARENT = ['cfg_udp', 'cfg_range', 'cfg_tcp', 'udp_host_ports', 'tcp_host_ports',
               'stun_port_expected_bound']


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def normalize(r):
    cfg_udp = r.get('configured_udp_port', 0) or 0
    cfg_rs = r.get('configured_port_range_start', 0) or 0
    cfg_re = r.get('configured_port_range_end', 0) or 0
    cfg_tcp = r.get('configured_tcp_port', 0) or 0
    udp = sorted(r.get('udp_host_ports', []))
    tcp = sorted(r.get('tcp_host_ports', []))
    return {
        'refused': r.get('refused') is True,
        'server_exit': r.get('server_exit'),
        'whip': [r.get('whip_create_status'), r.get('whip_delete_status')],
        'transports': sorted(r.get('candidate_transports', [])),
        'candidate_types': sorted(r.get('candidate_types', [])),
        'ports_released': r.get('server_ports_released') is True,
        'binary_unchanged': r.get('binary_unchanged') is True,
        'cfg_udp': cfg_udp, 'cfg_range': [cfg_rs, cfg_re], 'cfg_tcp': cfg_tcp,
        'udp_host_ports': udp, 'tcp_host_ports': tcp,
        'stun_port_expected_bound': r.get('stun_port_expected_bound') is True,
        'stun_3478_present': STUN_PORT in udp,
        'mux_port_present': (cfg_udp in udp) if cfg_udp > 0 else None,
        'udp_in_configured_range': (cfg_rs > 0 and cfg_re > 0 and len(udp) > 0
                                    and all(cfg_rs <= p <= cfg_re for p in udp)),
        'udp_empty': (udp == []),
        'tcp_port_honored': (tcp == [cfg_tcp]) if cfg_tcp > 0 else (tcp == []),
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
               '--udp-mode', stim['udp_mode'],
               '--tcp-mode', stim['tcp_mode'],
               '--force-tcp=' + ('true' if stim['force_tcp'] else 'false'),
               '--use-stun-port-as-ice=' + ('true' if stim['stun'] else 'false'),
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
    controls[f'stun_port_added_when_enabled_{side}'] = n['stun-on-mux']['stun_3478_present'] is True
    controls[f'stun_port_absent_when_disabled_{side}'] = n['stun-off-mux']['stun_3478_present'] is False
    controls[f'stun_port_mux_gated_{side}'] = n['stun-on-range']['stun_3478_present'] is False
    controls[f'stun_port_force_tcp_gated_{side}'] = (
        n['stun-on-force-tcp']['stun_3478_present'] is False
        and n['stun-on-force-tcp']['udp_empty'])
    controls[f'mux_port_present_when_configured_{side}'] = (
        n['stun-on-mux']['mux_port_present'] is True
        and n['stun-off-mux']['mux_port_present'] is True)
    controls[f'whip_exit_release_integrity_{side}'] = all(
        n[profile]['whip'] == [201, 200] and n[profile]['server_exit'] == 0
        and n[profile]['ports_released'] and n[profile]['binary_unchanged']
        and n[profile]['exit_integrity'] for profile in PROFILES)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_STUN_PORT_AS_ICE' if all_match else 'FAIL',
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
    'raw': RAW,
}
(a.output / 'execution.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')

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
(a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
                  'input_identity_stable': manifest['input_identity_stable']}))
raise SystemExit(0 if all_match else 1)