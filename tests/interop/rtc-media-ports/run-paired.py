#!/usr/bin/env python3
"""SPEC-0077 C32 — launched-MAIN media-port family paired Go/Rust differential.

Four ledger leaves are named: YAML:rtc.udp_port, YAML:rtc.tcp_port,
YAML:rtc.port_range_start, YAML:rtc.port_range_end. The locked Go oracle
(v1.13.10 e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate
are each launched as the REAL main server, five times, by the self-contained
rtc-media-ports observer (tests/interop/rtc-media-ports/observe.go). The observer
writes the media-port keys into the daemon YAML, reserves the configured ports in
a LOW band (40000-40999, decisively below the macOS ephemeral range 49152-65535),
releases them so the daemon can bind, drives an authenticated WHIP Create then
DELETE with a filter-less Pion offer, and extracts the HOST ICE candidate PORTS
per transport from the daemon's SDP answer. This harness wraps that observer per
side per profile and compares the normalized port-selection semantics cross-side.

Upstream consumers (locked Go, pinned mediatransportutil-f234b534b095):
    rtcconfig/webrtc_config.go:127-130
        if rtcConf.ICEPortRangeStart != 0 && rtcConf.ICEPortRangeEnd != 0 {
            s.SetEphemeralUDPPortRange(start, end)   // dedicated range, TAKES PRECEDENCE
        }
    rtcconfig/webrtc_config.go:131-171
        else if rtcConf.UDPPort.Valid() {
            CreateUDPMuxesFromPorts(...) -> s.SetICEUDPMux(...)   // udp_port mux
        }
    rtcconfig/webrtc_config.go:180-185
        if rtcConf.TCPPort != 0 { net.ListenTCP("tcp", &TCPAddr{Port: TCPPort}) }
    The range branch is checked FIRST, so when both port_range_start/end and
    udp_port are configured the dedicated range wins and the mux port is unused.

Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
    config projection + precedence: lk-config/src/normalize.rs:442-460
        `if r.port_range_start != 0 && r.port_range_end != 0 { UdpMode::Range }`
        `else if r.udp_port.valid() { UdpMode::Mux { ports } } else { Disabled }`
        — the SAME range-over-mux precedence as Go :127-130 before :131-171.
    net_config: livecloud/src/interfaces.rs:335-348
        Mux -> (udp_port = ports.first(), port_range = (0,0));
        Range -> (udp_port = None, port_range = (start,end));
        udp_enabled = !Disabled && !force_tcp; tcp_port = rtc.tcp_port.
    mux port rotation: interfaces.rs:260-271 mux_ports_for_cpu
        (force_tcp || !Mux -> empty; else take(cpus)), :279 startup_mux_ports.
    server assembly: livecloud/src/server.rs:330 net_config, :332
        tcp_port = (rtc.tcp_port != 0).then_some(..), :333 force_tcp guard,
        :336-343 start_with_configured_ports(net_cfg, startup_mux_ports,
        standalone_stun_mux_port, tcp_port, ..).
    socket binds: rtc-runtime/src/net/mod.rs:344-347 cfg.udp_port =
        ports.first().or(standalone_port); :363-394 UDP mux binds (advertised
        port = bound.port()); :396-400 ONE wildcard TCP listen(port) -> bound
        port; :658/662 bind_dedicated_host -> udp::bind_in_range(host.bind,
        cfg.port_range); udp.rs:33-39 bind_in_range((0,0)) -> OS-ephemeral.

Candidate provenance (load-bearing): the observer's Pion driver configures NO
port preference and NO interface filter of its own
(webrtc.NewPeerConnection(Configuration{})), so every recorded host candidate
PORT originates from the DAEMON's bound media sockets and depends on the
media-port keys being consumed. With node_ip=127.0.0.1 + use_external_ip=false
the Go NAT1To1 catch-all (webrtc_config.go:111-116) masks candidate IPs to
127.0.0.1 but NOT ports, so the PORT is the surviving discriminating observable.

Discriminator choice (empirically grounded on BOTH sides, load-bearing): the
configured ports are reserved in the LOW band 40000-40999, decisively below the
macOS ephemeral floor 49152. An accept-but-ignore mutant that falls back to
OS-chosen ephemeral ports therefore lands OUTSIDE the configured band, so the
port-selection axis is robust rather than probabilistic:
  - udp-mux: udp_port=U -> the sole UDP host port is EXACTLY U (mux);
  - udp-range: port_range=[A,B] -> every UDP host port lies IN [A,B] (dedicated
    cyclic search; the specific ports/count are nondeterministic per launch);
  - range-precedence: udp_port=U (outside [A,B]) AND port_range=[A,B] -> every
    UDP host port lies in [A,B] and U is UNUSED (range wins, Go :127-130);
  - tcp-only: force_tcp + tcp_port=T -> ZERO UDP host ports, the sole TCP host
    port is EXACTLY T, transport catalog ['tcp'];
  - ephemeral-baseline: no udp_port, no port_range, tcp_port=0 -> UDP host ports
    are OS-ephemeral (outside the low band), ZERO TCP host ports (tcp_port=0
    overrides the Go default 7881), transport catalog ['udp']. This baseline
    carries NO configured media port, so it SURVIVES an accept-but-ignore mutant
    (the mutant's ephemeral fallback matches it) while the four configured
    profiles FAIL — specific, not indiscriminate (see the mutant receipt).

Normalization (scope limit): the dedicated-range ports and the host-candidate
COUNT are nondeterministic per launch (cyclic random start, per-interface
gathering), so we do NOT compare them byte-equal cross-side. Instead we compare
DERIVED, deterministic port-selection semantics: tcp_port_honored (TCP host ports
== [configured tcp_port], or == [] when tcp_port=0), udp_mux_exact (UDP host ports
== [configured udp_port] when a mux port is set), udp_in_configured_range (non-empty
and every UDP host port within [start,end]), udp_mux_port_unused (the configured
mux port is absent from the UDP host ports), udp_empty, udp_all_ephemeral (non-empty
and every UDP host port above the low band), plus the transport catalog, the
candidate-type catalog, the WHIP statuses, the daemon exit code, port release and
binary immutability. The raw configured values and observed port lists are recorded
per side for transparency but are NOT cross-compared as byte-equal (they are
per-side reservations / nondeterministic). No comparison field is deleted; these
are explicit scope bounds. We do NOT claim the daemon's full ICE surface is
identical — only the media-port selection consumer this harness drives. The low
media band is shared, so the ten observer invocations run sequentially.

Out of scope (honest residual): the srflx/relay candidate surface, multi-port mux
rotation across >1 configured port and CPU-count truncation, the standalone STUN
mux port (use_stun_port_as_ice), public NAT / NAT1To1 rewrite profiles, other
platforms, ICE restart / dedicated-host refresh and exhaustion recovery (separately
witnessed in the port_range historical evidence), and the generated
--rtc.udp_port / --rtc.tcp_port / --rtc.port_range_* flags remain open. This
harness proves the launched-MAIN media-port SELECTION consumer surface only
(single mux port, dedicated range containment, range-over-mux precedence, TCP mux
port, force_tcp UDP suppression, tcp_port=0). Every original observer exit code is
preserved; no pipe filters a result.
"""
import argparse, hashlib, json, pathlib, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.udp_port', 'YAML:rtc.tcp_port',
          'YAML:rtc.port_range_start', 'YAML:rtc.port_range_end']
PROFILES = ['udp-mux', 'udp-range', 'range-precedence', 'tcp-only', 'ephemeral-baseline']
CONFIGURED = ('udp-mux', 'udp-range', 'range-precedence', 'tcp-only')
ALIVE = tuple(PROFILES)  # no refusal profile: media-port keys never refuse startup here
LOW_BAND_END = 40999     # configured band 40000-40999; macOS ephemeral floor 49152

# Per-profile observer stimulus. tcp_mode=on reserves an explicit low TCP port T
# (so the shared-host default 7881 is never touched); tcp_mode=zero writes an
# explicit tcp_port=0 for the baseline. force_tcp is set only for tcp-only.
STIMULUS = {
    'udp-mux':            {'udp_mode': 'mux',        'tcp_mode': 'on',   'force_tcp': False},
    'udp-range':          {'udp_mode': 'range',      'tcp_mode': 'on',   'force_tcp': False},
    'range-precedence':   {'udp_mode': 'precedence', 'tcp_mode': 'on',   'force_tcp': False},
    'tcp-only':           {'udp_mode': 'ephemeral',  'tcp_mode': 'on',   'force_tcp': True},
    'ephemeral-baseline': {'udp_mode': 'ephemeral',  'tcp_mode': 'zero', 'force_tcp': False},
}

# Expected DERIVED observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
CONTRACT = {
    'udp-mux':            {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp', 'udp'], 'candidate_types': ['host'], 'tcp_port_honored': True,  'udp_mux_exact': True,  'udp_in_configured_range': False, 'udp_mux_port_unused': False, 'udp_empty': False, 'udp_all_ephemeral': False},
    'udp-range':          {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp', 'udp'], 'candidate_types': ['host'], 'tcp_port_honored': True,  'udp_mux_exact': None,  'udp_in_configured_range': True,  'udp_mux_port_unused': False, 'udp_empty': False, 'udp_all_ephemeral': False},
    'range-precedence':   {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp', 'udp'], 'candidate_types': ['host'], 'tcp_port_honored': True,  'udp_mux_exact': False, 'udp_in_configured_range': True,  'udp_mux_port_unused': True,  'udp_empty': False, 'udp_all_ephemeral': False},
    'tcp-only':           {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['tcp'],        'candidate_types': ['host'], 'tcp_port_honored': True,  'udp_mux_exact': None,  'udp_in_configured_range': False, 'udp_mux_port_unused': False, 'udp_empty': True,  'udp_all_ephemeral': False},
    'ephemeral-baseline': {'refused': False, 'server_exit': 0, 'whip': [201, 200], 'transports': ['udp'],        'candidate_types': ['host'], 'tcp_port_honored': True,  'udp_mux_exact': None,  'udp_in_configured_range': False, 'udp_mux_port_unused': False, 'udp_empty': False, 'udp_all_ephemeral': True},
}
# Cross-side comparison uses ONLY deterministic derived semantics + catalogs +
# lifecycle. Raw per-side reserved values and nondeterministic port lists are
# recorded in the observables for transparency but are NOT in COMPARED.
COMPARED = ['refused', 'server_exit', 'whip', 'transports', 'candidate_types',
            'ports_released', 'binary_unchanged', 'tcp_port_honored',
            'udp_mux_exact', 'udp_in_configured_range', 'udp_mux_port_unused',
            'udp_empty', 'udp_all_ephemeral']
TRANSPARENT = ['cfg_udp', 'cfg_range', 'cfg_tcp', 'udp_host_ports', 'tcp_host_ports']


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
        'tcp_port_honored': (tcp == [cfg_tcp]) if cfg_tcp > 0 else (tcp == []),
        'udp_mux_exact': (udp == [cfg_udp]) if cfg_udp > 0 else None,
        'udp_in_configured_range': (cfg_rs > 0 and cfg_re > 0 and len(udp) > 0
                                    and all(cfg_rs <= p <= cfg_re for p in udp)),
        'udp_mux_port_unused': (cfg_udp > 0 and cfg_udp not in udp),
        'udp_empty': (udp == []),
        'udp_all_ephemeral': (len(udp) > 0 and all(p > LOW_BAND_END for p in udp)),
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
        'configured': profile in CONFIGURED,
        'go_contract_ok': go_ok,
        'rust_contract_ok': rust_ok,
        'cross_side_equal': equal,
        'go_observables': {k: g[k] for k in COMPARED + TRANSPARENT},
        'rust_observables': {k: r[k] for k in COMPARED + TRANSPARENT},
    }

controls = {}
for side in ('go', 'rust'):
    n = {profile: NORM[profile][side] for profile in PROFILES}
    controls[f'mux_udp_port_exact_{side}'] = n['udp-mux']['udp_mux_exact'] is True
    controls[f'range_containment_and_precedence_{side}'] = (
        n['udp-range']['udp_in_configured_range']
        and n['range-precedence']['udp_in_configured_range']
        and n['range-precedence']['udp_mux_port_unused'])
    controls[f'tcp_mux_port_honored_{side}'] = all(
        n[profile]['tcp_port_honored'] for profile in CONFIGURED)
    controls[f'tcp_only_suppresses_udp_{side}'] = (
        n['tcp-only']['udp_empty'] and n['tcp-only']['transports'] == ['tcp'])
    controls[f'ephemeral_baseline_no_config_{side}'] = (
        n['ephemeral-baseline']['udp_all_ephemeral']
        and n['ephemeral-baseline']['tcp_port_honored']
        and n['ephemeral-baseline']['transports'] == ['udp'])
    controls[f'whip_exit_release_integrity_{side}'] = all(
        n[profile]['whip'] == [201, 200] and n[profile]['server_exit'] == 0
        and n[profile]['ports_released'] and n[profile]['binary_unchanged']
        and n[profile]['exit_integrity'] for profile in ALIVE)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_MEDIA_PORTS' if all_match else 'FAIL',
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
    'profiles': {name: {'match': e['match'], 'configured': e['configured'],
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