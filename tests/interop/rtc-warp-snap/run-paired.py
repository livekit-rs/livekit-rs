#!/usr/bin/env python3
"""SPEC-0077 C51 — launched-MAIN rtc.enable_warp SNAP paired Go/Rust differential.

Two ledger leaves are named: YAML:rtc.enable_warp and
CONFIG_ENABLED_CAPABILITY:rtc.enable_warp. The locked Go oracle (v1.13.10
e5655ac2 == binary 4bc2378e) and the Rust livecloud release candidate are each
launched as the REAL main server, twice (enable_warp on/off), by the
self-contained rtc-warp-snap observer (tests/interop/rtc-warp-snap/observe.go).
The observer writes enable_warp plus the gating media-port keys into the daemon
YAML, reserves the configured media ports in a LOW band (40000-40999, below the
macOS ephemeral range 49152-65535), releases the reservations so the daemon can
bind, drives an authenticated MEDIA-FREE signal join over WebSocket /rtc with a
modern client (protocol>5), and records whether the daemon's subscriber SDP
OFFER carries the `a=sctp-init:<base64 INIT>` attribute (SNAP = SCTP INIT in SDP,
the data-channel half of WARP). This harness wraps that observer per side per
profile and compares the normalized SNAP selection semantics cross-side.

This advances the main_server_consumer tier (NOT_RUN->MET): the FULL assembled
livecloud executable consumes rtc.enable_warp end-to-end. The C05-sped batch
already attested official_differential_lifecycle + component_behavior at the
MEDIA-ENGINE level (a fixed Pion WARP peer, not the assembled server); the
c20-warp diff was STARTUP-LOG-ONLY (no real SNAP observation), which is why
main_server_consumer stayed NOT_RUN. This harness boots the real MAIN binary.

Upstream consumer (locked Go v1.13.10 e5655ac2, pinned pion WARP forks):
    pkg/config/config.go:135-138      EnableWarp bool `yaml:"enable_warp"` (default false)
    pkg/service/roommanager.go:487    enableWarp := !useOneShotSignallingMode && r.config.RTC.EnableWarp
    pkg/rtc/transport.go:389-393      se.EnableSped(true); se.EnableSctpSnap(true) + Debugw("enable warp")
    pion peerconnection.go:2894-2895  localSctpInit only when dataChannelsRequested != 0 && sctp.enableSnap
    pion sdp.go:396-397               a=sctp-init:<base64 INIT chunk>
    So enable_warp turns on the SNAP a=sctp-init attribute in the subscriber offer,
    gated off by one-shot signalling (WHIP), which the /rtc signal join is NOT.

Rust mirror (complete launched-MAIN consumer chain, all anchors re-verified):
    config parse:       lk-config/src/official.rs:320   enable_warp: bool = false.
    config projection:  lk-config/src/normalize.rs:605  enable_warp passthrough.
    engine consumer:    rtc-media/src/engine.rs:310     let warp = rtc_cfg.enable_warp && !spec.one_shot.
    engine debug:       rtc-media/src/engine.rs:312     tracing::debug!("enable warp").
    SNAP opt-in:        rtc-media/src/engine.rs:323     .set_snap_enabled(warp)  (str0m a=sctp-init).
    SPED opt-in:        rtc-media/src/engine.rs:324     .set_sped_enabled(warp)  (attested by C05, see scope).
    str0m SNAP:         src/sctp/snap.rs, src/sdp/data.rs:1505 (write), src/change/sdp.rs (offer).
    answer gating:      vendor/str0m PATCHES.md Patch 24 snap-answer-requires-local-optin
                        (answer reciprocal a=sctp-init conditional on local opt-in, pion :3014 parity).

WHY THE MEDIA-FREE SIGNAL JOIN (load-bearing): the warp gate is
`enable_warp && !one_shot`. The legacy /rtc WebSocket join is the NORMAL
(non-one-shot) signalling path, so warp == enable_warp here. The daemon's own
subscriber peer-connection offer is emitted on join (auto_subscribe=true) before
any media; it carries m=application (data channels) and, when SNAP is opted in,
a=sctp-init. The observer publishes no track and sends no media, so the a=sctp-init
attribute originates from the daemon's enable_warp consumption, not from a client.

Discriminator choice (empirically grounded on BOTH sides, load-bearing):
sctp_init_present = (an `a=sctp-init:` attribute line in the daemon subscriber offer
SDP, trimmed-line prefix match). application_present = (an `m=application` media
section, the data-channel precondition for SNAP). The subscriber offer is sent
BEFORE trickle ICE gathering, so the host-candidate catalog is EMPTY on both sides
and carries no signal; a=sctp-init is the clean session-level observable. With
node_ip=127.0.0.1 + use_external_ip=false the Go NAT1To1 catch-all masks candidate
IPs to 127.0.0.1 but does NOT touch the a=sctp-init attribute. An accept-but-ignore
mutant that ignores enable_warp never calls set_snap_enabled, so a=sctp-init is
absent even when enabled:
  - warp-on:  enable_warp=true  -> a=sctp-init PRESENT;
  - warp-off: enable_warp=false -> a=sctp-init ABSENT (m=application still PRESENT).
warp-on FAILS the mutant while warp-off SURVIVES — specific, not indiscriminate.

Normalization (scope limit): the a=sctp-init base64 INIT blob is NONDETERMINISTIC
(verification tag / timestamps / supported-extension params differ per launch and
per stack: Go emits a longer INIT than str0m), and the offer SDP foundation/priority/
ice-ufrag/ice-pwd/session-id are nondeterministic per launch. So we do NOT compare
the SDP or the INIT blob byte-equal cross-side. Instead we compare DERIVED,
deterministic semantics: sctp_init_present (the discriminator), application_present,
join_received, offer_received, signal_upgrade_status, refused, the daemon exit code,
port release and binary immutability. The raw SDP hash, the a=sctp-init line(s) and
the configured ports are recorded per side for transparency but are NOT cross-compared
as byte-equal. No comparison field is deleted; these are explicit scope bounds. We do
NOT claim the daemon's full WARP surface is identical — only the enable_warp SNAP
selection consumer this harness drives. The four observer invocations run sequentially
(shared low media-port band + loopback host).

HONEST SCOPE (what main_server_consumer MET attests here, and what stays open):
This harness proves the assembled MAIN server consumes rtc.enable_warp end-to-end
through normalize->ServerConfig->rtc-core->engine and emits a=sctp-init (SNAP) when
enabled. The SAME `warp` bool feeds set_sped_enabled (engine.rs:324), so the SPED
opt-in wiring is reached by the proven chain; but SPED (DTLS-in-STUN) is a WIRE
behavior observable only during the DTLS handshake with a WARP client, NOT in the
SDP offer. SPED wire behavior is attested at the MEDIA-ENGINE level by the C05-sped
paired Go/Pion differential (component_behavior + official_differential_lifecycle
MET), and is NOT separately re-observed in the assembled server by this SDP-only
harness. The item state therefore STAYS PARTIAL: full browser/Agent WebTransport
authentication, the assembled-server SPED wire face, and the one-shot (WHIP) warp-off
gate path remain open residuals. This is the SNAP half of the full-server enable_warp
consumer, not a claim of complete WARP feature parity.

Privacy normalization (commit boundary): the manifest embeds raw daemon server_log
provenance. Any RFC1918 private IPv4 literal is rewritten to RFC 5737 TEST-NET-1
(192.0.2.<last-octet>) before writing, preserving the last octet for correlation
while removing the real network identity. The redaction is pattern-based (no real
literal is embedded in this source). The loopback-only fixture config normally
emits no private literal, so this is a defensive no-op guard, not a data transform.

Out of scope (honest residual): the assembled-server SPED DTLS-in-STUN wire face
(C05 media-engine attestation stands), full browser/Agent WebTransport authentication,
the one-shot/WHIP warp-off gate path (WHIP forces one-shot, not driven here), the
generated --rtc.enable_warp flag item (separate OPEN_AUDIT leaf, NOT touched), other
platforms, and the full ICE/DTLS/SCTP surface remain open. Every original observer
exit code is preserved; no pipe filters a result.
"""
import argparse, hashlib, json, os, pathlib, re, signal, subprocess

FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
COVERS = ['YAML:rtc.enable_warp', 'CONFIG_ENABLED_CAPABILITY:rtc.enable_warp']
PROFILES = ['warp-on', 'warp-off']

# Per-profile observer stimulus. enable_warp is the only driven config item; the
# /rtc signal join is non-one-shot so warp == enable_warp. browser=Chrome and
# protocol=15 (>5) on every profile: enable_warp has NO browser/protocol capability
# gate (unlike use_ice_lite), so the discriminator is enable_warp alone.
STIMULUS = {
    'warp-on':  {'enable_warp': True,  'browser': 'Chrome', 'protocol': 15},
    'warp-off': {'enable_warp': False, 'browser': 'Chrome', 'protocol': 15},
}

# Expected DERIVED observables per profile, derived from the locked Go oracle
# (empirically probed) and confirmed identical on the Rust candidate. The
# differential asserts these on EACH side independently, then compares cross-side.
# application_present is True on BOTH profiles: data channels are negotiated
# regardless of warp; only the a=sctp-init SNAP attribute is warp-gated.
CONTRACT = {
    'warp-on':  {'refused': False, 'server_exit': 0, 'signal_upgrade_status': 101, 'join_received': True, 'offer_received': True, 'application_present': True, 'sctp_init_present': True},
    'warp-off': {'refused': False, 'server_exit': 0, 'signal_upgrade_status': 101, 'join_received': True, 'offer_received': True, 'application_present': True, 'sctp_init_present': False},
}
# Cross-side comparison uses ONLY deterministic derived semantics + lifecycle. The
# nondeterministic SDP hash, the nondeterministic a=sctp-init base64 blob, the empty
# candidate catalog and the raw per-side reserved ports are recorded in the
# observables for transparency but NOT in COMPARED.
COMPARED = ['refused', 'server_exit', 'signal_upgrade_status', 'join_received',
            'offer_received', 'sctp_init_present', 'application_present',
            'ports_released', 'binary_unchanged']
TRANSPARENT = ['candidate_types', 'candidate_transports', 'host_candidate_count',
               'udp_host_ports', 'tcp_host_ports', 'host_candidate_ips',
               'offer_sdp_sha256', 'sctp_init_lines', 'cfg_range', 'cfg_tcp']

# RFC1918 private IPv4 -> RFC 5737 TEST-NET-1, preserving the last octet.
_PRIVATE_IP = re.compile(
    r'\b(?:192\.168\.\d{1,3}|10\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2[0-9]|3[01])\.\d{1,3})\.(\d{1,3})\b')


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
        'sctp_init_present': r.get('sctp_init_present') is True,
        'application_present': r.get('application_present') is True,
        'ports_released': r.get('server_ports_released') is True,
        'binary_unchanged': r.get('binary_unchanged') is True,
        'candidate_types': sorted(r.get('candidate_types', []) or []),
        'candidate_transports': sorted(r.get('candidate_transports', []) or []),
        'host_candidate_count': r.get('host_candidate_count', 0) or 0,
        'udp_host_ports': sorted(r.get('udp_host_ports', []) or []),
        'tcp_host_ports': sorted(r.get('tcp_host_ports', []) or []),
        'host_candidate_ips': sorted(r.get('host_candidate_ips', []) or []),
        'offer_sdp_sha256': r.get('offer_sdp_sha256'),
        'sctp_init_lines': r.get('sctp_init_lines', []) or [],
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
if binaries['go']['sha256'] != FIXED_GO_SHA:
    raise SystemExit('fixed Go oracle identity mismatch')


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


signal.signal(signal.SIGTERM, interrupted)

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
               '--enable-warp=' + ('true' if stim['enable_warp'] else 'false'),
               '--browser', stim['browser'],
               '--protocol', str(stim['protocol']),
               '--enable-loopback=true']
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, start_new_session=True)
        try:
            _, stderr = proc.communicate(timeout=60)
        except BaseException:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                proc.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()
            raise
        result_path = out / 'result.json'
        result = json.loads(result_path.read_text()) if result_path.exists() else {}
        n = normalize(result)
        n['observer_exit'] = proc.returncode
        # The observer exits 0 iff the profile ran cleanly (alive shutdown); a
        # nonzero exit is an infrastructure integrity fail.
        n['launch_identity_matches'] = all(
            result.get(key) == binaries[side]['sha256']
            for key in ('binary_sha256_before', 'binary_sha256_after'))
        n['exit_integrity'] = ((proc.returncode == 0) and n['binary_unchanged']
                               and n['ports_released'] and n['launch_identity_matches']
                               and sha(a.observer) == observer_sha)
        NORM[profile][side] = n
        RAW[profile][side] = {'exit': proc.returncode, 'result': result,
                              'stderr': stderr[-2000:]}

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
    controls[f'sctp_init_present_when_enabled_{side}'] = n['warp-on']['sctp_init_present'] is True
    controls[f'sctp_init_absent_when_disabled_{side}'] = n['warp-off']['sctp_init_present'] is False
    # Data channels are negotiated on BOTH profiles (SNAP precondition, not warp-gated).
    controls[f'application_negotiated_both_profiles_{side}'] = all(
        n[profile]['application_present'] is True for profile in PROFILES)
    controls[f'join_offer_upgrade_integrity_{side}'] = all(
        n[profile]['join_received'] and n[profile]['offer_received']
        and n[profile]['signal_upgrade_status'] == 101 and n[profile]['server_exit'] == 0
        and n[profile]['ports_released'] and n[profile]['binary_unchanged']
        and n[profile]['exit_integrity'] for profile in PROFILES)

controls['campaign_identity_stable'] = (
    all(sha(getattr(a, side)) == binaries[side]['sha256'] for side in ('go', 'rust'))
    and sha(a.observer) == observer_sha)

failed_profiles = [name for name, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls

manifest = {
    'status': 'PASS_LAUNCHED_MAIN_WARP_SNAP' if all_match else 'FAIL',
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