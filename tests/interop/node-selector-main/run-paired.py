#!/usr/bin/env python3
"""SPEC-0077 C24 — launched-MAIN node_selector construction paired Go/Rust differential.

Fresh paired servers (locked Go oracle v1.13.10 e5655ac2 == binary 4bc2378e vs the
Rust livecloud release candidate) are launched per profile with the SAME base
config; only the `node_selector.kind` / `node_selector.regions` / top-level
`region` surface under test differs. Two ledger leaves are named:
YAML:node_selector.kind and YAML:node_selector.regions.

Upstream builds the selector while WIRING services, not while loading config:
`InitializeServer` -> `NewRoomAllocator` (service/roomallocator.go:43) ->
`selector.CreateNodeSelector` (routing/selector/interfaces.go:33-66), after
getConfig/ValidateKeys/LoadTURNSecrets. A construction error is printed with
`fmt.Println(err)` and exits 1 (cmd/server/main.go:190-192), so the refusal is
bare upstream text on STDOUT. The Rust mirror is lk-config `check_selector`
(unsupported.rs:60-89), called from `validate_server_wiring` in
livecloud::server::run; it reproduces the same three refusals verbatim.

Two observable families, both deterministic:
  startup_error profiles — the server must FAIL at the real selector consumer
      with the exact upstream text on stdout and exit 1:
        unknown kind                     -> "unsupported node selector"
        regionaware + blank region       -> "current region cannot be blank"
        regionaware + region not in a
          non-empty regions list         -> "unknown lat and lon for the current
                                            region"
  alive profiles — the kinds upstream constructs without a fatal validation must
      serve HTTP and shut down cleanly (exit 0) on both sides:
        kind "" (upstream maps to "any"), any, cpuload, sysload, random,
        regionaware with a matching region, and regionaware with a region set but
        an EMPTY regions list (NewRegionAwareSelector only rejects when
        len(regions) > 0 and the current region is absent — regionaware.go:57).

The regionaware empty-regions profile is the load-bearing boundary: an empty list
with a region set constructs (alive), while a non-empty list omitting the region
is fatal (startup_error). A consumer that ignored the len(regions) > 0 guard, or
the blank-region guard, or the unknown-kind guard would flip an exit+text or a
serving boolean and FAIL (see the check_selector-disabled positive control
mutant, which fails exactly the three refusal profiles and passes the seven
alive profiles).

Shutdown calibration (measured, not assumed — inherited from the proven
config-wiring-startup harness): the alive rows settle --settle seconds after the
port first accepts before signalling. Signalling immediately is NOT a
startup-acceptance measurement: the official binary accepts TCP before Start()
sets s.running (server.go:343-345), and Stop() early-returns on !running.Swap
(server.go:390) without closing doneChan, so Start() blocks; main.go:313 loops
`for i := range 2`, so a second signal releases it. terminate() sends SIGTERM,
then a second SIGTERM, then SIGKILL, and records which was needed.

Normalization (scope limit): the alive observable is {http reachable, exit==0,
no SIGKILL} — the per-side shutdown timing (sigterm_to_exit_s, whether a second
SIGTERM was needed) legitimately differs between the Go and Rust event loops and
is RECORDED but NOT compared. The refusal observable is the exact exit code and
stdout bytes. No comparison field is deleted; these are explicit scope bounds.

Out of scope (honest residual, W6): cross-node SelectNode RUNTIME semantics
(region-aware nearest-node ordering, cpu/sysload filtering across a multi-node
cluster). Single-node local routing never invokes SelectNode, so this harness
proves the STARTUP CONSTRUCTION surface only — the same surface the existing
config-wiring-startup receipt covers, re-bound here to the current clean release
candidate with a gate-recognized paired receipt. Configs are emitted as YAML
strings (no PyYAML dependency). Every original exit code is preserved; no pipe
filters the result.
"""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, tempfile, time

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
p.add_argument('--settle', type=float, default=2.0)
p.add_argument('--shutdown-window', type=float, default=45.0)
a = p.parse_args()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)

COVERS = ['YAML:node_selector.kind', 'YAML:node_selector.regions']

# kind: startup_error -> expect {exit, stdout}; alive -> serve HTTP + exit 0.
PROFILES = [
    {'name': 'ns-unsupported-kind', 'kind': 'startup_error', 'sel_kind': 'bogus-kind',
     'expect': {'exit': 1, 'stdout': 'unsupported node selector\n'}},
    {'name': 'ns-regionaware-blank', 'kind': 'startup_error', 'sel_kind': 'regionaware',
     'expect': {'exit': 1, 'stdout': 'current region cannot be blank\n'}},
    {'name': 'ns-regionaware-unknown', 'kind': 'startup_error', 'sel_kind': 'regionaware',
     'region': 'US-west', 'regions': [{'name': 'US-east', 'lat': 1.0, 'lon': 2.0}],
     'expect': {'exit': 1, 'stdout': 'unknown lat and lon for the current region\n'}},
    {'name': 'ns-empty-kind', 'kind': 'alive', 'sel_kind': ''},
    {'name': 'ns-any', 'kind': 'alive', 'sel_kind': 'any'},
    {'name': 'ns-cpuload', 'kind': 'alive', 'sel_kind': 'cpuload'},
    {'name': 'ns-sysload', 'kind': 'alive', 'sel_kind': 'sysload'},
    {'name': 'ns-random', 'kind': 'alive', 'sel_kind': 'random'},
    {'name': 'ns-regionaware-match', 'kind': 'alive', 'sel_kind': 'regionaware',
     'region': 'US-east', 'regions': [{'name': 'US-east', 'lat': 1.0, 'lon': 2.0}]},
    {'name': 'ns-regionaware-empty-regions', 'kind': 'alive', 'sel_kind': 'regionaware',
     'region': 'US-east', 'regions': []},
]


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def freeport():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def base_env():
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_'))}


def config_yaml(prof, http, iceudp):
    """YAML string base config; rtc.tcp_port=0 avoids the shared 7881 ICE port."""
    lines = [f'port: {http}', 'bind_addresses: [127.0.0.1]',
             'keys:', '  devkey: secretsecretsecretsecretsecretsecret',
             'rtc:', '  tcp_port: 0', f'  udp_port: {iceudp}',
             '  node_ip: 127.0.0.1', 'logging:', '  level: error']
    if 'region' in prof:
        lines.append(f'region: {prof["region"]}')
    lines.append('node_selector:')
    lines.append(f'  kind: "{prof["sel_kind"]}"')
    if 'regions' in prof:
        if not prof['regions']:
            lines.append('  regions: []')
        else:
            lines.append('  regions:')
            for r in prof['regions']:
                lines.append(f'  - name: {r["name"]}')
                lines.append(f'    lat: {r["lat"]}')
                lines.append(f'    lon: {r["lon"]}')
    return '\n'.join(lines) + '\n'


def wait_http(proc, port, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            return False
        try:
            s = socket.create_connection(('127.0.0.1', port), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def terminate(proc, window):
    """SIGTERM, then a second SIGTERM, then SIGKILL. Records which was needed."""
    started = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    second = False
    try:
        code = proc.wait(timeout=window)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGTERM)
        second = True
        try:
            code = proc.wait(timeout=window)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            code = proc.wait()
    return {'exit_code': code, 'sigterm_to_exit_s': round(time.monotonic() - started, 3),
            'sigkill_required': code == -signal.SIGKILL, 'second_sigterm_sent': second}


def run_refusal(binary, prof, work):
    http, iceudp = freeport(), freeport()
    cfg = work / 'c.yaml'
    cfg.write_text(config_yaml(prof, http, iceudp))
    outpath, errpath = work / 'stdout.txt', work / 'stderr.txt'
    # A genuine refusal self-exits 1 in well under a second. Use Popen + a bounded
    # wait (not subprocess.run's hard timeout) so a server that STARTS instead of
    # refusing — the accept-but-ignore mutant — is recorded as a contract failure
    # rather than crashing the harness. Collect-all-then-assert needs every row.
    with outpath.open('wb') as out, errpath.open('wb') as err:
        proc = subprocess.Popen([str(binary), '--config', str(cfg)], stdout=out,
                                stderr=err, env=base_env(),
                                stdin=subprocess.DEVNULL, start_new_session=True)
        started = False
        try:
            rc = proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            started = True
            os.killpg(proc.pid, signal.SIGKILL)
            rc = proc.wait()
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    return {'config_sha256': sha(cfg), 'http_port': http, 'exit': rc,
            'started_instead_of_refusing': started,
            'stdout': outpath.read_text(errors='replace'),
            'stderr': errpath.read_text(errors='replace')}


def run_alive(binary, prof, work):
    http, iceudp = freeport(), freeport()
    cfg = work / 'c.yaml'
    cfg.write_text(config_yaml(prof, http, iceudp))
    log = (work / 'server.log').open('wb')
    proc = subprocess.Popen([str(binary), '--config', str(cfg)], stdout=log,
                            stderr=subprocess.STDOUT, env=base_env(),
                            stdin=subprocess.DEVNULL, start_new_session=True)
    rec = {'config_sha256': sha(cfg), 'http_port': http, 'pid': proc.pid}
    try:
        reached = wait_http(proc, http)
        alive = proc.poll() is None
        if alive:
            time.sleep(a.settle)
            alive = proc.poll() is None
        details = terminate(proc, a.shutdown_window) if alive else {
            'exit_code': proc.returncode, 'sigterm_to_exit_s': None,
            'sigkill_required': False, 'second_sigterm_sent': False}
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        log.close()
    rec.update({'http_reachable': reached, 'survived_grace': alive, **details,
                'log': (work / 'server.log').read_text(errors='replace')})
    return rec


def alive_norm(rec):
    return {'http': bool(rec.get('http_reachable')),
            'exit0': rec.get('exit_code') == 0,
            'clean': not rec.get('sigkill_required', False)}


def alive_ok(rec):
    n = alive_norm(rec)
    return n['http'] and n['exit0'] and n['clean'] and rec.get('survived_grace', False)


manifest = {'status': 'INCOMPLETE',
            'scope': ('launched-MAIN node_selector STARTUP CONSTRUCTION surface: '
                      'CreateNodeSelector / check_selector unknown-kind, regionaware blank-region '
                      'and regionaware unknown-lat-lon refusals (bare stdout + exit 1), plus the '
                      'kinds upstream constructs without fatal validation (empty/any/cpuload/sysload/'
                      'random/regionaware-matching/regionaware-empty-regions) serving HTTP and exiting 0. '
                      'Re-binds the config-wiring-startup surface to the current clean release candidate '
                      'with a gate-recognized paired receipt. NOT a cross-node SelectNode runtime claim (W6).'),
            'binaries': {}, 'profiles': {}, 'items': COVERS,
            'settle_s': a.settle, 'shutdown_window_s': a.shutdown_window}
for side in ('go', 'rust'):
    b = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(b), 'sha256': sha(b)}


def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')


save()

# Collect every profile on both sides first, then assert (no fail-fast masking).
for prof in PROFILES:
    name = prof['name']
    entry = {'profile': name, 'kind': prof['kind'], 'covers': COVERS, 'expect': prof.get('expect')}
    for side, binary in (('go', a.go), ('rust', a.rust)):
        with tempfile.TemporaryDirectory(prefix=f'ns-{side}-') as d:
            work = pathlib.Path(d)
            if prof['kind'] == 'alive':
                entry[side] = run_alive(binary.resolve(), prof, work)
            else:
                entry[side] = run_refusal(binary.resolve(), prof, work)
    if prof['kind'] == 'alive':
        entry['go_contract_ok'] = alive_ok(entry['go'])
        entry['rust_contract_ok'] = alive_ok(entry['rust'])
        entry['go_norm'] = alive_norm(entry['go'])
        entry['rust_norm'] = alive_norm(entry['rust'])
        entry['cross_side_equal'] = entry['go_norm'] == entry['rust_norm']
    else:
        exp = prof['expect']
        entry['go_contract_ok'] = entry['go']['exit'] == exp['exit'] and entry['go']['stdout'] == exp['stdout']
        entry['rust_contract_ok'] = entry['rust']['exit'] == exp['exit'] and entry['rust']['stdout'] == exp['stdout']
        entry['cross_side_equal'] = (entry['go']['exit'] == entry['rust']['exit']
                                     and entry['go']['stdout'] == entry['rust']['stdout'])
    entry['match'] = entry['go_contract_ok'] and entry['rust_contract_ok'] and entry['cross_side_equal']
    manifest['profiles'][name] = entry
    save()
    print(f'{prof["kind"]:13s} {name}: match={entry["match"]} '
          f'go_ok={entry["go_contract_ok"]} rust_ok={entry["rust_contract_ok"]}', flush=True)

P = manifest['profiles']


def refused(name, side, text):
    return P[name][side]['exit'] == 1 and P[name][side]['stdout'] == text


def serves(name, side):
    return alive_ok(P[name][side])


controls = {}
for side in ('go', 'rust'):
    controls[f'unknown_kind_refused_{side}'] = refused('ns-unsupported-kind', side, 'unsupported node selector\n')
    controls[f'blank_region_refused_{side}'] = refused('ns-regionaware-blank', side, 'current region cannot be blank\n')
    controls[f'unknown_latlon_refused_{side}'] = refused('ns-regionaware-unknown', side, 'unknown lat and lon for the current region\n')
    controls[f'empty_kind_serves_{side}'] = serves('ns-empty-kind', side)
    controls[f'any_serves_{side}'] = serves('ns-any', side)
    controls[f'cpuload_serves_{side}'] = serves('ns-cpuload', side)
    controls[f'sysload_serves_{side}'] = serves('ns-sysload', side)
    controls[f'random_serves_{side}'] = serves('ns-random', side)
    controls[f'regionaware_match_serves_{side}'] = serves('ns-regionaware-match', side)
    controls[f'regionaware_empty_regions_serves_{side}'] = serves('ns-regionaware-empty-regions', side)
    # The len(regions)>0 boundary is load-bearing: empty list serves, non-empty
    # list omitting the region is fatal. A consumer dropping the guard flips this.
    controls[f'regionaware_boundary_specificity_{side}'] = (
        serves('ns-regionaware-empty-regions', side)
        and refused('ns-regionaware-unknown', side, 'unknown lat and lon for the current region\n'))

failed_profiles = [n for n, e in P.items() if not e['match']]
failed_controls = [k for k, v in controls.items() if not v]
all_match = not failed_profiles and not failed_controls
manifest['cross_profile_controls'] = controls
manifest['failed_profiles'] = failed_profiles
manifest['failed_controls'] = failed_controls
manifest['case_count'] = len(PROFILES)
manifest['status'] = 'PASS_LAUNCHED_MAIN_NODE_SELECTOR' if all_match else 'FAIL'
save()

batch = {'status': manifest['status'], 'case_count': len(PROFILES), 'items': COVERS,
         'profiles': {n: {'match': e['match'], 'kind': e['kind'],
                          'go_contract_ok': e['go_contract_ok'], 'rust_contract_ok': e['rust_contract_ok'],
                          'cross_side_equal': e['cross_side_equal']}
                      for n, e in P.items()},
         'cross_profile_controls': controls,
         'failed_profiles': failed_profiles, 'failed_controls': failed_controls,
         'binaries': manifest['binaries'],
         'input_identity_stable': manifest['binaries']['go']['sha256'] == '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc',
         'harness_sha256': sha(pathlib.Path(__file__).resolve())}
(a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({'status': manifest['status'], 'case_count': len(PROFILES),
                  'failed_profiles': failed_profiles, 'failed_controls': failed_controls}))
raise SystemExit(0 if all_match else 1)