#!/usr/bin/env python3
"""SPEC-0077 C21 — launched-MAIN root-logger sampling paired Go/Rust differential.

Fresh paired servers (locked Go oracle v1.13.10 e5655ac2 vs Rust livecloud) are
launched per profile with the SAME base config; only the logging-sampling keys
under test differ (delivered via YAML for yaml-* profiles, via generated CLI argv
for cli-* profiles). Each side is driven by probe.mjs with N concurrent real /rtc
WebSocket joins whose JWT carries a top-level `metadata` claim LARGER than
limit.max_metadata_size (16). The size gate fires at the RTC owner stage on BOTH
sides and emits exactly ONE root WARN per request:
  Go   : "error handling request"        (service/utils.go:85; the metadata
         rejection StartSession->ErrMetadataExceedsLimits roommanager.go:297
         closes the signal chan, surfaced as "connection closed by media" :790)
  Rust : "error starting signal session" (signal/start.rs:46, "metadata size
         exceeds limits" check_owner_profile_sizes signal/mod.rs:194)
The message text / HTTP status legitimately differ at the SIGNAL/HTTP layer and
are OUT OF SCOPE here (they belong to the SIGNAL/HTTP items). The sampling
observable is the COUNT of root WARN lines, which each side must independently
match against the upstream formula first + floor((N-first)/thereafter), and the
two sides must agree.

Root sampler contract (Go protocol-stable/logger/logger.go:205-216 == Rust
crates/livecloud/src/logging.rs:39-53):
  sample=false                 -> no sampler -> all N WARNs
  sample=true                  -> Sampler(1s, first, thereafter)
    first      = item_sample_initial  if !=0 else 20
    thereafter = item_sample_interval if !=0 else 100
  sample_initial / sample_interval (WITHOUT item_) are accepted-but-UNUSED
    UPSTREAM (Go config.go:39/41 only defined+copied, never consumed; the
    config.go:30 comment is stale). Rust mirrors this exactly. The inert-named
    profiles prove both sides ignore these keys identically (count unchanged).
  item_sample_seconds selects the ROOM-scope sampler (WithItemSampler); the
    root-scope stimulus here never enters a livekit.room_logger span, so the two
    item_sample_seconds items are DEFERRED (stay PARTIAL), not covered here.

Configs are emitted as JSON (a valid YAML subset both loaders accept) so the
harness needs no PyYAML dependency. NOT a byte-identical stderr comparison
(timestamps/caller differ); structural count equivalence of the sampling policy
is the gate. Every assertion is falsifiable: a sampler that ignores
item_sample_initial/interval, or a flag path that drops the sampling keys, would
produce a different count and FAIL (see the hardcoded-sampler positive control).
"""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, time, urllib.request

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
a = p.parse_args()
here = pathlib.Path(__file__).resolve().parent
PROBE = here / 'probe.mjs'
a.output.mkdir(parents=True, exist_ok=True)
SECRET = 'owned-logging-sampling-fixture-secret-0123456789'
MSG = {'go': 'error handling request', 'rust': 'error starting signal session'}
METADATA_BYTES = 64  # > limit.max_metadata_size=16 -> gate fires on every request

# profile -> (logging_yaml dict OR None, extra argv, N, expected root-WARN count, covered item ids)
# When logging_yaml is None the sampling keys come ONLY from argv (flag input path);
# the base config still sets logging.json/level for stderr capture.
PROFILES = {
    'yaml-sample-on-i2-j3': (
        {'sample': True, 'item_sample_initial': 2, 'item_sample_interval': 3}, [], 12, 5,
        ['YAML:logging.sample', 'YAML:logging.item_sample_initial', 'YAML:logging.item_sample_interval']),
    'yaml-sample-on-i3-j4': (
        {'sample': True, 'item_sample_initial': 3, 'item_sample_interval': 4}, [], 15, 6,
        ['YAML:logging.item_sample_initial', 'YAML:logging.item_sample_interval']),
    'yaml-sample-off': (
        {'sample': False}, [], 12, 12,
        ['YAML:logging.sample']),
    'yaml-inert-named': (
        {'sample': True, 'sample_initial': 999, 'sample_interval': 999,
         'item_sample_initial': 2, 'item_sample_interval': 3}, [], 12, 5,
        ['YAML:logging.sample_initial', 'YAML:logging.sample_interval']),
    'yaml-default-sampler': (
        {'sample': True, 'item_sample_initial': 0, 'item_sample_interval': 0}, [], 25, 20,
        ['YAML:logging.item_sample_initial', 'YAML:logging.item_sample_interval']),
    'cli-sample-on': (
        None, ['--logging.sample=true', '--logging.item_sample_initial=2', '--logging.item_sample_interval=3'], 12, 5,
        ['generated_flags:--logging.sample', 'generated_flags:--logging.item_sample_initial',
         'generated_flags:--logging.item_sample_interval']),
    'cli-inert-named': (
        None, ['--logging.sample=true', '--logging.sample_initial=999', '--logging.sample_interval=999',
               '--logging.item_sample_initial=2', '--logging.item_sample_interval=3'], 12, 5,
        ['generated_flags:--logging.sample_initial', 'generated_flags:--logging.sample_interval']),
}

def base_cfg(port, mt, mu, logging_yaml):
    logging = {'json': True, 'level': 'info'}
    if logging_yaml is not None:
        logging.update(logging_yaml)
    return {'port': port, 'bind_addresses': ['127.0.0.1'],
        'rtc': {'tcp_port': mt, 'udp_port': mu, 'node_ip': '127.0.0.1', 'use_external_ip': False,
                'enable_loopback_candidate': True, 'ips': {'includes': ['127.0.0.1/32']}},
        'keys': {'devkey': SECRET}, 'turn': {'enabled': False},
        'room': {'auto_create': True, 'empty_timeout': 60},
        'limit': {'max_metadata_size': 16},
        'signal_relay': {'connect_attempts': 1},
        'logging': logging}

manifest = {'status': 'INCOMPLETE',
    'scope': ('launched-MAIN root-logger sampling: YAML logging.sample/item_sample_initial/'
              'item_sample_interval/sample_initial/sample_interval and the matching generated CLI flags; '
              'count-only cross-side differential of root WARN lines under the upstream sampling formula; '
              'message text/HTTP status are SIGNAL/HTTP items (out of scope); item_sample_seconds room-scope '
              'DEFERRED; not media/RTP/webhook/ENV claim'),
    'binaries': {}, 'profiles': {}, 'items': sorted({i for _, _, _, _, cov in PROFILES.values() for i in cov})}
for side in ['go', 'rust']:
    b = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(b), 'sha256': hashlib.sha256(b.read_bytes()).hexdigest()}

def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2) + '\n')
save()

def free(port):
    with socket.socket() as s:
        return s.connect_ex(('127.0.0.1', port)) != 0

def wait_ready(url, proc, deadline=40):
    end = time.monotonic() + deadline
    while True:
        assert proc.poll() is None, ('server exited', proc.returncode)
        try:
            with urllib.request.urlopen(url + '/', timeout=1) as r:
                if r.status == 200:
                    return
        except (OSError, TimeoutError):
            if time.monotonic() >= end:
                raise
        time.sleep(.1)

def count_warns(text, msg):
    n = 0
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if obj.get('level') == 'warn' and obj.get('msg') == msg:
            n += 1
    return n

def run_side(side, binary, port, name, logging_yaml, extra_argv, N):
    mt, mu = port + 1, port + 2
    assert free(port) and free(mt) and free(mu), ('occupied ports', name, side, port)
    cpath = a.output / f'{name}-{side}.yaml'
    cpath.write_text(json.dumps(base_cfg(port, mt, mu, logging_yaml)))
    log = (a.output / f'{name}-{side}.stdout.log').open('wb')
    argv = [str(binary), '--dev', '--config', str(cpath), '--port', str(port)] + extra_argv
    proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.PIPE)
    rec = {'pid': proc.pid, 'config_sha256': hashlib.sha256(cpath.read_bytes()).hexdigest(),
           'argv': argv[1:]}
    try:
        wait_ready(f'http://127.0.0.1:{port}', proc)
        pout = a.output / f'{name}-{side}.probe.json'
        env = dict(os.environ, RTC_URL=f'http://127.0.0.1:{port}', SIDE=side, PROFILE=name,
                   OUTPUT=str(pout), N=str(N), METADATA_BYTES=str(METADATA_BYTES))
        pr = subprocess.run(['node', str(PROBE)], env=env, capture_output=True, text=True, timeout=90)
        rec['probe_rc'] = pr.returncode
        rec['probe_stdout'] = pr.stdout.strip()
        if pr.stderr.strip():
            rec['probe_stderr'] = pr.stderr.strip()[:500]
        time.sleep(0.6)  # let the final WARN flush before SIGINT
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
        try:
            data = proc.stderr.read() if proc.stderr else b''
            rc = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill(); rc = proc.wait(); data = b''; rec['forced_kill'] = True
        log.close()
    text = data.decode('utf-8', errors='replace')
    (a.output / f'{name}-{side}.stderr.txt').write_text(text)
    rec['exit'] = rc
    rec['stderr_bytes'] = len(data)
    rec['root_warn_count'] = count_warns(text, MSG[side])
    rec['probe'] = json.loads(pout.read_text()) if pout.exists() else None
    return rec

try:
    for idx, (name, (logging_yaml, extra_argv, N, expected, covers)) in enumerate(PROFILES.items()):
        rec = {'profile': name, 'n': N, 'expected_root_warns': expected, 'covers': covers,
               'logging_yaml': logging_yaml, 'extra_argv': extra_argv}
        manifest['profiles'][name] = rec
        for side, base in [('go', 20700), ('rust', 20800)]:
            port = base + idx * 10
            rec[side] = run_side(side, getattr(a, side).resolve(), port, name, logging_yaml, extra_argv, N)
        save()
        # Per-side: server clean exit, probe fired, count matches the upstream formula.
        for side in ['go', 'rust']:
            r = rec[side]
            assert r['exit'] == 0, (name, side, 'server exit', r['exit'])
            assert r['probe'] is not None and r['probe']['status'] == 'PASS', (name, side, 'probe', r.get('probe'))
            assert r['probe']['diagnostics']['sent'] == N, (name, side, 'sent', r['probe']['diagnostics'])
            assert r['root_warn_count'] == expected, (name, side, 'count', r['root_warn_count'], 'expected', expected)
        # Cross-side count equality (the sampling observable; text/status scoped out).
        assert rec['go']['root_warn_count'] == rec['rust']['root_warn_count'], (name, 'cross-side count', rec)
        rec['match'] = True
        save()

    pf = manifest['profiles']
    def cnt(n, s):
        return pf[n][s]['root_warn_count']
    # Cross-profile controls: the policy genuinely responds to each key, not a fixed count.
    controls = {
        # sample toggle changes the count (on=5 vs off=12) on BOTH sides
        'sample_toggle_effective': all(cnt('yaml-sample-on-i2-j3', s) == 5 and cnt('yaml-sample-off', s) == 12 for s in ['go', 'rust']),
        # item_sample_initial/interval drive the formula (i2-j3 N12=5 vs i3-j4 N15=6)
        'item_keys_drive_formula': all(cnt('yaml-sample-on-i2-j3', s) == 5 and cnt('yaml-sample-on-i3-j4', s) == 6 for s in ['go', 'rust']),
        # named sample_initial/interval are inert: same count as without them (5)
        'named_keys_inert_yaml': all(cnt('yaml-inert-named', s) == cnt('yaml-sample-on-i2-j3', s) == 5 for s in ['go', 'rust']),
        # default branch (item_*==0 -> 20/100): N25 -> 20, distinct from sample-off
        'default_20_100_branch': all(cnt('yaml-default-sampler', s) == 20 for s in ['go', 'rust']),
        # CLI flag path reaches the same policy as YAML (cli-sample-on == yaml i2-j3 == 5)
        'cli_flag_path_effective': all(cnt('cli-sample-on', s) == cnt('yaml-sample-on-i2-j3', s) == 5 for s in ['go', 'rust']),
        # CLI named keys inert too
        'named_keys_inert_cli': all(cnt('cli-inert-named', s) == cnt('cli-sample-on', s) == 5 for s in ['go', 'rust']),
    }
    manifest['cross_profile_controls'] = controls
    for k, v in controls.items():
        assert v, ('cross-profile control failed', k, controls)
    manifest['status'] = 'PASS_LAUNCHED_MAIN_LOGGING_SAMPLING_ROOT'
    save()
finally:
    save()

print(json.dumps({'status': manifest['status'],
                  'profiles': {n: {'go': manifest['profiles'][n]['go']['root_warn_count'],
                                   'rust': manifest['profiles'][n]['rust']['root_warn_count'],
                                   'expected': manifest['profiles'][n]['expected_root_warns']}
                               for n in PROFILES},
                  'controls': manifest.get('cross_profile_controls')}, indent=2))