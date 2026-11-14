#!/usr/bin/env python3
"""SPEC-0077 C22 — launched-MAIN YAML key_file consumer paired Go/Rust differential.

Fresh paired servers (locked Go oracle v1.13.10 e5655ac2 == binary 4bc2378e vs
Rust livecloud candidate d1dad0fc) are launched per profile with the SAME base
config; only the YAML `key_file` surface under test differs. The item is
YAML:key_file: the YAML-surface key-file startup reader (stat / others-permission
gate / open / read / YAML-decode REPLACING inline keys) and the ~/$VAR expansion
that feeds it, proven to reach the assembled MAIN server's auth consumer.

This is COMPLEMENTARY to tests/interop/cli-generated-raw-keyfile (which exercises
the generated CLI/ENV raw-byte key_file path with mode-0600 files and keys:{}).
That addon never tests the YAML-surface others-permission gate, the file-over-
inline REPLACEMENT, the empty-file EOF, or simple YAML ~/$VAR expansion. C22
covers exactly those YAML-surface boundaries.

Two observable families, both deterministic:
  auth profiles  — the server starts; the unchanged official livekit-server-sdk
                   2.19.2 (probe.mjs) mints HS256 JWTs and drives the public
                   livekit.RoomService Twirp surface. The configured file secret
                   must authenticate (bounded actual readiness, not sleep) and
                   do Create/List/Delete; a wrong secret, an unknown key, and the
                   REPLACED inline key must each be cleanly rejected (401). Each
                   side independently satisfies its accept/reject contract AND
                   the two sides must agree attempt-for-attempt.
  startup_error  — the server must FAIL at the real key-file consumer with the
                   exact upstream text on stdout and exit 1:
                     others-readable (0o644) -> "key file others permissions must be set to 0"
                     empty file              -> "EOF"
                     no key_file + no keys   -> "one of key-file or keys must be provided"
                   (non-dev: Go getConfig injects devkey:secret placeholder in
                   --dev, so the no-keys error is only observable without --dev.)

Configs are emitted as JSON (a valid YAML subset both loaders accept) so the
harness needs no PyYAML dependency. Key files are created mode-0600 (or 0o644 for
the permission gate) in a private temp dir removed after the owned children exit.
JWT/secrets/room IDs never enter evidence (probe.mjs emits only accept/reject
booleans + SDK identity). Every assertion is falsifiable: a server that ignored
key_file (kept inline keys), skipped the permission gate, or dropped ~/$VAR
expansion would flip an accept/reject or an exit/text and FAIL (see the
file-over-inline merge-instead-of-replace positive control mutant, which fails
exactly yaml-keyfile-over-inline and passes the other six profiles).
"""
import argparse, hashlib, json, os, pathlib, signal, socket, subprocess, tempfile, time

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
p.add_argument('--interop-package', type=pathlib.Path,
               default=pathlib.Path(__file__).resolve().parent.parent / 'package.json')
p.add_argument('--node', default='node')
a = p.parse_args()
here = pathlib.Path(__file__).resolve().parent
PROBE = here / 'probe.mjs'
a.output.mkdir(parents=True, exist_ok=True)

FS = 'file-secret-value-more-than-32-chars-long'
IS = 'inline-secret-value-more-than-32-chars-long'
WRONG = FS + '-wrong'

# attempt sets: each {label,key,secret,expect}. accept runs first (readiness).
ATT_VALID = [
    {'label': 'file_secret', 'key': 'filekey', 'secret': FS, 'expect': 'accept'},
    {'label': 'wrong_secret', 'key': 'filekey', 'secret': WRONG, 'expect': 'reject'},
    {'label': 'unknown_key', 'key': 'nokey', 'secret': FS, 'expect': 'reject'},
]
ATT_OVER_INLINE = [
    {'label': 'file_secret', 'key': 'filekey', 'secret': FS, 'expect': 'accept'},
    {'label': 'inline_key_unknown', 'key': 'inlinekey', 'secret': IS, 'expect': 'reject'},
    {'label': 'file_wrong_secret', 'key': 'filekey', 'secret': IS, 'expect': 'reject'},
]
ATT_EXPAND = [
    {'label': 'file_secret', 'key': 'filekey', 'secret': FS, 'expect': 'accept'},
    {'label': 'wrong_secret', 'key': 'filekey', 'secret': WRONG, 'expect': 'reject'},
]

# profile -> dict. kind: 'auth' (server starts + SDK probe) | 'startup_error'.
PROFILES = {
    'yaml-keyfile-valid': dict(kind='auth', content={'filekey': FS}, perm=0o600,
                               inline={}, attempts=ATT_VALID, crud=True),
    'yaml-keyfile-over-inline': dict(kind='auth', content={'filekey': FS}, perm=0o600,
                                     inline={'inlinekey': IS}, attempts=ATT_OVER_INLINE, crud=True),
    'yaml-keyfile-env-expand': dict(kind='auth', content={'filekey': FS}, perm=0o600,
                                    inline={}, attempts=ATT_EXPAND, crud=True, via='env'),
    'yaml-keyfile-tilde-expand': dict(kind='auth', content={'filekey': FS}, perm=0o600,
                                      inline={}, attempts=ATT_EXPAND, crud=True, via='tilde'),
    'yaml-keyfile-others-perm': dict(kind='startup_error', content={'filekey': FS}, perm=0o644,
                                     inline={}, expect_exit=1,
                                     expect_stdout='key file others permissions must be set to 0\n'),
    'yaml-keyfile-empty': dict(kind='startup_error', content='', perm=0o600,
                               inline={}, expect_exit=1, expect_stdout='EOF\n'),
    'yaml-no-keys-nondev': dict(kind='startup_error', no_keyfile=True, inline={},
                                expect_exit=1,
                                expect_stdout='one of key-file or keys must be provided\n'),
}
COVERS = ['YAML:key_file']


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def freeport():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def base_env():
    # Strip any ambient LiveKit credential/config env so only the profile's
    # key_file supplies keys (no phantom inline/ENV fallback).
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_'))}


def write_keyfile(path, content, perm):
    body = '' if content == '' else ''.join(f'{k}: {v}\n' for k, v in content.items())
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, perm)
    with os.fdopen(fd, 'w') as f:
        f.write(body)
    os.chmod(path, perm)
    assert os.stat(path).st_mode & 0o777 == perm, (path, oct(os.stat(path).st_mode & 0o777))


manifest = {'status': 'INCOMPLETE',
            'scope': ('launched-MAIN YAML key_file consumer: stat/others-permission gate/open/read/'
                      'YAML-decode REPLACING inline keys, ~/$VAR expansion, and the file secret reaching '
                      'the assembled MAIN auth consumer (official SDK 2.19.2 accept/reject + CRUD). '
                      'Complementary to cli-generated-raw-keyfile (generated CLI/ENV raw-byte path). '
                      'Not a media/RTP/webhook claim; not the generated-flag or create-join-token path.'),
            'binaries': {}, 'profiles': {}, 'items': COVERS}
for side in ['go', 'rust']:
    b = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(b), 'sha256': sha(b)}


def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2) + '\n')


save()

# SDK/Node identity preflight (unchanged official package), before any server.
identity_probe = subprocess.run(
    [a.node, str(PROBE), '--identity-only'],
    input=json.dumps({'interop_package': str(a.interop_package.resolve())}).encode(),
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40)
assert identity_probe.returncode == 0, ('identity preflight', identity_probe.stderr.decode()[:400])
identity = json.loads(identity_probe.stdout)
assert identity['status'] == 'PASS_IDENTITY_ONLY' and identity['sdk_version'] == '2.19.2'
manifest['sdk_identity'] = identity['sdk_identity']
save()


def run_side(side, binary, name, prof, tmpdir):
    port, udp = freeport(), freeport()
    env = base_env()
    keyfile_value = None
    if not prof.get('no_keyfile'):
        via = prof.get('via')
        if via == 'env':
            d = os.path.join(tmpdir, f'{name}-{side}-envdir')
            os.makedirs(d, exist_ok=True)
            actual = os.path.join(d, 'keys.yaml')
            write_keyfile(actual, prof['content'], prof['perm'])
            env['C22_KEYDIR'] = d
            keyfile_value = '$C22_KEYDIR/keys.yaml'
        elif via == 'tilde':
            d = os.path.join(tmpdir, f'{name}-{side}-homedir')
            os.makedirs(d, exist_ok=True)
            actual = os.path.join(d, 'keys.yaml')
            write_keyfile(actual, prof['content'], prof['perm'])
            env['HOME'] = d
            keyfile_value = '~/keys.yaml'
        else:
            actual = os.path.join(tmpdir, f'{name}-{side}.yaml')
            write_keyfile(actual, prof['content'], prof['perm'])
            keyfile_value = actual
    cfg = {'port': port, 'bind_addresses': ['127.0.0.1'],
           'rtc': {'tcp_port': 0, 'udp_port': udp, 'node_ip': '127.0.0.1',
                   'use_external_ip': False, 'enable_loopback_candidate': True},
           'keys': prof['inline'], 'logging': {'level': 'error'}}
    if keyfile_value is not None:
        cfg['key_file'] = keyfile_value
    cpath = a.output / f'{name}-{side}.config.json'
    cpath.write_text(json.dumps(cfg))
    outpath = a.output / f'{name}-{side}.stdout.txt'
    errpath = a.output / f'{name}-{side}.stderr.txt'
    argv = [str(binary), '--config-body', json.dumps(cfg)]
    rec = {'pid': None, 'config_sha256': sha(cpath), 'argv_tail': ['--config-body', '<json>'],
           'key_file_value': keyfile_value, 'key_file_perm': oct(prof['perm']) if not prof.get('no_keyfile') else None,
           'http_port': port}
    with outpath.open('wb') as out, errpath.open('wb') as err:
        proc = subprocess.Popen(argv, stdout=out, stderr=err, env=env,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        rec['pid'] = proc.pid
        try:
            if prof['kind'] == 'auth':
                payload = {'interop_package': str(a.interop_package.resolve()),
                           'url': f'http://127.0.0.1:{port}', 'attempts': prof['attempts'],
                           'crud': prof.get('crud', False)}
                pr = subprocess.run([a.node, str(PROBE)], input=json.dumps(payload).encode(),
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, env=env)
                rec['probe_rc'] = pr.returncode
                pout = a.output / f'{name}-{side}.probe.json'
                try:
                    probe = json.loads(pr.stdout.decode())
                except (json.JSONDecodeError, ValueError):
                    probe = {'status': 'FAIL', 'unparsed': pr.stdout.decode()[:500]}
                pout.write_text(json.dumps(probe, indent=2) + '\n')
                rec['probe'] = {'status': probe.get('status'), 'crud': probe.get('crud'),
                                'attempts': probe.get('attempts'), 'failure_stage': probe.get('failure_stage')}
                rec['server_alive_through_probe'] = proc.poll() is None
                if proc.poll() is None:
                    proc.send_signal(signal.SIGINT)
                try:
                    rc = proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill(); rc = proc.wait(); rec['forced_kill'] = True
            else:
                # startup_error: the server must exit on its own at the consumer.
                try:
                    rc = proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill(); rc = proc.wait(); rec['unexpected_start'] = True
            rec['exit'] = rc
        finally:
            if proc.poll() is None:
                proc.kill(); proc.wait()
    rec['stdout'] = outpath.read_text(errors='replace')
    rec['stderr'] = errpath.read_text(errors='replace')
    return rec


def auth_ok(rec, prof):
    pr = rec.get('probe') or {}
    if pr.get('status') != 'PASS':
        return False
    if rec.get('exit') != 0 or rec.get('forced_kill') or not rec.get('server_alive_through_probe'):
        return False
    if prof.get('crud') and not pr.get('crud'):
        return False
    att = pr.get('attempts') or []
    if len(att) != len(prof['attempts']):
        return False
    return all(r.get('ok') and r.get('expect') == e['expect'] and r.get('label') == e['label']
               for r, e in zip(att, prof['attempts']))


def startup_ok(rec, prof):
    return (rec.get('exit') == prof['expect_exit']
            and rec.get('stdout') == prof['expect_stdout']
            and not rec.get('unexpected_start') and not rec.get('forced_kill'))


def cross_side_equal(rec, prof):
    """Both sides agree on the observable. Defensive: a missing probe field is
    inequality, never a silent pass."""
    if prof['kind'] == 'auth':
        gp = (rec['go'].get('probe') or {})
        rp = (rec['rust'].get('probe') or {})
        ga, ra = gp.get('attempts'), rp.get('attempts')
        if not isinstance(ga, list) or not isinstance(ra, list):
            return False
        g = [(r.get('label'), r.get('expect'), r.get('accepted')) for r in ga]
        r = [(r.get('label'), r.get('expect'), r.get('accepted')) for r in ra]
        return g == r and gp.get('crud') == rp.get('crud')
    return (rec['go'].get('exit') == rec['rust'].get('exit')
            and rec['go'].get('stdout') == rec['rust'].get('stdout')
            and rec['go'].get('stderr') == rec['rust'].get('stderr'))


try:
    with tempfile.TemporaryDirectory(prefix='c22-keyfile-') as tmpdir:
        for name, prof in PROFILES.items():
            rec = {'profile': name, 'kind': prof['kind'], 'covers': COVERS}
            manifest['profiles'][name] = rec
            for side in ['go', 'rust']:
                rec[side] = run_side(side, getattr(a, side).resolve(), name, prof, tmpdir)
            # Collect every profile (no fail-fast) so a mutant run reports exactly
            # which profiles diverge and which still match — specificity, not a
            # single aborted assertion. Each side must independently satisfy its
            # contract AND the two sides must agree.
            for side in ['go', 'rust']:
                rec[side + '_contract_ok'] = (auth_ok(rec[side], prof) if prof['kind'] == 'auth'
                                              else startup_ok(rec[side], prof))
            rec['cross_side_equal'] = cross_side_equal(rec, prof)
            rec['match'] = bool(rec['go_contract_ok'] and rec['rust_contract_ok'] and rec['cross_side_equal'])
            save()
        manifest['private_files_removed'] = True

    pf = manifest['profiles']

    def accepted(name, side, label):
        for r in ((pf[name][side].get('probe') or {}).get('attempts') or []):
            if r.get('label') == label:
                return r.get('accepted')
        return None

    # Cross-profile controls: the consumer genuinely responds to key_file, not a
    # fixed accept/reject. Each must hold on BOTH sides. Fail-closed: a missing
    # attempt (None) satisfies neither `is True` nor `is False`.
    controls = {
        'file_secret_authenticates': all(accepted('yaml-keyfile-valid', s, 'file_secret') is True for s in ['go', 'rust']),
        'wrong_and_unknown_rejected': all(accepted('yaml-keyfile-valid', s, 'wrong_secret') is False
                                          and accepted('yaml-keyfile-valid', s, 'unknown_key') is False for s in ['go', 'rust']),
        'inline_key_replaced': all(accepted('yaml-keyfile-over-inline', s, 'inline_key_unknown') is False for s in ['go', 'rust']),
        'file_key_wins_over_inline': all(accepted('yaml-keyfile-over-inline', s, 'file_secret') is True for s in ['go', 'rust']),
        'env_expansion_effective': all(accepted('yaml-keyfile-env-expand', s, 'file_secret') is True for s in ['go', 'rust']),
        'tilde_expansion_effective': all(accepted('yaml-keyfile-tilde-expand', s, 'file_secret') is True for s in ['go', 'rust']),
        'others_perm_gate': all(pf['yaml-keyfile-others-perm'][s]['stdout'] == 'key file others permissions must be set to 0\n'
                                and pf['yaml-keyfile-others-perm'][s]['exit'] == 1 for s in ['go', 'rust']),
        'empty_file_eof': all(pf['yaml-keyfile-empty'][s]['stdout'] == 'EOF\n'
                              and pf['yaml-keyfile-empty'][s]['exit'] == 1 for s in ['go', 'rust']),
        'no_keys_error': all(pf['yaml-no-keys-nondev'][s]['stdout'] == 'one of key-file or keys must be provided\n'
                             and pf['yaml-no-keys-nondev'][s]['exit'] == 1 for s in ['go', 'rust']),
    }
    manifest['cross_profile_controls'] = controls
    failed_profiles = [n for n in PROFILES if not pf[n]['match']]
    failed_controls = [k for k, v in controls.items() if not v]
    passed = not failed_profiles and not failed_controls
    manifest['status'] = 'PASS_LAUNCHED_MAIN_KEY_FILE' if passed else 'FAIL'
    save()

    def per_profile(n):
        r = pf[n]
        base = {'kind': r['kind'], 'match': r['match'],
                'go_contract_ok': r['go_contract_ok'], 'rust_contract_ok': r['rust_contract_ok'],
                'cross_side_equal': r['cross_side_equal'],
                'go_exit': r['go']['exit'], 'rust_exit': r['rust']['exit']}
        if r['kind'] == 'auth':
            gp, rp = (r['go'].get('probe') or {}), (r['rust'].get('probe') or {})
            base.update({'go_attempts': gp.get('attempts'), 'rust_attempts': rp.get('attempts'),
                         'go_crud': gp.get('crud'), 'rust_crud': rp.get('crud'),
                         'go_probe_status': gp.get('status'), 'rust_probe_status': rp.get('status')})
        else:
            base.update({'go_stdout': r['go']['stdout'], 'rust_stdout': r['rust']['stdout']})
        return base

    # Self-contained receipt the ledger gate can classify (both identities, item-named,
    # non-empty case inventory). status PASS only when every profile matches and every
    # control holds; a mutant run writes status FAIL with the diverging profiles named.
    batch = {
        'status': 'PASS' if passed else 'FAIL',
        'batch': 'spec0077-c22-key-file',
        'date': '2026-11-14',
        'scope': manifest['scope'],
        'go_binary_sha256': manifest['binaries']['go']['sha256'],
        'rust_binary_sha256': manifest['binaries']['rust']['sha256'],
        'go_version': '1.13.10',
        'fixed_go_source': 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07',
        'items': COVERS,
        'profiles': list(PROFILES),
        'case_count': len(PROFILES),
        'failed_profiles': failed_profiles,
        'failed_controls': failed_controls,
        'method': ('Each profile launches the assembled MAIN binary (Go oracle 4bc2378e / Rust candidate '
                   'd1dad0fc) with --config-body <json-as-yaml> carrying a YAML key_file (mode 0600, or 0644 '
                   'for the permission gate) and keys:{} (or an inline key for the replacement profile). '
                   'auth profiles: the unchanged official livekit-server-sdk 2.19.2 mints HS256 JWTs and drives '
                   'livekit.RoomService ListRooms/CreateRoom/DeleteRoom; the file secret must authenticate via '
                   'bounded actual readiness and wrong/unknown/replaced-inline secrets must be cleanly rejected '
                   '(401). startup_error profiles: the server must exit 1 with the exact upstream consumer text '
                   'on stdout. Each side independently satisfies its contract AND the two sides agree.'),
        'normalization': ('auth: cross-side comparison is the per-attempt (label,expect,accepted) boolean tuple '
                          'plus CRUD success, never JWT/secret/room-ID bytes. startup_error: exact exit code + '
                          'stdout text + stderr equality. SDK/Node identity is recomputed per probe and must '
                          'match the preflight.'),
        'harness': {'path': 'tests/interop/key-file-main/run-paired.py', 'sha256': sha(here / 'run-paired.py'),
                    'probe': 'tests/interop/key-file-main/probe.mjs', 'probe_sha256': sha(PROBE),
                    'readme_sha256': sha(here / 'README.md') if (here / 'README.md').exists() else None},
        'sdk_identity': manifest['sdk_identity'],
        'cross_profile_controls': controls,
        'per_profile': {n: per_profile(n) for n in PROFILES},
    }
    (a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2) + '\n')
finally:
    save()

print(json.dumps({'status': manifest['status'],
                  'profiles': {n: {'kind': manifest['profiles'][n]['kind'],
                                   'match': manifest['profiles'][n].get('match')}
                               for n in PROFILES if n in manifest['profiles']},
                  'failed_profiles': [n for n in PROFILES
                                      if n in manifest['profiles'] and not manifest['profiles'][n].get('match')],
                  'controls': manifest.get('cross_profile_controls')}, indent=2))
raise SystemExit(0 if manifest['status'] == 'PASS_LAUNCHED_MAIN_KEY_FILE' else 1)