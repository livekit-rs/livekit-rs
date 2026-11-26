#!/usr/bin/env python3
"""C103 lane: the WebTransport generated-flag rows and the key-file channel, paired on launched
daemons and judged by what the running node does.

SPEC-0077 rows driven here:

    generated_flags:--webtransport.port           references/livekit-stable pkg/config/config.go:124,
                                                    GenerateCLIFlags at 940-1035, updateFromCLI at 1074-1105
    generated_flags:--webtransport.tls_cert_file  config.go:125 -> pkg/service/webtransport.go (X509KeyPair)
    generated_flags:--webtransport.tls_key_file   config.go:126 -> the same call, key second
    generated_flags:--key_file                    config.go:96 + 820-850 (ValidateKeys), the base flag
                                                    `key-file` is a *different* name at main.go:56-59 and
                                                    config.go:1109, so precedence is measurable

The upstream mechanism is `GenerateCLIFlags`: the flag name is the YAML path (`webtransport.port`,
`key_file`) and the env name is `LIVEKIT_` + that path upper-cased with dots turned into underscores.
Because `ToCLIFlagNames` dedup only skips an *exact* name match (config.go:803), the base spelling
`--key-file` and the generated spelling `--key_file` are two live flags for one field, and the explicit
`if c.IsSet("key-file")` branch runs after the generated loop - so argv order is not what decides.
Rust mirrors this in `crates/lk-config/src/generated.rs:392` (env `LIVEKIT_KEY_FILE`),
`crates/lk-config/src/cli.rs:691` (`"key-file"`), and `crates/lk-config/src/keys.rs:83-99`.

Nothing here reads a config struct. A channel is claimed by an effect the daemon produced:

  webtransport.port          the UDP socket the process holds while it is alive (lsof), and the
                             `webtransport listener started` line strictly before `starting LiveKit
                             server`; port 0 must hold nothing
  webtransport.tls_*_file    either a listener that answers, or the refusal the named material
                             produces - and the pair (YAML valid / flags mismatched) vs (YAML
                             mismatched / flags valid) decides which side's file the loader opened
  key_file                   which secret the *running node* accepts on POST /rtc/v1/validate:
                             a token the key set signs reaches the handler (HTTP 400 "join_request is
                             required"), a token it does not sign is refused (HTTP 401 "invalid API
                             key"). Both cells are required for a row to claim anything.
  key_file failures          the refusal sentences ValidateKeys emits: the 0o007 permission rule,
                             `stat`, `EOF`, and the yaml decode of a sequence into map[string]string

Modes:
  --record        run every profile against both pinned binaries and write the receipt.
  --report        re-derive the verdict from a recorded execution.json without launching anything.
  --selftest-gate apply one named mutation to a recorded manifest and require the gate to redden.
  --only          development subset; it reports PARTIAL_SUBSET_NOT_ACCEPTANCE, never a pass.

Primitives (port allocation, socket observation, owned stores, the launcher, the address masker, the
table parser) are imported from the accepted C102 lane rather than copied: importing leaves that file
byte-identical, so its filed receipt stays re-derivable.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import importlib.util
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
C102_DIR = REPO / 'tests/interop/base-flag-channels'

_spec = importlib.util.spec_from_file_location('c102_harness', str(C102_DIR / 'run-paired.py'))
C102 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(C102)

BATCH = 'C103-20261127-generated-flag-channels'
SCHEMA = 'spec0077-c103-generated-flag-channels/1'
IP_SALT = 'c103-flags'
ORACLE_SHA256 = C102.ORACLE_SHA256
ACCEPTANCE_SHA256 = C102.ACCEPTANCE_SHA256
FIXED_GO_COMMIT = C102.FIXED_GO_COMMIT
REFERENCE = C102.REFERENCE
OFF_LIMITS = C102.OFF_LIMITS
HOST_STORE_PORT = C102.HOST_STORE_PORT
COVERS = ['generated_flags:--webtransport.port',
          'generated_flags:--webtransport.tls_cert_file',
          'generated_flags:--webtransport.tls_key_file',
          'generated_flags:--key_file']

# Fixture vocabulary. Every value here is this lane's own invention and is masked in the receipt;
# the key ids stay readable because they are the claim ("which kid the running node accepts").
YAML_KEYS = {'c103y': 'c103yamlinekeysecret00123456789ab'}
FILE_A = {'c103a': 'c103keyfileaecret000123456789abcdef'}
FILE_B = {'c103b': 'c103keyfilebsecret000123456789abcdef'}
FILE_FLAG = {'c103n': 'c103keysflagsecret000123456789abcd'}
SHORT_KEY = {'c103s': 'tooshort'}
SECRET_ALIASES = [(info['secret'], alias) for alias, info in {
    'SEC-Y': {'secret': YAML_KEYS['c103y']}, 'SEC-A': {'secret': FILE_A['c103a']},
    'SEC-B': {'secret': FILE_B['c103b']}, 'SEC-N': {'secret': FILE_FLAG['c103n']}}.items()]
ALL_KEYS = dict(YAML_KEYS) | dict(FILE_A) | dict(FILE_B) | dict(FILE_FLAG) | dict(SHORT_KEY)
CERT_SUBJECT = 'wt.c103.test'

# Accepted/rejected are the two cells a key-file row can claim; anything else is recorded verbatim
# (masked) so an unexpected shape cannot read as either.
ACCEPTED = 'handler'          # HTTP 400 "join_request is required": the grant parsed, the body did not
REJECTED = 'bad-key'         # HTTP 401 "invalid API key"


class Masker(C102.Masker):
    """The C102 masker, with this lane's own secret vocabulary."""

    def secrets(self, text: str) -> str:
        text = super().secrets(text)
        for real, alias in SECRET_ALIASES:
            text = text.replace(real, f'<{alias}>')
        return text


# --------------------------------------------------------------------------- auth channel
def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def jwt(kid: str, secret: str, ttl: int = 120) -> str:
    now = int(time.time())
    header = b64u(json.dumps({'alg': 'HS256', 'typ': 'JWT'}).encode())
    body = b64u(json.dumps({'iss': kid, 'sub': f'{kid}-subject', 'nbf': now - 5,
                            'exp': now + ttl,
                            'video': {'room': 'c103room', 'roomJoin': True}}).encode())
    signing = f'{header}.{body}'.encode()
    sig = b64u(hmac.new(secret.encode(), signing, hashlib.sha256).digest())
    return f'{signing.decode()}.{sig}'


def validate_probe(port: int, token: str) -> dict:
    """Ask the running node to validate a token. The endpoint shape is what makes this a channel:
    an accepted token gets past authentication and is rejected for the empty body, a rejected token
    never gets that far."""
    req = urllib.request.Request(f'http://127.0.0.1:{port}/rtc/v1/validate', data=b'{}',
                                 headers={'Authorization': f'Bearer {token}',
                                          'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=4) as resp:
            return {'status': resp.status, 'body': resp.read()[:120].decode(errors='replace')}
    except urllib.error.HTTPError as exc:
        return {'status': exc.code, 'body': exc.read()[:160].decode(errors='replace')}
    except Exception as exc:  # noqa: BLE001
        return {'status': None, 'body': f'{type(exc).__name__}: {exc}'[:160]}


def classify(probe: dict) -> str:
    body = probe.get('body') or ''
    if probe.get('status') == 400 and 'join_request is required' in body:
        return ACCEPTED
    if probe.get('status') == 401 and 'invalid API key' in body:
        return REJECTED
    return f'other:{probe.get("status")}'


def auth_probe(port: int, kids: list, masker: Masker) -> dict:
    out = {}
    for kid in kids:
        probe = validate_probe(port, jwt(kid, ALL_KEYS[kid]))
        verdict = classify(probe)
        out[kid] = {'verdict': verdict, 'status': probe['status'],
                    'body_masked': masker.text(probe['body'])[:120]}
    return out


# --------------------------------------------------------------------------- fixtures
def gen_cert(work: Path, name: str) -> dict:
    cert = work / f'{name}-cert.pem'
    key = work / f'{name}-key.pem'
    if not (cert.is_file() and key.is_file()):
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(cert), '-days', '2',
                        '-subj', f'/CN={name}.{CERT_SUBJECT}'],
                       capture_output=True, timeout=180, check=True)
    return {'cert': cert, 'key': key, 'cert_der': C102.pem_sha256(cert)}


def keys_file(work: Path, name: str, pairs: dict, mode: int = 0o600, raw: str | None = None) -> Path:
    path = work / name
    body = raw if raw is not None else '\n'.join(f'{k}: {v}' for k, v in pairs.items()) + '\n'
    path.write_text(body, encoding='utf-8')
    os.chmod(path, mode)
    return path


def file_mode(path: Path) -> str:
    return oct(os.stat(path).st_mode & 0o777)


def build_ctx(out_dir: Path, hold: float) -> dict:
    used = set()
    names = ('http', 'store', 'wt1', 'wt2', 'yra', 'yrb')
    ports = {name: C102.alloc_port(used) for name in names}
    roles = {name.upper(): f'<{name.upper()}>' for name in names}
    port_roles = {ports[name]: name.upper() for name in names}
    work = out_dir / 'run'
    for sub in ('configs', 'logs', 'fixtures'):
        (work / sub).mkdir(parents=True, exist_ok=True)
    fx = work / 'fixtures'
    cert_a, cert_b = gen_cert(fx, 'a'), gen_cert(fx, 'b')
    files = {
        'keys_a': keys_file(fx, 'keys-a.yaml', FILE_A, 0o600),
        'keys_b': keys_file(fx, 'keys-b.yaml', FILE_B, 0o600),
        'keys_world': keys_file(fx, 'keys-world.yaml', FILE_A, 0o644),
        'keys_empty': keys_file(fx, 'keys-empty.yaml', {}, 0o600, raw=''),
        'keys_seq': keys_file(fx, 'keys-seq.yaml', {}, 0o600, raw='- one\n- two\n'),
        'keys_short': keys_file(fx, 'keys-short.yaml', SHORT_KEY, 0o600),
        'keys_missing': fx / 'keys-absent.yaml',
        'dir_755': fx / 'keysdir-755',
        'dir_700': fx / 'keysdir-700',
    }
    for name in ('keysdir-755', 'keysdir-700'):
        (fx / name).mkdir(exist_ok=True)
    os.chmod(files['dir_755'], 0o755)
    os.chmod(files['dir_700'], 0o700)
    modes = {k: (file_mode(v) if Path(v).exists() else None)
             for k, v in list(files.items()) + [('cert_a', files['keys_a'])]}
    return dict(work=work, fx=fx, hold=hold, roles=roles, port_roles=port_roles, used=used,
                cert_a=cert_a, cert_b=cert_b, files=files, modes=modes,
                **{f'{k}_port': v for k, v in ports.items()})


# --------------------------------------------------------------------------- observation extras
def udp_holders(port: int) -> int:
    """Whoever holds a UDP endpoint shows up here; the C99 bind-probe answer was not trustworthy
    on this host, so the lane reads the socket table instead of racing it."""
    out = subprocess.run(['lsof', '-nP', f'-iUDP:{port}'], capture_output=True, text=True)
    return len([l for l in out.stdout.splitlines()[1:] if l.strip()])


def tcp_listeners(port: int) -> int:
    return C102.listeners_on(port)


# ------------------------------------------------------------------------------- config shapes
def base_cfg(ctx, keys=True, wt=None) -> str:
    """`wt` is a (port, cert, key) tuple rendered into the webtransport block, so a profile can put
    the losing value in the YAML and the winning one on argv."""
    body = (f'port: {ctx["http_port"]}\n'
            f'bind_addresses: [127.0.0.1]\n'
            f'redis:\n  address: 127.0.0.1:{ctx["store_port"]}\n')
    if keys:
        body += 'keys:\n' + ''.join(f'  {k}: {v}\n' for k, v in YAML_KEYS.items())
    body += 'rtc:\n  tcp_port: 0\n'
    if wt is not None:
        port, cert, key = wt
        body += (f'webtransport:\n  port: {port}\n'
                 f'  tls_cert_file: {cert}\n  tls_key_file: {key}\n')
    return body


# --------------------------------------------------------------------------- claim vocabulary
# Every needle is written in the masked vocabulary the receipt keeps: `<HTTP> <STORE> <WT1> <WT2>
# <YRA> <YRB>` for allocated ports and `<WORK>` for the fixture directory, because `evaluate` runs
# on the masked record - the same text `--report` re-derives from a filed receipt.
LISTENER_START = 'webtransport listener started'
LISTENER_STOP = 'webtransport listener stopped'
SERVER_READY = 'starting LiveKit server'
WT_BOTH_NAMES = 'webtransport requires webtransport.tls_cert_file/tls_key_file'
KEY_MISMATCH = 'tls: private key does not match public key'
KEY_PERM = 'key file others permissions must be set to 0'
KEY_DIR = 'is a directory'
KEY_EOF = 'EOF'
KEY_SEQ = 'cannot unmarshal !!seq into map[string]string'
KEY_SHORT = 'secret is too short, should be at least 32 characters for security'
CLI_PARSE = 'invalid value "" for flag -webtransport.port'
AUTH_LOG_NEEDLE = 'error handling request'
NO_SUCH_FILE = 'no such file or directory'


def profiles(ctx) -> list:
    out = []
    roles = ctx['roles']
    fx = '<WORK>/fixtures'
    wt1, wt2 = roles['WT1'], roles['WT2']
    yra = roles['YRA']
    ca, cb = ctx['cert_a'], ctx['cert_b']
    files = ctx['files']
    real_wt1 = ctx['wt1_port']

    def add(pid, rows, claims, argv=(), env=None, cfg=None, expect_exit=0, refuse_wait=None,
            probe=(), ports=(), note='', ready=True):
        out.append({'id': pid, 'rows': rows, 'claims': claims, 'argv': list(argv),
                    'env': dict(env or {}), 'cfg': cfg if cfg is not None else base_cfg(ctx),
                    'expect_exit': expect_exit, 'refuse_wait': refuse_wait,
                    'probe_kids': list(probe), 'probe_ports': list(ports), 'note': note,
                    'ready': ready})

    def wt_argv(port, cert=None, key=None):
        argv = ['--webtransport.port', str(port)]
        if cert is not None:
            argv += ['--webtransport.tls_cert_file', str(cert)]
        if key is not None:
            argv += ['--webtransport.tls_key_file', str(key)]
        return argv

    # ---------------------------------------------------------------- WebTransport, flag channel
    add('wt-flags-valid', ['generated_flags:--webtransport.port',
                           'generated_flags:--webtransport.tls_cert_file',
                           'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [LISTENER_START, SERVER_READY], 'log_not': [KEY_MISMATCH, WT_BOTH_NAMES],
         'udp_has': [wt1], 'listener_order': True, 'node_alive': 1,
         'auth': {'c103y': ACCEPTED}, 'release_ports': True},
        argv=wt_argv(real_wt1, ca['cert'], ca['key']), ports=[('wt', real_wt1)],
        probe=['c103y'], note='the flag triple alone brings the listener up on a config with no webtransport block')

    add('wt-flag-port-beats-yaml', ['generated_flags:--webtransport.port'],
        {'log_all': [LISTENER_START], 'udp_has': [wt1], 'udp_not': [yra], 'listener_order': True,
         'node_alive': 1, 'auth': {'c103y': ACCEPTED}, 'release_ports': True},
        argv=['--webtransport.port', str(real_wt1)],
        cfg=base_cfg(ctx, wt=(ctx['yra_port'], ca['cert'], ca['key'])),
        ports=[('wt', real_wt1), ('yra', ctx['yra_port'])], probe=['c103y'],
        note='the YAML port is the losing channel, so it must stay unbound')

    add('wt-flag-material-beats-yaml', ['generated_flags:--webtransport.tls_cert_file',
                                        'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [LISTENER_START], 'log_not': [KEY_MISMATCH], 'udp_has': [wt1],
         'listener_order': True, 'node_alive': 1, 'release_ports': True},
        argv=wt_argv(real_wt1, ca['cert'], ca['key']),
        cfg=base_cfg(ctx, wt=(ctx['yra_port'], ca['cert'], cb['key'])),
        ports=[('wt', real_wt1)], probe=['c103y'],
        note='YAML carries a mismatched pair: a clean start proves the loader opened the flag files')

    add('wt-flag-material-mismatch', ['generated_flags:--webtransport.tls_cert_file',
                                      'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [KEY_MISMATCH], 'log_not': [LISTENER_START], 'node_alive': 0,
         'store_no_node_after': True},
        argv=wt_argv(real_wt1, ca['cert'], cb['key']),
        cfg=base_cfg(ctx, wt=(ctx['yra_port'], ca['cert'], ca['key'])),
        expect_exit=1, refuse_wait=10.0, ports=[('wt', real_wt1)],
        note='the reverse half of the pair above: a flag pair that does not match must refuse')

    add('wt-env-port-and-material', ['generated_flags:--webtransport.port',
                                     'generated_flags:--webtransport.tls_cert_file',
                                     'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [LISTENER_START], 'udp_has': [wt1], 'listener_order': True, 'node_alive': 1,
         'auth': {'c103y': ACCEPTED}, 'release_ports': True},
        env={'LIVEKIT_WEBTRANSPORT_PORT': str(real_wt1),
             'LIVEKIT_WEBTRANSPORT_TLS_CERT_FILE': str(ca['cert']),
             'LIVEKIT_WEBTRANSPORT_TLS_KEY_FILE': str(ca['key'])},
        ports=[('wt', real_wt1)], probe=['c103y'])

    add('wt-env-beats-yaml', ['generated_flags:--webtransport.port',
                              'generated_flags:--webtransport.tls_cert_file',
                              'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [LISTENER_START], 'log_not': [KEY_MISMATCH], 'udp_has': [wt1],
         'udp_not': [yra], 'node_alive': 1, 'release_ports': True},
        env={'LIVEKIT_WEBTRANSPORT_PORT': str(real_wt1),
             'LIVEKIT_WEBTRANSPORT_TLS_CERT_FILE': str(ca['cert']),
             'LIVEKIT_WEBTRANSPORT_TLS_KEY_FILE': str(ca['key'])},
        cfg=base_cfg(ctx, wt=(ctx['yra_port'], ca['cert'], cb['key'])),
        ports=[('wt', real_wt1), ('yra', ctx['yra_port'])],
        note='env is the third channel, and the one that loses here is the YAML pair')

    add('wt-cert-flag-without-key', ['generated_flags:--webtransport.tls_cert_file'],
        {'log_all': [WT_BOTH_NAMES], 'log_not': [LISTENER_START], 'node_alive': 0,
         'store_no_node_after': True},
        argv=['--webtransport.port', str(real_wt1),
              '--webtransport.tls_cert_file', str(ca['cert'])],
        expect_exit=1, refuse_wait=10.0, ports=[('wt', real_wt1)],
        note='the gate is both names, so half a pair has to refuse')

    add('wt-empty-material-flags', ['generated_flags:--webtransport.tls_cert_file',
                                    'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [WT_BOTH_NAMES], 'log_not': [LISTENER_START], 'node_alive': 0,
         'store_no_node_after': True},
        argv=wt_argv(real_wt1, '', ''), expect_exit=1, refuse_wait=10.0, ports=[('wt', real_wt1)],
        note='an empty string is passed, not omitted: IsSet is true and the value names nothing')

    add('wt-empty-cert-overrides-yaml', ['generated_flags:--webtransport.tls_cert_file'],
        {'log_all': [WT_BOTH_NAMES], 'log_not': [LISTENER_START], 'node_alive': 0,
         'store_no_node_after': True},
        argv=['--webtransport.tls_cert_file', ''],
        cfg=base_cfg(ctx, wt=(ctx['yra_port'], ca['cert'], ca['key'])),
        expect_exit=1, refuse_wait=10.0, ports=[('wt', ctx['yra_port'])],
        note='an empty flag value replaces a complete YAML pair rather than being ignored')

    add('wt-port-empty-string', ['generated_flags:--webtransport.port'],
        {'log_all': [CLI_PARSE, 'Incorrect Usage'], 'log_not': [LISTENER_START, SERVER_READY],
         'node_alive': 0, 'store_no_node_after': True},
        argv=['--webtransport.port', ''], expect_exit=1, refuse_wait=10.0,
        ports=[('wt', real_wt1), ('http', ctx['http_port'])],
        note='the uint flag parser rejects the empty spelling before any daemon exists')

    add('wt-port-zero-beats-yaml', ['generated_flags:--webtransport.port'],
        {'log_not': [LISTENER_START], 'udp_empty': True, 'node_alive': 1,
         'auth': {'c103y': ACCEPTED}, 'release_ports': True},
        argv=['--webtransport.port', '0'],
        cfg=base_cfg(ctx, wt=(ctx['yra_port'], ca['cert'], ca['key'])),
        ports=[('wt', ctx['yra_port']), ('http', ctx['http_port'])], probe=['c103y'],
        note='0 is a value, not an absence: the YAML listener has to lose')

    add('wt-missing-files-flags', ['generated_flags:--webtransport.tls_cert_file',
                                   'generated_flags:--webtransport.tls_key_file'],
        {'log_all': [f'{fx}/absent-cert.pem', NO_SUCH_FILE], 'log_not': [f'{fx}/absent-key.pem'],
         'node_alive': 0, 'store_no_node_after': True},
        argv=wt_argv(real_wt1, ctx['fx'] / 'absent-cert.pem', ctx['fx'] / 'absent-key.pem'),
        expect_exit=1, refuse_wait=10.0, ports=[('wt', real_wt1)],
        note='the refusal names the certificate, which is the read order, not a combined check')

    # ------------------------------------------------------------------------- key_file channel
    add('key-file-flag-auth', ['generated_flags:--key_file'],
        {'log_all': [SERVER_READY], 'log_not': [KEY_PERM, LISTENER_START],
         'auth': {'c103a': ACCEPTED, 'c103y': REJECTED},
         'node_alive': 1, },
        argv=['--key-file', str(files['keys_a'])], probe=['c103a', 'c103y'],
        note='the base spelling: the file replaces the inline map, so the YAML key stops working')

    add('key-file-underscore-flag', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103y': REJECTED}, 'node_alive': 1,
         },
        argv=['--key_file', str(files['keys_a'])], probe=['c103a', 'c103y'],
        note='the generated spelling is a real flag, not an alias the parser tolerates by accident')

    add('key-file-env', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103y': REJECTED}, 'node_alive': 1,
         },
        env={'LIVEKIT_KEY_FILE': str(files['keys_a'])}, probe=['c103a', 'c103y'],
        note='LIVEKIT_KEY_FILE is the generated env name (LIVEKIT_ + the YAML path, upper-cased)')

    add('key-file-dash-beats-underscore', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103b': REJECTED, 'c103y': REJECTED}, 'node_alive': 1},
        argv=['--key-file', str(files['keys_a']), '--key_file', str(files['keys_b'])],
        probe=['c103a', 'c103b', 'c103y'],
        note='two live flags for one field: the explicit IsSet("key-file") branch runs last')

    add('key-file-underscore-first', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103b': REJECTED, 'c103y': REJECTED}, 'node_alive': 1},
        argv=['--key_file', str(files['keys_b']), '--key-file', str(files['keys_a'])],
        probe=['c103a', 'c103b', 'c103y'],
        note='argv order is not what decides, so this row must land on the same key as the one above')

    add('key-file-flag-beats-env', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103b': REJECTED, 'c103y': REJECTED}, 'node_alive': 1},
        argv=['--key-file', str(files['keys_a'])],
        env={'LIVEKIT_KEY_FILE': str(files['keys_b'])}, probe=['c103a', 'c103b', 'c103y'])

    add('key-file-beats-keys-flag', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103n': REJECTED, 'c103y': REJECTED}, 'node_alive': 1},
        argv=['--key-file', str(files['keys_a']), '--keys',
              f"c103n: {FILE_FLAG['c103n']}"], probe=['c103a', 'c103n', 'c103y'],
        note='ValidateKeys prefers the file, so the --keys map that was applied later must lose')

    add('key-file-empty-value', ['generated_flags:--key_file'],
        {'auth': {'c103a': REJECTED, 'c103y': ACCEPTED}, 'node_alive': 1},
        argv=['--key-file', ''], probe=['c103a', 'c103y'],
        note='an empty path clears the field, and an empty field falls through to the inline keys')

    add('key-file-env-expansion', ['generated_flags:--key_file'],
        {'auth': {'c103a': ACCEPTED, 'c103y': REJECTED}, 'node_alive': 1,
         },
        argv=['--key-file', '$C103FIXTURES/keys-a.yaml'],
        env={'C103FIXTURES': str(ctx['fx'])}, probe=['c103a', 'c103y'],
        note='the path is expanded by the daemon, so the argv line never contains the real directory')

    add('key-file-world-readable', ['generated_flags:--key_file'],
        {'log_all': [KEY_PERM], 'node_alive': 0, 'store_left_clean': True,
         },
        argv=['--key-file', str(files['keys_world'])], expect_exit=1, refuse_wait=10.0,
        ports=[('http', ctx['http_port'])],
        note='perm & 0o007 must be zero, and the refusal happens before any listener')

    add('key-file-dir-755', ['generated_flags:--key_file'],
        {'log_all': [KEY_PERM], 'log_not': [KEY_DIR], 'node_alive': 0,
         'store_left_clean': True},
        argv=['--key-file', str(files['dir_755'])], expect_exit=1, refuse_wait=10.0,
        ports=[('http', ctx['http_port'])],
        note='a directory with default permissions fails the same mode test, before the read is tried')

    add('key-file-dir-700', ['generated_flags:--key_file'],
        {'log_all': [f'read {fx}/keysdir-700', KEY_DIR], 'log_not': [KEY_PERM],
         'node_alive': 0, 'store_left_clean': True},
        argv=['--key-file', str(files['dir_700'])], expect_exit=1, refuse_wait=10.0,
        ports=[('http', ctx['http_port'])],
        note='with the mode cleared the read is attempted, and only then does it report a directory')

    add('key-file-missing', ['generated_flags:--key_file'],
        {'log_all': [f'stat {fx}/keys-absent.yaml', NO_SUCH_FILE], 'node_alive': 0,
         'store_left_clean': True},
        argv=['--key-file', str(files['keys_missing'])], expect_exit=1, refuse_wait=10.0,
        ports=[('http', ctx['http_port'])])

    add('key-file-empty-file', ['generated_flags:--key_file'],
        {'log_all': [KEY_EOF], 'node_alive': 0, 'store_left_clean': True},
        argv=['--key-file', str(files['keys_empty'])], expect_exit=1, refuse_wait=10.0,
        ports=[('http', ctx['http_port'])],
        note='a zero-byte file is a decoder EOF, not an empty key map: the node must not start')

    add('key-file-yaml-seq', ['generated_flags:--key_file'],
        {'log_all': [KEY_SEQ], 'node_alive': 0, 'store_left_clean': True},
        argv=['--key-file', str(files['keys_seq'])], expect_exit=1, refuse_wait=10.0,
        ports=[('http', ctx['http_port'])])

    add('key-file-short-secret', ['generated_flags:--key_file'],
        {'log_all': [KEY_SHORT], 'auth': {'c103s': ACCEPTED}, 'node_alive': 1,
         },
        argv=['--key-file', str(files['keys_short'])], probe=['c103s', 'c103y'],
        note='the short secret is an Errorw, not a refusal: the node still serves it')
    return out


# ------------------------------------------------------------------------------- one profile run
def run_entry(ctx, prof, masker, binaries) -> dict:
    entry = {'profile': prof['id'], 'rows': prof['rows'], 'claims': prof['claims'],
             'note': prof['note'], 'expect_exit': prof['expect_exit'],
             'refuse_wait': prof['refuse_wait'], 'probe_kids': prof['probe_kids'],
             'probe_ports': [name for name, _ in prof['probe_ports']], 'runs': {}}
    plain = ctx['store_port']
    for side in ('go', 'rust'):
        binary = binaries[side]
        ready_port = ctx['http_port'] if prof['ready'] else None
        C102.redis_cli(plain, 'FLUSHALL')
        before = C102.dbsize(plain)
        if before != 0:
            raise RuntimeError(f'store not clean entering {prof["id"]}/{side}: {before}')

        def on_alive(proc, cfg_path, ready_port=ready_port):
            observed = {'alive_store': C102.store_snapshot(plain)}
            if prof['probe_kids'] and ready_port:
                observed['auth'] = auth_probe(ready_port, prof['probe_kids'], masker)
            return observed

        rec = C102.launch_pair(binary, list(prof['argv']), prof['env'], prof['cfg'], ctx['work'],
                               f'{side}-{prof["id"]}', side, masker, ready_port, ctx['hold'],
                               prof['refuse_wait'], on_alive=on_alive)
        obs = rec.pop('observed', {}) or {}
        # A refusal never reaches the readiness probe, so "no listener" is only honest when read after
        # the process is gone; a start reads the socket table while it is alive and the port table
        # again after it is not.
        rec['auth'] = obs.get('auth') or {}
        rec['store'] = {'before_dbsize': before, 'alive': obs.get('alive_store') or {},
                        'after': C102.store_snapshot(plain)}
        rec['post_ports'] = {name: {'tcp': tcp_listeners(port), 'udp': udp_holders(port)}
                             for name, port in prof['probe_ports']}
        entry['runs'][side] = rec
    return entry


# ------------------------------------------------------------------------------- claims/legs
# The one vocabulary evaluate understands. A claim key outside this set would be recorded and then
# silently ignored, which is how a receipt grows a field that asserts nothing.
CLAIM_KEYS = {'log_all', 'log_any', 'log_not', 'udp_has', 'udp_not', 'udp_empty', 'listener_order',
              'node_alive', 'auth', 'store_left_clean', 'store_no_node_after', 'release_ports'}


def needle_slug(needle: str) -> str:
    """A readable, process-stable suffix. `hash()` is randomised per interpreter, and a leg id that
    moved between --record and --report would make verdict_stable_across_rederivation and every
    FAILED_LEG gate target meaningless."""
    return re.sub(r'[^0-9a-z]', '', str(needle).lower())[:14] or 'x'


def L(pid, side, suffix) -> str:
    return f'{pid}::{side}:{suffix}'


def has_needle(pid, side, needle) -> str:
    return L(pid, side, f'has-{needle_slug(needle)}')


def leg(name, side, kind, why='', **fields):
    item = {'leg': name, 'side': side, 'kind': kind, 'why': why}
    item.update(fields)
    return item


def rederive_leg(item) -> bool:
    kind = item['kind']
    if kind == 'has':
        return all(needle in item['haystack'] for needle in item['needles'])
    if kind == 'hasnt':
        return not any(needle in item['haystack'] for needle in item['needles'])
    if kind == 'any':
        return bool(item['needles']) and any(needle in item['haystack'] for needle in item['needles'])
    if kind == 'nonzero':
        return bool(item['actual'])
    if kind == 'eq':
        return item['expect'] == item['actual']
    raise RuntimeError(f'leg {item["leg"]} has kind {kind!r}, which this re-deriver cannot check')


def evaluate(entry) -> list:
    """Pure over the receipt, so --record, --report and --selftest-gate grade the same legs."""
    unknown = set(entry['claims']) - CLAIM_KEYS
    if unknown:
        raise RuntimeError(f'{entry["profile"]} carries claims nothing evaluates: {sorted(unknown)}')
    legs = []
    for side in ('go', 'rust'):
        rec = entry['runs'][side]
        log = rec.get('log_masked') or ''
        claims = entry['claims']
        sockets = rec.get('sockets_masked') or {}
        udp = sockets.get('udp') or []
        held = ' '.join(list(udp) + list(sockets.get('tcp') or []))
        pid = entry['profile']

        def emit(suffix, kind, **fields):
            legs.append(leg(f'{pid}::{side}:{suffix}', side, kind, **fields))

        for needle in claims.get('log_all', []):
            emit(f'has-{needle_slug(needle)}', 'has', needles=[needle], haystack=log,
                 why='the channel effect has to be visible in the daemon log')
        for needle in claims.get('log_not', []):
            emit(f'not-{needle_slug(needle)}', 'hasnt', needles=[needle], haystack=log,
                 why='a channel that lost must stay silent')
        if claims.get('log_any'):
            emit('any', 'any', needles=claims['log_any'], haystack=log,
                 why='a refusal has to name at least one plausible reason')
        want = entry.get('expect_exit')
        if want == 'nonzero':
            emit('exit', 'nonzero', actual=rec['exit'])
        elif want is not None:
            emit('exit', 'eq', expect=want, actual=rec['exit'], why='the raw exit code of the launch')
        for role in claims.get('udp_has', []):
            emit(f'udp-{role.strip("<>")}', 'eq', expect=True, actual=role in udp,
                 why='the UDP endpoint the flag named is the one the process holds')
        for role in claims.get('udp_not', []):
            emit(f'udp-not-{role.strip("<>")}', 'eq', expect=False, actual=role in held,
                 why='the losing channel port must never be bound')
        if 'udp_empty' in claims:
            emit('udp-empty', 'eq', expect=[], actual=sorted(udp),
                 why='port 0 starts no listener at all')
        if claims.get('listener_order'):
            emit('order', 'eq', expect=True, actual=strictly_before(log, LISTENER_START, SERVER_READY),
                 why='the listener line has to precede the readiness line')
        if 'node_alive' in claims:
            alive = (rec.get('store') or {}).get('alive') or {}
            emit('node', 'eq', expect=claims['node_alive'], actual=len(alive.get('node_ids') or []),
                 why='the registry row exists only while the daemon is alive')
        for kid, verdict in (claims.get('auth') or {}).items():
            cell = ((rec.get('auth') or {}).get(kid) or {}).get('verdict')
            emit(f'auth-{kid}', 'eq', expect=verdict, actual=cell,
                 why='the running node either signs this key in or keeps it out')
        if claims.get('store_left_clean'):
            after = ((rec.get('store') or {}).get('after') or {}).get('keys')
            emit('store-clean', 'eq', expect=[], actual=after,
                 why='a refused start writes nothing it then has to unwind')
        # Measured split, not an assumption: a key-file refusal happens before either node touches
        # the store, but a WebTransport material refusal happens after Rust has written its version
        # marker. Both sides still register nothing, so that is what the row claims; the marker
        # difference is pinned by webtransport_refusal_marker_split_pinned instead of being averaged.
        if claims.get('store_no_node_after'):
            emit('node-gone', 'eq', expect=[],
                 actual=((rec.get('store') or {}).get('after') or {}).get('node_ids'),
                 why='a refused start never registers a node')
        if claims.get('release_ports'):
            post = rec.get('post_ports') or {}
            for name in sorted(post):
                emit(f'release-{name}', 'eq', expect={'tcp': 0, 'udp': 0}, actual=post[name],
                     why='the socket is released when the daemon is gone')
    seen = {}
    for item in legs:
        seen[item['leg']] = seen.get(item['leg'], 0) + 1
        if seen[item['leg']] > 1:
            item['leg'] = f"{item['leg']}#{seen[item['leg']] - 1}"
    return legs


def strictly_before(text, first, second) -> bool:
    a, b = text.find(first), text.find(second)
    return a != -1 and b != -1 and a < b


# ------------------------------------------------------------------------------- controls
PROFILE_COUNT = 28
WT_ROWS = ('wt-flags-valid', 'wt-flag-port-beats-yaml', 'wt-flag-material-beats-yaml',
           'wt-flag-material-mismatch', 'wt-env-port-and-material', 'wt-env-beats-yaml',
           'wt-cert-flag-without-key', 'wt-empty-material-flags', 'wt-empty-cert-overrides-yaml',
           'wt-port-empty-string', 'wt-port-zero-beats-yaml', 'wt-missing-files-flags')
C102_FILED_HARNESS_SHA = 'fff0615af856f95fc329a5aee3bbafcae2ce795474edde88176c27ca10086a22'
STATUS_PASS = 'PASS_LAUNCHED_GENERATED_FLAG_CHANNELS'
WT_MATERIAL_REFUSALS = ('wt-flag-material-mismatch', 'wt-cert-flag-without-key',
                        'wt-empty-material-flags', 'wt-empty-cert-overrides-yaml',
                        'wt-missing-files-flags')


def sentence_after(log: str, needle: str, width: int = 160) -> str:
    """The refusal sentence: the masked text from the needle to the end of its line. Measured on both
    daemons, a startup refusal is printed as one bare line (no encoder prefix, no timestamp), so the
    line is the whole sentence - and stopping at the newline keeps the *next* line, where the two
    sides legitimately differ, out of the comparison."""
    idx = log.find(needle)
    if idx < 0:
        return ''
    end = log.find('\n', idx)
    return log[idx:len(log) if end < 0 else end][:width]


def build_controls(doc, live=False) -> dict:
    entries = doc['profiles']
    by_id = {e['profile']: e for e in entries}
    runs = [(e['profile'], side, e['runs'][side]) for e in entries for side in ('go', 'rust')]
    C = {}
    C['no_development_subset'] = not doc.get('subset')
    C['every_profile_ran_both_sides'] = all(
        set(e['runs']) == {'go', 'rust'} and all(r.get('exit') is not None for r in e['runs'].values())
        for e in entries)
    C['profile_inventory_complete'] = len(entries) == PROFILE_COUNT
    C['every_profile_names_a_row'] = all(
        e['rows'] and set(e['rows']) <= set(COVERS) for e in entries)
    C['claims_vocabulary_closed'] = all(not (set(e['claims']) - CLAIM_KEYS) for e in entries)
    C['webtransport_group_is_the_named_twelve'] = sorted(
        e['profile'] for e in entries if e['profile'] in WT_ROWS) == sorted(WT_ROWS)

    # the auth channel has to be able to say two different things, on both sides
    probed = [e for e in entries if (e['claims'].get('auth') or {})]
    C['auth_rows_present'] = len(probed) >= 12
    C['auth_claim_shows_both_classes'] = all(
        ACCEPTED in set((e['claims']['auth'] or {}).values()) and
        REJECTED in set(e['claims']['auth'].values())
        for e in probed if len(e['claims']['auth']) > 1)
    C['auth_cells_recorded_not_missing'] = all(
        (e['runs'][side].get('auth') or {}).get(kid) is not None
        for e in probed for side in ('go', 'rust') for kid in e['claims']['auth'])
    C['auth_pair_identical_across_sides'] = all(
        {k: v['verdict'] for k, v in (e['runs']['go'].get('auth') or {}).items()} ==
        {k: v['verdict'] for k, v in (e['runs']['rust'].get('auth') or {}).items()}
        for e in probed)
    # These two controls are named after specific rows. On a --only subset the rows are absent, and
    # an absent row must read False rather than raise: only a full sweep may claim them.
    precedence = [by_id.get(pid) for pid in ('key-file-dash-beats-underscore',
                                             'key-file-underscore-first')]
    C['key_precedence_pair_pinned'] = all(p is not None for p in precedence) and all(
        {k: v['verdict'] for k, v in (e['runs'][side].get('auth') or {}).items()} ==
        {'c103a': ACCEPTED, 'c103b': REJECTED, 'c103y': REJECTED}
        for e in precedence for side in ('go', 'rust'))
    env_only_row = by_id.get('key-file-env')
    C['env_only_row_carries_two_cells'] = bool(env_only_row) and sorted(
        {k: v['verdict'] for k, v in (env_only_row['runs']['go'].get('auth') or {}).items()}) == [
        'c103a', 'c103y']

    # the listener claims must actually split the profiles
    bound = [e['profile'] for e in entries if (e['claims'].get('udp_has') or [])]
    silent = [e['profile'] for e in entries if e['claims'].get('udp_empty') or e['claims'].get('node_alive') == 0]
    C['webtransport_listener_claim_split'] = bool(bound) and bool(silent) and not (set(bound) & set(silent))
    C['webtransport_losing_port_never_bound'] = all(
        not any(role in ' '.join((r.get('sockets_masked') or {}).get('udp', []) +
                                 (r.get('sockets_masked') or {}).get('tcp', []))
                for role in e['claims']['udp_not'])
        for e in entries for r in e['runs'].values() if e['claims'].get('udp_not'))
    C['listener_line_precedes_readiness'] = all(
        strictly_before(r.get('log_masked') or '', LISTENER_START, SERVER_READY)
        for e in entries if e['claims'].get('listener_order') for r in e['runs'].values())

    refusals = [e for e in entries if e.get('refuse_wait') is not None]
    starts = [e for e in entries if e.get('refuse_wait') is None]
    C['refusal_rows_self_ended'] = bool(refusals) and all(
        r.get('self_exited') for e in refusals for r in e['runs'].values())
    C['sigkill_never_used'] = not any('SIGKILL' in (r.get('signals') or []) for _, _, r in runs)
    C['refusal_leaves_no_listener'] = bool(refusals) and all(
        all(v == {'tcp': 0, 'udp': 0} for v in (r.get('post_ports') or {}).values())
        and (r.get('post_ports') or {})
        for e in refusals for r in e['runs'].values())
    clean_rows = [e for e in refusals if e['claims'].get('store_left_clean')]
    C['refusal_writes_nothing'] = bool(clean_rows) and all(
        ((r.get('store') or {}).get('after') or {}).get('keys') == []
        for e in clean_rows for r in e['runs'].values())
    node_rows = [e for e in refusals if e['claims'].get('store_no_node_after')]
    C['refusal_rows_leave_no_node'] = bool(node_rows) and all(
        ((r.get('store') or {}).get('after') or {}).get('node_ids') == []
        for e in node_rows for r in e['runs'].values())
    # The finding this lane found: Go's WebTransport-material refusal reaches the store before it
    # writes anything, Rust has already written its version marker. Pinned in that direction - if a
    # later change aligns the two, this reddens and the rows get re-read rather than ageing quietly.
    marker = [by_id.get(pid) for pid in WT_MATERIAL_REFUSALS]
    C['webtransport_refusal_marker_split_pinned'] = all(m is not None for m in marker) and all(
        ((m['runs']['go'].get('store') or {}).get('after') or {}).get('keys') == [] and
        ((m['runs']['rust'].get('store') or {}).get('after') or {}).get('keys') == ['livekit_version']
        for m in marker)
    C['started_rows_register_one_node'] = bool(starts) and all(
        len((((e['runs'][side].get('store') or {}).get('alive') or {}).get('node_ids') or [])) == 1
        for e in starts if 'node_alive' in e['claims'] for side in ('go', 'rust'))
    C['node_row_gone_after_exit'] = all(
        ((e['runs'][side].get('store') or {}).get('after') or {}).get('node_ids') == []
        for e in starts for side in ('go', 'rust'))
    C['version_marker_survives_clean_shutdown'] = all(
        'livekit_version' in ((e['runs'][side].get('store') or {}).get('after') or {}).get('keys', [])
        for e in starts for side in ('go', 'rust'))
    paired = [(e, needle) for e in refusals for needle in (e['claims'].get('log_all') or [])[:1]]
    C['refusal_sentence_identical_across_sides'] = bool(paired) and all(
        sentence_after(e['runs']['go']['log_masked'], needle) ==
        sentence_after(e['runs']['rust']['log_masked'], needle) and
        needle in e['runs']['go']['log_masked'] for e, needle in paired)

    # Measured, not assumed: the oracle prints one `error handling request` WARN per /rtc/v1/validate
    # call that ends in an HTTP error status - the 401 bad-key shape and the 400 join_request shape
    # alike - while the Rust node answers byte-comparable statuses and logs nothing. So the pin counts
    # lines against probes: if Rust gains the line, or Go stops printing it for the accepted-key 400,
    # this reddens instead of quietly reading green off "at least one line".
    probed = [e for e in entries if (e['runs']['go'].get('auth') or {})]
    C['auth_rejection_logging_pinned'] = len(probed) >= 6 and all(
        e['runs']['go']['log_masked'].count(AUTH_LOG_NEEDLE) == len(e['runs']['go']['auth']) and
        e['runs']['rust']['log_masked'].count(AUTH_LOG_NEEDLE) == 0 for e in probed)
    C['store_flushed_before_every_launch'] = all(
        (r.get('store') or {}).get('before_dbsize') == 0 for _, _, r in runs)
    C['ready_rows_reach_the_daemon'] = all(r.get('ready') for e in starts for r in e['runs'].values())
    return C


# ------------------------------------------------------------------ provenance/privacy controls
def build_provenance_controls(doc, live=False) -> dict:
    bins = doc['binaries']
    C = {}
    now = {side: (sha256_file_now(bins[side].get('path', '')) if bins[side].get('path') else None)
           for side in ('go', 'rust')}
    doc['binary_hash_now'] = now
    C['binaries_unchanged'] = all(now[s] is not None and now[s] == bins[s]['sha256']
                                 for s in ('go', 'rust'))
    bins['go']['is_fixed_oracle'] = now['go'] == ORACLE_SHA256
    bins['rust']['is_acceptance_candidate'] = now['rust'] == ACCEPTANCE_SHA256
    C['go_is_fixed_oracle'] = bool(bins['go']['is_fixed_oracle'])
    C['rust_is_acceptance_candidate'] = bool(bins['rust']['is_acceptance_candidate'])
    C['recorded_oracle_matches_pinned'] = bins['go']['sha256'] == ORACLE_SHA256
    C['recorded_candidate_matches_pinned'] = bins['rust']['sha256'] == ACCEPTANCE_SHA256
    C['reference_commit_matches'] = doc['reference']['go_checkout_commit'] == FIXED_GO_COMMIT
    C['imported_primitives_are_the_accepted_lane'] = doc['c102_harness_sha256'] == C102_FILED_HARNESS_SHA
    harness_now = C102.sha256_file(Path(__file__).resolve())
    doc['harness_sha256_now'] = harness_now
    C['harness_unchanged_during_run'] = (
        harness_now == doc['harness_sha256_at_start'] if live
        else doc['harness_sha256'] == doc['harness_sha256_at_start'])
    C['harness_unchanged_since_run'] = doc['harness_sha256'] == harness_now

    blob = json.dumps(doc['profiles'], ensure_ascii=False, sort_keys=True)
    whole = json.dumps(doc, ensure_ascii=False, sort_keys=True)
    C['off_limits_never_named'] = not re.search(r'\b788[12]\b', blob)
    C['host_store_never_named'] = ':6379' not in blob
    C['no_machine_address_in_receipt'] = not C102.forbidden_addresses(whole)
    C['no_working_paths_in_profile_records'] = not any(
        needle in blob for needle in (str(REPO), str(Path.home()),
                                      str(Path(doc.get('work_dir') or '\x00')), '/private/tmp/'))
    C['fixture_secrets_never_raw'] = not any(real in whole for real, _ in SECRET_ALIASES)
    C['argv_never_raw'] = not any('argv_raw' in r or ('argv' in r and 'argv_masked' not in r)
                                  for _, _, r in [(e['profile'], side, e['runs'][side])
                                                  for e in doc['profiles'] for side in ('go', 'rust')])
    C['raw_streams_recorded'] = all(
        re.fullmatch(r'[0-9a-f]{64}', r['raw']['stdout_sha256']) and r['raw']['bytes'] > 0
        for _, _, r in [(e['profile'], side, e['runs'][side]) for e in doc['profiles']
                        for side in ('go', 'rust')])
    bounds = doc.get('listeners_bounds') or {}
    C['off_limit_ports_untouched'] = bool(bounds) and all(
        v['before'] == v['after'] for v in bounds.values())
    return C


def sha256_file_now(path: str):
    p = Path(path)
    return C102.sha256_file(p) if p.is_file() else None


# ------------------------------------------------------------------------------- verdict
def finish(doc, out=None, live=False, write=True) -> dict:
    for entry in doc['profiles']:
        legs = evaluate(entry)
        for item in legs:
            item['ok'] = rederive_leg(item)
        entry['legs'] = legs
        entry['problems'] = sorted(l['leg'] for l in legs if not l['ok'])
        entry['match'] = not entry['problems']
    doc['legs'] = [l for e in doc['profiles'] for l in e['legs']]
    doc['leg_count'] = len(doc['legs'])
    verdicts = {l['leg']: l['ok'] for l in doc['legs']}
    if live or 'leg_verdicts_at_record' not in doc:
        doc['leg_verdicts_at_record'] = verdicts
    controls = build_controls(doc, live=live)
    controls.update(build_provenance_controls(doc, live=live))
    controls['verdict_stable_across_rederivation'] = (verdicts == doc['leg_verdicts_at_record'])
    doc['cross_profile_controls'] = controls
    doc['failed_profiles'] = sorted(e['profile'] for e in doc['profiles'] if not e['match'])
    doc['failed_legs'] = sorted(l['leg'] for l in doc['legs'] if not l['ok'])
    doc['failed_controls'] = sorted(k for k, v in controls.items() if not v)
    doc['status_gates'] = {
        'binaries_unchanged': controls['binaries_unchanged'],
        'go_is_fixed_oracle': controls['go_is_fixed_oracle'],
        'rust_is_acceptance_candidate': controls['rust_is_acceptance_candidate'],
        'reference_commit_matches': controls['reference_commit_matches'],
        'imported_primitives_are_the_accepted_lane':
            controls['imported_primitives_are_the_accepted_lane'],
        'harness_unchanged_during_run': controls['harness_unchanged_during_run'],
        'harness_unchanged_since_run': controls['harness_unchanged_since_run'],
        'no_failed_profiles': not doc['failed_profiles'],
        'no_failed_controls': not doc['failed_controls'],
        'no_failed_legs': not doc['failed_legs'],
    }
    doc['unmet_gates'] = sorted(k for k, v in doc['status_gates'].items() if not v)
    doc['live_run'] = live
    doc['status'] = ('PARTIAL_SUBSET_NOT_ACCEPTANCE' if doc.get('subset')
                     else STATUS_PASS if not doc['unmet_gates'] else 'FAIL')
    if write and out is not None:
        (Path(out) / 'execution.json').write_text(
            json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        batch = {'status': doc['status'], 'batch': doc['batch'], 'covers': doc['covers'],
                 'profiles': len(doc['profiles']), 'legs': doc['leg_count'],
                 'controls': len(controls), 'failed_profiles': doc['failed_profiles'],
                 'failed_legs': doc['failed_legs'], 'failed_controls': doc['failed_controls'],
                 'unmet_gates': doc['unmet_gates'], 'status_gates': doc['status_gates'],
                 'profile_verdicts': {e['profile']: {'match': e['match'], 'rows': e['rows'],
                                                     'go_exit': e['runs']['go']['exit'],
                                                     'rust_exit': e['runs']['rust']['exit'],
                                                     'problems': e['problems']}
                                      for e in doc['profiles']},
                 'controls': controls, 'binary_hash_now': doc['binary_hash_now'],
                 'harness_sha256_now': doc['harness_sha256_now'], 'binaries': doc['binaries'],
                 'reference': doc['reference'], 'harness_sha256': doc['harness_sha256'],
                 'c102_harness_sha256': doc['c102_harness_sha256']}
        (Path(out) / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False,
                                                        sort_keys=True) + '\n')
    return {'all_ok': not doc['unmet_gates'],
            'summary': {k: doc[k] for k in ('status', 'leg_count', 'failed_profiles',
                                            'failed_controls', 'failed_legs', 'unmet_gates')}}


# ------------------------------------------------------------------------------- gate sweep
def rec(doc, pid, side):
    for entry in doc['profiles']:
        if entry['profile'] == pid:
            return entry['runs'][side]
    raise KeyError(pid)


def en(doc, pid):
    for entry in doc['profiles']:
        if entry['profile'] == pid:
            return entry
    raise KeyError(pid)


def claims(doc, pid):
    for entry in doc['profiles']:
        if entry['profile'] == pid:
            return entry['claims']
    raise KeyError(pid)


GATE_INJECTIONS = (
    {'case': 'wt_go_exit_drift', 'expect': 'FAILED_LEG', 'target': L('wt-flags-valid', 'go', 'exit'),
     'apply': lambda d: rec(d, 'wt-flags-valid', 'go').update(exit=1),
     'why': 'a launched row whose exit moved no longer matches the contract'},
    {'case': 'wt_listener_never_bound', 'expect': 'FAILED_LEG',
     'target': L('wt-flags-valid', 'rust', 'udp-WT1'),
     'apply': lambda d: rec(d, 'wt-flags-valid', 'rust').update(sockets_masked={'tcp': ['<HTTP>'],
                                                                                'udp': []}),
     'why': 'the claim is the held UDP endpoint, not the log line'},
    {'case': 'wt_yaml_port_now_bound', 'expect': 'FAILED_LEG',
     'target': L('wt-flag-port-beats-yaml', 'go', 'udp-not-YRA'),
     'apply': lambda d: rec(d, 'wt-flag-port-beats-yaml', 'go')['sockets_masked']['udp'].append('<YRA>'),
     'why': 'if the YAML port is bound too, the flag did not beat it'},
    {'case': 'wt_flag_material_loses', 'expect': 'FAILED_LEG',
     'target': L('wt-flag-material-beats-yaml', 'rust', 'udp-WT1'),
     'apply': lambda d: rec(d, 'wt-flag-material-beats-yaml', 'rust').update(
         sockets_masked={'tcp': ['<HTTP>'], 'udp': []}),
     'why': 'a start with no listener cannot prove the flag pair was the one loaded'},
    {'case': 'wt_mismatch_now_starts', 'expect': 'FAILED_LEG',
     'target': has_needle('wt-flag-material-mismatch', 'go', KEY_MISMATCH),
     'apply': lambda d: rec(d, 'wt-flag-material-mismatch', 'go').update(log_masked=''),
     'why': 'the mismatched flag pair has to keep refusing'},
    {'case': 'wt_env_channel_dead', 'expect': 'FAILED_LEG',
     'target': L('wt-env-port-and-material', 'go', 'udp-WT1'),
     'apply': lambda d: rec(d, 'wt-env-port-and-material', 'go').update(
         sockets_masked={'tcp': ['<HTTP>'], 'udp': []}),
     'why': 'the env spelling is claimed to reach the same listener'},
    {'case': 'wt_zero_port_starts_listener', 'expect': 'FAILED_LEG',
     'target': L('wt-port-zero-beats-yaml', 'go', 'udp-empty'),
     'apply': lambda d: rec(d, 'wt-port-zero-beats-yaml', 'go').update(
         sockets_masked={'tcp': ['<HTTP>'], 'udp': ['<YRA>']}),
     'why': '0 means no listener, so a bound port reddens the row'},
    {'case': 'wt_parse_refusal_disappears', 'expect': 'FAILED_LEG',
     'target': has_needle('wt-port-empty-string', 'rust', CLI_PARSE),
     'apply': lambda d: rec(d, 'wt-port-empty-string', 'rust').update(log_masked=''),
     'why': 'the empty uint has to stay a usage error'},
    {'case': 'wt_missing_key_named_instead', 'expect': 'FAILED_LEG',
     'target': L('wt-missing-files-flags', 'go',
                 f'not-{needle_slug("<WORK>/fixtures/absent-key.pem")}'),
     'apply': lambda d: rec(d, 'wt-missing-files-flags', 'go').update(
         log_masked=rec(d, 'wt-missing-files-flags', 'go')['log_masked'] + 'x <WORK>/fixtures/absent-key.pem'),
     'why': 'the refusal names the certificate first; naming the key would be a different loader'},
    {'case': 'wt_listener_order_swapped', 'expect': 'FAILED_LEG',
     'target': L('wt-flags-valid', 'rust', 'order'),
     'apply': lambda d: rec(d, 'wt-flags-valid', 'rust').update(
         log_masked=rec(d, 'wt-flags-valid', 'rust')['log_masked']
         .replace(SERVER_READY, '#').replace(LISTENER_START, SERVER_READY).replace('#', LISTENER_START)),
     'why': 'the listener line must precede the readiness line'},
    {'case': 'wt_release_leaks', 'expect': 'FAILED_LEG',
     'target': L('wt-flags-valid', 'go', 'release-wt'),
     'apply': lambda d: rec(d, 'wt-flags-valid', 'go')['post_ports'].update(wt={'tcp': 0, 'udp': 1}),
     'why': 'a socket still held after teardown is a leaked listener'},
    {'case': 'key_flag_auth_flip', 'expect': 'FAILED_LEG',
     'target': L('key-file-flag-auth', 'go', 'auth-c103a'),
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'go')['auth']['c103a'].update(verdict=REJECTED),
     'why': 'the file the flag named is the key set the node must sign with'},
    {'case': 'key_yaml_still_live', 'expect': 'FAILED_LEG',
     'target': L('key-file-flag-auth', 'rust', 'auth-c103y'),
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'rust')['auth']['c103y'].update(verdict=ACCEPTED),
     'why': 'the inline map has to lose once a key file is supplied'},
    {'case': 'key_underscore_flag_unread', 'expect': 'FAILED_LEG',
     'target': L('key-file-underscore-flag', 'go', 'auth-c103a'),
     'apply': lambda d: rec(d, 'key-file-underscore-flag', 'go')['auth'].pop('c103a'),
     'why': 'a missing cell is not a pass; the generated spelling has to be probed'},
    {'case': 'key_env_row_dead', 'expect': 'FAILED_LEG',
     'target': L('key-file-env', 'rust', 'auth-c103a'),
     'apply': lambda d: rec(d, 'key-file-env', 'rust')['auth']['c103a'].update(verdict='other:503'),
     'why': 'an unexpected status is recorded as itself, never as acceptance'},
    {'case': 'key_precedence_flips', 'expect': 'FAILED_LEG',
     'target': L('key-file-dash-beats-underscore', 'go', 'auth-c103b'),
     'apply': lambda d: rec(d, 'key-file-dash-beats-underscore', 'go')['auth']['c103b'].update(
         verdict=ACCEPTED),
     'why': 'if the underscore spelling wins, the branch order claim is wrong'},
    {'case': 'key_precedence_pair_split', 'expect': 'FAILED_CONTROL',
     'target': 'key_precedence_pair_pinned',
     'apply': lambda d: rec(d, 'key-file-underscore-first', 'go')['auth']['c103b'].update(
         verdict=ACCEPTED),
     'why': 'argv order must not decide, so the two precedence rows must agree'},
    {'case': 'key_beats_keys_flag_lost', 'expect': 'FAILED_LEG',
     'target': L('key-file-beats-keys-flag', 'go', 'auth-c103n'),
     'apply': lambda d: rec(d, 'key-file-beats-keys-flag', 'go')['auth']['c103n'].update(
         verdict=ACCEPTED),
     'why': 'ValidateKeys prefers the file, so the --keys map must stay out'},
    {'case': 'key_empty_value_cleared_yaml', 'expect': 'FAILED_LEG',
     'target': L('key-file-empty-value', 'rust', 'auth-c103y'),
     'apply': lambda d: rec(d, 'key-file-empty-value', 'rust')['auth']['c103y'].update(
         verdict=REJECTED),
     'why': 'an empty path falls through to the inline keys; if it does not, the node has no keys'},
    {'case': 'key_expansion_not_applied', 'expect': 'FAILED_CONTROL',
     'target': 'auth_cells_recorded_not_missing',
     'apply': lambda d: rec(d, 'key-file-env-expansion', 'go').update(auth={}),
     # the control reads the recorded cells, so the mutation has to empty them rather than drop one
     'why': 'an empty probe map would make every auth leg read None and every control read vacuously green'},
    {'case': 'key_perm_rule_silent', 'expect': 'FAILED_LEG',
     'target': has_needle('key-file-world-readable', 'go', KEY_PERM),
     'apply': lambda d: rec(d, 'key-file-world-readable', 'go').update(log_masked=''),
     'why': 'the 0o007 rule is the whole row'},
    {'case': 'key_dir755_now_reads', 'expect': 'FAILED_LEG',
     'target': has_needle('key-file-dir-755', 'rust', KEY_PERM),
     'apply': lambda d: rec(d, 'key-file-dir-755', 'rust').update(log_masked=''),
     'why': 'a directory with other bits set must fail the mode test before the read'},
    {'case': 'key_dir700_perm_wording', 'expect': 'FAILED_LEG',
     'target': L('key-file-dir-700', 'go', f'not-{needle_slug(KEY_PERM)}'),
     'apply': lambda d: rec(d, 'key-file-dir-700', 'go').update(
         log_masked=rec(d, 'key-file-dir-700', 'go')['log_masked'] + 'x ' + KEY_PERM),
     'why': 'the 0700 row is the read failure; carrying the permission sentence would merge the two'},
    {'case': 'key_missing_stat_line', 'expect': 'FAILED_LEG',
     'target': has_needle('key-file-missing', 'go', f'stat <WORK>/fixtures/keys-absent.yaml'),
     'apply': lambda d: rec(d, 'key-file-missing', 'go').update(log_masked=NO_SUCH_FILE),
     'why': 'the op prefix is part of the contract, not decoration'},
    {'case': 'key_empty_file_starts', 'expect': 'FAILED_LEG',
     'target': L('key-file-empty-file', 'rust', 'exit'),
     'apply': lambda d: rec(d, 'key-file-empty-file', 'rust').update(exit=0),
     'why': 'an EOF decode must stay a refusal'},
    {'case': 'key_seq_wording_lost', 'expect': 'FAILED_LEG',
     'target': has_needle('key-file-yaml-seq', 'go', KEY_SEQ),
     'apply': lambda d: rec(d, 'key-file-yaml-seq', 'go').update(log_masked=''),
     'why': 'the decode says a sequence cannot land in map[string]string'},
    {'case': 'key_short_now_refuses', 'expect': 'FAILED_LEG',
     'target': L('key-file-short-secret', 'go', 'auth-c103s'),
     'apply': lambda d: rec(d, 'key-file-short-secret', 'go')['auth']['c103s'].update(
         verdict=REJECTED),
     'why': 'a short secret is logged, not enforced; refusing it would be a behaviour change'},
    {'case': 'auth_pair_diverges', 'expect': 'FAILED_CONTROL', 'target': 'auth_pair_identical_across_sides',
     'apply': lambda d: rec(d, 'key-file-underscore-flag', 'rust')['auth']['c103y'].update(
         verdict=ACCEPTED),
     'why': 'the pair claim is that both nodes sign the same set'},
    {'case': 'auth_claim_one_class', 'expect': 'FAILED_CONTROL',
     'target': 'auth_claim_shows_both_classes',
     'apply': lambda d: claims(d, 'key-file-flag-auth').update(
         auth={'c103a': ACCEPTED, 'c103y': ACCEPTED}),
     'why': 'a row that only ever says accepted proves the probe cannot fail'},
    {'case': 'auth_logging_split_averaged', 'expect': 'FAILED_CONTROL',
     'target': 'auth_rejection_logging_pinned',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'rust').update(
         log_masked=rec(d, 'key-file-flag-auth', 'rust')['log_masked'] + AUTH_LOG_NEEDLE),
     'why': 'the oracle logs a rejected request and Rust does not; that split is a finding, not noise'},
    {'case': 'auth_log_line_count_drift', 'expect': 'FAILED_CONTROL',
     'target': 'auth_rejection_logging_pinned',
     'apply': lambda d: rec(d, 'key-file-dash-beats-underscore', 'go').update(
         log_masked=rec(d, 'key-file-dash-beats-underscore', 'go')['log_masked']
         .replace(AUTH_LOG_NEEDLE, '', 1)),
     'why': 'the pin counts one line per probe, so a side that logs only sometimes cannot pass it'},
    {'case': 'listener_claim_rows_collapse', 'expect': 'FAILED_CONTROL',
     'target': 'webtransport_listener_claim_split',
     'apply': lambda d: claims(d, 'wt-cert-flag-without-key').update(udp_has=['<WT1>']),
     'why': 'a refusal row that also claims a held listener collapses the bound/silent split the '
            'other rows depend on'},
    {'case': 'losing_port_bound_anyway', 'expect': 'FAILED_CONTROL',
     'target': 'webtransport_losing_port_never_bound',
     'apply': lambda d: rec(d, 'wt-env-beats-yaml', 'go')['sockets_masked']['tcp'].append('<YRA>'),
     'why': 'the control reads both socket lists, not only the UDP one the leg checks'},
    {'case': 'profile_inventory_short', 'expect': 'FAILED_CONTROL',
     'target': 'profile_inventory_complete',
     'apply': lambda d: d['profiles'].pop(),
     'why': 'a receipt with fewer rows than the lane defines is not the acceptance sweep'},
    {'case': 'row_leaf_unnamed', 'expect': 'FAILED_CONTROL', 'target': 'every_profile_names_a_row',
     'apply': lambda d: en(d, 'wt-flags-valid').update(rows=[]),
     'why': 'the artifact has to name the ledger item it serves'},
    {'case': 'dead_claim_key', 'expect': 'HARNESS_REFUSAL', 'target': 'evaluate',
     'apply': lambda d: claims(d, 'wt-flags-valid').update(auth2={'c103y': ACCEPTED}),
     'why': 'evaluate refuses a claim it does not implement, so no row can carry an unjudged field'},
    {'case': 'store_not_flushed', 'expect': 'FAILED_CONTROL', 'target': 'store_flushed_before_every_launch',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'go')['store'].update(before_dbsize=4),
     'why': 'a profile that starts on a dirty store cannot read the registry effect'},
    {'case': 'refusal_survives_the_wait', 'expect': 'FAILED_CONTROL',
     'target': 'refusal_rows_self_ended',
     'apply': lambda d: rec(d, 'key-file-world-readable', 'go').update(self_exited=False),
     'why': 'a refusal the harness had to stop is not the daemon refusing'},
    {'case': 'sigkill_used', 'expect': 'FAILED_CONTROL', 'target': 'sigkill_never_used',
     'apply': lambda d: rec(d, 'wt-flags-valid', 'go').update(signals=['SIGKILL']),
     'why': 'the lane tears down with SIGINT and says so'},
    {'case': 'refusal_leaves_a_listener', 'expect': 'FAILED_CONTROL',
     'target': 'refusal_leaves_no_listener',
     'apply': lambda d: rec(d, 'key-file-world-readable', 'go')['post_ports'].update(
         http={'tcp': 1, 'udp': 0}),
     'why': 'a refused start must not hold the HTTP port'},
    {'case': 'refusal_writes_keys', 'expect': 'FAILED_CONTROL', 'target': 'refusal_writes_nothing',
     'apply': lambda d: rec(d, 'key-file-world-readable', 'go')['store']['after'].update(
         keys=['livekit_version']),
     'why': 'the key-file refusals are claimed as writing nothing at all, on both sides'},
    {'case': 'refusal_registers_a_node', 'expect': 'FAILED_CONTROL',
     'target': 'refusal_rows_leave_no_node',
     'apply': lambda d: rec(d, 'wt-cert-flag-without-key', 'rust')['store']['after'].update(
         node_ids=['ND_xxxxxxxx']),
     'why': 'the shared half of the refusal guarantee is that no node row is left behind'},
    {'case': 'refusal_marker_split_healed', 'expect': 'FAILED_CONTROL',
     'target': 'webtransport_refusal_marker_split_pinned',
     'apply': lambda d: rec(d, 'wt-empty-material-flags', 'rust')['store']['after'].update(keys=[]),
     'why': 'the marker asymmetry is a measured finding; if the two sides converge the pin reddens'},
    {'case': 'node_row_leaks', 'expect': 'FAILED_CONTROL', 'target': 'node_row_gone_after_exit',
     'apply': lambda d: rec(d, 'wt-flags-valid', 'go')['store']['after'].update(node_ids=['ND_xxxxxxxx']),
     'why': 'deregistration is part of the started-row claim'},
    {'case': 'version_marker_lost', 'expect': 'FAILED_CONTROL',
     'target': 'version_marker_survives_clean_shutdown',
     'apply': lambda d: rec(d, 'wt-flags-valid', 'go')['store']['after'].update(keys=[]),
     'why': 'the lane claims key names, not a count, because the marker outlives the node'},
    {'case': 'never_reaches_readiness', 'expect': 'FAILED_CONTROL', 'target': 'ready_rows_reach_the_daemon',
     'apply': lambda d: rec(d, 'wt-flags-valid', 'rust').update(ready=False),
     'why': 'an auth cell from a daemon that never answered is meaningless'},
    {'case': 'refusal_sentence_pair_split', 'expect': 'FAILED_CONTROL',
     'target': 'refusal_sentence_identical_across_sides',
     # The appended clause keeps every needle leg green - the substring is still there - so only the
     # sentence-parity control can catch a side that decorates the oracle's wording.
     'apply': lambda d: rec(d, 'wt-flag-material-mismatch', 'rust').update(
         log_masked=rec(d, 'wt-flag-material-mismatch', 'rust')['log_masked'].replace(
             KEY_MISMATCH, KEY_MISMATCH + ': keys may not be consistent')),
     'why': 'the whole sentence is the parity claim, not just the substring a needle leg matches'},
    {'case': 'machine_address_leaks', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: rec(d, 'wt-flags-valid', 'go').update(log_masked='seen 10.113.4.21 here'),
     'why': 'an RFC 1918 address in the receipt is the developer host, not fixture vocabulary'},
    {'case': 'host_lan_address_leaks', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'rust').update(log_masked='nodeIP 192.168.9.44'),
     'why': 'the 192.168/16 spelling of a LAN address is equally forbidden'},
    {'case': 'fixture_secret_leaks', 'expect': 'FAILED_CONTROL', 'target': 'fixture_secrets_never_raw',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'go').update(
         log_masked='loaded ' + FILE_A['c103a']),
     'why': 'the signing secret is masked everywhere, including in a fixture'},
    {'case': 'working_path_leaks', 'expect': 'FAILED_CONTROL', 'target': 'no_working_paths_in_profile_records',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'go').update(log_masked=f'read {REPO}/x.yaml'),
     'why': 'a profile record must not carry the repository path'},
    {'case': 'off_limits_named', 'expect': 'FAILED_CONTROL', 'target': 'off_limits_never_named',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'go').update(log_masked='port 7881 in use'),
     'why': 'the lane may not even mention the host server ports'},
    {'case': 'host_store_named', 'expect': 'FAILED_CONTROL', 'target': 'host_store_never_named',
     'apply': lambda d: rec(d, 'key-file-flag-auth', 'rust').update(log_masked='redis 127.0.0.1:6379'),
     'why': 'the developer store is never an input, so it never appears'},
    {'case': 'off_limit_ports_moved', 'expect': 'FAILED_CONTROL', 'target': 'off_limit_ports_untouched',
     'apply': lambda d: d['listeners_bounds'].update({'7881': {'before': 0, 'after': 1}}),
     'why': 'nothing here may take or drop a listener on the host ports'},
    {'case': 'candidate_hash_drift', 'expect': 'FAILED_CONTROL',
     'target': 'recorded_candidate_matches_pinned',
     'apply': lambda d: d['binaries']['rust'].update(sha256='0' * 64),
     'why': 'a receipt that records a different image than the pinned candidate cannot be cited'},
    {'case': 'oracle_hash_drift', 'expect': 'FAILED_CONTROL', 'target': 'recorded_oracle_matches_pinned',
     'apply': lambda d: d['binaries']['go'].update(sha256='0' * 64),
     'why': 'the oracle identity is part of every row in this lane'},
    {'case': 'binary_replaced_on_disk', 'expect': 'FAILED_CONTROL', 'target': 'binaries_unchanged',
     'apply': lambda d: d['binaries']['rust'].update(path='/nonexistent/livecloud'),
     'why': 'the tree may not have moved underneath the receipt'},
    {'case': 'reference_commit_moved', 'expect': 'FAILED_CONTROL', 'target': 'reference_commit_matches',
     'apply': lambda d: d['reference'].update(go_checkout_commit='f' * 40),
     'why': 'the sentences compared here were read from one commit'},
    {'case': 'harness_drift', 'expect': 'FAILED_CONTROL', 'target': 'harness_unchanged_since_run',
     'apply': lambda d: d.update(harness_sha256='0' * 64),
     'why': 're-deriving a verdict against an edited harness is not re-derivation'},
    {'case': 'imported_primitives_drift', 'expect': 'FAILED_CONTROL',
     'target': 'imported_primitives_are_the_accepted_lane',
     'apply': lambda d: d.update(c102_harness_sha256='0' * 64),
     'why': 'this lane borrows the accepted C102 primitives, so it must show which file it borrowed'},
    {'case': 'raw_stream_empty', 'expect': 'FAILED_CONTROL', 'target': 'raw_streams_recorded',
     'apply': lambda d: rec(d, 'wt-flag-material-mismatch', 'go')['raw'].update(bytes=0),
     'why': 'comparing two empty streams is how a refusal row reads as a pass'},
    {'case': 'development_subset', 'expect': 'STATUS_NOT_PASS', 'target': 'status',
     'apply': lambda d: d.update(subset=True),
     'why': 'a --only run scores a subset and is never an acceptance'},
)


# The two re-deriver properties the leg vocabulary depends on are checked directly: a leg must be
# able to say no, and an unknown kind must raise rather than read as a pass.
GATE_INJECTIONS = GATE_INJECTIONS + (
    {'case': 'has_leg_refuses_empty_log', 'expect': 'DIRECT_FALSE', 'target': 'rederive_leg',
     'apply': None, 'direct': lambda: rederive_leg({'leg': 'x', 'kind': 'has', 'needles': ['evidence'],
                                                    'haystack': ''}),
     'why': 'the needle legs are what make a silent daemon a failure, not a success'},
    {'case': 'any_leg_refuses_no_needles', 'expect': 'DIRECT_FALSE', 'target': 'rederive_leg',
     'apply': None, 'direct': lambda: rederive_leg({'leg': 'x', 'kind': 'any', 'needles': [],
                                                    'haystack': 'anything'}),
     'why': 'an empty needle list cannot be satisfied, so it is not counted as a pass'},
    {'case': 'rederive_rejects_unknown_kind', 'expect': 'DIRECT_RAISE', 'target': 'rederive_leg',
     'apply': None, 'direct': lambda: rederive_leg({'leg': 'x', 'kind': 'not-a-kind',
                                                    'actual': 1, 'expect': 1}),
     'why': 'a kind the re-deriver does not know would otherwise return a comparison against None'},
    {'case': 'empty_profile_set', 'expect': 'STATUS_NOT_PASS', 'target': 'status',
     'apply': lambda d: d.update(profiles=[]),
     'why': 'zero rows cannot produce a pass: the inventory control reddens rather than all() over an '
            'empty list reading green'},
    {'case': 'missing_side', 'expect': 'HARNESS_REFUSAL', 'target': 'evaluate',
     'apply': lambda d: en(d, 'wt-flags-valid')['runs'].pop('rust'),
     'why': 'a profile without both sides is not a pair'},
    {'case': 'missing_raw_stream', 'expect': 'HARNESS_REFUSAL', 'target': 'build_provenance_controls',
     'apply': lambda d: en(d, 'wt-flags-valid')['runs']['go'].pop('raw'),
     'why': 'the captured-stream provenance is part of every row'},
)


# ------------------------------------------------------------------------------- gate sweep driver
def selftest_gate(doc, out, receipt=None):
    import copy as _copy
    # The sweep mutates deep copies and calls finish(write=False). Hashing the receipt it read before
    # and after proves the audit is read-only instead of asserting it - C101 filed a gate mode that
    # overwrote the artifact it was auditing.
    before = C102.sha256_file(Path(receipt)) if receipt else None
    baseline = _copy.deepcopy(doc)
    finish(baseline, out=None, live=False, write=False)
    leg_ids = {l['leg'] for l in baseline['legs']}
    control_ids = set(baseline['cross_profile_controls'])
    results = []
    for inj in GATE_INJECTIONS:
        hit, entry_ok, detail = False, True, ''
        if inj['expect'] in ('DIRECT_RAISE', 'DIRECT_FALSE'):
            try:
                value = inj['direct']()
                hit = (inj['expect'] == 'DIRECT_FALSE' and value is False)
                detail = f'returned {value!r}'
            except Exception as exc:  # noqa: BLE001
                hit = inj['expect'] == 'DIRECT_RAISE'
                detail = f'{type(exc).__name__}: {exc}'
            results.append({'case': inj['case'], 'expect': inj['expect'], 'target': inj['target'],
                            'discriminated': hit, 'why': inj['why'], 'detail': detail})
            continue
        if inj['expect'] == 'FAILED_LEG':
            anchor = inj['target'] in leg_ids
        elif inj['expect'] == 'FAILED_CONTROL':
            anchor = inj['target'] in control_ids
        else:
            anchor = True
        if not anchor:
            results.append({'case': inj['case'], 'expect': inj['expect'], 'target': inj['target'],
                            'discriminated': False, 'why': inj['why'],
                            'detail': 'the target does not exist in the unmutated receipt, so this '
                                      'case could only ever appear to pass'})
            continue
        mutated = _copy.deepcopy(doc)
        try:
            inj['apply'](mutated)
            finish(mutated, out=None, live=False, write=False)
        except Exception as exc:  # noqa: BLE001 - a harness refusal is itself a verdict
            entry_ok = False
            detail = f'{type(exc).__name__}: {exc}'
        if inj['expect'] == 'FAILED_LEG':
            hit = inj['target'] in mutated.get('failed_legs', [])
        elif inj['expect'] == 'FAILED_CONTROL':
            hit = inj['target'] in mutated.get('failed_controls', [])
        elif inj['expect'] == 'HARNESS_REFUSAL':
            hit = not entry_ok
        elif inj['expect'] == 'STATUS_NOT_PASS':
            hit = mutated.get('status') != STATUS_PASS
        else:
            raise ValueError(f'unknown expectation {inj["expect"]}')
        results.append({'case': inj['case'], 'expect': inj['expect'], 'target': inj['target'],
                        'discriminated': hit, 'why': inj['why'], 'detail': detail or None,
                        'failed_legs_under_case': (len(mutated.get('failed_legs', []))
                                                   if entry_ok else None),
                        'failed_controls_under_case': (len(mutated.get('failed_controls', []))
                                                       if entry_ok else None)})
    after = C102.sha256_file(Path(receipt)) if receipt else None
    results.append({'case': 'sweep_writes_no_receipt', 'expect': 'RECEIPT_HASH_UNCHANGED',
                    'target': str(receipt) if receipt else None,
                    'discriminated': bool(before) and before == after,
                    'why': 'the audit runs against the filed artifact and may not alter it',
                    'detail': f'{before} -> {after}'})
    report = {'batch': doc['batch'], 'cases': len(results),
              'gate_injections': len(GATE_INJECTIONS),
              'receipt_sha256_before': before, 'receipt_sha256_after': after,
              'baseline_legs': len(leg_ids), 'baseline_controls': len(control_ids),
              'all_discriminated': all(r['discriminated'] for r in results),
              'undiscriminated': [r['case'] for r in results if not r['discriminated']],
              'results': results}
    Path(out).mkdir(parents=True, exist_ok=True)
    (Path(out) / 'gate-falsification.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + '\n')
    return report


# ------------------------------------------------------------------------------- manifest
def git_output(cwd, *args) -> str:
    out = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True)
    return out.stdout.strip()


def new_doc(bins, ctx, harness_sha, c102_sha, bounds, hold, subset) -> dict:
    # Tier 5: both sides are the launched production binaries, paired on identical YAML. Tier 4 would
    # understate it - the C102 sibling lane files the same shape as official_differential_lifecycle.
    return {'schema': SCHEMA, 'batch': BATCH, 'covers': COVERS, 'tier': 5, 'live_run': True,
            'skip_launch': False, 'subset': bool(subset),
            'platform': {'system': sys.platform, 'arch': os.uname().machine,
                         'python': sys.version.split()[0]},
            'binaries': bins,
            'reference': {'go_checkout': 'references/livekit-stable',
                          'go_commit': FIXED_GO_COMMIT,
                          'go_checkout_commit': git_output(REFERENCE, 'rev-parse', 'HEAD'),
                          'upstream_reads': {
                              'pkg/config/config.go:803': 'ToCLIFlagNames dedups only on an exact '
                                                          'name, so --key-file and --key_file coexist',
                              'pkg/config/config.go:940-1035': 'GenerateCLIFlags derives the flag '
                                                               'name from the YAML path and the env '
                                                               'name as LIVEKIT_ + that path',
                              'pkg/config/config.go:1074-1120': 'updateFromCLI runs the generated '
                                                                'loop first and the explicit '
                                                                'IsSet("key-file") branch after it',
                              'pkg/config/config.go:820-855': 'ValidateKeys: 0o007 permission rule, '
                                                              'EOF on an empty file, the sequence '
                                                              'decode error, the short-secret warning',
                              'cmd/server/main.go:52-101': 'the base flag set, including key-file',
                              'pkg/service/webtransport.go': 'the port/cert/key triple consumed at '
                                                             'InitializeServer',
                              'crates/lk-config/src/generated.rs:392': 'LIVEKIT_KEY_FILE on the '
                                                                       'generated side',
                              'crates/lk-config/src/cli.rs:691': 'the base "key-file" spelling',
                              'crates/lk-config/src/keys.rs:83-99': 'the same precedence order in Rust',
                              'crates/livecloud/src/webtransport.rs:105-151': 'build_server_tls reads '
                                                                             'the flag/env material'}},
            'harness': str(HERE / 'run-paired.py'),
            'harness_sha256_at_start': harness_sha, 'harness_sha256': harness_sha,
            'imports_from': str(C102_DIR / 'run-paired.py'),
            'c102_harness_sha256_at_start': c102_sha, 'c102_harness_sha256': c102_sha,
            'imports_reason': 'the launcher, port allocator, socket readers, owned-store helpers and '
                              'the address masker are the accepted C102 primitives; importing (not '
                              'copying) leaves that file byte-identical, so the C102 receipt stays '
                              're-derivable',
            'work_dir': str(ctx['work']),
            'ports': {k[:-5]: v for k, v in ctx.items() if k.endswith('_port')},
            'port_roles': {str(k): v for k, v in ctx['port_roles'].items()},
            'ip_salt': IP_SALT, 'off_limits': list(OFF_LIMITS), 'host_store_port': HOST_STORE_PORT,
            'hold_seconds': hold, 'listeners_bounds': bounds, 'store_flush_ok': True,
            'redis': {'plain_role': 'STORE'},
            'fixtures': {'key_ids': sorted(ALL_KEYS), 'secrets': ['<SEC-A>', '<SEC-B>', '<SEC-N>',
                                                                  '<SEC-Y>'],
                         'cert_subject': CERT_SUBJECT,
                         'cert_a_der_sha256': C102.pem_sha256(Path(ctx['cert_a']['cert'])),
                         'cert_b_der_sha256': C102.pem_sha256(Path(ctx['cert_b']['cert'])),
                         'modes': ctx['modes']},
            'profiles': []}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--report', help='re-derive the verdict from a filed execution.json')
    ap.add_argument('--selftest-gate', help='apply named mutations to a recorded execution.json')
    ap.add_argument('--out', default='/tmp/c103-lane')
    ap.add_argument('--go', default='/tmp/c99-build/livekit-server-go')
    ap.add_argument('--rust', default=str(REPO / 'target/release/livecloud'))
    ap.add_argument('--only', help='comma-separated profile ids; a subset is never an acceptance')
    ap.add_argument('--hold', type=float, default=0.8)
    a = ap.parse_args()
    out = Path(a.out)
    (out / 'logs').mkdir(parents=True, exist_ok=True)
    harness_sha = C102.sha256_file(Path(__file__).resolve())
    c102_sha = C102.sha256_file(C102_DIR / 'run-paired.py')
    if a.report or a.selftest_gate:
        src = Path(a.report or a.selftest_gate)
        if src.is_dir():
            src = src / 'execution.json'
        doc = json.loads(src.read_text())
        if a.report:
            result = finish(doc, out=None, live=False, write=False)
            (out / 'report-rederivation.txt').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps(result['summary'], indent=2))
            return 0 if result['all_ok'] else 1
        report = selftest_gate(doc, out, src)
        print(json.dumps({k: report[k] for k in ('cases', 'baseline_legs', 'baseline_controls',
                                                 'all_discriminated', 'undiscriminated')}, indent=2))
        return 0 if report['all_discriminated'] else 1
    if not a.record:
        ap.error('choose --record, --report or --selftest-gate')
    bins = {}
    for side, path in (('go', a.go), ('rust', a.rust)):
        pth = Path(path)
        if not pth.is_file():
            print(f'missing {side} binary: {pth}', file=sys.stderr)
            return 2
        bins[side] = {'path': str(pth), 'sha256': C102.sha256_file(pth)}
    for side, pinned in (('go', ORACLE_SHA256), ('rust', ACCEPTANCE_SHA256)):
        if bins[side]['sha256'] != pinned:
            print(f'refusing: the {side} image is {bins[side]["sha256"]}, pinned {pinned}',
                  file=sys.stderr)
            return 2
    bounds = {str(port): {'before': C102.listeners_on(port), 'after': None}
              for port in list(OFF_LIMITS) + [HOST_STORE_PORT]}
    stores = {}
    ctx = None
    try:
        ctx = build_ctx(out, a.hold)
        masker = Masker(ctx['port_roles'], {str(ctx['work']): '<WORK>', str(REPO): '<REPO>',
                                           str(Path.home()): '<HOME>'})
        profs = profiles(ctx)
        stores['plain'] = C102.start_store(ctx['store_port'])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and C102.redis_cli(ctx['store_port'], 'PING') != 'PONG':
            time.sleep(0.2)
        if C102.dbsize(ctx['store_port']) != 0:
            print('owned store is not empty at startup', file=sys.stderr)
            return 2
        if a.only:
            wanted = set(a.only.split(','))
            profs = [p for p in profs if p['id'] in wanted]
            print(f'development subset: {sorted(wanted)}', flush=True)
        doc = new_doc(bins, ctx, harness_sha, c102_sha, bounds, a.hold, a.only)
        binaries = {side: Path(info['path']) for side, info in bins.items()}
        for prof in profs:
            doc['profiles'].append(run_entry(ctx, prof, masker, binaries))
            print(f"  ran {prof['id']}", flush=True)
        doc['redis_final'] = {'plain': C102.dbsize(ctx['store_port'])}
        for key in list(bounds):
            bounds[key]['after'] = C102.listeners_on(int(key))
        result = finish(doc, out=out, live=True)
        (out / 'harness-sha256.txt').write_text(harness_sha + '\n')
        for entry in doc['profiles']:
            for side in ('go', 'rust'):
                log = entry['runs'][side].get('log_masked') or ''
                (out / 'logs' / f"{side}-{entry['profile']}.log").write_text(log)
        (out / 'profiles.json').write_text(json.dumps(
            [{'profile': e['profile'], 'rows': e['rows'], 'claims': e['claims'], 'note': e['note'],
              'expect_exit': e['expect_exit'], 'refuse_wait': e['refuse_wait'],
              'probe_kids': e['probe_kids'], 'probe_ports': e['probe_ports'],
              'match': e['match'], 'problems': e['problems'],
              'argv': [r['argv_masked'] for r in (e['runs']['go'], e['runs']['rust'])],
              'auth': {side: {kid: cell['verdict'] for kid, cell in
                              (e['runs'][side].get('auth') or {}).items()}
                       for side in ('go', 'rust')}}
             for e in doc['profiles']], indent=2, ensure_ascii=False, sort_keys=True) + '\n')
        print(json.dumps(result['summary'], indent=2))
        return 0 if result['all_ok'] else 1
    finally:
        for proc in stores.values():
            try:
                C102.stop_store(proc, 0)
            except Exception:  # noqa: BLE001
                pass
        if ctx is not None and C102.listeners_on(ctx['store_port']):
            print('warning: the owned store port still has a listener', file=sys.stderr)


if __name__ == '__main__':
    raise SystemExit(main())
