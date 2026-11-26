#!/usr/bin/env python3
"""SPEC-0077 C101: paired CLI surfaces — built-in flags and commands on both binaries.

Ledger rows driven here:

    builtin_flags:--help              references/livekit-stable cmd/server/main.go:139 (urfave/cli v3
    builtin_flags:--version            auto-added help/version, command_setup.go:241-266)
    commands:help                     urfave/cli v3 built-in `help` (help.go:83-143)
    commands:help-verbose             cmd/server/commands.go:90-102
    commands:ports                    cmd/server/commands.go:49-88
    commands:generate-keys            cmd/server/commands.go:41-47
    commands:create-join-token        cmd/server/main.go:154-176, commands.go:104-167
    commands:list-nodes               cmd/server/main.go:178-186, commands.go:170-271

Why a pair and not a parser test: every one of these surfaces is reached through the real `main()`
of each product, so the claim is about the shipped binary — which stream a refusal lands on, what
exit code the process ends with, and whether the text agrees once the parts that cannot agree
(program name, node id, clock, random key material, the developer host's own address) are masked.
Comparisons run on unpiped stdout and stderr with the original exit codes preserved.

Four things this lane refuses to do:

  * It never compares the *order* of `help-verbose` rows as a parity claim without first showing
    the oracle is stable. Measured: the official binary reorders its own generated-flag block
    between runs, so the harness runs the oracle three times, records `oracle_order_unstable`, and
    only compares order when the oracle's own order is invariant. What is always compared is the
    row set and each row's type/default/env spelling.
  * It never compares a `generate-keys` or `create-join-token` value byte for byte. The key is
    random and the JWT carries iat/nbf/exp, so the row asserts the *shape*, that two runs differ,
    that the printed pair verifies against itself, and the load-bearing leg: a token minted by one
    implementation's CLI authenticates against a daemon launched by the other (`/rtc/validate`
    answers 200 `success`), with a wrong-secret mint answering 401 in identical wording on both
    sides so the 200 cannot come from an auth bypass.
  * It never lets masking turn an empty comparison into a pass. Each profile declares which
    volatile tokens must have appeared; the mask must have matched them at run time **and** its
    placeholder must be present in the stored stream, so `--report` can reproduce the claim offline
    instead of comparing two already-empty strings.
  * It never stores a machine address. The host's LAN address is replaced with a keyed tag
    (`<IP#a1b2c3d4>`), so the table still proves both sides printed *the same* address while the
    receipt proves nothing about which address it was. A whole-receipt sweep through the shared
    classifier backs the claim.

Modes: default launches both binaries; `--report` re-derives a verdict from a recorded
execution.json without launching; `--selftest-gate` re-derives it under named mutations and
requires each to redden the check it targets.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
import importlib.util
import json
import os
import pathlib
import re
import signal
import socket
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
FIXED_GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
FIXED_GO_COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
OFF_LIMITS = (7881, 7882)      # the developer host's own server: never bound, never named in argv
IP_SALT = 'c101-cli'           # committed; makes <IP#…> tags reproducible without the address
GO_NAMES = ('livekit-server',)
RUST_NAMES = ('livecloud', 'livekit-server')
COVERS = [
    'builtin_flags:--help', 'builtin_flags:--version', 'commands:help',
    'commands:help-verbose', 'commands:ports', 'commands:generate-keys',
    'commands:create-join-token', 'commands:list-nodes',
]
KEY = 'APIC101PAIR'
SECRET = 'c101secretc101secretc101secretc101'
WRONG_SECRET = 'c101wrongsecretc101wrongsecretc101!'

sys.path.insert(0, str(REPO / 'tests' / 'interop'))
_spec = importlib.util.spec_from_file_location(
    'redact_evidence', REPO / 'tests' / 'interop' / 'redact-evidence.py')
redact = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(redact)      # same classifier the filing step uses


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def h8(text):
    return hashlib.sha256(text.encode()).hexdigest()[:12]


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def listeners(ports):
    out = {}
    for port in ports:
        r = subprocess.run(['lsof', '-nP', f'-iTCP:{port}', '-sTCP:LISTEN'],
                           capture_output=True, text=True)
        out[str(port)] = max(0, r.stdout.count('\n') - 1)
    return out


# --------------------------------------------------------------------- masking
# Each rule is (name, regex, replacement, placeholder). `volatile` names declared by a profile must
# have matched at least once and must leave their placeholder in the stored stream.
def ip_tag(addr):
    """Keyed and letter-only: two addresses keep distinct tags through the table digit collapse, the
    same address tags the same way on both sides, and the address itself never reaches the receipt."""
    digest = hashlib.sha256((IP_SALT + addr).encode()).digest()
    return 'IP#' + ''.join(chr(65 + b % 26) for b in digest[:8])


MASKS = (
    ('zap_console_header',
     re.compile(r'^\d{4}-\d{2}-\d{2}T[\d:.]+[+-]\d{4}\t[A-Z]+\tlivekit\t[\w./-]+:\d+\t', re.M),
     '<LOGHDR>\t', '<LOGHDR>'),
    ('node_id', re.compile(r'\bND_[0-9A-Za-z]{8,16}\b'), '<NODE_ID>', '<NODE_ID>'),
    ('table_time', re.compile(r'\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b'
                              r'|\b\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\b'), '<TS>', '<TS>'),
    ('api_key', re.compile(r'\bAPI[0-9A-Za-z]{8,16}\b'), '<KEY>', '<KEY>'),
    ('jwt', re.compile(r'\b[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b'),
     '<JWT>', '<JWT>'),
    ('pid', re.compile(r'\bpid[=:] ?\d{2,7}\b'), '<PID>', '<PID>'),
    ('ipv4', re.compile(r'(?<![\w.])(\d{1,3}(?:\.\d{1,3}){3})(?![\w.])'), None, '<IP#'),
    ('ipv6', re.compile(r'(?<![\w:])((?:[0-9A-Fa-f]{1,4}:){1,7}[0-9A-Fa-f]{1,4}(?:%\w+)?)'),
     None, '<IP#'),
)
IPV4_RE, IPV6_RE = MASKS[6][1], MASKS[7][1]
# LiveKit's node table refreshes CPU%/memory/load every few seconds, so two CLI reads of the *same*
# registry can print different digits. Inside table rows only digits are volatile: masking them keeps
# the row, column and label comparison meaningful instead of flaky.
TABLE_ROW = re.compile(r'(?m)^(.*│.*)$')
DIGITS = re.compile(r'\d+(?:\.\d+)?')
NODE_TOKEN = re.compile(r'\bND_[0-9A-Za-z]{8,16}\b')


def normalize_table(text, keep_ids=False):
    """Collapse metric digits inside table rows; node ids survive when the row needs them.

    Without the protection a digit inside `ND_…` is collapsed too, and the id can no longer be
    tied back to the daemon that printed it.
    """
    if not keep_ids:
        return TABLE_ROW.sub(lambda m: DIGITS.sub('#', m.group(1)), text)

    def sub_row(m):
        row = m.group(1)
        spans = [x.span() for x in NODE_TOKEN.finditer(row)]
        pieces, pos = [], 0
        for hit in DIGITS.finditer(row):
            if any(s <= hit.start() < e for s, e in spans):
                continue
            pieces.append(row[pos:hit.start()])
            pieces.append('#')
            pos = hit.end()
        pieces.append(row[pos:])
        return ''.join(pieces)

    return TABLE_ROW.sub(sub_row, text)


def mask(text, secrets=(), tables=False, keep_node_ids=False):
    """Apply the vocabulary and report what each rule actually matched.

    `keep_node_ids` is what lets the list-nodes rows stay checkable: the row count and the tie to
    the daemon's own `starting LiveKit server` line both need the id, and a node id is an ephemeral
    per-process label, not a credential. Addresses are still replaced by their keyed tags.
    """
    counts = {}
    for name, regex, repl, placeholder in MASKS:
        if name == 'node_id' and keep_node_ids:
            counts[name] = len(regex.findall(text))
            continue
        if name in ('ipv4', 'ipv6'):
            hits = set()

            def sub(m, hits=hits):
                addr = m.group(1)
                if redact.classify(addr) == 'mask':
                    hits.add(addr)
                    return f'<{ip_tag(addr)}>'
                return addr

            text = regex.sub(sub, text)
            counts[name] = len(hits)
            continue
        found = regex.findall(text)
        text = regex.sub(repl, text)
        counts[name] = len(found)
    for secret in secrets:
        if secret and secret in text:
            text = text.replace(secret, '<SECRET>')
            counts['printed_secret'] = counts.get('printed_secret', 0) + 1
    if tables:
        text = normalize_table(text, keep_node_ids)
        counts['table_digits'] = 1
    return text, counts


def nameless(text):
    """Collapse the product-name difference so the rest of the stream can be compared exactly.

    The name delta is product identity, not behaviour: it is recorded per profile (`name_delta`)
    rather than silently absorbed here.
    """
    for name in RUST_NAMES + GO_NAMES:
        text = re.sub(rf'\b{re.escape(name)}\b', '<NAME>', text)
    return text


# --------------------------------------------------------------------- jwt
def b64u(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def unb64u(text):
    return base64.urlsafe_b64decode(text + '=' * (-len(text) % 4))


def split_jwt(token):
    parts = token.split('.')
    if len(parts) != 3:
        return None
    try:
        header = json.loads(unb64u(parts[0]))
        payload = json.loads(unb64u(parts[1]))
    except Exception:
        return None
    return {'header': header, 'payload': payload, 'sig': parts[2],
            'signing': f'{parts[0]}.{parts[1]}'.encode()}


def verify_jwt(token, secret):
    parsed = split_jwt(token)
    if parsed is None:
        return {'parses': False}
    want = b64u(hmac.new(secret.encode(), parsed['signing'], hashlib.sha256).digest())
    return {'parses': True, 'alg': parsed['header'].get('alg'),
            'signature_verifies': hmac.compare_digest(want, parsed['sig']),
            'claims': sorted(parsed['payload']), 'header_keys': list(parsed['header']),
            'payload_order': list(parsed['payload'])}


def admin_token(secret, room='c101-probe'):
    """A harness-minted token with the admin grants ListRooms needs. Used only as the API-shape
    control; the cross-implementation claim rides on the CLI-minted join token."""
    now = int(time.time())
    part = lambda o: b64u(json.dumps(o, separators=(',', ':')).encode())
    head = part({'alg': 'HS256', 'typ': 'JWT'})
    body = part({'iss': KEY, 'sub': 'c101-observer', 'nbf': now - 5, 'exp': now + 600,
                 'video': {'room': room, 'roomJoin': True, 'roomAdmin': True, 'roomList': True}})
    return f'{head}.{body}.' + b64u(hmac.new(secret.encode(), f'{head}.{body}'.encode(),
                                             hashlib.sha256).digest())


# --------------------------------------------------------------------- capture
def run_one(binary, argv, env=None, timeout=45):
    """Spawn one CLI invocation with the streams kept separate and the exit code preserved."""
    with open(os.devnull, 'rb') as devnull:
        proc = subprocess.Popen([str(binary)] + [str(a) for a in argv], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, stdin=devnull,
                                env={**os.environ, **(env or {})}, start_new_session=True)
        timed_out, started = False, time.monotonic()
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, signal.SIGKILL)
            out, err = proc.communicate()
    decode = lambda b: b.decode('utf-8', 'replace')
    raw_out, raw_err = decode(out), decode(err)
    return {'exit': proc.returncode, 'raw_stdout': raw_out, 'raw_stderr': raw_err,
            'stdout_bytes_raw': len(out), 'stderr_bytes_raw': len(err),
            'stdout_sha256_raw': hashlib.sha256(out).hexdigest(),
            'stderr_sha256_raw': hashlib.sha256(err).hexdigest(),
            'elapsed_ms': int((time.monotonic() - started) * 1000), 'timed_out': timed_out,
            'streams': [n for n, b in (('stdout', out), ('stderr', err)) if decode(b).strip()]}


def profile_argv(prof, ctx):
    """Expand the argv template against this run's fixtures (paths, ports, key material)."""
    return [str(ctx.get(tok[1:-1], tok) if tok.startswith('{') and tok.endswith('}') else tok)
            for tok in prof['argv']]


def argv_for_record(argv):
    """The receipt may show the shape of the invocation but never the fixture secret."""
    return [a.replace(SECRET, '<SECRET>').replace(WRONG_SECRET, '<SECRET>') for a in argv]
# --------------------------------------------------------------------- profiles
# `exit`/`streams`/`needles` state the contract the row owes; a measured disagreement with the
# declaration is a finding, not a normalization. Every value below was read off the fixed oracle
# binary before being written here (the scratch sweep is described in README.md), and `divergence`
# rows name the side whose behaviour is the exception.
REFUSAL_NO_KEYS = 'one of key-file or keys must be provided'
HELP_SECTIONS = ['NAME:', 'USAGE:', 'COMMANDS:', 'GLOBAL OPTIONS:']

PROFILES = [
    {'id': 'help-root-flag', 'row': 'builtin_flags:--help', 'kind': 'text', 'phase': 'cli',
     'argv': ['--help'], 'exit': 0, 'streams': ('stdout',), 'needles': HELP_SECTIONS,
     'forbidden': ['generated (default:'], 'volatile': []},
    {'id': 'help-short-flag', 'row': 'builtin_flags:--help', 'kind': 'text', 'phase': 'cli',
     'argv': ['-h'], 'exit': 0, 'streams': ('stdout',), 'needles': HELP_SECTIONS,
     'forbidden': ['generated (default:'], 'volatile': []},
    {'id': 'help-command', 'row': 'commands:help', 'kind': 'text', 'phase': 'cli',
     'argv': ['help'], 'exit': 0, 'streams': ('stdout',), 'needles': HELP_SECTIONS,
     'forbidden': ['generated (default:'], 'volatile': []},
    {'id': 'help-topic-ports', 'row': 'commands:help', 'kind': 'text', 'phase': 'cli',
     'argv': ['help', 'ports'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['NAME:', 'ports -'], 'forbidden': [], 'volatile': [],
     'note': 'the built-in help action prints the subcommand help; no startup config read happens'},
    {'id': 'help-flag-topic', 'row': 'commands:help', 'kind': 'text', 'phase': 'cli',
     'argv': ['--help', 'ports'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['NAME:', 'ports -'], 'forbidden': [], 'volatile': [],
     'note': 'the flag spelling of the same action, measured to agree with `help ports`'},
    {'id': 'help-topic-unknown', 'row': 'commands:help', 'kind': 'text', 'phase': 'cli',
     'argv': ['help', 'no-such-topic'], 'exit': 3, 'streams': ('stderr',),
     'needles': ["No help topic for 'no-such-topic'"], 'forbidden': [], 'volatile': [],
     'note': 'urfave/cli v3 returns 3 for an unknown help topic; stdout stays empty'},
    {'id': 'help-dash-verbose', 'row': 'commands:help-verbose', 'kind': 'text', 'phase': 'cli',
     'argv': ['help', '--verbose'], 'exit': 1, 'streams': ('stdout', 'stderr'),
     'needles': ['flag provided but not defined: -verbose', 'Incorrect Usage:'],
     'forbidden': [], 'volatile': [],
     'note': 'the row is a refusal on both sides: `help-verbose` is a subcommand, not a help flag'},
    {'id': 'help-verbose', 'row': 'commands:help-verbose', 'kind': 'verbose', 'phase': 'cli',
     'argv': ['help-verbose'], 'exit': 0, 'streams': ('stdout',),
     'needles': HELP_SECTIONS, 'forbidden': [], 'volatile': [], 'repeat': {'go': 3, 'rust': 1}},
    {'id': 'help-verbose-with-persistent-flag', 'row': 'commands:help-verbose', 'kind': 'verbose',
     'phase': 'cli', 'argv': ['help-verbose', '--dev'], 'exit': 0, 'streams': ('stdout',),
     'needles': HELP_SECTIONS, 'forbidden': [], 'volatile': [], 'repeat': {'go': 1, 'rust': 1},
     'note': 'a root flag placed after the subcommand still parses'},
    {'id': 'unknown-command-needs-keys', 'row': 'commands:help', 'kind': 'text', 'phase': 'cli',
     'argv': ['no-such-command'], 'exit': 1, 'streams': ('stdout',),
     'needles': [REFUSAL_NO_KEYS], 'forbidden': [], 'volatile': [],
     'note': 'the app-level Before hook runs before command dispatch, so an unknown command is '
             'refused for missing keys rather than for being unknown'},
    {'id': 'unknown-root-flag', 'row': 'builtin_flags:--help', 'kind': 'text', 'phase': 'cli',
     'argv': ['--no-such-flag'], 'exit': 1, 'streams': ('stdout', 'stderr'),
     'needles': ['Incorrect Usage: flag provided but not defined: -no-such-flag'] + HELP_SECTIONS,
     'forbidden': [], 'volatile': [],
     'note': 'the refusal lands on stderr while the usage dump lands on stdout'},
    {'id': 'version-long', 'row': 'builtin_flags:--version', 'kind': 'text', 'phase': 'cli',
     'argv': ['--version'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['version 1.13.10'], 'forbidden': [], 'volatile': [], 'exact': True},
    {'id': 'version-short', 'row': 'builtin_flags:--version', 'kind': 'text', 'phase': 'cli',
     'argv': ['-v'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['version 1.13.10'], 'forbidden': [], 'volatile': [], 'exact': True},
    {'id': 'version-alias-binary', 'row': 'builtin_flags:--version', 'kind': 'text',
     'phase': 'cli', 'argv': ['--version'], 'exit': 0, 'streams': ('stdout',),
     'rust_binary': 'alias', 'needles': ['livekit-server version 1.13.10'], 'forbidden': [],
     'volatile': [], 'exact': True, 'no_normalize': True,
     'note': 'the Rust tree ships a livekit-server-named alias binary; run through it the version '
             'line needs no normalization at all'},
    {'id': 'ports-no-config', 'row': 'commands:ports', 'kind': 'text', 'phase': 'cli',
     'argv': ['ports'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['TCP Ports', '7880', '7881', '50000-60000'], 'forbidden': [REFUSAL_NO_KEYS],
     'volatile': [], 'note': 'ports does not require keys, and prints the built-in defaults'},
    {'id': 'ports-config-before', 'row': 'commands:ports', 'kind': 'text', 'phase': 'cli',
     'argv': ['--config', '{cfg_ports}', 'ports'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['55999'], 'forbidden': ['7880'], 'volatile': [],
     'note': 'the config replaces the HTTP port in the printed table'},
    {'id': 'ports-config-after', 'row': 'commands:ports', 'kind': 'text', 'phase': 'cli',
     'argv': ['ports', '--config', '{cfg_ports}'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['55999'], 'forbidden': ['7880'], 'volatile': [],
     'note': 'a persistent root flag after the subcommand still reaches the config read'},
    {'id': 'ports-key-file', 'row': 'commands:ports', 'kind': 'text', 'phase': 'cli',
     'argv': ['--key-file', '{kf_yaml}', 'ports'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['7880'], 'forbidden': [], 'volatile': []},
    {'id': 'ports-key-file-b64', 'row': 'commands:ports', 'kind': 'text', 'phase': 'cli',
     'argv': ['--key-file', '{kf_b64}', 'ports'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['7880'], 'forbidden': [], 'volatile': [],
     'note': 'a base64-encoded JSON key file is accepted by the config loader'},
    {'id': 'generate-keys', 'row': 'commands:generate-keys', 'kind': 'keys', 'phase': 'cli',
     'argv': ['generate-keys'], 'exit': 0, 'streams': ('stdout',),
     'needles': ['API Key:', 'API Secret:'], 'forbidden': [],
     'volatile': ['api_key', 'generated_secret'], 'repeat': {'go': 2, 'rust': 2},
     'mask_generated': True},
    {'id': 'token-with-keys', 'row': 'commands:create-join-token', 'kind': 'token', 'phase': 'cli',
     'argv': ['--keys', '{keypair}', 'create-join-token', '--room', '{room}',
              '--identity', 'alice'],
     'exit': 0, 'streams': ('stdout',), 'needles': ['Token: '], 'forbidden': [],
     'volatile': ['jwt']},
    {'id': 'token-recorder', 'row': 'commands:create-join-token', 'kind': 'token', 'phase': 'cli',
     'argv': ['--keys', '{keypair}', 'create-join-token', '--room', '{room}',
              '--identity', 'rec', '--recorder'],
     'exit': 0, 'streams': ('stdout',), 'needles': ['Token: '], 'forbidden': [],
     'volatile': ['jwt'], 'recorder': True},
    {'id': 'token-from-config', 'row': 'commands:create-join-token', 'kind': 'token',
     'phase': 'cli', 'argv': ['--config', '{cfg_keys}', 'create-join-token', '--room', '{room}',
                              '--identity', 'alice'],
     'exit': 0, 'streams': ('stdout',), 'needles': ['Token: '], 'forbidden': [],
     'volatile': ['jwt']},
    {'id': 'token-no-keys', 'row': 'commands:create-join-token', 'kind': 'text', 'phase': 'cli',
     'argv': ['create-join-token', '--room', '{room}', '--identity', 'alice'],
     'exit': 1, 'streams': ('stdout',), 'needles': ['stat : no such file or directory'],
     'forbidden': ['Token: '], 'volatile': [],
     'note': 'the oracle stats the empty KeyFile before refusing, so the message is the stat '
             'error and not the keys refusal; the exit code is what is shared'},
    {'id': 'token-missing-identity', 'row': 'commands:create-join-token', 'kind': 'text',
     'phase': 'cli', 'argv': ['--keys', '{keypair}', 'create-join-token', '--room', '{room}'],
     'exit': 1, 'streams': ('stdout', 'stderr'),
     'needles': ['Required flag "identity" not set', 'NAME:'], 'forbidden': [],
     'volatile': []},
    {'id': 'token-unknown-flag', 'row': 'commands:create-join-token', 'kind': 'text',
     'phase': 'cli', 'argv': ['--keys', '{keypair}', 'create-join-token', '--room', '{room}',
                              '--identity', 'a', '--valid-for', '1h'],
     'exit': 1, 'streams': ('stdout', 'stderr'),
     'needles': ['flag provided but not defined: -valid-for', 'Incorrect Usage:'],
     'forbidden': [], 'volatile': [],
     'note': 'create-join-token has no validity flag on either side; the token is fixed at 30 days'},
    {'id': 'token-key-file-b64', 'row': 'commands:create-join-token', 'kind': 'text',
     'phase': 'cli', 'argv': ['--key-file', '{kf_b64}', 'create-join-token', '--room', '{room}',
                              '--identity', 'alice'],
     'exit': 1, 'streams': ('stdout',),
     'needles': ['yaml: unmarshal errors:', 'cannot unmarshal !!str'], 'forbidden': ['Token: '],
     'volatile': [],
     'note': 'the base64 spelling is a config-loader feature, not a key-file feature: the token '
             'command reads the file as YAML on both sides'},
    {'id': 'token-key-file-yaml', 'row': 'commands:create-join-token', 'kind': 'divergence',
     'phase': 'cli', 'argv': ['--key-file', '{kf_yaml}', 'create-join-token', '--room', '{room}',
                              '--identity', 'alice'],
     'streams': None, 'needles': [], 'forbidden': [], 'volatile': [],
     'rust_volatile': ['jwt'],
     'divergence': 'the oracle panics (reflect.Value.Set on the nil Keys map inside createToken) '
                   'while the Rust binary mints the token; adopting the panic would ship a crash, '
                   'so the row pins both sides exactly and is filed as a scoped divergence',
     'go_exit': 2, 'rust_exit': 0,
     'go_needles': ['panic: reflect: reflect.Value.Set using unaddressable value'],
     'rust_needles': ['Token: ']},
    {'id': 'list-nodes-empty-registry', 'row': 'commands:list-nodes', 'kind': 'nodes',
     'phase': 'nodes_empty', 'argv': ['--config', '{cfg_redis}', 'list-nodes'],
     'exit': 0, 'streams': ('stdout', 'stderr'), 'needles': ['ID', 'IP Address'],
     'forbidden': [], 'volatile': ['zap_console_header'], 'min_rows': 0,
     'note': 'the empty-store shape: the header renders and no data row exists, which is what '
             'makes the registered row below a change rather than a constant'},
    {'id': 'list-nodes-host-default', 'row': 'commands:list-nodes', 'kind': 'nodes',
     'phase': 'cli', 'argv': ['list-nodes'], 'exit': None, 'streams': None,
     'compare_streams': ('stdout',), 'needles': [], 'forbidden': [],
     'volatile': [], 'min_rows': None, 'digits': True, 'scoped': True,
     'note': 'no --config means the built-in 127.0.0.1:6379, i.e. whatever the developer host '
             'already runs. The row is scoped: exit parity, the table header/columns and the '
             'reachability of the default address are claimed, while the registry content (the '
             'host\'s own nodes, and the oracle\'s single-node-routing stderr notice) is only '
             'recorded, because it is not this lane\'s fixture'},
    {'id': 'list-nodes-registered', 'row': 'commands:list-nodes', 'kind': 'nodes',
     'phase': 'nodes_registered', 'argv': ['--config', '{cfg_redis}', 'list-nodes'],
     'exit': 0, 'streams': ('stdout', 'stderr'), 'needles': ['ID', 'IP Address', '(SERVING)'],
     'forbidden': [], 'volatile': ['table_time', 'zap_console_header'],
     'min_rows': 2, 'digits': True,
     'note': 'both daemons are registered in the owned store; each CLI must show both node ids'},
    {'id': 'list-nodes-after-exit', 'row': 'commands:list-nodes', 'kind': 'nodes',
     'phase': 'after_exit', 'argv': ['--config', '{cfg_redis}', 'list-nodes'],
     'exit': 0, 'streams': ('stdout', 'stderr'), 'needles': ['ID'], 'forbidden': ['(SERVING)'],
     'volatile': ['zap_console_header'], 'min_rows': 0,
     'note': 'the registry is empty again once both daemons exited'},
    {'id': 'list-nodes-closed-redis', 'row': 'commands:list-nodes', 'kind': 'text',
     'phase': 'cli', 'argv': ['--config', '{cfg_closed}', 'list-nodes'], 'exit': 1,
     'streams': ('stdout', 'stderr'),
     'needles': ['unable to connect to redis', 'connect: connection refused'],
     'forbidden': [], 'volatile': ['table_time'],
     'note': 'the five-attempt dial loop is logged by both sides; ~1.8 s, not a hang'},
]
PROFILES_BY_ID = {p['id']: p for p in PROFILES}
PLACEHOLDER_FOR = {
    'zap_console_header': '<LOGHDR>', 'node_id': '<NODE_ID>', 'table_time': '<TS>',
    'api_key': '<KEY>', 'jwt': '<JWT>', 'pid': '<PID>', 'ipv4': '<IP#', 'ipv6': '<IP#',
    'generated_secret': '<GEN_SECRET>', 'table_digits': '#', 'printed_secret': '<SECRET>',
}


# --------------------------------------------------------------------- comparators
FLAG_ROW_HEAD = re.compile(r'^\s+(--\S+)')
FLAG_ROW_ENV = re.compile(r'\[\$([A-Za-z0-9_]+)\]\s*$')
GEN_SECRET_RE = re.compile(r'(?m)^(API Secret:\s+)([0-9A-Za-z]{40,48})\s*$')


def verbose_rows(text):
    """Parse the generated-flag block of a urfave/cli v3 help dump.

    Rows come in three shapes: `--flag type  desc [$ENV]`, `--flag type [ --flag type ]  desc`
    (a repeatable flag) and `--flag type  desc` with no env token. Measured 296 rows, 6 of them
    without an env suffix, which is why this is a parser and not one regex.
    """
    rows = {}
    for line in text.splitlines():
        m = FLAG_ROW_HEAD.match(line)
        if not m:
            continue
        env = ''
        em = FLAG_ROW_ENV.search(line)
        head = line
        if em:
            env = '$' + em.group(1)
            head = line[:em.start()]
        toks = head.split()
        flag = toks[0]
        type_token = toks[1] if len(toks) > 1 and not toks[1].startswith('[') else ''
        rest = toks[2:] if type_token else toks[1:]
        rows[flag] = [type_token, ' '.join(rest), env]
    return rows


def token_from(stdout):
    for line in stdout.splitlines():
        if line.startswith('Token: '):
            return line[len('Token: '):].strip()
    return None


def keys_from(text):
    key = secret = None
    for line in text.splitlines():
        if line.startswith('API Key:'):
            key = line.split(':', 1)[1].strip()
        elif line.startswith('API Secret:'):
            secret = line.split(':', 1)[1].strip()
    return key, secret


def mask_generated_secret(rec):
    """The printed secret must not reach the receipt, but its absence has to stay checkable."""
    found = []
    for stream in ('raw_stdout', 'raw_stderr'):
        def sub(m):
            found.append(m.group(2))
            return m.group(1) + '<GEN_SECRET>'
        rec[stream] = GEN_SECRET_RE.sub(sub, rec[stream])
    rec['generated_secrets_len'] = sorted({len(s) for s in found})
    rec['generated_secret_tags'] = sorted({h8(s) for s in found})
    return rec


def sanitize(rec, prof):
    """Turn a raw capture into the stored form: masked, with the volatile evidence recorded and the
    kind-specific reading taken from the raw text before it is thrown away."""
    rec['generated_secrets_len'] = []
    rec['generated_secret_tags'] = []
    if prof.get('mask_generated'):
        mask_generated_secret(rec)
    secrets = [SECRET, WRONG_SECRET]
    keep_ids = prof['kind'] == 'nodes'
    out, counts = mask(rec['raw_stdout'], secrets, prof.get('digits', False), keep_ids)
    err, counts2 = mask(rec['raw_stderr'], secrets, prof.get('digits', False), keep_ids)
    for key, value in counts2.items():
        counts[key] = counts.get(key, 0) + value
    counts['generated_secret'] = counts.get('generated_secret', 0) + len(rec['generated_secret_tags'])
    rec['stdout'] = out
    rec['stderr'] = err
    rec['mask_counts'] = counts
    rec['streams_masked'] = [n for n in ('stdout', 'stderr') if rec[n].strip()]
    if prof['kind'] == 'keys':
        printed_key, printed_secret = keys_from(rec['raw_stdout'])
        rec['parsed_keys'] = [{'key': printed_key,
                               'secret_placeholder': printed_secret == '<GEN_SECRET>'}]
    if prof['kind'] == 'token':
        token = token_from(rec['raw_stdout'])
        if token:
            info = verify_jwt(token, SECRET)
            payload = split_jwt(token)['payload']
            video = payload.get('video') or {}
            rec['token_info'] = {'alg': info['alg'], 'claims': info['claims'],
                                 'header_keys': info['header_keys'],
                                 'payload_order': info['payload_order'],
                                 'signature_verifies': info['signature_verifies'],
                                 'sig_length': len(token.split('.')[2]),
                                 'iss_matches_fixture': payload.get('iss') == KEY,
                                 'room_claim': video.get('room'),
                                 'recorder': bool(video.get('recorder')),
                                 'hidden': bool(video.get('hidden')),
                                 'can_publish': video.get('canPublish'),
                                 'valid_for_days': round(
                                     (payload.get('exp', 0) - payload.get('nbf', 0)) / 86400, 2)}
    if prof['kind'] == 'verbose':
        rec['rows'] = verbose_rows(rec['raw_stdout'])
        # The full parsed order, not a sample: capping it makes two runs of the same binary look
        # like they disagree on the flag *set* when only the first N rows were sampled.
        rec['flag_order'] = list(rec['rows'])
        lines = [nameless(l) for l in rec['stdout'].splitlines() if l.strip()]
        rec['lines_sha256'] = hashlib.sha256(
            '\n'.join(sorted(lines)).encode()).hexdigest()
        rec['line_count'] = len(lines)
    return rec


def stream_text(rec, stream, prof):
    """What a comparison is allowed to look at: the masked stream, optionally name-collapsed."""
    text = rec[stream]
    return text if prof.get('no_normalize') else nameless(text)


def base_problems(prof, g, r):
    """Checks every paired row owes: declared exit, stream split, needles, forbidden text, the
    masked agreement, and the volatile-mask evidence that keeps the agreement meaningful."""
    problems = []
    exits = {'go': prof.get('go_exit', prof.get('exit')),
             'rust': prof.get('rust_exit', prof.get('exit'))}
    for side, rec in (('go', g), ('rust', r)):
        if exits[side] is not None and rec['exit'] != exits[side]:
            problems.append(f'declared_exit_{side}')
        if prof.get('streams') is not None:
            if tuple(rec['streams']) != tuple(prof['streams']):
                problems.append(f'stream_split_{side}')
        if rec['timed_out']:
            problems.append(f'timed_out_{side}')
        raw_streams = rec['stdout'] + '\n' + rec['stderr']
        joined = raw_streams if prof.get('no_normalize') else nameless(raw_streams)
        needles = prof['needles'] + prof.get(f'{side}_needles', [])
        for needle in needles:
            if needle not in joined:
                problems.append(f'needle_{side}:{needle[:24]}')
        for bad in prof['forbidden']:
            if bad in joined:
                problems.append(f'forbidden_{side}:{bad[:16]}')
        for name in prof['volatile'] + prof.get(f'{side}_volatile', []):
            if not rec['mask_counts'].get(name):
                problems.append(f'volatile_never_matched_{side}:{name}')
                continue
            ph = PLACEHOLDER_FOR[name]
            if ph not in rec['stdout'] and ph not in rec['stderr']:
                problems.append(f'volatile_placeholder_missing_{side}:{name}')
    if prof['kind'] != 'divergence' and g['exit'] != r['exit']:
        problems.append('exit_split')
    if prof.get('scoped'):
        if not (g['stdout'].strip() or g['stderr'].strip()
                or r['stdout'].strip() or r['stderr'].strip()):
            problems.append('vacuous_scoped_row_empty')
    if prof['kind'] not in ('verbose', 'keys', 'divergence') and not prof.get('scoped'):
        for stream in prof.get('compare_streams') or ('stdout', 'stderr'):
            if stream_text(g, stream, prof) != stream_text(r, stream, prof):
                problems.append(f'{stream}_differs')
        cmp_streams = prof.get('compare_streams') or ('stdout', 'stderr')
        if not any(stream_text(x, s, prof).strip() for x in (g, r) for s in cmp_streams):
            problems.append('vacuous_both_streams_empty')
    return problems


def table_view(text):
    """The list-nodes table reduced to what can be compared: ids, address cells, labels, shape.

    The CLI wraps wide tables, so a node occupies several `│` lines and only one of them carries the
    id. Counting rows by id-bearing lines keeps an empty registry empty instead of mistaking the
    wrapped header for a data row.
    """
    lines = [l for l in text.splitlines() if l.startswith('│')]
    id_at = [i for i, l in enumerate(lines) if NODE_TOKEN.search(l)]
    head = lines[:id_at[0]] if id_at else lines
    body = lines[id_at[0]:] if id_at else []
    return {'rows': len(id_at), 'lines': len(lines),
            'columns': sorted({l.count('│') for l in lines}),
            'header': [[c.strip() for c in l.split('│')[1:-1]] for l in head],
            'ids': NODE_TOKEN.findall(text),
            'labels': sorted({c for l in body for c in l.split('│') if c.strip().startswith('(')}),
            'ip_cells': [c.strip() for l in body for c in l.split('│')[1:-1] if 'IP#' in c]}


def compare(prof, pair):
    """One profile's verdict. `pair` is {'go': [runs…], 'rust': [runs…]} in argv order."""
    g, r = pair['go'][0], pair['rust'][0]
    problems = base_problems(prof, g, r)
    observed = {'bytes': {side: [pair[side][i]['stdout_bytes_raw'] + pair[side][i]['stderr_bytes_raw']
                                 for i in range(len(pair[side]))] for side in ('go', 'rust')}}
    if prof['kind'] == 'keys':
        for side in ('go', 'rust'):
            runs = pair[side]
            parsed = [x['parsed_keys'][0] for x in runs]
            if any(not p['key'] or not p['secret_placeholder'] for p in parsed):
                problems.append(f'keys_unparseable_{side}')
                continue
            if not all(re.fullmatch(r'API[0-9A-Za-z]{8,16}', p['key']) for p in parsed):
                problems.append(f'key_shape_{side}')
            lens = [l for x in runs for l in x['generated_secrets_len']]
            # Go's RandomSecret() is base62 of 32 random bytes, so the printed length is 43 or 44
            # depending on the value; pinning one number would be a flake, not a contract.
            if any(not 43 <= l <= 44 for l in lens) or not lens:
                problems.append(f'secret_shape_{side}')
            tags = {t for x in runs for t in x['generated_secret_tags']}
            if len(tags) < sum(len(x['generated_secret_tags']) for x in runs):
                problems.append(f'generated_values_repeat_{side}')
            observed[side] = {'keys': [p['key'] for p in parsed],
                              'secret_lengths': [x['generated_secrets_len'] for x in runs],
                              'secret_tags': sorted(tags)}
    elif prof['kind'] == 'token':
        claims, orders = {}, {}
        for side in ('go', 'rust'):
            info = pair[side][0].get('token_info') or {}
            if not info:
                problems.append(f'token_unparsed_{side}')
                continue
            observed[side] = {'alg': info['alg'], 'claims': info['claims'],
                              'header_keys': info['header_keys'],
                              'payload_order': info['payload_order'],
                              'signature_verifies': info['signature_verifies'],
                              'sig_length': info['sig_length']}
            if not info['signature_verifies']:
                problems.append(f'token_signature_{side}')
            if info['alg'] != 'HS256':
                problems.append(f'token_alg_{side}')
            if not info['iss_matches_fixture']:
                problems.append(f'token_iss_{side}')
            if info.get('recorder') != bool(prof.get('recorder')):
                problems.append(f'token_recorder_grant_{side}')
            claims[side] = set(info['claims'])
            orders[side] = {'header': info['header_keys'], 'payload': info['payload_order']}
        if len(claims) == 2:
            if claims['go'] != claims['rust']:
                problems.append('claim_set_differs')
            # Key order inside a JWT is not part of its meaning: the verifier reads claims by name,
            # and both sides' tokens are accepted by the other's daemon. It is recorded so the
            # difference stays visible instead of being compared away.
            observed['claim_order_scoped'] = orders
    elif prof['kind'] == 'nodes':
        for side in ('go', 'rust'):
            view = table_view(pair[side][0]['stdout'])
            observed[side] = view
            min_rows = prof.get('min_rows', 1)
            if min_rows is not None and view['rows'] < min_rows:
                problems.append(f'rows_below_min_{side}:{view["rows"]}')
            if view['rows'] > 0 and not view['ip_cells']:
                problems.append(f'rows_without_addresses_{side}')
            if not view['header']:
                problems.append(f'table_header_missing_{side}')
        if prof.get('scoped'):
            # This row reads whatever registry the developer host's built-in 127.0.0.1:6379 holds.
            # Its content is outside the lane, so the claim is table shape + exit parity, and the
            # observed row counts are recorded rather than compared.
            observed['host_registry_rows'] = {side: observed[side]['rows'] for side in ('go', 'rust')}
            observed['host_registry_ids'] = {side: observed[side]['ids'] for side in ('go', 'rust')}
            if observed['go']['header'] != observed['rust']['header']:
                problems.append('table_headers_differ')
            if observed['go']['columns'] != observed['rust']['columns']:
                problems.append('table_columns_differ')
        else:
            if len(pair['go'][0]['stdout']) != len(pair['rust'][0]['stdout']):
                problems.append('table_width_differs')
            if sorted(observed['go']['ids']) != sorted(observed['rust']['ids']):
                problems.append('table_ids_differ')
            if observed['go']['columns'] != observed['rust']['columns']:
                problems.append('table_columns_differ')
            if observed['go']['ip_cells'] != observed['rust']['ip_cells']:
                problems.append('table_addresses_differ')
    elif prof['kind'] == 'verbose':
        rows = {side: pair[side][0].get('rows', {}) for side in ('go', 'rust')}
        only = {side: sorted(set(rows[side]) - set(rows[other]))
                for side, other in (('go', 'rust'), ('rust', 'go'))}
        observed['flag_rows'] = {side: len(rows[side]) for side in ('go', 'rust')}
        observed['rows_only_in'] = only
        if any(only.values()):
            problems.append('flag_row_sets_differ')
        if not rows['go']:
            problems.append('flag_rows_empty')          # an empty parse would make every check below vacuous
        shared = sorted(set(rows['go']) & set(rows['rust']))
        mismatched = [f for f in shared if rows['go'][f] != rows['rust'][f]]
        observed['row_field_mismatches'] = mismatched[:12]
        if mismatched:
            problems.append('flag_row_fields_differ')
        if pair['go'][0]['lines_sha256'] != pair['rust'][0]['lines_sha256']:
            problems.append('sorted_stream_differs')
        observed['line_count'] = {side: pair[side][0]['line_count'] for side in ('go', 'rust')}
        repeats = pair['go']
        if prof.get('repeat', {}).get('go', 1) >= 2 and len(repeats) < 2:
            problems.append('oracle_repeat_missing')
        else:
            orders = [x.get('flag_order', []) for x in repeats]
            observed['oracle_order_runs'] = len(orders)
            observed['oracle_row_sets_equal'] = len({tuple(sorted(o)) for o in orders}) == 1
            # One sample cannot show stability, so a single-run row has no measured order claim at
            # all: it records the difference instead of passing or failing on it.
            observed['oracle_order_stability_measured'] = len(orders) >= 2
            observed['oracle_order_unstable'] = (len({tuple(o) for o in orders}) > 1
                                                 if len(orders) >= 2 else None)
            if observed['oracle_order_stability_measured'] and not observed['oracle_order_unstable']:
                # Order is only compared once the oracle has been shown to keep it; otherwise an
                # order claim would be a comparison against map-iteration noise.
                if tuple(rows['go']) != tuple(rows['rust']):
                    problems.append('flag_row_order_differs')
            else:
                observed['row_order_scoped'] = tuple(rows['go']) != tuple(rows['rust'])
    elif prof['kind'] == 'divergence':
        observed['divergence'] = prof['divergence']
        observed['exits'] = {'go': g['exit'], 'rust': r['exit']}
        observed['streams'] = {side: pair[side][0]['streams'] for side in ('go', 'rust')}
    return {'problems': problems, 'observed': observed}


# --------------------------------------------------------------------- fixtures
def write_fixtures(work, ctx):
    (work / 'kf.yaml').write_text(f'{KEY}: {SECRET}\n')
    (work / 'kf-b64.txt').write_text(
        base64.b64encode(json.dumps({KEY: SECRET}).encode()).decode())
    (work / 'cfg-ports.yaml').write_text(
        f'keys:\n  {KEY}: {SECRET}\nport: 55999\n'
        f'rtc:\n  tcp_port: 55990\n  udp_port: 55991\n  port_range_start: 55992\n'
        f'  port_range_end: 55996\n')
    (work / 'cfg-keys.yaml').write_text(f'keys:\n  {KEY}: {SECRET}\nport: 55999\n')
    (work / 'cfg-closed.yaml').write_text(
        f'keys:\n  {KEY}: {SECRET}\nredis:\n  address: 127.0.0.1:{ctx["closed_port"]}\n'
        f'logging:\n  level: info\nwebtransport:\n  port: 0\n')
    (work / 'cfg-redis.yaml').write_text(
        f'keys:\n  {KEY}: {SECRET}\nredis:\n  address: 127.0.0.1:{ctx["redis_port"]}\n'
        f'logging:\n  level: info\nwebtransport:\n  port: 0\n')
    for side in ('go', 'rust'):
        (work / f'daemon-{side}.yaml').write_text(
            f'keys:\n  {KEY}: {SECRET}\nport: {ctx[f"http_{side}_port"]}\n'
            f'rtc:\n  tcp_port: {ctx[f"tcp_{side}_port"]}\n  udp_port: {ctx[f"udp_{side}_port"]}\n'
            f'  port_range_start: {ctx[f"udp_{side}_port"] + 1}\n'
            f'  port_range_end: {ctx[f"udp_{side}_port"] + 6}\n'
            f'redis:\n  address: 127.0.0.1:{ctx["redis_port"]}\n'
            f'logging:\n  level: info\nwebtransport:\n  port: 0\n')
    ctx.update({'kf_yaml': work / 'kf.yaml', 'kf_b64': work / 'kf-b64.txt',
                'cfg_ports': work / 'cfg-ports.yaml', 'cfg_keys': work / 'cfg-keys.yaml',
                'cfg_closed': work / 'cfg-closed.yaml', 'cfg_redis': work / 'cfg-redis.yaml',
                'daemon_go': work / 'daemon-go.yaml', 'daemon_rust': work / 'daemon-rust.yaml',
                'keypair': f'{KEY}: {SECRET}', 'room': 'c101-room'})
    return ctx


def owned_redis(work, port):
    conf = work / 'redis.conf'
    conf.write_text(f'port {port}\nbind 127.0.0.1\nsave ""\ndir {work}\n'
                    f'dbfilename owned-c101.rdb\nlogfile {work}/redis.log\n')
    proc = subprocess.Popen(['redis-server', str(conf)], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if subprocess.run(['redis-cli', '-p', str(port), 'PING'],
                          capture_output=True).stdout.strip() == b'PONG':
            return proc
        time.sleep(0.1)
    raise SystemExit('owned redis never answered PING')


def redis_cmd(port, *args):
    return subprocess.run(['redis-cli', '-p', str(port)] + [str(a) for a in args],
                          capture_output=True, text=True).stdout.strip()


def stop(proc, window=30.0):
    """SIGINT, then a second one, then SIGKILL. The count that ended the process and how long the
    daemon took to release its ports are part of the measurement, not cleanup detail."""
    sent, started = [], time.monotonic()
    for sig in (signal.SIGINT, signal.SIGINT, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            break
        sent.append(signal.Signals(sig).name)
        try:
            proc.wait(timeout=window / 3)
            break
        except subprocess.TimeoutExpired:
            continue
    return {'signals': sent, 'exit': proc.returncode, 'killed_not_exited':
            sent[-1:] == ['SIGKILL'] if sent else True,
            'to_exit_ms': int((time.monotonic() - started) * 1000)}


def launch(binary, cfg_path, http_port, log_path, settle=2.0):
    log = open(log_path, 'wb')
    proc = subprocess.Popen([str(binary), '--config', str(cfg_path)], stdout=log,
                            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                            start_new_session=True)
    ready, deadline = False, time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', http_port), timeout=0.5):
                ready = True
                break
        except OSError:
            if proc.poll() is not None:
                break
            time.sleep(0.2)
    time.sleep(settle)
    return proc, log, ready, log_path


def daemon_node_ids(log_path):
    """The node id the daemon itself announced, so the CLI table can be tied to a live pid."""
    text = pathlib.Path(log_path).read_text(errors='replace')
    return {'ids': sorted(set(re.findall(r'\bND_[0-9A-Za-z]{6,16}\b', text))),
            'starting_line': bool(re.search(r'starting LiveKit server', text)),
            'pid_seen': bool(re.search(r'"portHttp":\s*\d+', text))}


# --------------------------------------------------------------------- driver
def stripped_env():
    """No LIVEKIT_/LIVECLOUD_/OTEL_ variable may leak into the pair: the row under test is the argv
    and the config file, and an inherited env channel would make the two sides read different
    setups while looking identical."""
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_', 'OTEL_'))}


def run_profile(prof, bins, ctx):
    pair = {}
    for side in ('go', 'rust'):
        binary = bins['rust_alias']['path'] if (side == 'rust' and prof.get('rust_binary')
                                                == 'alias') else bins[side]['path']
        argv = [str(a).format(**{k: str(v) for k, v in ctx.items()}) for a in profile_argv(prof, ctx)]
        runs = []
        for _ in range(prof.get('repeat', {}).get(side, 1)):
            rec = run_one(binary, argv, env=ctx['env'])
            sanitize(rec, prof)
            if prof['kind'] == 'verbose' and len(rec['stdout']) > 1600:
                rec['stdout'] = rec['stdout'][:1600]
                rec['stdout_truncated'] = True
            if prof['kind'] in ('keys', 'verbose'):
                rec['stdout'] = rec['stdout'][:1600]
            for raw in ('raw_stdout', 'raw_stderr'):
                rec.pop(raw, None)
            rec['argv'] = argv_for_record(argv)
            rec['binary'] = str(binary)
            runs.append(rec)
        pair[side] = runs
    verdict = compare(prof, pair)
    return {'profile': prof['id'], 'row': prof['row'], 'kind': prof['kind'],
            'phase': prof['phase'], 'declared_exit': prof.get('exit'),
            'declared_streams': list(prof['streams']) if prof.get('streams') else None,
            'needles': prof['needles'], 'forbidden': prof['forbidden'],
            'volatile': prof['volatile'], 'runs': pair, 'scoped': prof.get('scoped', False),
            'go': pair['go'][0], 'rust': pair['rust'][0],
            'oracle_repeats': len(pair['go']), 'problems': verdict['problems'],
            'match': not verdict['problems'], 'observed': verdict['observed'],
            'note': prof.get('note'), 'divergence': prof.get('divergence')}


# --------------------------------------------------------------------- legs
def http_call(port, method, path, token=None, body=b'{}'):
    """One API call against a launched daemon. A refusal is the result, so nothing raises."""
    try:
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=12)
        conn.putrequest(method, path)
        if method == 'POST':
            conn.putheader('Content-Type', 'application/json')
            conn.putheader('Content-Length', str(len(body)))
        if token:
            conn.putheader('Authorization', 'Bearer ' + token)
        conn.putheader('Connection', 'close')
        conn.endheaders()
        if method == 'POST':
            conn.send(body)
        resp = conn.getresponse()
        payload = resp.read()[:240].decode('utf-8', 'replace')
        conn.close()
        return {'status': resp.status, 'body': payload}
    except Exception as exc:
        return {'status': None, 'body': None, 'error': f'{type(exc).__name__}'}


def mint(side, bins, cfg_path, room, identity='alice', recorder=False):
    """Run one binary's create-join-token and return the raw token (kept in memory only)."""
    argv = ['--config', str(cfg_path), 'create-join-token', '--room', room, '--identity', identity]
    if recorder:
        argv.append('--recorder')
    rec = run_one(bins[side]['path'], argv)
    return token_from(rec['raw_stdout']), rec['exit'], len(rec['raw_stderr'])


VALIDATE = '/rtc/validate?protocol=13'
LISTROOMS = '/twirp/livekit.RoomService/ListRooms'


def run_legs(bins, ctx, daemons, work):
    """The tier-5 half of the claim: a token minted by one product's CLI has to be honoured by a
    daemon launched from the other, and the negatives must be refused in identical wording so the
    acceptance cannot come from an auth bypass."""
    legs = []

    def add(lid, expect_status, expect_body, actual, why):
        ok = (actual.get('status') == expect_status
              and (expect_body is None or expect_body in (actual.get('body') or '')))
        legs.append({'leg': lid, 'expect': {'status': expect_status, 'body_contains': expect_body},
                     'actual': {'status': actual.get('status'),
                                'body': (actual.get('body') or '')[:160],
                                'error': actual.get('error')},
                     'ok': ok, 'why': why})

    for host in ('go', 'rust'):
        other = 'rust' if host == 'go' else 'go'
        port = ctx[f'http_{host}_port']
        tok, rc, errb = mint(other, bins, ctx[f'daemon_{other}'], 'c101-xfeed')
        add(f'validate_{other}_minted_on_{host}_daemon', 200, 'success',
            http_call(port, 'GET', VALIDATE, tok),
            f'token minted by the {other} binary, served by the {host} daemon')
        if rc != 0 or not tok:
            legs[-1]['mint_anomaly'] = {'exit': rc, 'stderr_bytes': errb}
        same, _, _ = mint(host, bins, ctx[f'daemon_{host}'], 'c101-selffeed')
        add(f'validate_{host}_minted_on_{host}_daemon', 200, 'success',
            http_call(port, 'GET', VALIDATE, same),
            f'the same-side control for {host}: a self-minted token must also work')
        rt, _, _ = mint(other, bins, ctx[f'daemon_{other}'], 'c101-rec', recorder=True)
        add(f'validate_recorder_{other}_on_{host}', 200, 'success',
            http_call(port, 'GET', VALIDATE, rt),
            'the --recorder grant mints a token the other daemon accepts as well')
        add(f'noauth_rejected_by_{host}', 401, 'no permissions to access the room',
            http_call(port, 'GET', VALIDATE, None),
            'the same endpoint refuses an unauthenticated join, so the 200 above is grant-driven')
        bad, _, _ = mint(other, bins, ctx[f'daemon_{other}'], 'c101-xfeed')
        tampered = bad.rsplit('.', 1)[0] + '.' + ('A' * len(bad.rsplit('.', 1)[1]))
        add(f'wrong_signature_rejected_by_{host}', 401, 'token signature is invalid',
            http_call(port, 'GET', VALIDATE, tampered),
            'a signature that does not verify is refused, so acceptance is keyed on the secret')
        add(f'cli_token_admin_denied_on_{host}', 401, 'permissions denied',
            http_call(port, 'POST', LISTROOMS, tok),
            'the join-only grant reaches authorization (not authentication) and is refused '
            'identically by both products')
        add(f'admin_token_listrooms_on_{host}', 200, 'rooms',
            http_call(port, 'POST', LISTROOMS, admin_token(SECRET)),
            'the API shape control: with the right grant the same call answers 200 on both')
        add(f'twirp_wrong_case_path_404_on_{host}', 404, 'page not found',
            http_call(port, 'POST', '/twirp/Livekit.RoomService/ListRooms', admin_token(SECRET)),
            'the mount naming is part of the surface: the service package is lower-cased on both')

    # The registry legs tie the CLI table to the processes that produced it.
    for side in ('go', 'rust'):
        rc, out = None, ''
        rec = run_one(bins[side]['path'], ['--config', str(ctx['cfg_redis']), 'list-nodes'])
        ids = sorted(set(re.findall(r'ND_[0-9A-Za-z]{6,16}', rec['raw_stdout'])))
        legs.append({'leg': f'cli_{side}_sees_both_daemons',
                     'expect': {'ids': sorted(ctx['daemon_ids'])}, 'actual': {'ids': ids,
                                                                              'exit': rec['exit']},
                     'ok': ids == sorted(ctx['daemon_ids']) and rec['exit'] == 0,
                     'why': 'each product\'s list-nodes reads the registry the other product wrote'})
    return legs


def run_key_legs(bins, ctx):
    """`generate-keys` is only meaningful if the printed pair is the pair the product then signs
    with, so chain it: generate, mint with the printed key via the CLI, verify the JWT independently
    and prove a wrong secret does NOT verify."""
    legs = []
    for side in ('go', 'rust'):
        rec = run_one(bins[side]['path'], ['generate-keys'])
        key, secret = keys_from(rec['raw_stdout'])
        shape = {'key_shape': bool(re.fullmatch(r'API[0-9A-Za-z]{8,16}', key or '')),
                 'secret_shape': bool(re.fullmatch(r'[0-9A-Za-z]{43,44}', secret or ''))}
        tok, rc, _ = None, None, None
        if key and secret:
            argv = ['--keys', f'{key}: {secret}', 'create-join-token', '--room', 'c101-chain',
                    '--identity', 'chain']
            m = run_one(bins[side]['path'], argv)
            tok = token_from(m['raw_stdout'])
            rc = m['exit']
        good = bool(tok) and verify_jwt(tok, secret)['signature_verifies']
        neg = bool(tok) and not verify_jwt(tok, WRONG_SECRET)['signature_verifies']
        iss = bool(tok) and (split_jwt(tok)['payload'].get('iss') == key)
        legs.append({'leg': f'generated_pair_chains_{side}',
                     'expect': {'exit': 0, 'verifies_with_printed_secret': True,
                                'refused_with_wrong_secret': True, 'iss_is_printed_key': True},
                     'actual': {'exit': rc, 'verifies_with_printed_secret': good,
                                'refused_with_wrong_secret': neg, 'iss_is_printed_key': iss,
                                'minted': bool(tok)},
                     'ok': all(shape.values()) and good and neg and iss and rc == 0,
                     'why': 'the printed secret has to be the signing key, not a display value',
                     'shape': shape, 'secret_length': len(secret or ''),
                     'secret_tag': h8(secret) if secret else None, 'key': key})
    return legs


def run_post_exit_legs(bins, ctx):
    legs = []
    for side in ('go', 'rust'):
        rec = run_one(bins[side]['path'], ['--config', str(ctx['cfg_redis']), 'list-nodes'])
        ids = sorted(set(re.findall(r'ND_[0-9A-Za-z]{6,16}', rec['raw_stdout'])))
        legs.append({'leg': f'cli_{side}_empty_after_exit', 'expect': {'ids': []},
                     'actual': {'ids': ids, 'exit': rec['exit']},
                     'ok': not ids and rec['exit'] == 0,
                     'why': 'both products deregister on shutdown, so the CLI must go back to empty'})
    keys = redis_cmd(ctx['redis_port'], 'KEYS', '*').split()
    legs.append({'leg': 'owned_store_left_clean', 'expect': {'node_keys': []},
                 'actual': {'keys': sorted(keys)}, 'ok': 'nodes' not in keys,
                 'why': 'the store the run created holds no node record once the daemons are gone'})
    return legs


# --------------------------------------------------------------------- receipt
def refuse_filed_slot(path):
    """A filed evidence slot is immutable history. Re-deriving it must write beside it, not into it."""
    text = str(path)
    if '/server/evidence/' in text:
        raise SystemExit(f'refusing to rewrite a filed slot: {path}\n'
                         f'copy it to a scratch dir first: cp {path} /tmp/laneCLI/')


def rederive_leg(leg):
    """A leg's verdict is computable from what it recorded, so `--report` can re-check it offline."""
    expect, actual = leg['expect'], leg['actual']
    if 'ids' in expect:
        return actual.get('ids') == expect['ids'] and actual.get('exit') == 0
    if 'node_keys' in expect:
        return 'nodes' not in actual.get('keys', [])
    if 'verifies_with_printed_secret' in expect:
        shape_ok = all(leg.get('shape', {}).values())
        return (shape_ok and all(actual.get(k) == v for k, v in expect.items())
                and actual.get('minted'))
    body = expect.get('body_contains')
    return (actual.get('status') == expect.get('status')
            and (body is None or body in (actual.get('body') or '')))


def build_controls(manifest, P):
    """Lane-level claims: the ones that are about the whole run rather than one profile."""
    controls = {}
    controls['profiles_paired'] = all(
        len(e['runs']['go']) >= 1 and len(e['runs']['rust']) >= 1 for e in P.values())
    controls['profile_count_matches_table'] = len(P) == len(PROFILES)
    controls['every_declared_exit_recorded'] = all(
        e['declared_exit'] is not None or e['kind'] in ('divergence',) or e.get('scoped')
        for e in P.values())
    text_rows = [e for e in P.values() if e['kind'] in ('text', 'nodes')]
    controls['no_vacuous_empty_pair'] = bool(text_rows) and all(
        (e['go']['stdout'] + e['go']['stderr'] + e['rust']['stdout'] + e['rust']['stderr']).strip()
        for e in text_rows)
    controls['volatile_masks_reproduced'] = bool(
        [e for e in P.values() if e['volatile']]) and all(
        not [p for p in e['problems'] if p.startswith('volatile_')] for e in P.values())
    hv = P.get('help-verbose')
    controls['help_verbose_oracle_repeated'] = bool(hv) and hv['oracle_repeats'] >= 2
    controls['help_verbose_order_instability_measured'] = bool(hv) and (
        'oracle_order_unstable' in hv['observed'])
    controls['help_verbose_row_sets_equal'] = bool(hv) and hv['observed'].get(
        'oracle_row_sets_equal', False)
    ports = P.get('ports-no-config')
    cfgp = P.get('ports-config-before')
    after = P.get('ports-config-after')
    controls['ports_reflects_config'] = bool(ports and cfgp) and (
        '7880' in ports['go']['stdout'] and '55999' in cfgp['go']['stdout']
        and '7880' not in cfgp['go']['stdout'] and '7880' not in cfgp['rust']['stdout'])
    controls['persistent_flag_reaches_subcommand'] = bool(after) and (
        '55999' in after['go']['stdout'] and '55999' in after['rust']['stdout'])
    vrows = [P.get('version-long'), P.get('version-short')]
    controls['version_delta_is_name_only'] = all(vrows) and all(
        abs(v['go']['stdout_bytes_raw'] - v['rust']['stdout_bytes_raw']) == 5
        and stream_text(v['go'], 'stdout', PROFILES_BY_ID[v['profile']])
        == stream_text(v['rust'], 'stdout', PROFILES_BY_ID[v['profile']]) for v in vrows)
    controls['alias_binary_needs_no_normalization'] = bool(P.get('version-alias-binary')) and (
        P['version-alias-binary']['rust']['stdout'] == 'livekit-server version 1.13.10\n')
    reg = P.get('list-nodes-registered')
    if reg:
        ids_in_table = sorted(set(re.findall(r'ND_[0-9A-Za-z]{6,16}', reg['go']['stdout'])))
        controls['table_ids_tied_to_daemon_logs'] = (
            ids_in_table == sorted(manifest.get('daemon_ids') or []) and bool(ids_in_table))
        controls['both_clis_show_same_rows'] = (
            reg['observed']['go']['rows'] == reg['observed']['rust']['rows']
            == len(ids_in_table))
    empty = P.get('list-nodes-empty-registry')
    controls['empty_registry_shape'] = bool(empty) and all(
        re.findall(r'ND_[0-9A-Za-z]{6,16}', empty[side]['stdout']) == []
        and 'IP Address' in empty[side]['stdout'] for side in ('go', 'rust'))
    gone = P.get('list-nodes-after-exit')
    controls['registry_emptied_after_exit'] = bool(gone) and all(
        re.findall(r'ND_[0-9A-Za-z]{6,16}', gone[side]['stdout']) == [] for side in ('go', 'rust'))
    host = P.get('list-nodes-host-default')
    controls['host_default_row_scoped'] = bool(host) and all(
        p not in host['problems'] for p in ('exit_split', 'table_headers_differ',
                                            'table_columns_differ', 'vacuous_scoped_row_empty'))
    controls['host_default_delta_recorded_not_compared'] = bool(host) and (
        set(host['observed'].get('host_registry_rows', {})) == {'go', 'rust'}
        and ((bool(host['observed']['go']['header']) and bool(host['observed']['rust']['header']))
             or all('unable to connect to' in host[side]['stdout'] + host[side]['stderr']
                    for side in ('go', 'rust')))
        and not any(p.startswith(('table_ids_differ', 'table_addresses_differ',
                                  'table_width_differs')) for p in host['problems']))
    div = P.get('token-key-file-yaml')
    controls['key_file_panic_divergence_pinned'] = bool(div) and (
        div['go']['exit'] == 2 and div['rust']['exit'] == 0
        and 'panic: reflect: reflect.Value.Set using unaddressable value' in div['go']['stderr']
        and '<JWT>' in div['rust']['stdout'])
    hv2 = P.get('help-verbose-with-persistent-flag')
    controls['persistent_verbose_order_scoped'] = bool(hv2) and (
        hv2['observed'].get('oracle_order_stability_measured') is False
        and 'row_order_scoped' in hv2['observed'])
    gk = P.get('generate-keys')
    samples = [(s, l) for s in ('go', 'rust') if gk
               for item in gk['observed'].get(s, {}).get('secret_lengths', [])
               for l in (item if isinstance(item, list) else [item])]
    controls['generated_secret_lengths_in_range'] = bool(samples) and all(
        43 <= l <= 44 for _, l in samples) and len(samples) == (
        len(gk['runs']['go']) + len(gk['runs']['rust']))
    tok = [e for e in P.values() if e['kind'] == 'token']
    controls['claim_order_scoped_not_compared'] = bool(tok) and all(
        'claim_order_scoped' in e['observed'] for e in tok)
    controls['jwt_claims_agree'] = bool(tok) and all('claim_set_differs' not in e['problems']
                                                     for e in tok)
    legs = manifest['legs']
    for leg in legs:
        controls[f"leg_{leg['leg']}"] = leg['ok']
    cross = [l for l in legs if '_minted_on_' in l['leg'] and '_daemon' in l['leg']
             and l['leg'].startswith('validate_')]
    controls['cross_acceptance_symmetric'] = len(cross) >= 2 and all(l['ok'] for l in cross)
    negs = [l for l in legs if l['leg'].startswith(('noauth_', 'wrong_signature_'))]
    controls['negative_controls_refused'] = bool(negs) and all(l['ok'] for l in negs)
    tw = [l for l in legs if l['leg'].startswith('twirp_wrong_case_path_404_on_')]
    controls['twirp_mount_naming_parity'] = bool(tw) and all(l['ok'] for l in tw)
    text = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
    controls['no_machine_address_in_receipt'] = not redact.find_forbidden(text)
    controls['fixture_secret_not_in_receipt'] = (SECRET not in text and WRONG_SECRET not in text)
    argv_blob = json.dumps([rec['argv'] for e in P.values()
                            for side in ('go', 'rust') for rec in e['runs'][side]])
    fixture_blob = json.dumps({k: v for k, v in manifest['fixtures'].items()
                               if k.endswith('_text')})
    controls['off_limit_ports_never_named'] = not re.search(
        r'\b788[12]\b', argv_blob + fixture_blob)
    before, after_list = manifest['listeners_before'], manifest['listeners_after']
    controls['off_limit_ports_untouched'] = all(
        before[str(p)] == after_list[str(p)] for p in OFF_LIMITS)
    owned_redis_port = manifest['ports']['redis_port']
    controls['owned_store_not_host_default'] = (owned_redis_port != 6379
                                                and '127.0.0.1:%s' % owned_redis_port
                                                in manifest['fixtures']['cfg_redis_text'])
    controls['daemon_readiness_observed'] = bool(manifest['daemons']) and all(
        d['ready'] for d in manifest['daemons'].values())
    controls['daemons_exited_without_sigkill'] = bool(manifest['daemons']) and all(
        not d['stop']['killed_not_exited'] for d in manifest['daemons'].values())
    bins = manifest['binaries']
    now = {}
    for side in ('go', 'rust', 'rust_alias'):
        path = pathlib.Path(bins.get(side, {}).get('path', ''))
        now[side] = sha(path) if path and path.exists() else None
    manifest['binary_hash_now'] = now
    controls['binaries_unchanged'] = all(
        now[s] is not None and now[s] == bins[s]['sha256'] for s in ('go', 'rust'))
    bins['go']['is_fixed_oracle'] = (now['go'] == FIXED_GO_SHA256)
    controls['reference_commit_matches'] = (
        manifest['reference']['go_checkout_commit'] == FIXED_GO_COMMIT)
    harness_now = sha(pathlib.Path(__file__).resolve())
    canonical = manifest.get('harness_sha256') or manifest['harness_sha256_at_start']
    manifest['harness_sha256_now'] = harness_now
    controls['harness_unchanged_during_run'] = (
        harness_now == manifest['harness_sha256_at_start'] if live_flag(manifest)
        else canonical == manifest['harness_sha256_at_start'])
    controls['harness_unchanged_since_run'] = (canonical == harness_now)
    return controls


def live_flag(manifest):
    return bool(manifest.get('live_run'))


def write_receipt(manifest, out):
    (out / 'execution.json').write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + '\n')


def finish(manifest, out, live=False, write=True):
    manifest['live_run'] = live
    P = {}
    for entry in manifest['profiles']:
        prof = PROFILES_BY_ID[entry['profile']]
        verdict = compare(prof, entry['runs'])
        # The serialized entry keeps `go`/`rust` aliases into the first run; a re-derivation must
        # read the records themselves, otherwise a control would judge a stale copy of them.
        entry['go'] = entry['runs']['go'][0]
        entry['rust'] = entry['runs']['rust'][0]
        entry['problems'] = verdict['problems']
        entry['observed'] = verdict['observed']
        entry['match'] = not verdict['problems']
        P[entry['profile']] = entry
    for leg in manifest['legs']:
        leg['ok'] = rederive_leg(leg)
    controls = build_controls(manifest, P)
    manifest['cross_profile_controls'] = controls
    manifest['failed_profiles'] = sorted(n for n, e in P.items() if not e['match'])
    manifest['failed_controls'] = sorted(k for k, v in controls.items() if not v)
    manifest['failed_legs'] = sorted(l['leg'] for l in manifest['legs'] if not l['ok'])
    manifest['status_gates'] = {
        'binaries_unchanged': controls['binaries_unchanged'],
        'reference_commit_matches': controls['reference_commit_matches'],
        'go_is_fixed_oracle': manifest['binaries']['go']['is_fixed_oracle'],
        'no_failed_profiles': not manifest['failed_profiles'],
        'no_failed_controls': not manifest['failed_controls'],
        'harness_unchanged_during_run': controls['harness_unchanged_during_run'],
        'harness_unchanged_since_run': controls['harness_unchanged_since_run'],
    }
    all_ok = all(manifest['status_gates'].values())
    manifest['unmet_gates'] = [k for k, v in manifest['status_gates'].items() if not v]
    # --skip-store leaves the launched phases out, so it gets its own label rather than a subset of
    # the acceptance gates that happens to be true.
    manifest['status'] = ('PARTIAL_OFFLINE_CLI_ONLY_NOT_ACCEPTANCE' if manifest.get('skip_store')
                          else 'PASS_LAUNCHED_MAIN_CLI_SURFACES' if all_ok else 'FAIL')
    manifest['harness_sha256'] = (manifest['harness_sha256_at_start'] if live
                                  else manifest.get('harness_sha256')
                                  or manifest['harness_sha256_at_start'])
    summary = {'status': manifest['status'], 'cases': len(P), 'legs': len(manifest['legs']),
               'controls': len(controls), 'failed_profiles': manifest['failed_profiles'],
               'failed_controls': manifest['failed_controls'],
               'failed_legs': manifest['failed_legs'], 'unmet_gates': manifest['unmet_gates'],
               'all_problems': sorted(f"{n}::{p}" for n, e in P.items() for p in e['problems']),
               'harness_sha256_now': manifest['harness_sha256_now'],
               'binaries_unchanged': controls['binaries_unchanged']}
    if not write:
        # The gate sweep audits in-memory copies; writing them would replace the receipt under audit
        # with the last injection.
        return {'all_ok': all_ok, 'summary': summary}
    write_receipt(manifest, out)
    batch = {'status': manifest['status'], 'case_count': len(P), 'leg_count': len(manifest['legs']),
             'covers': COVERS,
             'profiles': {n: {'match': e['match'], 'kind': e['kind'], 'row': e['row'],
                              'go_exit': e['go']['exit'], 'rust_exit': e['rust']['exit'],
                              'problems': e['problems']} for n, e in P.items()},
             'legs': {l['leg']: l['ok'] for l in manifest['legs']},
             'controls': controls, 'failed_profiles': manifest['failed_profiles'],
             'failed_controls': manifest['failed_controls'],
             'status_gates': manifest['status_gates'], 'unmet_gates': manifest['unmet_gates'],
             'binary_hash_now': manifest['binary_hash_now'],
             'harness_sha256_now': manifest['harness_sha256_now'],
             'binaries': manifest['binaries'], 'reference': manifest['reference'],
             'harness_sha256': manifest['harness_sha256']}
    (out / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False,
                                               sort_keys=True) + '\n')
    return {'all_ok': all_ok, 'summary': summary}


# --------------------------------------------------------------------- gate sweep
def pf(doc, pid, side, run=0):
    for entry in doc['profiles']:
        if entry['profile'] == pid:
            return entry['runs'][side][run]
    raise KeyError(pid)


def leg(doc, name):
    for item in doc['legs']:
        if item['leg'] == name:
            return item
    raise KeyError(name)


def blank(rec):
    rec['stdout'] = ''
    rec['stderr'] = ''
    return rec


def strip_placeholder(doc, pid, token):
    for entry in doc['profiles']:
        if entry['profile'] != pid:
            continue
        for side in ('go', 'rust'):
            for rec in entry['runs'][side]:
                for stream in ('stdout', 'stderr'):
                    rec[stream] = rec[stream].replace(token, '')
    return doc


def narrow_table(rec):
    """Keep the header, drop the data rows: the shape stays, the evidence goes."""
    rec['stdout'] = '\n'.join(l for l in rec['stdout'].splitlines() if 'ND_' not in l) + '\n'
    return rec


GATE_INJECTIONS = (
    {'case': 'exit_drift', 'expect': 'FAILED_PROFILE', 'target': 'declared_exit_go',
     'apply': lambda d: pf(d, 'help-root-flag', 'go').update(exit=1),
     'why': 'a builtin flag whose measured exit moved no longer matches the oracle contract'},
    {'case': 'exit_split_unpinned', 'expect': 'FAILED_PROFILE', 'target': 'exit_split',
     'apply': lambda d: pf(d, 'generate-keys', 'rust').update(exit=1),
     'why': 'outside the pinned divergence row the pair must agree on the exit code'},
    {'case': 'stream_split_drift', 'expect': 'FAILED_PROFILE', 'target': 'stream_split_go',
     'apply': lambda d: pf(d, 'help-topic-unknown', 'go').update(streams=['stdout']),
     'why': 'the help-topic refusal has to land on stderr with stdout empty'},
    {'case': 'refusal_prints_nothing', 'expect': 'FAILED_PROFILE',
     'target': 'vacuous_both_streams_empty',
     'apply': lambda d: [blank(pf(d, 'list-nodes-closed-redis', s)) for s in ('go', 'rust')],
     'why': 'a row that compares two empty strings proves nothing about the refusal'},
    {'case': 'masked_text_differs', 'expect': 'FAILED_PROFILE', 'target': 'stdout_differs',
     'apply': lambda d: pf(d, 'ports-no-config', 'rust').update(
         stdout=pf(d, 'ports-no-config', 'rust')['stdout'] + 'X'),
     'why': 'the ports table is claimed to agree byte for byte after masking'},
    {'case': 'volatile_mask_never_matched', 'expect': 'FAILED_PROFILE',
     'target': 'volatile_never_matched_go:api_key',
     'apply': lambda d: pf(d, 'generate-keys', 'go')['mask_counts'].update(api_key=0),
     'why': 'if the volatile token was never seen, the masked agreement is over an absent value'},
    {'case': 'volatile_placeholder_absent', 'expect': 'FAILED_PROFILE',
     'target': 'volatile_placeholder_missing_go:table_time',
     'apply': lambda d: strip_placeholder(d, 'list-nodes-registered', '<TS>'),
     'why': 'the count says a timestamp was masked but the stored stream shows none'},
    {'case': 'claim_set_differs', 'expect': 'FAILED_PROFILE', 'target': 'claim_set_differs',
     'apply': lambda d: pf(d, 'token-with-keys', 'rust')['token_info']['claims'].remove('identity'),
     'why': 'the JWT claim sets are claimed equal across the pair'},
    {'case': 'token_signature_fails', 'expect': 'FAILED_PROFILE', 'target': 'token_signature_rust',
     'apply': lambda d: pf(d, 'token-from-config', 'rust')['token_info'].update(
         signature_verifies=False),
     'why': 'a token that does not verify against the fixture secret is not a minted token'},
    {'case': 'generated_values_repeat', 'expect': 'FAILED_PROFILE',
     'target': 'generated_values_repeat_go',
     'apply': lambda d: [pf(d, 'generate-keys', 'go', i).update(
         generated_secret_tags=['aaaaaaaaaaaa']) for i in (0, 1)],
     'why': 'a generator that prints the same secret twice is not generating keys'},
    {'case': 'secret_length_out_of_range', 'expect': 'FAILED_PROFILE',
     'target': 'secret_shape_go',
     'apply': lambda d: pf(d, 'generate-keys', 'go').update(generated_secrets_len=[40]),
     'why': 'base62 of 32 random bytes prints 43 or 44 characters; anything else is a different '
            'generator, not a lucky draw'},
    {'case': 'secret_sample_missing', 'expect': 'FAILED_CONTROL',
     'target': 'generated_secret_lengths_in_range',
     'apply': lambda d: pf(d, 'generate-keys', 'rust').update(generated_secrets_len=[]),
     'why': 'the range claim needs one measured length per printed secret'},
    {'case': 'flag_row_sets_differ', 'expect': 'FAILED_PROFILE', 'target': 'flag_row_sets_differ',
     'apply': lambda d: pf(d, 'help-verbose', 'rust')['rows'].pop('--dev', None),
     'why': 'the generated-flag row set is the parity claim for help-verbose'},
    {'case': 'flag_row_fields_differ', 'expect': 'FAILED_PROFILE',
     'target': 'flag_row_fields_differ',
     'apply': lambda d: pf(d, 'help-verbose', 'rust')['rows'].update(
         **{'--port': ['uint', 'the port to listen on', '$DIFFERENT']}),
     'why': 'a flag whose type, description or env name differs is a surface difference'},
    {'case': 'oracle_repeat_missing', 'expect': 'FAILED_PROFILE', 'target': 'oracle_repeat_missing',
     'apply': lambda d: [e.__setitem__('runs', dict(e['runs'], go=e['runs']['go'][:1]))
                         for e in d['profiles'] if e['profile'] == 'help-verbose'],
     'why': 'without repeats the order claim silently rests on a single sample'},
    {'case': 'persistent_verbose_order_claim_returns', 'expect': 'FAILED_CONTROL',
     'target': 'persistent_verbose_order_scoped',
     'apply': lambda d: [e.__setitem__('runs', dict(e['runs'], go=e['runs']['go'] + e['runs']['go'][:1]))
                         for e in d['profiles'] if e['profile'] == 'help-verbose-with-persistent-flag'],
     'why': 'a single oracle sample cannot show order stability; if the row starts claiming it, the '
            'scoping has been dropped rather than measured'},
    {'case': 'sorted_stream_differs', 'expect': 'FAILED_PROFILE', 'target': 'sorted_stream_differs',
     'apply': lambda d: pf(d, 'help-verbose', 'rust').update(lines_sha256='0' * 64),
     'why': 'the order-insensitive equality of the two help dumps is the strong claim'},
    {'case': 'table_ids_differ', 'expect': 'FAILED_PROFILE', 'target': 'table_ids_differ',
     'apply': lambda d: pf(d, 'list-nodes-registered', 'rust').update(
         stdout=pf(d, 'list-nodes-registered', 'rust')['stdout'].replace('ND_', 'NX_', 1)),
     'why': 'each CLI must read the same registry entries out of the same store'},
    {'case': 'rows_below_min', 'expect': 'FAILED_PROFILE', 'target': 'rows_below_min_rust:0',
     'apply': lambda d: narrow_table(pf(d, 'list-nodes-registered', 'rust')),
     'why': 'a table with only its header proves the row claim about nothing'},
    {'case': 'scoped_header_differ', 'expect': 'FAILED_PROFILE', 'target': 'table_headers_differ',
     'apply': lambda d: pf(d, 'list-nodes-host-default', 'go').update(
         stdout=pf(d, 'list-nodes-host-default', 'go')['stdout'].replace(' ID ', ' Idx ', 1)),
     'why': 'even the scoped host-default row owes the same table header across the pair'},
    {'case': 'scoped_row_loses_its_evidence', 'expect': 'FAILED_CONTROL',
     'target': 'host_default_delta_recorded_not_compared',
     'apply': lambda d: pf(d, 'list-nodes-host-default', 'go').update(stdout=''),
     'why': 'if the default-address row prints no table and names no refusal, the scoping has '
            'nothing left to record'},
    {'case': 'table_columns_differ', 'expect': 'FAILED_PROFILE', 'target': 'table_columns_differ',
     'apply': lambda d: pf(d, 'list-nodes-registered', 'rust').update(
         stdout=pf(d, 'list-nodes-registered', 'rust')['stdout'].replace('│ CPUs ', '', 1)),
     'why': 'the column set is part of the printed surface'},
    {'case': 'divergence_row_unpinned', 'expect': 'FAILED_PROFILE', 'target': 'declared_exit_go',
     'apply': lambda d: pf(d, 'token-key-file-yaml', 'go').update(exit=0),
     'why': 'the scoped divergence is only evidence while the oracle panic is still measured'},
    {'case': 'cross_acceptance_breaks', 'expect': 'FAILED_CONTROL',
     'target': 'leg_validate_go_minted_on_rust_daemon',
     'apply': lambda d: leg(d, 'validate_go_minted_on_rust_daemon')['actual'].update(status=401),
     'why': 'the load-bearing tier-5 claim: the Rust daemon honours a Go-minted token'},
    {'case': 'cross_acceptance_body_drifts', 'expect': 'FAILED_CONTROL',
     'target': 'leg_validate_rust_minted_on_go_daemon',
     'apply': lambda d: leg(d, 'validate_rust_minted_on_go_daemon')['actual'].update(body=''),
     'why': 'a 200 with no body is not the validate answer'},
    {'case': 'negative_control_accepts', 'expect': 'FAILED_CONTROL',
     'target': 'leg_noauth_rejected_by_go',
     'apply': lambda d: leg(d, 'noauth_rejected_by_go')['actual'].update(
         status=200, body='success'),
     'why': 'if an unauthenticated join succeeds, the acceptance legs prove nothing'},
    {'case': 'tampered_signature_accepted', 'expect': 'FAILED_CONTROL',
     'target': 'leg_wrong_signature_rejected_by_rust',
     'apply': lambda d: leg(d, 'wrong_signature_rejected_by_rust')['actual'].update(status=200),
     'why': 'acceptance must be keyed on the secret, not on the header'},
    {'case': 'admin_denial_drifts', 'expect': 'FAILED_CONTROL',
     'target': 'leg_cli_token_admin_denied_on_rust',
     'apply': lambda d: leg(d, 'cli_token_admin_denied_on_rust')['actual'].update(status=200,
                                                                                  body='rooms'),
     'why': 'the join-only grant must stay a grant, not an admin credential'},
    {'case': 'mount_naming_drifts', 'expect': 'FAILED_CONTROL',
     'target': 'leg_twirp_wrong_case_path_404_on_go',
     'apply': lambda d: leg(d, 'twirp_wrong_case_path_404_on_go')['actual'].update(status=200),
     'why': 'the twirp mount naming difference (package case) is part of the contract'},
    {'case': 'generated_chain_breaks', 'expect': 'FAILED_CONTROL',
     'target': 'leg_generated_pair_chains_go',
     'apply': lambda d: leg(d, 'generated_pair_chains_go')['actual'].update(
         verifies_with_printed_secret=False),
     'why': 'the printed secret has to be the key the product actually signs with'},
    {'case': 'registry_leak_after_exit', 'expect': 'FAILED_CONTROL',
     'target': 'registry_emptied_after_exit',
     'apply': lambda d: pf(d, 'list-nodes-after-exit', 'go').update(
         stdout=pf(d, 'list-nodes-after-exit', 'go')['stdout'] + '│ ND_LeftOver1 │\n'),
     'why': 'a node still listed after shutdown means the deregistration claim is false'},
    {'case': 'store_left_nonempty', 'expect': 'FAILED_CONTROL', 'target': 'leg_owned_store_left_clean',
     'apply': lambda d: leg(d, 'owned_store_left_clean')['actual'].update(keys=['nodes']),
     'why': 'the run owns this store and must leave it without a node record'},
    {'case': 'cli_loses_a_daemon', 'expect': 'FAILED_CONTROL', 'target': 'leg_cli_go_sees_both_daemons',
     'apply': lambda d: leg(d, 'cli_go_sees_both_daemons')['actual'].update(ids=['ND_OnlyOne12']),
     'why': 'each product must be able to read what the other wrote'},
    {'case': 'off_limit_port_named', 'expect': 'FAILED_CONTROL',
     'target': 'off_limit_ports_never_named',
     'apply': lambda d: d['fixtures'].update(
         daemon_go_text=d['fixtures']['daemon_go_text'] + '  tcp_port: 7881\n'),
     'why': 'the harness must never point a daemon at the developer host\'s own ports'},
    {'case': 'machine_address_in_receipt', 'expect': 'FAILED_CONTROL',
     'target': 'no_machine_address_in_receipt',
     'apply': lambda d: pf(d, 'list-nodes-host-default', 'go').update(
         stdout=pf(d, 'list-nodes-host-default', 'go')['stdout'] + ' 10.117.44.9\n'),
     'why': 'a receipt that carries a host address cannot be committed as evidence'},
    {'case': 'fixture_secret_in_receipt', 'expect': 'FAILED_CONTROL',
     'target': 'fixture_secret_not_in_receipt',
     'apply': lambda d: pf(d, 'ports-config-before', 'go').update(
         stdout=pf(d, 'ports-config-before', 'go')['stdout'] + SECRET),
     'why': 'the fixture secret belongs in argv shape only, never in the stored stream'},
    {'case': 'host_ports_were_touched', 'expect': 'FAILED_CONTROL', 'target': 'off_limit_ports_untouched',
     'apply': lambda d: d['listeners_after'].update({'7881': d['listeners_before']['7881'] + 1}),
     'why': 'the run must leave the developer host\'s own listeners exactly as it found them'},
    {'case': 'store_phase_never_ran', 'expect': 'FAILED_CONTROL',
     'target': 'daemon_readiness_observed',
     'apply': lambda d: d.update(daemons={}),
     'why': 'readiness over zero daemons is not readiness'},
    {'case': 'daemon_never_ready', 'expect': 'FAILED_CONTROL', 'target': 'daemon_readiness_observed',
     'apply': lambda d: d['daemons']['go'].update(ready=False),
     'why': 'an acceptance leg against a daemon that never listened would be measuring nothing'},
    {'case': 'daemon_needed_sigkill', 'expect': 'FAILED_CONTROL',
     'target': 'daemons_exited_without_sigkill',
     'apply': lambda d: d['daemons']['rust']['stop'].update(killed_not_exited=True),
     'why': 'a process that had to be killed never demonstrated a clean shutdown'},
    {'case': 'binary_identity_drifted', 'expect': 'FAILED_PROFILE',
     'target': 'FAILED:binaries_unchanged',
     'apply': lambda d: d['binaries']['go'].update(sha256='0' * 64),
     'why': 'the pair is only comparable while both binaries are the ones recorded'},
    {'case': 'harness_identity_drifted', 'expect': 'FAILED_PROFILE',
     'target': 'FAILED:harness_unchanged_during_run',
     'apply': lambda d: d.update(harness_sha256_at_start='0' * 64),
     'why': 'the receipt must name the harness that produced it'},
    {'case': 'harness_canonical_drifted', 'expect': 'FAILED_PROFILE',
     'target': 'FAILED:harness_unchanged_since_run',
     'apply': lambda d: d.update(harness_sha256='0' * 64),
     'why': 'the filed harness hash has to still be the one on disk when the verdict is re-read'},
    {'case': 'oracle_moved_off_commit', 'expect': 'FAILED_PROFILE',
     'target': 'FAILED:reference_commit_matches',
     'apply': lambda d: d['reference'].update(go_checkout_commit='0' * 40),
     'why': 'the oracle has to be the pinned source, not whatever is checked out now'},
    {'case': 'baseline_untouched', 'expect': 'clean', 'target': None, 'apply': None,
     'why': 'the sweep itself must not invent a failure'},
    {'case': 'sweep_writes_no_receipt', 'expect': 'self-audit', 'target': None, 'apply': None,
     'why': 'the sweep must audit the copy in memory, not edit the evidence it is checking'},
)


def selftest_gate(doc, out):
    src = pathlib.Path(out) / 'execution.json'
    before = src.read_bytes() if src.exists() else None
    rows = []
    base = json.loads(json.dumps(doc))
    result = finish(base, out, live=False, write=False)
    rows.append({'case': 'baseline_untouched', 'injected': 'nothing', 'expect': 'clean',
                 'discriminated': result['all_ok'], 'status': result['summary']['status'],
                 'failed_profiles': result['summary']['failed_profiles'],
                 'failed_controls': result['summary']['failed_controls'],
                 'why': 'the recorded run has to read as clean before any mutation means anything'})
    for spec in GATE_INJECTIONS:
        if spec['case'] in ('baseline_untouched', 'sweep_writes_no_receipt'):
            continue
        copy = json.loads(json.dumps(doc))
        spec['apply'](copy)
        res = finish(copy, out, live=False, write=False)
        s = res['summary']
        if spec['expect'] == 'FAILED_PROFILE':
            if spec['target'].startswith('FAILED:'):
                hit = spec['target'].split(':', 1)[1] in s['unmet_gates']
            else:
                # Matched against the reported problem, not merely the profile id: a row that
                # reddens for an unrelated reason has not discriminated the gate under test.
                hit = any(spec['target'] in p for p in s['all_problems'])
        else:
            hit = spec['target'] in s['failed_controls']
        rows.append({'case': spec['case'], 'injected': spec['target'], 'expect': spec['expect'],
                     'discriminated': bool(hit) and not res['all_ok'], 'status': s['status'],
                     'failed_profiles': s['failed_profiles'], 'failed_controls': s['failed_controls'],
                     'problems': s['all_problems'][:14],
                     'unmet_gates': s['unmet_gates'], 'why': spec['why']})
    after = src.read_bytes() if src.exists() else None
    rows.append({'case': 'sweep_writes_no_receipt',
                 'injected': 'the sweep must audit the copy in memory, not edit the evidence',
                 'expect': 'clean', 'discriminated': before is not None and before == after,
                 'why': 'the first version of this mode overwrote the receipt it was auditing'})
    all_ok = all(r['discriminated'] for r in rows)
    (pathlib.Path(out) / 'gate-falsification.json').write_text(
        json.dumps({'cases': rows, 'all_discriminated': all_ok,
                    'harness_sha256': sha(pathlib.Path(__file__).resolve())},
                   indent=2, ensure_ascii=False, sort_keys=True) + '\n')
    undiscriminated = [r['case'] for r in rows if not r['discriminated']]
    print(json.dumps({'all_discriminated': all_ok, 'cases': len(rows),
                      'undiscriminated': undiscriminated}, indent=2))
    return 0 if all_ok else 1


# --------------------------------------------------------------------- entry
def git_out(*args):
    return subprocess.run(['git'] + list(args), capture_output=True, text=True).stdout.strip()


def build_identity(a):
    rust = pathlib.Path(a.rust).resolve()
    alias = rust.parent / 'livekit-server'
    ref = REPO / 'references' / 'livekit-stable'
    return {
        'binaries': {
            'go': {'path': str(pathlib.Path(a.go).resolve()), 'sha256': sha(a.go),
                   'identity': 'oracle (LiveKit v1.13.10 @ e5655ac, built to /tmp/c99-build)'},
            'rust': {'path': str(rust), 'sha256': sha(rust),
                     'identity': 'committed production binary of this repository'},
            'rust_alias': {'path': str(alias), 'sha256': sha(alias) if alias.exists() else None,
                           'identity': 'livekit-server-named alias of the same Rust build'},
        },
        'reference': {'repo': str(ref), 'go_checkout_commit': git_out('-C', str(ref), 'rev-parse', 'HEAD'),
                      'dirty_paths': len([l for l in git_out('-C', str(ref), 'status', '--porcelain').splitlines() if l]),
                      'fixed_commit': FIXED_GO_COMMIT,
                      'go_source_reads': ['cmd/server/commands.go:41-102',
                                          'cmd/server/commands.go:104-167',
                                          'cmd/server/commands.go:170-271',
                                          'cmd/server/main.go:139-186',
                                          'pkg/service/xtwirp mount (pkg/service/server.go:127-131)']},
        'rust_source_reads': ['crates/lk-config/src/cli.rs', 'crates/lk-compat/src/twirp/room_service.rs:20',
                              'crates/livekit/src/main.rs'],
        'harness_sha256_at_start': sha(pathlib.Path(__file__).resolve()),
        'harness_sha256': sha(pathlib.Path(__file__).resolve()),
        'python': sys.version.split()[0], 'platform': os.uname().sysname + ' ' + os.uname().machine,
        'ip_salt': IP_SALT, 'covers': COVERS, 'tier': 'main_server_consumer',
        'argv_sanitization': ['LIVEKIT_*/LIVECLOUD_*/OTEL_* removed from the child env',
                             'the fixture secret replaced by <SECRET> in every recorded argv',
                             'generated API secrets replaced by <GEN_SECRET> in the stored stream',
                             'machine addresses replaced by <IP#hash> keyed on the committed salt'],
    }


def allocate_ctx(work):
    ports = {'redis_port': free_port(), 'closed_port': free_port(),
             'http_go_port': free_port(), 'http_rust_port': free_port(),
             'tcp_go_port': free_port(), 'tcp_rust_port': free_port(),
             'udp_go_port': free_port(), 'udp_rust_port': free_port()}
    for name, value in ports.items():
        if value in OFF_LIMITS:
            raise SystemExit(f'refusing to allocate an off-limit port: {name}={value}')
    ports['env'] = stripped_env()
    ports['room'] = 'c101-room'
    return write_fixtures(work, ports)


def fixture_evidence(work, names):
    out = {}
    for stem, filename in names:
        path = work / filename
        raw = path.read_text()
        out[stem] = str(path)
        out[stem + '_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        out[stem + '_text'] = raw.replace(SECRET, '<SECRET>').replace(WRONG_SECRET, '<SECRET>')
    return out


def offline_main(src, mode):
    refuse_filed_slot(src)
    doc = json.loads(src.read_text())
    if mode == 'report':
        result = finish(doc, src.parent, live=False)
        print(json.dumps(result['summary'], indent=2))
        return 0 if result['all_ok'] else 1
    return selftest_gate(doc, src.parent)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--go', help='path to the fixed Go oracle binary')
    ap.add_argument('--rust', help='path to the committed Rust binary')
    ap.add_argument('--output', help='directory for execution.json / BATCH.json')
    ap.add_argument('--report', help='re-derive the verdict from a recorded execution.json')
    ap.add_argument('--selftest-gate', help='re-derive under named mutations')
    ap.add_argument('--skip-store', action='store_true',
                    help='drop the owned-redis rows and the registry legs')
    a = ap.parse_args()

    if a.report or a.selftest_gate:
        src = pathlib.Path(a.report or a.selftest_gate).resolve()
        if not src.exists():
            raise SystemExit(f'no such receipt: {src}')
        return offline_main(src, 'report' if a.report else 'selftest-gate')
    for name in ('go', 'rust', 'output'):
        if getattr(a, name) is None:
            ap.error(f'--{name} is required unless --report/--selftest-gate is used')

    out = pathlib.Path(a.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    work = out / 'work'
    work.mkdir(exist_ok=True)
    manifest = build_identity(a)
    ctx = allocate_ctx(work)
    manifest['ports'] = {k: v for k, v in ctx.items() if k.endswith('_port')}
    manifest['fixtures'] = fixture_evidence(work, [
        ('kf_yaml', 'kf.yaml'), ('kf_b64', 'kf-b64.txt'), ('cfg_ports', 'cfg-ports.yaml'),
        ('cfg_keys', 'cfg-keys.yaml'), ('cfg_closed', 'cfg-closed.yaml'),
        ('cfg_redis', 'cfg-redis.yaml'), ('daemon_go', 'daemon-go.yaml'),
        ('daemon_rust', 'daemon-rust.yaml')])
    manifest['fixtures']['ports'] = manifest['ports']
    manifest['listeners_before'] = listeners(list(OFF_LIMITS))
    manifest['off_limits'] = list(OFF_LIMITS)
    manifest['profiles'] = []
    manifest['legs'] = []
    manifest['daemon_ids'] = []
    manifest['daemons'] = {}
    manifest['live_run'] = True
    manifest['skip_store'] = a.skip_store

    redis_proc = None
    daemons = {}
    try:
        for prof in PROFILES:
            if prof['phase'] != 'cli':
                continue
            if a.skip_store and 'closed' in prof['id']:
                continue
            manifest['profiles'].append(run_profile(prof, manifest['binaries'], ctx))
        if not a.skip_store:
            redis_proc = owned_redis(work, ctx['redis_port'])
            manifest['redis'] = {'owned': True, 'port': ctx['redis_port'],
                                 'dbsize_before': redis_cmd(ctx['redis_port'], 'DBSIZE')}
            for prof in PROFILES:
                if prof['phase'] == 'nodes_empty':
                    manifest['profiles'].append(run_profile(prof, manifest['binaries'], ctx))
            for side in ('go', 'rust'):
                proc, log, ready, log_path = launch(
                    manifest['binaries'][side]['path'], ctx[f'daemon_{side}'],
                    ctx[f'http_{side}_port'], work / f'daemon-{side}.log')
                daemons[side] = {'proc': proc, 'log': log, 'ready': ready, 'port':
                                 ctx[f'http_{side}_port'], 'log_path': log_path}
                info = daemon_node_ids(log_path)
                manifest['daemons'][side] = {'pid': proc.pid, 'ready': ready,
                                             'http_port': ctx[f'http_{side}_port'],
                                             'log_sha256': hashlib.sha256(
                                                 log_path.read_bytes()).hexdigest(),
                                             'announced_node_ids': info['ids'],
                                             'announced_starting_line': info['starting_line'],
                                             'config': ctx[f'daemon_{side}'].name,
                                             'tcp_port': ctx[f'tcp_{side}_port'],
                                             'udp_port': ctx[f'udp_{side}_port']}
            ctx['daemon_ids'] = sorted({i for d in manifest['daemons'].values()
                                        for i in d['announced_node_ids']})
            manifest['daemon_ids'] = ctx['daemon_ids']
            for prof in PROFILES:
                if prof['phase'] == 'nodes_registered':
                    manifest['profiles'].append(run_profile(prof, manifest['binaries'], ctx))
            manifest['legs'] += run_legs(manifest['binaries'], ctx, daemons, work)
            manifest['legs'] += run_key_legs(manifest['binaries'], ctx)
            for side, d in daemons.items():
                d['log'].flush()
                manifest['daemons'][side]['stop'] = stop(d['proc'])
                manifest['daemons'][side]['held_port_after_stop'] = (
                    listeners([d['port']])[str(d['port'])] > 0)
            for prof in PROFILES:
                if prof['phase'] == 'after_exit':
                    manifest['profiles'].append(run_profile(prof, manifest['binaries'], ctx))
            manifest['legs'] += run_post_exit_legs(manifest['binaries'], ctx)
            manifest['redis']['dbsize_after'] = redis_cmd(ctx['redis_port'], 'DBSIZE')
    finally:
        for side, d in daemons.items():
            if d['proc'].poll() is None:
                stop(d['proc'])
            d['log'].close()
        if redis_proc is not None and redis_proc.poll() is None:
            os.killpg(redis_proc.pid, signal.SIGTERM)
            try:
                redis_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(redis_proc.pid, signal.SIGKILL)
        manifest['listeners_after'] = listeners(list(OFF_LIMITS))
        manifest['redis'] = manifest.get('redis', {'owned': False})
        for cfg_path in work.glob('*.yaml'):
            cfg_path.chmod(0o600)
    result = finish(manifest, out, live=True)
    print(json.dumps(result['summary'], indent=2, ensure_ascii=False))
    return 0 if result['all_ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
