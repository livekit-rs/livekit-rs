#!/usr/bin/env python3
"""SPEC-0077 C25 — launched-CLI `list-nodes` config-notice + Redis mode-log paired
Go/Rust differential.

One ledger leaf is named: CLI:list-nodes.ConfigNoticeAndModeLog.

Upstream `list-nodes` (cmd/server/commands.go:170 `listNodes`) first calls
`getConfig` (cmd/server/main.go:196-246), which installs the logger
(`config.InitLoggerFromConfig`, main.go:208) and, when `conf.Development`, logs
`starting in development mode` (main.go:211) and — only when no keys were
provided — `no keys provided, using placeholder keys` with the devkey/secret
fields (main.go:214). It then builds the router, whose Redis client constructor
`GetRedisClient` (protocol/redis/redis.go) logs exactly one `connecting to redis`
mode line BEFORE the Ping: Sentinel (`redis.go:101`, fields sentinel/addr/
masterName), else Cluster (`redis.go:132`, cluster/addr), else simple
(`redis.go:145`, simple/addr). Sentinel wins over Cluster wins over Address.

The Rust mirror is wired on the SAME CLI path: livecloud lib.rs:109-127
(`EntryAction::ListNodes`) runs `logging::init` + `logging::replay_config_notices`
(logging.rs:306, callers cmd/server/main.go:211/214) before `cli_nodes::list_nodes`,
which builds the Redis client through `redis_tls::redis_endpoint` ->
`log_redis_mode` (redis_tls.rs:186, callers protocol/redis/redis.go:101/132/145).
The zap encoder shortens the caller to its last two path components
(zap.rs:206 `short_caller`, unit-tested at zap.rs:484), so `cmd/server/main.go:211`
renders as `server/main.go:211` and `protocol/redis/redis.go:145` as
`redis/redis.go:145`, byte-identical to the Go ShortCallerEncoder.

Observable (deterministic, no Redis infrastructure required): the config-notice
and `connecting to redis` mode lines on STDERR, emitted before the dial. The
vectors point Redis at closed loopback ports so the Ping fails fast and the
command exits non-zero — the dial OUTCOME, the node table and the go-redis
internal `redis:` diagnostics are OTHER items (Redis.TlsServerStartupConsumer,
the sentinel/cluster enabled-capability leaves, Redis.SentinelDiscoveryDiagnostics)
and are deliberately OUT OF SCOPE here. This harness compares only the notice +
mode-log surface that this leaf names.

Normalization (scope limit, inherited from the proven c22 cli-redis-startup-tls
harness): only the VALUE of zap's wall-clock field (`ts`/`time`) is masked, keeping
the key name and the JSON shape of the value (number -> <number>, string ->
<string>), so a key rename or a number-to-string encoder change stays a real
divergence. Two comparisons are made per vector:
  1. the full clock-masked line BYTES (catches zap field order / rendering / caller),
  2. the parsed JSON payload with the clock key removed (catches semantic fields).
No comparison field is deleted; the dial error / node table / diagnostics are
explicit scope bounds, recorded raw beside the receipt.

Non-vacuity: each vector declares the EXACT notice+mode payload sequence the Go
oracle must emit (derived from its development/keys/topology semantics). A side
passes its contract only if it emits that sequence; Go==Rust alone would be
vacuous if both emitted nothing. The disabled-consumer positive control mutant
(no-op `replay_config_notices` + `log_redis_mode`) drops the notice and mode lines
and FAILS exactly these vectors while the Go oracle stays correct.

Configs are emitted as YAML strings (no PyYAML dependency). Every original exit
code is preserved; no pipe filters the result. Collect-all-then-assert.
"""
import argparse, hashlib, json, os, pathlib, re, subprocess, tempfile

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--output', required=True, type=pathlib.Path)
p.add_argument('--timeout', type=float, default=60.0)
a = p.parse_args()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)

COVERS = ['CLI:list-nodes.ConfigNoticeAndModeLog']
GO_ORACLE_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'

CLOCK_FIELD = re.compile(r'"(ts|time)":("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)')
NOTICE_KEYS = ('starting in development mode', 'no keys provided, using placeholder keys',
               'connecting to redis')

# Each vector: development flag, whether keys are supplied, and the Redis topology.
# `expect` is derived below from these semantics — the Go oracle contract.
VECTORS = [
    {'name': 'dev-simple-nokeys', 'development': True, 'keys': False,
     'address': '127.0.0.1:1'},
    {'name': 'dev-simple-withkeys', 'development': True, 'keys': True,
     'address': '127.0.0.1:1'},
    {'name': 'prod-simple', 'development': False, 'keys': True,
     'address': '127.0.0.1:1'},
    {'name': 'dev-sentinel-master', 'development': True, 'keys': False,
     'sentinel': ['127.0.0.1:1', '127.0.0.1:2'], 'master': 'mymaster'},
    {'name': 'dev-sentinel-empty-master', 'development': True, 'keys': False,
     'sentinel': ['127.0.0.1:1', '127.0.0.1:2'], 'master': ''},
    {'name': 'dev-sentinel-precedence', 'development': True, 'keys': False,
     'sentinel': ['127.0.0.1:1', '127.0.0.1:2'], 'master': 'mymaster',
     'address': '127.0.0.1:9'},
    {'name': 'dev-cluster', 'development': True, 'keys': False,
     'cluster': ['127.0.0.1:1', '127.0.0.1:2', '127.0.0.1:3']},
    {'name': 'dev-cluster-precedence', 'development': True, 'keys': False,
     'cluster': ['127.0.0.1:1', '127.0.0.1:2', '127.0.0.1:3'],
     'address': '127.0.0.1:9'},
    {'name': 'prod-sentinel', 'development': False, 'keys': True,
     'sentinel': ['127.0.0.1:1', '127.0.0.1:2'], 'master': 'mymaster'},
    {'name': 'prod-cluster', 'development': False, 'keys': True,
     'cluster': ['127.0.0.1:1', '127.0.0.1:2', '127.0.0.1:3']},
]


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def base_env():
    return {k: v for k, v in os.environ.items()
            if not k.startswith(('LIVEKIT_', 'LIVECLOUD_'))}


def config_yaml(v):
    """YAML string config: JSON zap logging at info so the notice + mode lines are
    rendered in the production encoder order both binaries share."""
    lines = ['development: %s' % ('true' if v['development'] else 'false'),
             'logging:', '  json: true', '  level: info']
    if v['keys']:
        lines += ['keys:', '  devkey: secretsecretsecretsecretsecretsecret']
    lines.append('redis:')
    if v.get('sentinel'):
        lines.append('  sentinel_addresses:')
        lines += ['  - %s' % s for s in v['sentinel']]
        lines.append('  sentinel_master_name: "%s"' % v.get('master', ''))
    if v.get('cluster'):
        lines.append('  cluster_addresses:')
        lines += ['  - %s' % s for s in v['cluster']]
    if v.get('address'):
        lines.append('  address: %s' % v['address'])
    return '\n'.join(lines) + '\n'


def expected_payloads(v):
    """The exact notice+mode JSON payload sequence (clock key excluded) the Go
    oracle must emit for this vector, derived from upstream semantics. This is the
    non-vacuous contract: a side that emits nothing, or the wrong branch, fails."""
    out = []
    if v['development']:
        out.append({'level': 'info', 'logger': 'livekit', 'caller': 'server/main.go:211',
                    'msg': 'starting in development mode'})
        if not v['keys']:
            out.append({'level': 'info', 'logger': 'livekit', 'caller': 'server/main.go:214',
                        'msg': 'no keys provided, using placeholder keys',
                        'API Key': 'devkey', 'API Secret': 'secret'})
    if v.get('sentinel'):
        out.append({'level': 'info', 'logger': 'livekit', 'caller': 'redis/redis.go:101',
                    'msg': 'connecting to redis', 'sentinel': True,
                    'addr': list(v['sentinel']), 'masterName': v.get('master', '')})
    elif v.get('cluster'):
        out.append({'level': 'info', 'logger': 'livekit', 'caller': 'redis/redis.go:132',
                    'msg': 'connecting to redis', 'cluster': True, 'addr': list(v['cluster'])})
    else:
        out.append({'level': 'info', 'logger': 'livekit', 'caller': 'redis/redis.go:145',
                    'msg': 'connecting to redis', 'simple': True, 'addr': v.get('address', '')})
    return out


def mask_clock(line):
    def repl(m):
        shape = 'string' if m.group(2)[0] == '"' else 'number'
        return '"%s":<%s>' % (m.group(1), shape)
    return CLOCK_FIELD.sub(repl, line)


def notice_mode_lines(text):
    """The config-notice + Redis mode lines, in emission order. go-redis internal
    `redis:` diagnostics, the dial-error stdout and the node table never match these
    msg keys, so they are excluded by construction (they are other items)."""
    return [l.strip() for l in text.splitlines()
            if any(k in l for k in NOTICE_KEYS)]


def masked_bytes(text):
    return [mask_clock(l) for l in notice_mode_lines(text)]


def parsed_payloads(text):
    out = []
    for l in notice_mode_lines(text):
        try:
            d = json.loads(l)
        except json.JSONDecodeError:
            out.append({'unparsed': l})
            continue
        d.pop('ts', None)
        d.pop('time', None)
        out.append(d)
    return out


def invoke(binary, v, work):
    cfg = work / 'c.yaml'
    cfg.write_text(config_yaml(v))
    outpath, errpath = work / 'stdout.txt', work / 'stderr.txt'
    timed_out = False
    with outpath.open('wb') as out, errpath.open('wb') as err:
        proc = subprocess.Popen([str(binary), 'list-nodes', '--config', str(cfg)],
                                stdout=out, stderr=err, env=base_env(),
                                stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            rc = proc.wait(timeout=a.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, 9)
            rc = proc.wait()
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait()
    err_text = errpath.read_text(errors='replace')
    return {'config_sha256': sha(cfg), 'exit': rc, 'timed_out': timed_out,
            'stdout': outpath.read_text(errors='replace'), 'stderr': err_text,
            'notice_mode_raw': notice_mode_lines(err_text),
            'notice_mode_masked': masked_bytes(err_text),
            'parsed': parsed_payloads(err_text)}


manifest = {'status': 'INCOMPLETE',
            'scope': ('launched-CLI list-nodes CONFIG-NOTICE + REDIS MODE-LOG surface: '
                      'getConfig development-mode notices (main.go:211 starting in development '
                      'mode, main.go:214 no keys provided/placeholder keys) and the single '
                      'GetRedisClient connecting-to-redis mode line (redis.go:101 sentinel / '
                      ':132 cluster / :145 simple, with addr/masterName), emitted before the '
                      'dial. Compares clock-masked line bytes AND parsed payload. NOT a dial '
                      'outcome / node table / go-redis diagnostics claim (other items).'),
            'binaries': {}, 'vectors': {}, 'items': COVERS, 'timeout_s': a.timeout}
for side in ('go', 'rust'):
    b = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(b), 'sha256': sha(b)}


def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')


save()

# Collect every vector on both sides first, then assert (no fail-fast masking).
for v in VECTORS:
    name = v['name']
    expect = expected_payloads(v)
    entry = {'vector': name, 'covers': COVERS, 'development': v['development'],
             'keys': v['keys'], 'expect_parsed': expect}
    for side, binary in (('go', a.go), ('rust', a.rust)):
        with tempfile.TemporaryDirectory(prefix=f'cn-{side}-') as d:
            entry[side] = invoke(binary.resolve(), v, pathlib.Path(d))
    # Non-vacuous per-side contract: emit exactly the expected notice+mode sequence.
    entry['go_contract_ok'] = (entry['go']['parsed'] == expect and not entry['go']['timed_out'])
    entry['rust_contract_ok'] = (entry['rust']['parsed'] == expect and not entry['rust']['timed_out'])
    entry['cross_side_bytes_equal'] = entry['go']['notice_mode_masked'] == entry['rust']['notice_mode_masked']
    entry['cross_side_parsed_equal'] = entry['go']['parsed'] == entry['rust']['parsed']
    entry['match'] = (entry['go_contract_ok'] and entry['rust_contract_ok']
                      and entry['cross_side_bytes_equal'] and entry['cross_side_parsed_equal'])
    manifest['vectors'][name] = entry
    save()
    print(f'{name:28s} match={entry["match"]} go_ok={entry["go_contract_ok"]} '
          f'rust_ok={entry["rust_contract_ok"]} bytes_eq={entry["cross_side_bytes_equal"]}', flush=True)

V = manifest['vectors']


def has_line(name, side, caller, msg):
    return any(d.get('caller') == caller and d.get('msg') == msg for d in V[name][side]['parsed'])


def mode_branch(name, side):
    for d in V[name][side]['parsed']:
        if d.get('msg') == 'connecting to redis':
            for b in ('sentinel', 'cluster', 'simple'):
                if d.get(b) is True:
                    return b
    return None


controls = {}
for side in ('go', 'rust'):
    # Development-mode notice present in dev, absent in prod.
    controls[f'dev_notice_present_{side}'] = has_line('dev-simple-nokeys', side, 'server/main.go:211', 'starting in development mode')
    controls[f'prod_no_dev_notice_{side}'] = not has_line('prod-simple', side, 'server/main.go:211', 'starting in development mode')
    # Placeholder-keys notice only when dev AND no keys.
    controls[f'placeholder_keys_when_nokeys_{side}'] = has_line('dev-simple-nokeys', side, 'server/main.go:214', 'no keys provided, using placeholder keys')
    controls[f'no_placeholder_when_keys_{side}'] = not has_line('dev-simple-withkeys', side, 'server/main.go:214', 'no keys provided, using placeholder keys')
    # The three Redis mode branches.
    controls[f'simple_mode_{side}'] = mode_branch('dev-simple-nokeys', side) == 'simple'
    controls[f'sentinel_mode_{side}'] = mode_branch('dev-sentinel-master', side) == 'sentinel'
    controls[f'cluster_mode_{side}'] = mode_branch('dev-cluster', side) == 'cluster'
    # masterName carried, including the empty-string boundary.
    controls[f'sentinel_master_set_{side}'] = any(d.get('masterName') == 'mymaster' for d in V['dev-sentinel-master'][side]['parsed'])
    controls[f'sentinel_master_empty_{side}'] = any(d.get('masterName') == '' for d in V['dev-sentinel-empty-master'][side]['parsed'])
    # Sentinel/Cluster precedence over a also-set address.
    controls[f'sentinel_precedence_{side}'] = mode_branch('dev-sentinel-precedence', side) == 'sentinel'
    controls[f'cluster_precedence_{side}'] = mode_branch('dev-cluster-precedence', side) == 'cluster'
    # The mode line is emitted even though the dial fails (pre-Ping consumer).
    controls[f'mode_line_before_failed_dial_{side}'] = (
        mode_branch('prod-simple', side) == 'simple' and V['prod-simple'][side]['exit'] != 0)

failed_vectors = [n for n, e in V.items() if not e['match']]
failed_controls = [k for k, val in controls.items() if not val]
all_match = not failed_vectors and not failed_controls
manifest['cross_profile_controls'] = controls
manifest['failed_vectors'] = failed_vectors
manifest['failed_controls'] = failed_controls
manifest['case_count'] = len(VECTORS)
manifest['status'] = 'PASS_LAUNCHED_CLI_CONFIG_NOTICE' if all_match else 'FAIL'
save()

batch = {'status': manifest['status'], 'case_count': len(VECTORS), 'items': COVERS,
         'profiles': {n: {'match': e['match'], 'go_contract_ok': e['go_contract_ok'],
                          'rust_contract_ok': e['rust_contract_ok'],
                          'cross_side_bytes_equal': e['cross_side_bytes_equal'],
                          'cross_side_parsed_equal': e['cross_side_parsed_equal']}
                      for n, e in V.items()},
         'cross_profile_controls': controls,
         'failed_profiles': failed_vectors, 'failed_controls': failed_controls,
         'binaries': manifest['binaries'],
         'input_identity_stable': manifest['binaries']['go']['sha256'] == GO_ORACLE_SHA,
         'harness_sha256': sha(pathlib.Path(__file__).resolve())}
(a.output / 'BATCH.json').write_text(json.dumps(batch, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({'status': manifest['status'], 'case_count': len(VECTORS),
                  'failed_profiles': failed_vectors, 'failed_controls': failed_controls}))
raise SystemExit(0 if all_match else 1)