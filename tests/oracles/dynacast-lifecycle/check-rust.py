#!/usr/bin/python3 -B
"""C59 compiled Rust reader corruption runner: no builds or synthesized Rust output.

--reference is actual name-projected Go JSONL; --raw-reference is the actual raw
Go artifact. Both must pass independent strict Python checks before execution.
--list-mutations only lists controls. --self-test checks actual artifacts and
mutations in memory; it never starts Rust or writes files. Campaign receipts
retain original process exits, logs and independent output, including failures.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
TEST = 'regression::dynacast_notify::fixed_go_dynacast_lifecycle_differential'
TIMES = [0, 100, 200, 300, 400, 1099, 1100, 1101, 9999, 10000, 10099, 10100, 10199, 10200, 10300]
SCENARIOS = ['restart-settled', 'restart-same', 'restart-pending', 'restart-repeat',
             'resend-high', 'resend-low', 'resend-pending', 'resend-restart', 'resend-muted']
MATRIX = [(scenario, n) for scenario in SCENARIOS for n in (1, 2)]
EMIT = {'restart-settled': (10100,), 'restart-same': (200,),
        'restart-pending': (1100,), 'restart-repeat': (10200,), 'resend-high': (),
        'resend-low': (100, 200), 'resend-pending': (200, 1100),
        'resend-restart': (10100, 10200), 'resend-muted': (300, 400)}
EQUALITY = r'assertion `left == right` failed'


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def safe_hash(path):
    try:
        return file_hash(path)
    except OSError:
        return None


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key: ' + key)
        result[key] = value
    return result


def same(actual, wanted, path='rows'):
    # In particular, bool is not int, and an integral float is not an integer.
    require(type(actual) is type(wanted), path + ': wrong type')
    if isinstance(wanted, dict):
        require(actual.keys() == wanted.keys(), path + ': fields differ')
        for key in wanted:
            same(actual[key], wanted[key], path + '.' + key)
    elif isinstance(wanted, list):
        require(len(actual) == len(wanted), path + ': count differs')
        for i, value in enumerate(wanted):
            same(actual[i], value, path + '[' + str(i) + ']')
    else:
        require(actual == wanted, path + ': value differs')


def decode(data):
    def invalid_constant(value):
        raise ValueError('invalid JSON constant: ' + value)
    return [json.loads(line, object_pairs_hook=unique, parse_constant=invalid_constant)
            for line in data.decode('utf-8').splitlines()]


def encode(rows, duplicate_key=None):
    def render(value, path):
        if isinstance(value, dict):
            parts = []
            for key, item in value.items():
                item_path = path + (key,)
                part = json.dumps(key) + ':' + render(item, item_path)
                parts.append(part)
                if item_path == duplicate_key:
                    parts.append(part)
            return '{' + ','.join(parts) + '}'
        if isinstance(value, list):
            return '[' + ','.join(render(item, path + (i,))
                                  for i, item in enumerate(value)) + ']'
        return json.dumps(value, separators=(',', ':'), allow_nan=False)
    return ('\n'.join(render(row, (i,)) for i, row in enumerate(rows)) + '\n').encode()


def get_at(rows, path):
    value = rows
    for key in path:
        value = value[key]
    return value


def path_text(path):
    return 'rows' + ''.join('[' + str(key) + ']' if isinstance(key, int) else '.' + key
                            for key in path)


def first_layer(scenario, at):
    if scenario in ('restart-settled', 'resend-restart') and 100 <= at < 10100:
        return 2
    if scenario == 'restart-same' and 100 <= at < 200:
        return 2
    if scenario == 'restart-pending' and at < 1100:
        return 2
    if scenario == 'restart-repeat' and 100 <= at < 10200:
        return 2
    if scenario == 'resend-high':
        return 2
    if scenario == 'resend-pending' and at < 1100:
        return 1
    return 0


def snapshot(n, first):
    return {'codecs': [{'codec': name, 'qualities': [
        {'quality': q, 'enabled': q <= spatial} for q in range(3)]}
        for name, spatial in zip(['vp8', 'vp9'][:n], [first, 2][:n])]}


def expected_contract():
    # These expectations are only an in-memory validator, never a Go artifact
    # or Rust output. Warmup callbacks are excluded; receiver samples are not.
    rows = []
    for scenario, n in MATRIX:
        observations = []
        for at in TIMES:
            first = first_layer(scenario, at)
            observations.append({'at_ms': at,
                                 'events': [snapshot(n, first)] if at in EMIT[scenario] else [],
                                 'expected_layers': [
                                     {'codec': name, 'spatial': spatial}
                                     for name, spatial in zip(['vp8', 'vp9'][:n], [first, 2][:n])]})
        rows.append({'scenario': scenario, 'codec_count': n, 'pause_ms': 1000,
                     'observations': observations})
    return rows


def project(rows, names):
    result = copy.deepcopy(rows)
    for row in result:
        for observation in row['observations']:
            entries = observation['expected_layers'] + [
                codec for event in observation['events'] for codec in event['codecs']]
            for entry in entries:
                require(type(entry['codec']) is str and entry['codec'] in names,
                        'unexpected codec in name-only projection')
                entry['codec'] = names[entry['codec']]
    return result


def counts(rows):
    observations = [o for row in rows for o in row['observations']]
    events = [e for o in observations for e in o['events']]
    codecs = [c for e in events for c in e['codecs']]
    return {'rows': len(rows), 'checkpoints': len(observations), 'events': len(events),
            'empty_event_checkpoints': sum(not o['events'] for o in observations),
            'codec_snapshots': len(codecs), 'qualities': sum(len(c['qualities']) for c in codecs),
            'receiver_layer_readings': sum(len(o['expected_layers']) for o in observations)}


def load_references(raw_path, reference_path):
    raw, reference = raw_path.read_bytes(), reference_path.read_bytes()
    raw_rows = decode(raw)
    names = {'video/VP8': 'vp8', 'video/VP9': 'vp9'}
    contract = expected_contract()
    same(raw_rows, project(contract, {value: key for key, value in names.items()}))
    # Use the existing Go oracle's public strict loader as an additional check,
    # not as the source of this runner's expected vectors or mutation results.
    validator = runpy.run_path(str(HERE / 'validate.py'))
    same(validator['load'](raw), raw_rows)
    projected = project(raw_rows, names)
    same(project(projected, {value: key for key, value in names.items()}), raw_rows)
    rows = decode(reference)
    same(rows, projected)
    same(rows, contract)
    return rows, {'raw_reference_sha256': digest(raw), 'reference_sha256': digest(reference),
                  'raw_strict_loader': 'local strict contract plus validate.load(actual raw bytes)',
                  'projection_name_only_verified': True, 'reference_counts': counts(rows)}


def mutation_plan():
    plan = []
    observation = (1, 'observations', 11)  # settled restart, two-codec timer commit
    event = observation + ('events', 0)
    codec = event + ('codecs', 0)
    quality = codec + ('qualities', 0)
    layers = observation + ('expected_layers',)
    layer = layers + (0,)

    def add(name, path, action, value=None, kind='value', rust=EQUALITY,
            check_path=None, intent=None):
        check_path = path if check_path is None else check_path
        if kind == 'json':
            python = 'JSONDecodeError'
        elif kind == 'duplicate-key':
            python = '^' + re.escape('duplicate JSON key: ' + path[-1]) + '$'
        elif kind == 'order':
            python = '^' + re.escape(path_text(check_path)) + r'.*: value differs$'
        else:
            suffix = {'value': 'value differs', 'type': 'wrong type',
                      'fields': 'fields differ', 'count': 'count differs'}[kind]
            python = '^' + re.escape(path_text(check_path) + ': ' + suffix) + '$'
        plan.append({'name': name, 'path': path, 'action': action, 'value': value,
                     'python_kind': kind, 'expected_python_rejection': python,
                     'expected_rust_rejection': rust, 'intent': intent or name})

    def typed(name, path, value, expected_type):
        add(name, path, 'set', value, kind='type',
            rust=r'invalid type:[^\n]*expected ' + expected_type)

    # Every field at every object depth: missing, duplicate JSON key, unknown
    # field, and wrong object type. Duplicate keys target exact nested paths.
    objects = [
        ('row', (0,), ['scenario', 'codec_count', 'pause_ms', 'observations'], 'MuteTrace'),
        ('observation', observation, ['at_ms', 'events', 'expected_layers'], 'MuteObservation'),
        ('event', event, ['codecs'], 'Snapshot'),
        ('codec', codec, ['codec', 'qualities'], 'CodecSnapshot'),
        ('quality', quality, ['quality', 'enabled'], 'Quality'),
        ('expected-layer', layer, ['codec', 'spatial'], 'ExpectedLayer'),
    ]
    for name, path, fields, struct in objects:
        add(name + '-unknown-field', path + ('unexpected',), 'set', 0, kind='fields',
            check_path=path, rust=r'unknown field `unexpected`')
        typed(name + '-object-null', path, None, 'struct ' + struct)
        for field in fields:
            field_path = path + (field,)
            add(name + '-missing-' + field, field_path, 'delete', kind='fields',
                check_path=path, rust=r'missing field `' + field + '`')
            add(name + '-duplicate-key-' + field, field_path, 'duplicate-key',
                kind='duplicate-key', rust=r'duplicate field `' + field + '`')

    for name, path, rust_type in [
        ('codec-count', (0, 'codec_count'), 'usize'), ('pause', (0, 'pause_ms'), 'u64'),
        ('timestamp', observation + ('at_ms',), 'u64'),
        ('quality', quality + ('quality',), 'i32'), ('spatial', layer + ('spatial',), 'i32'),
    ]:
        for suffix, value in [('bool', True), ('float', 1.0), ('string', '1'), ('null', None)]:
            typed(name + '-' + suffix, path, value, rust_type)
        if rust_type != 'i32':
            add(name + '-negative', path, 'set', -1,
                rust=r'invalid value:[^\n]*expected ' + rust_type)
    for name, path in [('scenario', (0, 'scenario')), ('event-codec', codec + ('codec',)),
                       ('layer-codec', layer + ('codec',))]:
        for suffix, value in [('number', 8), ('bool', True), ('null', None)]:
            typed(name + '-' + suffix, path, value, 'a string')
    for suffix, value in [('number', 1), ('string', 'true'), ('null', None)]:
        typed('enabled-' + suffix, quality + ('enabled',), value, 'a boolean')
    for name, path, value in [
        ('scenario-wrong', (0, 'scenario'), 'not-a-scenario'),
        ('codec-count-wrong', (0, 'codec_count'), 3), ('pause-wrong', (0, 'pause_ms'), 0),
        ('timestamp-wrong', observation + ('at_ms',), 10101),
        ('quality-out-of-range', quality + ('quality',), 3),
        ('spatial-off-not-low', layer + ('spatial',), -1),
        ('enabled-flipped', quality + ('enabled',), False),
    ]:
        add(name, path, 'set', value)
    for name, path in [('event-codec', codec + ('codec',)), ('layer-codec', layer + ('codec',))]:
        for suffix, value in [('raw-MIME', 'video/VP8'), ('case', 'VP8'), ('unknown', 'av1')]:
            add(name + '-' + suffix, path, 'set', value)

    for name, path in [
        ('row', ()), ('observation', (0, 'observations')),
        ('event', observation + ('events',)), ('codec', event + ('codecs',)),
        ('quality', codec + ('qualities',)), ('expected-layers', layers),
    ]:
        for action in ('drop', 'duplicate'):
            add(name + '-' + action, path, action, kind='count')
        if name != 'event':  # A one-event reversal would be vacuous.
            add(name + '-order', path, 'reverse', kind='order')
        if path:
            for suffix, value in [('null', None), ('object', {})]:
                typed(name + '-list-' + suffix, path, value, 'a sequence')
    add('event-order-across-checkpoints', (13, 'observations', 2, 'events'), 'swap',
        (13, 'observations', 6, 'events'), kind='order',
        intent='Swap distinct pending MEDIUM resend and committed LOW events, not equal snapshots')
    add('empty-checkpoint-removed', (0, 'observations'), 'remove-index', 5, kind='count',
        intent='The empty 1099 checkpoint is required, not a disposable absence of events')
    for name, action, rust in [('truncated-json', 'truncated', r'EOF while parsing'),
                               ('malformed-json', 'malformed', r'key must be a string'),
                               ('blank-json-line', 'blank-line', r'EOF while parsing a value')]:
        add(name, (), action, kind='json', rust=rust)
    add('empty-jsonl', (), 'empty', kind='count')

    # Every checkpoint in both codec counts has an event presence control.
    # Empty checkpoints receive complete, correctly typed snapshots, so failure
    # is semantic rather than an unrelated malformed-event rejection.
    for row, (scenario, n) in enumerate(MATRIX):
        for i, at in enumerate(TIMES):
            path = (row, 'observations', i, 'events')
            if at in EMIT[scenario]:
                if path == observation + ('events',):
                    continue  # Already covered by event-drop; do not repeat payloads.
                add('%s-n%d-at%d-event-omitted' % (scenario, n, at), path, 'drop', kind='count',
                    intent='Required committed/forced full-codec notification must be present')
            else:
                # A stale/early timer would commit LOW, not the temporary HIGH
                # receiver state installed by restart. Warmup and HIGH-resend
                # controls instead replay their actual initialized quality.
                forbidden_first = first_layer(scenario, at) if at == 0 or scenario == 'resend-high' else 0
                add('%s-n%d-at%d-forbidden-event' % (scenario, n, at), path, 'set',
                    [snapshot(n, forbidden_first)], kind='count',
                    intent='Silent checkpoint: no warmup, restart, HIGH resend, muted fanout or stale timer event')

    # Receiver controls independently cover restart HIGH, same-quality recovery,
    # old/shared/new deadlines and non-vacuous warm starts. No event is altered.
    for row, (scenario, n) in enumerate(MATRIX):
        checkpoints = {0, 100, 200, 1100, 10100, 10200}
        if scenario in ('restart-pending', 'resend-pending'):
            checkpoints.update((1099, 1101))
        if scenario in ('restart-settled', 'restart-repeat', 'resend-restart'):
            checkpoints.update((10000, 10099, 10199))
        if scenario == 'resend-muted':
            checkpoints.update((300, 400))
        for at in sorted(checkpoints):
            first = first_layer(scenario, at)
            add('%s-n%d-at%d-receiver-wrong' % (scenario, n, at),
                (row, 'observations', TIMES.index(at), 'expected_layers', 0, 'spatial'),
                'set', 0 if first else 2,
                intent='Receiver layer must reflect restart HIGH or the correct committed warmup/deadline state')
        if n == 2:
            add(scenario + '-untouched-vp9-not-high',
                (row, 'observations', 2, 'expected_layers', 1, 'spatial'), 'set', 0)
            for at in EMIT[scenario]:
                path = (row, 'observations', TIMES.index(at), 'events', 0, 'codecs')
                if path != event + ('codecs',):
                    add('%s-at%d-full-fanout-missing-vp9' % (scenario, at), path, 'drop', kind='count')
    for row in (12, 13):
        add('pending-resend-n%d-new-LOW-not-old-MEDIUM' % MATRIX[row][1],
            (row, 'observations', 2, 'events', 0, 'codecs', 0, 'qualities', 1, 'enabled'),
            'set', False, intent='Resend must use old committed MEDIUM, not pending LOW')
        add('pending-commit-n%d-stale-MEDIUM-not-LOW' % MATRIX[row][1],
            (row, 'observations', 6, 'events', 0, 'codecs', 0, 'qualities', 1, 'enabled'),
            'set', True, intent='1100 shared deadline must commit LOW despite the 200 resend')

    names = [control['name'] for control in plan]
    require(len(names) == len(set(names)), 'duplicate mutation name')
    for control in plan:
        re.compile(control['expected_rust_rejection'])
        if control['python_kind'] != 'json':
            re.compile(control['expected_python_rejection'])
    return plan


def mutations(rows):
    canonical = encode(rows)
    cases, hashes = [], set()
    for control in mutation_plan():
        changed = copy.deepcopy(rows)
        path, action, value = control['path'], control['action'], control['value']
        if action == 'duplicate-key':
            get_at(rows, path)
            payload = encode(rows, duplicate_key=path)
        elif action == 'truncated':
            payload = canonical[:-2] + b'\n'
        elif action == 'malformed':
            payload = b'{not-json}\n' + b'\n'.join(canonical.splitlines()[1:]) + b'\n'
        elif action == 'blank-line':
            payload = b'\n' + canonical
        elif action == 'empty':
            payload = b''
        else:
            if action in ('set', 'delete', 'swap'):
                target, key = get_at(changed, path[:-1]), path[-1]
                if action == 'set':
                    target[key] = copy.deepcopy(value)
                elif action == 'delete':
                    del target[key]
                else:
                    other, other_key = get_at(changed, value[:-1]), value[-1]
                    target[key], other[other_key] = other[other_key], target[key]
            else:
                target = get_at(changed, path)
                if action == 'drop':
                    target.pop()
                elif action == 'duplicate':
                    target.append(copy.deepcopy(target[0]))
                elif action == 'reverse':
                    target.reverse()
                elif action == 'remove-index':
                    target.pop(value)
                else:
                    raise ValueError('unknown mutation action: ' + action)
            payload = encode(changed)
        payload_hash = digest(payload)
        require(payload != canonical and payload_hash not in hashes,
                'vacuous or repeated mutation: ' + control['name'])
        hashes.add(payload_hash)
        try:
            same(decode(payload), rows)
        except (ValueError, UnicodeError) as error:
            correct = (isinstance(error, json.JSONDecodeError) if control['python_kind'] == 'json'
                       else re.search(control['expected_python_rejection'], str(error)) is not None)
            require(correct, 'wrong Python rejection for %s: %s' % (control['name'], error))
            evidence = dict(control, payload_sha256=payload_hash, payload_bytes=len(payload),
                            python_rejection=str(error), python_rejection_verified=True,
                            serialized_bytes_changed=True, unique_payload=True)
            cases.append((evidence, payload))
        else:
            raise ValueError('semantically unchanged mutation: ' + control['name'])
    return cases


def run_case(binary, reference, directory, expected_code, baseline, binary_hash,
             reference_hash, rejection=None, existing=False):
    directory.mkdir()
    output = directory / 'rust.jsonl'
    sentinel = b'C59 existing output must not be overwritten\n'
    if existing:
        with output.open('xb') as stream:
            stream.write(sentinel)
    require(existing or (not output.exists() and not output.is_symlink()), 'Rust output must be fresh')
    env = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C',
           'RUST_BACKTRACE': '0', 'RUST_TEST_THREADS': '1',
           'C59_GO_JSONL': str(reference), 'C59_RUST_JSONL': str(output)}
    for key in ('LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH', 'DYLD_FALLBACK_LIBRARY_PATH'):
        if key in os.environ:
            env[key] = os.environ[key]
    command = [str(binary), TEST, '--exact', '--ignored', '--nocapture']
    result = {'name': directory.name, 'command': command, 'env': env,
              'environment_policy': 'only the recorded environment is passed',
              'cwd': str(directory), 'expected_exit_code': expected_code,
              'expected_rejection': rejection, 'existing_output_control': existing,
              'output_was_fresh': not existing, 'binary_sha256_before': safe_hash(binary),
              'reference_sha256_before': safe_hash(reference),
              'output_sha256_before': digest(sentinel) if existing else None,
              'exit_code': None, 'timed_out': False}
    stdout_path, stderr_path = directory / 'stdout.log', directory / 'stderr.log'
    with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
        if result['binary_sha256_before'] != binary_hash or result['reference_sha256_before'] != reference_hash:
            result['launch_error'] = 'input or binary changed before launch; NOT RUN'
        else:
            try:
                process = subprocess.Popen(command, cwd=str(directory), env=env,
                                           stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
                try:
                    result['exit_code'] = process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    process.kill()
                    result['exit_code'] = process.wait()
                    result['timed_out'] = True
            except OSError as error:
                result['launch_error'] = str(error)
    result['binary_sha256_after'] = safe_hash(binary)
    result['reference_sha256_after'] = safe_hash(reference)
    for label, path in [('stdout', stdout_path), ('stderr', stderr_path)]:
        result[label] = {'path': str(path), 'sha256': file_hash(path), 'bytes': path.stat().st_size}
    regular_output = output.is_file() and not output.is_symlink()
    actual = output.read_bytes() if regular_output else None
    result['output'] = {'path': str(output), 'sha256': digest(actual) if actual is not None else None,
                        'bytes': len(actual) if actual is not None else None,
                        'regular_file_not_symlink': regular_output}
    stdout_text = stdout_path.read_text(errors='replace')
    log_text = stdout_text + '\n' + stderr_path.read_text(errors='replace')
    summary = re.findall(
        r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;',
        stdout_text, re.MULTILINE)
    status = 'ok' if expected_code == 0 else 'FAILED'
    wanted = ('ok', '1', '0', '0', '0') if expected_code == 0 else ('FAILED', '0', '1', '0', '0')
    result['test_summaries'] = summary
    result['checks'] = {
        'expected_exit_code': result['exit_code'] == expected_code,
        'not_timed_out': not result['timed_out'],
        'one_test_executed': (len(re.findall(r'^running 1 test$', stdout_text, re.MULTILINE)) == 1
                              and len(summary) == 1 and summary[0][:5] == wanted
                              and re.search(r'^test ' + re.escape(TEST) + r' \.\.\. ' + status + '$',
                                            stdout_text, re.MULTILINE) is not None),
        'binary_matches_initial': result['binary_sha256_before'] == binary_hash,
        'binary_unchanged': result['binary_sha256_after'] == binary_hash,
        'reference_matches_expected': result['reference_sha256_before'] == reference_hash,
        'reference_unchanged': result['reference_sha256_after'] == reference_hash,
    }
    if rejection is not None:
        match = re.search(rejection, log_text)
        result['checks']['intended_rejection'] = match is not None
        result['rejection_evidence'] = match.group(0) if match else None
    if existing:
        result['checks']['existing_output_unchanged'] = actual == sentinel
        result['independent_output_check'] = 'existing-output refusal only; sentinel is not Rust output'
    else:
        result['checks']['independently_emitted_output'] = actual is not None and bool(actual)
        if baseline is not None:
            result['checks']['output_bytes_equal_valid'] = actual == baseline
            result['checks']['output_sha256_equal_valid'] = actual is not None and digest(actual) == digest(baseline)
        elif expected_code != 0:
            result['checks']['valid_baseline_available'] = False
        else:
            result['independent_output_check'] = 'baseline must match strictly validated actual Go artifact'
    finish_result(result)
    return result, actual


def finish_result(result):
    result['passed'] = all(result['checks'].values())
    result['status'] = 'PASS' if result['passed'] else 'FAIL'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--reference', type=Path, help='actual projected Go JSONL')
    parser.add_argument('--raw-reference', type=Path, help='actual raw Go JSONL')
    parser.add_argument('--out', type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--list-mutations', action='store_true', help='inventory only; no execution claim')
    mode.add_argument('--self-test', action='store_true', help='actual artifact/mutation checks in memory; no Rust')
    args = parser.parse_args()
    plan = mutation_plan()
    if args.list_mutations:
        print(json.dumps({'scope': 'mutation inventory only; payload checks and Rust NOT RUN',
                          'mutations': [c['name'] for c in plan], 'total': len(plan),
                          'runtime_controls': ['valid', 'valid-repeat', 'existing-output']}, indent=2))
        return 0
    inputs = [('raw_reference', args.raw_reference), ('reference', args.reference)]
    if not args.self_test:
        inputs.append(('binary', args.binary))
    for label, path in inputs:
        if path is None or not path.is_absolute() or not path.is_file():
            parser.error('--' + label.replace('_', '-') + ' must be an absolute existing file')
    if not args.self_test and not os.access(args.binary, os.X_OK):
        parser.error('--binary must be executable')
    tracked = inputs + [('validator', HERE / 'validate.py'), ('harness', Path(__file__).resolve())]
    if args.self_test:
        require(args.binary is None and args.out is None, '--self-test does not accept --binary or --out')
        before = {label: file_hash(path) for label, path in tracked}
        rows, evidence = load_references(args.raw_reference, args.reference)
        cases = mutations(rows)
        unchanged = all(safe_hash(path) == before[label] for label, path in tracked)
        require(unchanged, 'self-test inputs or support changed')
        print(json.dumps(dict(evidence, scope='Python actual artifact/mutation self-test only; Rust NOT RUN',
                              mutations=[control for control, _ in cases], total=len(cases),
                              inputs_unchanged=unchanged, passed=True), indent=2))
        return 0
    if (args.out is None or not args.out.is_absolute() or args.out.parent != Path('/tmp')
            or not args.out.name.startswith('c59-rust-') or args.out.name == 'c59-rust-'
            or args.out.exists() or args.out.is_symlink()):
        parser.error('--out must be a fresh /tmp/c59-rust-* directory')
    args.out.mkdir()
    report = {'test': TEST, 'invocation': ['/usr/bin/python3', '-B', str(Path(__file__).resolve())] + sys.argv[1:],
              'binary': str(args.binary), 'reference': str(args.reference),
              'raw_reference': str(args.raw_reference),
              'scope': 'Compiled reader only; independent Rust output required on every fresh run, including malformed Go. No build or fabricated Go/Rust output.',
              'runs': [], 'not_run': [], 'passed': False,
              'mutation_controls': plan, 'negative_controls_planned': len(plan)}

    def guard_inputs():
        require(all(safe_hash(path) == report[label + '_sha256_before'] for label, path in tracked),
                'original input, binary or support changed; remaining runs blocked')

    def execute(reference, name, code, baseline, reference_hash, rejection=None, existing=False):
        guard_inputs()
        result, actual = run_case(args.binary, reference, args.out / name, code, baseline,
                                  report['binary_sha256_before'], reference_hash, rejection, existing)
        # Retain original exit/log receipt before checking concurrent changes.
        report['runs'].append(result)
        if name.startswith('negative-'):
            result['mutation_name'] = name.split('-', 2)[2]
        result['checks']['tracked_inputs_unchanged'] = all(
            safe_hash(path) == report[label + '_sha256_before'] for label, path in tracked)
        finish_result(result)
        guard_inputs()
        require(result['checks']['reference_matches_expected'] and result['checks']['reference_unchanged'],
                'case input changed; original exit/logs retained and remaining runs blocked')
        return result, actual

    try:
        for label, path in tracked:
            report[label + '_sha256_before'] = file_hash(path)
        rows, evidence = load_references(args.raw_reference, args.reference)
        report.update(evidence)
        cases = mutations(rows)
        report['mutation_controls'] = [control for control, _ in cases]
        valid, baseline = execute(args.reference, 'valid', 0, None, report['reference_sha256_before'])
        try:
            actual_rows = decode(baseline) if baseline else []
            same(actual_rows, rows)
            report['actual_counts'] = counts(actual_rows)
            valid['checks']['complete_actual_trace'] = True
        except (ValueError, UnicodeError) as error:
            valid['checks']['complete_actual_trace'] = False
            valid['actual_validation_error'] = str(error)
        finish_result(valid)
        if valid['passed']:
            report['baseline_sha256'] = digest(baseline)
            repeat, _ = execute(args.reference, 'valid-repeat', 0, baseline, report['reference_sha256_before'])
            if repeat['passed']:
                for index, (control, payload) in enumerate(cases, 1):
                    name = control['name']
                    reference = args.out / ('mutation-%03d-%s.jsonl' % (index, name))
                    with reference.open('xb') as stream:
                        stream.write(payload)
                    execute(reference, 'negative-%03d-%s' % (index, name), 101, baseline,
                            control['payload_sha256'], control['expected_rust_rejection'])
            else:
                report['blocked_reason'] = 'fresh valid repeat failed; mutations NOT RUN'
        else:
            report['blocked_reason'] = 'valid baseline failed; repeat/mutations NOT RUN'
        # Invalid Go plus an existing output proves create_new refusal takes
        # precedence over parsing. The sentinel is explicitly not an actual trace.
        refusal_input = args.out / 'existing-output-invalid-go.jsonl'
        refusal_payload = b'{not-json}\n'
        with refusal_input.open('xb') as stream:
            stream.write(refusal_payload)
        execute(refusal_input, 'existing-output', 101, baseline, digest(refusal_payload),
                rejection=r'kind: AlreadyExists', existing=True)
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        for label, path in tracked:
            after = safe_hash(path)
            report[label + '_sha256_after'] = after
            report[label + '_unchanged'] = after is not None and after == report.get(label + '_sha256_before')
        runs = report['runs']
        negatives = [r for r in runs if r.get('mutation_name') is not None]
        executed = {r['mutation_name'] for r in negatives if r['exit_code'] is not None}
        reason = report.get('blocked_reason', report.get('error', 'execution did not reach this check'))
        report['not_run'] = [{'name': c['name'], 'status': 'NOT RUN', 'reason': reason}
                             for c in plan if c['name'] not in executed]
        for name in ['valid', 'valid-repeat', 'existing-output']:
            if not any(r['name'] == name and r['exit_code'] is not None for r in runs):
                report['not_run'].append({'name': name, 'status': 'NOT RUN', 'reason': reason})
        report['counts'] = {'runs': len(runs), 'passed': sum(r['passed'] for r in runs),
                            'valid': sum(r['name'] == 'valid' for r in runs),
                            'valid_repeat': sum(r['name'] == 'valid-repeat' for r in runs),
                            'negatives': len(negatives), 'mutations': len(plan),
                            'negatives_passed': sum(r['passed'] for r in negatives),
                            'existing_output': sum(r['existing_output_control'] for r in runs)}
        report['passed'] = ('error' not in report and not report['not_run']
                            and all(report[label + '_unchanged'] for label, _ in tracked)
                            and len(negatives) == len(plan) and len(runs) == len(plan) + 3
                            and all(r['passed'] for r in runs))
        report['status'] = 'PASS' if report['passed'] else 'FAIL'
        report['result_path'] = str(args.out / 'result.json')
        with (args.out / 'result.json').open('x') as stream:
            json.dump(report, stream, indent=2, sort_keys=True)
            stream.write('\n')
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
