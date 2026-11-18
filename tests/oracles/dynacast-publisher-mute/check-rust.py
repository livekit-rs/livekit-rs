#!/usr/bin/python3 -B
"""Check an already compiled C58 reader; never build or synthesize Rust output.

--reference is the projected Go JSONL; --raw-reference is the actual raw Go
artifact, strictly loaded with validate.load before projection or execution.
--list-mutations needs no artifacts. --self-test needs both Go artifacts but
never launches Rust or writes files. Warm-start notifications are excluded.
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
TEST = 'regression::dynacast_notify::fixed_go_publisher_mute_differential'
TIMES = [0, 100, 200, 300, 1199, 1200, 1499, 1500, 1600, 1700, 1800, 1900, 2000]
MATRIX = [('muted-downgrade', 1), ('muted-downgrade', 2), ('muted-upgrade', 2),
          ('zero-muted', 1), ('zero-muted', 2), ('off-muted', 1), ('off-muted', 2),
          ('unmuted-control', 1), ('unmuted-control', 2)]
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


def encode(rows, duplicate_key=None):
    # Render a duplicate at its exact nested path, not the first similar token.
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

    if duplicate_key is None:
        lines = [json.dumps(row, separators=(',', ':'), allow_nan=False) for row in rows]
    else:
        lines = [render(row, (i,)) for i, row in enumerate(rows)]
    return ('\n'.join(lines) + '\n').encode()


def decode(data, validator):
    def invalid_constant(value):
        raise ValueError('invalid JSON constant: ' + value)
    return [json.loads(line, object_pairs_hook=validator.unique, parse_constant=invalid_constant)
            for line in data.decode('utf-8').splitlines()]


def get_at(rows, path):
    value = rows
    for key in path:
        value = value[key]
    return value


def path_text(path):
    return 'rows' + ''.join('[' + str(key) + ']' if isinstance(key, int) else '.' + key
                            for key in path)


def mutation_plan():
    plan = []
    # C58's first muted-downgrade n=2 unmute has one complete two-codec event.
    observation = (1, 'observations', 7)
    event = observation + ('events', 0)
    codec = event + ('codecs', 0)
    quality = codec + ('qualities', 0)
    layers = (1, 'observations', 5, 'expected_layers')
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

    def missing(name, path):
        add(name, path, 'delete', kind='fields', check_path=path[:-1],
            rust=r'missing field `' + re.escape(path[-1]) + '`')

    def unknown(name, path):
        add(name, path + ('unexpected',), 'set', 0, kind='fields', check_path=path,
            rust=r'unknown field `unexpected`')

    def typed(name, path, value, expected_type):
        add(name, path, 'set', value, kind='type',
            rust=r'invalid type:[^\n]*expected ' + expected_type)

    # Preserve all 60 C57 control names and failure modes before C58 additions.
    for name, path, fields in [
        ('row', (0,), ['scenario', 'codec_count', 'pause_ms', 'observations']),
        ('observation', observation, ['at_ms', 'events']),
        ('event', event, ['codecs']),
        ('codec', codec, ['codec', 'qualities']),
        ('quality', quality, ['quality', 'enabled']),
    ]:
        unknown(name + '-unknown-field', path)
        for field in fields:
            missing(name + '-missing-' + field, path + (field,))

    for name, path, value, expected_type in [
        ('codec-count-bool', (0, 'codec_count'), True, 'usize'),
        ('pause-bool', (0, 'pause_ms'), False, 'u64'),
        ('timestamp-bool', observation + ('at_ms',), True, 'u64'),
        ('quality-bool', quality + ('quality',), False, 'i32'),
        ('enabled-number', quality + ('enabled',), 1, 'a boolean'),
        ('enabled-string', quality + ('enabled',), 'true', 'a boolean'),
    ]:
        typed(name, path, value, expected_type)
    add('enabled-flipped', quality + ('enabled',), 'toggle')
    for name, path, value in [
        ('unexpected-raw-codec', codec + ('codec',), 'video/VP8'),
        ('unexpected-codec', codec + ('codec',), 'av1'),
        ('scenario-changed', (0, 'scenario'), 'not-a-scenario'),
        ('codec-count-changed', (0, 'codec_count'), 3),
        ('pause-changed', (0, 'pause_ms'), 999),
        ('timestamp-changed', observation + ('at_ms',), 1501),
    ]:
        add(name, path, 'set', value)
    add('timestamp-negative', observation + ('at_ms',), 'set', -1,
        rust=r'invalid value:[^\n]*expected u64')
    typed('timestamp-fraction', observation + ('at_ms',), 1500.5, 'u64')
    typed('events-null', observation + ('events',), None, 'a sequence')
    typed('qualities-null', codec + ('qualities',), None, 'a sequence')
    add('quality-out-of-range', quality + ('quality',), 'set', 3)
    add('early-expiry-event', (1, 'observations', 4, 'events'), 'copy-from',
        observation + ('events',), kind='count',
        intent='C57 early-expiry control: muted downgrade must not emit at 1199 ms')
    add('cancelled-new-codec-old-deadline', (2, 'observations', 5, 'events'), 'copy-from',
        (2, 'observations', 7, 'events'), kind='count',
        intent='C57 stale-deadline insertion adapted to C58: muted upgrade emits nothing at 1200 ms')

    for name, path in [
        ('row', ()), ('observation', (0, 'observations')),
        ('event', observation + ('events',)), ('codec', event + ('codecs',)),
        ('quality', codec + ('qualities',)),
    ]:
        for action in ['drop', 'duplicate']:
            add(name + '-' + action, path, action, kind='count')
        if name != 'event':
            add(name + '-order', path, 'reverse', kind='order')
    # All C58 emitted events in a row may be equal. Swap an event and an empty
    # checkpoint instead; swapping equal snapshots would be a vacuous control.
    add('event-order-across-checkpoints', (7, 'observations', 5, 'events'), 'swap',
        (7, 'observations', 6, 'events'), kind='count')
    for name, path in [
        ('row', (0, 'scenario')), ('observation', (1, 'observations', 7, 'at_ms')),
        ('event', event + ('codecs',)), ('codec', codec + ('codec',)),
        ('quality', quality + ('quality',)),
    ]:
        add(name + '-duplicate-json-key', path, 'duplicate-key', kind='duplicate-key',
            rust=r'duplicate field `' + re.escape(path[-1]) + '`')
    add('truncated-json', (), 'truncated', kind='json', rust=r'EOF while parsing')
    add('malformed-json', (), 'malformed', kind='json', rust=r'key must be a string')
    add('blank-json-line', (), 'blank-line', kind='json', rust=r'EOF while parsing a value')
    add('empty-jsonl', (), 'empty', kind='count')
    require(len(plan) == 60, 'C57 control inventory drift')
    for control in plan:
        control['inherited_c57'] = True

    missing('observation-missing-expected_layers', observation + ('expected_layers',))
    for action in ['drop', 'duplicate', 'reverse']:
        add('expected-layers-' + ('order' if action == 'reverse' else action), layers,
            action, kind='order' if action == 'reverse' else 'count')
    add('expected-layers-extra', layers, 'append', {'codec': 'av1', 'spatial': 0}, kind='count')
    typed('expected-layers-null', layers, None, 'a sequence')
    typed('expected-layers-object', layers, {}, 'a sequence')
    typed('expected-layer-null', layer, None, r'struct ExpectedLayer')
    unknown('expected-layer-unknown-field', layer)
    missing('expected-layer-missing-codec', layer + ('codec',))
    missing('expected-layer-missing-spatial', layer + ('spatial',))
    for name, value in [('raw-codec', 'video/VP8'), ('wrong-codec', 'av1'),
                        ('codec-case', 'VP8')]:
        add('expected-layer-' + name, layer + ('codec',), 'set', value)
    for name, value in [('number', 8), ('bool', True), ('null', None)]:
        typed('expected-layer-codec-' + name, layer + ('codec',), value, 'a string')
    add('expected-layer-spatial-wrong', layer + ('spatial',), 'set', 1)
    typed('expected-layer-spatial-bool', layer + ('spatial',), False, 'i32')
    typed('expected-layer-spatial-float', layer + ('spatial',), 0.0, 'i32')
    typed('expected-layer-spatial-null', layer + ('spatial',), None, 'i32')
    for name, path in [
        ('observation-expected-layers', observation + ('expected_layers',)),
        ('expected-layer-codec', layer + ('codec',)),
        ('expected-layer-spatial', layer + ('spatial',)),
        ('quality-enabled', quality + ('enabled',)),
    ]:
        add(name + '-duplicate-json-key', path, 'duplicate-key', kind='duplicate-key',
            rust=r'duplicate field `' + re.escape(path[-1]) + '`')

    typed('event-codec-number', codec + ('codec',), 8, 'a string')
    typed('event-codec-bool', codec + ('codec',), True, 'a string')
    typed('event-codec-null', codec + ('codec',), None, 'a string')
    typed('codecs-null', event + ('codecs',), None, 'a sequence')
    typed('codec-count-float', (0, 'codec_count'), 1.0, 'usize')
    typed('pause-float', (0, 'pause_ms'), 1000.0, 'u64')
    typed('timestamp-integral-float', observation + ('at_ms',), 1500.0, 'u64')
    typed('quality-float', quality + ('quality',), 0.0, 'i32')

    for row in [5, 6]:
        add('off-minus-one-as-zero-row%d' % row,
            (row, 'observations', 5, 'expected_layers', 0, 'spatial'), 'set', 0,
            intent='Receiver OFF is -1, not LOW=0, even with publisher muted')
    for name, row, checkpoint in [
        ('muted-downgrade', 1, 5), ('muted-upgrade', 2, 3),
        ('zero-muted', 4, 2), ('off-muted', 6, 5),
    ]:
        add(name + '-receiver-not-updated',
            (row, 'observations', checkpoint, 'expected_layers', 0, 'spatial'), 'set', 2,
            intent='Receiver commit must still advance while notifications are muted')
    add('muted-upgrade-vp9-not-updated', (2, 'observations', 3, 'expected_layers', 1, 'spatial'),
        'set', 1)
    add('muted-downgrade-premature-layer', (1, 'observations', 4, 'expected_layers', 0, 'spatial'),
        'set', 0)
    add('muted-upgrade-premature-layer', (2, 'observations', 2, 'expected_layers', 0, 'spatial'),
        'set', 0)
    add('untouched-vp9-layer-changed', (1, 'observations', 5, 'expected_layers', 1, 'spatial'),
        'set', 0)
    add('warm-start-event-illegally-included', (1, 'observations', 0, 'events'), 'copy-from',
        observation + ('events',), kind='count')
    for name, row, checkpoint in [
        ('muted-downgrade', 1, 5), ('muted-upgrade', 2, 3),
        ('zero-muted', 4, 2), ('off-muted', 6, 5),
    ]:
        add(name + '-notification-leaked', (row, 'observations', checkpoint, 'events'),
            'copy-from', (row, 'observations', 7, 'events'), kind='count')
    for row in range(len(MATRIX)):
        for checkpoint, name in [(7, 'unmute-event-omitted'),
                                 (8, 'repeated-false-event-omitted'),
                                 (11, 'second-unmute-event-omitted')]:
            if row == 1 and checkpoint == 7:
                continue  # Exactly the inherited event-drop payload; do not duplicate it.
            add(name + '-row%d' % row, (row, 'observations', checkpoint, 'events'),
                'drop', kind='count')
    for row in [7, 8]:
        add('unmuted-control-deadline-event-omitted-row%d' % row,
            (row, 'observations', 5, 'events'), 'drop', kind='count')
    for checkpoint, name in [(9, 'true-mute-notification'), (10, 'repeated-true-notification'),
                             (12, 'post-unmute-stray-notification')]:
        add(name, (1, 'observations', checkpoint, 'events'), 'copy-from',
            observation + ('events',), kind='count')

    names = [control['name'] for control in plan]
    require(len(names) == len(set(names)), 'duplicate mutation name')
    for control in plan:
        control.setdefault('inherited_c57', False)
        re.compile(control['expected_rust_rejection'])
        if control['python_kind'] != 'json':
            re.compile(control['expected_python_rejection'])
    return plan


def mutations(rows, validator):
    canonical = encode(rows)
    cases, hashes = [], set()
    for control in mutation_plan():
        changed = copy.deepcopy(rows)
        path, action, value = control['path'], control['action'], control['value']
        if action == 'duplicate-key':
            get_at(rows, path)  # A missing duplicate-key anchor is an error.
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
            if action in ('set', 'delete', 'toggle', 'copy-from', 'swap'):
                target, key = get_at(changed, path[:-1]), path[-1]
                if action == 'set':
                    target[key] = copy.deepcopy(value)
                elif action == 'delete':
                    del target[key]
                elif action == 'toggle':
                    require(type(target[key]) is bool, 'toggle anchor must be boolean')
                    target[key] = not target[key]
                elif action == 'copy-from':
                    target[key] = copy.deepcopy(get_at(rows, value))
                else:
                    other, other_key = get_at(changed, value[:-1]), value[-1]
                    require(encode([target[key]]) != encode([other[other_key]]),
                            'event-order control is vacuous')
                    target[key], other[other_key] = other[other_key], target[key]
            else:
                target = get_at(changed, path)
                if action == 'drop':
                    target.pop()
                elif action == 'duplicate':
                    target.append(copy.deepcopy(target[0]))
                elif action == 'reverse':
                    target.reverse()
                elif action == 'append':
                    target.append(copy.deepcopy(value))
                else:
                    raise ValueError('unknown mutation action: ' + action)
            payload = encode(changed)
        payload_hash = digest(payload)
        require(payload != canonical and payload_hash not in hashes,
                'vacuous or repeated mutation: ' + control['name'])
        hashes.add(payload_hash)
        try:
            validator.same(decode(payload, validator), rows)
        except (ValueError, UnicodeError) as error:
            if control['python_kind'] == 'json':
                correct = isinstance(error, json.JSONDecodeError)
            else:
                correct = re.search(control['expected_python_rejection'], str(error)) is not None
            require(correct, 'wrong Python rejection for %s: %s' % (control['name'], error))
            evidence = dict(control, payload_sha256=payload_hash, payload_bytes=len(payload),
                            python_rejection=str(error), python_rejection_verified=True,
                            serialized_bytes_changed=True, unique_payload=True)
            cases.append((evidence, payload))
        else:
            raise ValueError('semantically unchanged mutation: ' + control['name'])
    return cases


def counts(rows):
    observations = [o for row in rows for o in row['observations']]
    events = [e for o in observations for e in o['events']]
    codecs = [c for e in events for c in e['codecs']]
    return {'rows': len(rows), 'checkpoints': len(observations), 'events': len(events),
            'codec_snapshots': len(codecs),
            'qualities': sum(len(c['qualities']) for c in codecs),
            'receiver_layer_readings': sum(len(o['expected_layers']) for o in observations)}


def load_support():
    sys.path.insert(0, str(HERE))
    import validate
    project = runpy.run_path(str(HERE / 'project-codecs.py'))['project']
    return validate, project


def load_references(raw_path, reference_path, validator, project):
    raw = raw_path.read_bytes()
    # This is the actual Go input, never encode(expected()) or an inverse
    # projection of the reference passed off as a raw Go artifact.
    raw_rows = validator.load(raw)
    projected = project(copy.deepcopy(raw_rows))
    restored = copy.deepcopy(projected)
    names = {'vp8': 'video/VP8', 'vp9': 'video/VP9'}
    for row in restored:
        for observation in row['observations']:
            entries = list(observation['expected_layers'])
            entries += [codec for event in observation['events'] for codec in event['codecs']]
            for entry in entries:
                require(type(entry['codec']) is str and entry['codec'] in names,
                        'projection must include event and expected_layers codec names')
                entry['codec'] = names[entry['codec']]
    validator.same(restored, raw_rows)  # Projection may change names only, not ordering or data.
    reference = reference_path.read_bytes()
    rows = decode(reference, validator)
    validator.same(rows, projected)
    require(len(rows) == len(MATRIX), 'C58 requires nine rows')
    for index, (row, (scenario, n)) in enumerate(zip(rows, MATRIX)):
        validator.same([row['scenario'], row['codec_count'], row['pause_ms']],
                       [scenario, n, 0 if scenario == 'zero-muted' else 1000])
        validator.same([o['at_ms'] for o in row['observations']], TIMES)
        validator.same([len(o['events']) for o in row['observations']],
                       [int(i in (7, 8, 11) or (index in (7, 8) and i == 5))
                        for i in range(len(TIMES))])
        for observation in row['observations']:
            validator.same([layer['codec'] for layer in observation['expected_layers']],
                           ['vp8', 'vp9'][:n])
    validator.same(rows[1]['observations'][5]['expected_layers'],
                   [{'codec': 'vp8', 'spatial': 0}, {'codec': 'vp9', 'spatial': 2}])
    for row in (5, 6):
        validator.same(rows[row]['observations'][5]['expected_layers'][0],
                       {'codec': 'vp8', 'spatial': -1})
    require(len(rows[1]['observations'][7]['events'][0]['codecs']) == 2,
            'nested mutation anchor must contain both codecs')
    return rows, {'raw_reference_sha256': digest(raw), 'reference_sha256': digest(reference),
                  'raw_strict_loader': 'validate.load(actual raw reference bytes)',
                  'projection_name_only_verified': True, 'reference_counts': counts(rows)}


def run_case(binary, reference, directory, expected_code, baseline, initial_binary_hash,
             rejection=None, existing=False):
    directory.mkdir()
    output = directory / 'rust.jsonl'
    sentinel = b'C58 existing output must not be overwritten\n'
    if existing:
        with output.open('xb') as stream:
            stream.write(sentinel)
    require(existing or (not output.exists() and not output.is_symlink()),
            'Rust output must be fresh')
    env = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C',
           'RUST_BACKTRACE': '0', 'RUST_TEST_THREADS': '1',
           'C58_GO_JSONL': str(reference), 'C58_RUST_JSONL': str(output)}
    for key in ('LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH', 'DYLD_FALLBACK_LIBRARY_PATH'):
        if key in os.environ:
            env[key] = os.environ[key]
    command = [str(binary), TEST, '--exact', '--ignored', '--nocapture']
    result = {'name': directory.name, 'command': command, 'env': env,
              'environment_policy': 'only the recorded environment is passed',
              'cwd': str(directory), 'expected_exit_code': expected_code,
              'expected_rejection': rejection, 'existing_output_control': existing,
              'output_was_fresh': not existing, 'binary_sha256_before': file_hash(binary),
              'reference_sha256_before': file_hash(reference),
              'output_sha256_before': digest(sentinel) if existing else None}
    stdout_path, stderr_path = directory / 'stdout.log', directory / 'stderr.log'
    with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
        try:
            process = subprocess.Popen(command, cwd=str(directory), env=env,
                                       stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
            try:
                result['exit_code'] = process.wait(timeout=120)
                result['timed_out'] = False
            except subprocess.TimeoutExpired:
                process.kill()
                result['exit_code'] = process.wait()
                result['timed_out'] = True
        except OSError as error:
            result.update(exit_code=None, timed_out=False, launch_error=str(error))
    result['binary_sha256_after'] = file_hash(binary)
    result['reference_sha256_after'] = file_hash(reference)
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
        'binary_matches_initial': result['binary_sha256_before'] == initial_binary_hash,
        'binary_unchanged': result['binary_sha256_before'] == result['binary_sha256_after'],
        'reference_unchanged': result['reference_sha256_before'] == result['reference_sha256_after'],
    }
    if rejection is not None:
        match = re.search(rejection, log_text)
        result['checks']['intended_rejection'] = match is not None
        result['rejection_evidence'] = match.group(0) if match else None
    if existing:
        result['checks']['existing_output_unchanged'] = actual == sentinel
        result['independent_output_check'] = 'not applicable: separate existing-output control'
    else:
        result['checks']['independently_emitted_output'] = actual is not None and bool(actual)
        if baseline is not None:
            result['checks']['output_bytes_equal_valid'] = actual == baseline
            result['checks']['output_sha256_equal_valid'] = actual is not None and digest(actual) == digest(baseline)
        elif expected_code != 0:
            result['checks']['valid_baseline_available'] = False
        else:
            result['independent_output_check'] = 'baseline run; full trace must match the strictly loaded Go artifact'
    result['passed'] = all(result['checks'].values())
    result['status'] = 'PASS' if result['passed'] else 'FAIL'
    return result, actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--reference', type=Path, help='actual projected Go JSONL')
    parser.add_argument('--raw-reference', type=Path, help='actual raw Go JSONL for validate.load')
    parser.add_argument('--out', type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--list-mutations', action='store_true', help='inventory only; no artifact or execution claim')
    mode.add_argument('--self-test', action='store_true', help='validate actual Go artifacts and all mutations; no Rust')
    args = parser.parse_args()
    plan = mutation_plan()
    if args.list_mutations:
        print(json.dumps({'scope': 'mutation inventory only; payload checks and Rust NOT RUN',
                          'mutations': [c['name'] for c in plan], 'total': len(plan),
                          'inherited_c57': sum(c['inherited_c57'] for c in plan)}, indent=2))
        return 0
    inputs = [('raw_reference', args.raw_reference), ('reference', args.reference)]
    if not args.self_test:
        inputs.append(('binary', args.binary))
    for label, path in inputs:
        if path is None or not path.is_absolute() or not path.is_file():
            parser.error('--' + label.replace('_', '-') + ' must be an absolute existing file')
    if not args.self_test and not os.access(args.binary, os.X_OK):
        parser.error('--binary must be executable')
    if args.self_test:
        require(args.binary is None and args.out is None, '--self-test does not accept --binary or --out')
        before = {label: file_hash(path) for label, path in inputs}
        validator, project = load_support()
        rows, evidence = load_references(args.raw_reference, args.reference, validator, project)
        cases = mutations(rows, validator)
        unchanged = all(file_hash(path) == before[label] for label, path in inputs)
        require(unchanged, 'self-test inputs changed')
        print(json.dumps(dict(evidence, scope='Python artifact/mutation self-test only; Rust NOT RUN',
                              mutations=[control for control, _ in cases], total=len(cases),
                              inputs_unchanged=unchanged, passed=True), indent=2))
        return 0
    if (args.out is None or not args.out.is_absolute() or args.out.parent != Path('/tmp')
            or not args.out.name.startswith('c58-rust-') or args.out.name == 'c58-rust-'
            or args.out.exists() or args.out.is_symlink()):
        parser.error('--out must be a fresh /tmp/c58-rust-* directory')
    args.out.mkdir()
    support = [('validator', HERE / 'validate.py'), ('projector', HERE / 'project-codecs.py')]
    tracked = inputs + support
    report = {'test': TEST, 'invocation': ['/usr/bin/python3', '-B', str(Path(__file__).resolve())] + sys.argv[1:],
              'binary': str(args.binary), 'reference': str(args.reference),
              'raw_reference': str(args.raw_reference), 'harness_sha256': file_hash(Path(__file__).resolve()),
              'scope': 'Compiled reader only; fresh independent Rust output precedes Go input parsing/comparison. No build, fabricated Go artifact, or warm-start callbacks.',
              'runs': [], 'not_run': [], 'passed': False,
              'mutation_controls': plan, 'negative_controls_planned': len(plan)}
    try:
        for label, path in tracked:
            report[label + '_sha256_before'] = file_hash(path)
        validator, project = load_support()
        rows, evidence = load_references(args.raw_reference, args.reference, validator, project)
        report.update(evidence)
        cases = mutations(rows, validator)
        report['mutation_controls'] = [control for control, _ in cases]
        report['negative_controls_planned'] = len(cases)
        valid, baseline = run_case(args.binary, args.reference, args.out / 'valid', 0, None,
                                   report['binary_sha256_before'])
        report['runs'].append(valid)
        try:
            actual_rows = decode(baseline, validator) if baseline else []
            validator.same(actual_rows, rows)
            report['actual_counts'] = counts(actual_rows)
            valid['checks']['complete_actual_trace'] = True
        except (ValueError, UnicodeError) as error:
            valid['checks']['complete_actual_trace'] = False
            valid['actual_validation_error'] = str(error)
        valid['passed'] = all(valid['checks'].values())
        valid['status'] = 'PASS' if valid['passed'] else 'FAIL'
        if valid['passed']:
            report['baseline_sha256'] = digest(baseline)
            repeat, _ = run_case(args.binary, args.reference, args.out / 'valid-repeat', 0, baseline,
                                  report['binary_sha256_before'])
            report['runs'].append(repeat)
            if repeat['passed']:
                for index, (control, payload) in enumerate(cases, 1):
                    name = control['name']
                    reference = args.out / ('mutation-%03d-%s.jsonl' % (index, name))
                    with reference.open('xb') as stream:
                        stream.write(payload)
                    result, _ = run_case(args.binary, reference,
                                         args.out / ('negative-%03d-%s' % (index, name)), 101, baseline,
                                         report['binary_sha256_before'],
                                         rejection=control['expected_rust_rejection'])
                    result['mutation_name'] = name
                    result['checks']['reference_matches_mutation'] = (
                        result['reference_sha256_before'] == control['payload_sha256'])
                    result['passed'] = all(result['checks'].values())
                    result['status'] = 'PASS' if result['passed'] else 'FAIL'
                    report['runs'].append(result)
            else:
                report['blocked_reason'] = 'fresh valid repeat failed; dependent mutations NOT RUN'
        else:
            report['blocked_reason'] = 'valid baseline failed; dependent repeat/mutations NOT RUN'
        existing, _ = run_case(args.binary, args.reference, args.out / 'existing-output',
                               101, baseline, report['binary_sha256_before'],
                               rejection=r'kind: AlreadyExists', existing=True)
        report['runs'].append(existing)
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        for label, path in tracked:
            try:
                report[label + '_sha256_after'] = file_hash(path)
                report[label + '_unchanged'] = (report[label + '_sha256_after'] ==
                                                report.get(label + '_sha256_before'))
            except OSError as error:
                report[label + '_unchanged'] = False
                report[label + '_after_error'] = str(error)
        runs = report['runs']
        negatives = [r for r in runs if r.get('mutation_name') is not None]
        executed = {r['mutation_name'] for r in negatives}
        reason = report.get('blocked_reason', report.get('error', 'execution did not reach this check'))
        report['not_run'] = [{'name': name, 'status': 'NOT RUN', 'reason': reason}
                             for name in [c['name'] for c in plan] if name not in executed]
        for name in ['valid', 'valid-repeat', 'existing-output']:
            if not any(r['name'] == name for r in runs):
                report['not_run'].append({'name': name, 'status': 'NOT RUN', 'reason': reason})
        report['counts'] = {'runs': len(runs), 'passed': sum(r['passed'] for r in runs),
                            'valid': sum(r['name'] == 'valid' for r in runs),
                            'valid_repeat': sum(r['name'] == 'valid-repeat' for r in runs),
                            'negatives': len(negatives), 'mutations': len(plan),
                            'inherited_c57': sum(c['inherited_c57'] for c in plan),
                            'negatives_passed': sum(r['passed'] for r in negatives),
                            'existing_output': sum(r['existing_output_control'] for r in runs)}
        report['passed'] = ('error' not in report and not report['not_run']
                            and all(report.get(label + '_unchanged', False) for label, _ in tracked)
                            and len(negatives) == report['negative_controls_planned']
                            and report['counts']['valid'] == 1 and report['counts']['valid_repeat'] == 1
                            and report['counts']['existing_output'] == 1
                            and all(r['passed'] for r in runs))
        report['status'] = 'PASS' if report['passed'] else 'FAIL'
        report['result_path'] = str(args.out / 'result.json')
        text = json.dumps(report, indent=2, sort_keys=True) + '\n'
        with (args.out / 'result.json').open('x') as stream:
            stream.write(text)
        print(text, end='')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
