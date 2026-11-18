#!/usr/bin/python3 -B
"""Exercise an already compiled C57 Rust reader; never build or synthesize Rust output."""
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
sys.path.insert(0, str(HERE))
from validate import expected, require, same, unique

TEST = 'regression::dynacast_notify::fixed_go_initial_update_differential'
PROJECT = runpy.run_path(str(HERE / 'project-codecs.py'))['project']


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def encode(rows):
    return ('\n'.join(json.dumps(row, separators=(',', ':'), allow_nan=False)
                      for row in rows) + '\n').encode()


def decode(data):
    def invalid_constant(value):
        raise ValueError('invalid JSON constant: ' + value)
    return [json.loads(line, object_pairs_hook=unique, parse_constant=invalid_constant)
            for line in data.decode('utf-8').splitlines()]


def mutations(rows):
    cases = []
    # Pending-late with two codecs emits [LOW, HIGH] at 11500 ms.
    observation = (5, 'observations', 8)
    event = observation + ('events', 0)
    codec = event + ('codecs', 0)
    quality = codec + ('qualities', 0)

    def change(name, path, action, value=None):
        changed = copy.deepcopy(rows)
        target = changed
        for key in path[:-1]:
            target = target[key]
        key = path[-1] if path else None
        if action == 'set':
            target[key] = value
        elif action == 'delete':
            del target[key]
        else:
            target = target[key] if path else target
            if action == 'drop':
                target.pop()
            elif action == 'duplicate':
                target.append(copy.deepcopy(target[0]))
            elif action == 'reverse':
                target.reverse()
            else:
                raise ValueError(action)
        cases.append((name, encode(changed)))

    for name, path, fields in [
        ('row', (0,), ['scenario', 'codec_count', 'pause_ms', 'observations']),
        ('observation', observation, ['at_ms', 'events']),
        ('event', event, ['codecs']),
        ('codec', codec, ['codec', 'qualities']),
        ('quality', quality, ['quality', 'enabled']),
    ]:
        change(name + '-unknown-field', path + ('unexpected',), 'set', 0)
        for field in fields:
            change(name + '-missing-' + field, path + (field,), 'delete')

    for name, path, value in [
        ('codec-count-bool', (0, 'codec_count'), True),
        ('pause-bool', (0, 'pause_ms'), False),
        ('timestamp-bool', observation + ('at_ms',), True),
        ('quality-bool', quality + ('quality',), False),
        ('enabled-number', quality + ('enabled',), 1),
        ('enabled-string', quality + ('enabled',), 'true'),
        ('enabled-flipped', quality + ('enabled',),
         not rows[5]['observations'][8]['events'][0]['codecs'][0]['qualities'][0]['enabled']),
        ('unexpected-raw-codec', codec + ('codec',), 'video/VP8'),
        ('unexpected-codec', codec + ('codec',), 'av1'),
        ('scenario-changed', (0, 'scenario'), 'not-a-scenario'),
        ('codec-count-changed', (0, 'codec_count'), 3),
        ('pause-changed', (0, 'pause_ms'), 999),
        ('timestamp-changed', observation + ('at_ms',), 11501),
        ('timestamp-negative', observation + ('at_ms',), -1),
        ('timestamp-fraction', observation + ('at_ms',), 11500.5),
        ('events-null', observation + ('events',), None),
        ('qualities-null', codec + ('qualities',), None),
        ('quality-out-of-range', quality + ('quality',), 3),
        ('early-expiry-event', (5, 'observations', 5, 'events'),
         rows[5]['observations'][8]['events']),
        ('cancelled-new-codec-old-deadline', (13, 'observations', 8, 'events'),
         rows[13]['observations'][5]['events']),
    ]:
        change(name, path, 'set', value)

    for name, path in [
        ('row', ()),
        ('observation', (0, 'observations')),
        ('event', observation + ('events',)),
        ('codec', event + ('codecs',)),
        ('quality', codec + ('qualities',)),
    ]:
        for action in ['drop', 'duplicate']:
            change(name + '-' + action, path, action)
        if name != 'event':
            change(name + '-order', path, 'reverse')

    # Each checkpoint has at most one event: swap distinct events across checkpoints.
    changed = copy.deepcopy(rows)
    checkpoints = changed[11]['observations']
    require(checkpoints[3]['events'] != checkpoints[5]['events'], 'event-order control is vacuous')
    checkpoints[3]['events'], checkpoints[5]['events'] = (
        checkpoints[5]['events'], checkpoints[3]['events'])
    cases.append(('event-order-across-checkpoints', encode(changed)))

    canonical = encode(rows)
    for name, field, value in [
        ('row', 'scenario', rows[0]['scenario']),
        ('observation', 'at_ms', 0),
        ('event', 'codecs', rows[5]['observations'][8]['events'][0]['codecs']),
        ('codec', 'codec', rows[5]['observations'][8]['events'][0]['codecs'][0]['codec']),
        ('quality', 'quality', 0),
    ]:
        token = (json.dumps(field) + ':' + json.dumps(value, separators=(',', ':'))).encode()
        require(token in canonical, 'missing duplicate-key token')
        cases.append((name + '-duplicate-json-key', canonical.replace(token, token + b',' + token, 1)))
    cases.extend([
        ('truncated-json', canonical[:-2] + b'\n'),
        ('malformed-json', b'{not-json}\n' + b'\n'.join(canonical.splitlines()[1:]) + b'\n'),
        ('blank-json-line', b'\n' + canonical),
        ('empty-jsonl', b''),
    ])
    hashes = set()
    for name, payload in cases:
        require(payload != canonical and digest(payload) not in hashes, 'vacuous or repeated mutation: ' + name)
        hashes.add(digest(payload))
        try:
            same(decode(payload), rows)
        except (ValueError, UnicodeError):
            pass
        else:
            raise ValueError('semantically unchanged mutation: ' + name)
    require(len(cases) >= 20, 'insufficient negative controls')
    return cases


def counts(rows):
    observations = [o for row in rows for o in row['observations']]
    events = [e for o in observations for e in o['events']]
    codecs = [c for e in events for c in e['codecs']]
    return {'rows': len(rows), 'checkpoints': len(observations), 'events': len(events),
            'codec_snapshots': len(codecs),
            'qualities': sum(len(c['qualities']) for c in codecs)}


def run_case(binary, reference, directory, expected_code, baseline, existing=False):
    directory.mkdir()
    output = directory / 'rust.jsonl'
    sentinel = b'C57 existing output must not be overwritten\n'
    if existing:
        with output.open('xb') as stream:
            stream.write(sentinel)
    require(existing or not output.exists(), 'Rust output must be fresh')
    env = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C',
           'RUST_BACKTRACE': '0', 'RUST_TEST_THREADS': '1',
           'C57_GO_JSONL': str(reference), 'C57_RUST_JSONL': str(output)}
    for key in ('LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH', 'DYLD_FALLBACK_LIBRARY_PATH'):
        if key in os.environ:
            env[key] = os.environ[key]
    command = [str(binary), TEST, '--exact', '--ignored', '--nocapture']
    result = {'name': directory.name, 'command': command, 'env': env,
              'environment_policy': 'only the recorded environment is passed',
              'cwd': str(directory), 'expected_exit_code': expected_code,
              'existing_output_control': existing, 'output_was_fresh': not existing,
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
    result['reference_sha256_after'] = file_hash(reference)
    for label, path in [('stdout', stdout_path), ('stderr', stderr_path)]:
        result[label] = {'path': str(path), 'sha256': file_hash(path), 'bytes': path.stat().st_size}
    actual = output.read_bytes() if output.is_file() else None
    result['output'] = {'path': str(output), 'sha256': digest(actual) if actual is not None else None,
                        'bytes': len(actual) if actual is not None else None}
    stdout_text = stdout_path.read_text(errors='replace')
    summary = re.findall(
        r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;',
        stdout_text, re.MULTILINE)
    wanted = ('ok', '1', '0', '0', '0') if expected_code == 0 else ('FAILED', '0', '1', '0', '0')
    result['test_summaries'] = summary
    result['checks'] = {
        'expected_exit_code': result['exit_code'] == expected_code,
        'not_timed_out': not result['timed_out'],
        'one_test_executed': (len(re.findall(r'^running 1 test$', stdout_text, re.MULTILINE)) == 1
                              and len(summary) == 1 and summary[0][:5] == wanted
                              and re.search(r'^test ' + re.escape(TEST) + r' \.\.\. (ok|FAILED)$',
                                            stdout_text, re.MULTILINE) is not None),
        'reference_unchanged': result['reference_sha256_before'] == result['reference_sha256_after'],
    }
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
    result['passed'] = all(result['checks'].values())
    return result, actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    for label, path in [('binary', args.binary), ('reference', args.reference)]:
        if not path.is_absolute() or not path.is_file():
            parser.error('--' + label + ' must be an absolute existing file')
    if not os.access(args.binary, os.X_OK):
        parser.error('--binary must be executable')
    if (not args.out.is_absolute() or args.out.parent != Path('/tmp')
            or not args.out.name.startswith('c57-rust-') or args.out.name == 'c57-rust-'
            or args.out.exists() or args.out.is_symlink()):
        parser.error('--out must be a fresh /tmp/c57-rust-* directory')
    args.out.mkdir()
    report = {'test': TEST, 'invocation': ['/usr/bin/python3', '-B', str(Path(__file__).resolve()),
              '--binary', str(args.binary), '--reference', str(args.reference), '--out', str(args.out)],
              'binary': str(args.binary), 'reference': str(args.reference),
              'harness_sha256': file_hash(Path(__file__).resolve()),
              'scope': 'Compiled reader only; Rust emits before parsing/comparing Go input. No build or reference-driven Rust output.',
              'runs': [], 'passed': False}
    try:
        report['binary_sha256_before'] = file_hash(args.binary)
        raw = args.reference.read_bytes()
        report['reference_sha256_before'] = digest(raw)
        rows = decode(raw)
        same(rows, PROJECT(expected()))
        report['reference_counts'] = counts(rows)
        cases = mutations(rows)
        report['negative_controls_planned'] = len(cases)
        valid, baseline = run_case(args.binary, args.reference, args.out / 'valid', 0, None)
        report['runs'].append(valid)
        try:
            actual_rows = decode(baseline) if baseline else []
            same(actual_rows, PROJECT(expected()))
            report['actual_counts'] = counts(actual_rows)
            valid['checks']['complete_actual_trace'] = True
        except (ValueError, UnicodeError) as error:
            valid['checks']['complete_actual_trace'] = False
            valid['actual_validation_error'] = str(error)
        valid['passed'] = all(valid['checks'].values())
        for index, (name, payload) in enumerate(cases, 1):
            reference = args.out / ('mutation-%02d-%s.jsonl' % (index, name))
            with reference.open('xb') as stream:
                stream.write(payload)
            result, _ = run_case(args.binary, reference,
                                 args.out / ('negative-%02d-%s' % (index, name)), 101, baseline)
            report['runs'].append(result)
        existing, _ = run_case(args.binary, args.reference, args.out / 'existing-output',
                               101, baseline, existing=True)
        report['runs'].append(existing)
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        for label, path in [('binary', args.binary), ('reference', args.reference)]:
            try:
                report[label + '_sha256_after'] = file_hash(path)
                report[label + '_unchanged'] = (report[label + '_sha256_after'] ==
                                                report.get(label + '_sha256_before'))
            except OSError as error:
                report[label + '_unchanged'] = False
                report[label + '_after_error'] = str(error)
        runs = report['runs']
        negatives = [r for r in runs if r['name'].startswith('negative-')]
        report['counts'] = {'runs': len(runs), 'passed': sum(r['passed'] for r in runs),
                            'valid': sum(r['name'] == 'valid' for r in runs),
                            'negatives': len(negatives),
                            'negatives_passed': sum(r['passed'] for r in negatives),
                            'existing_output': sum(r['existing_output_control'] for r in runs)}
        report['passed'] = ('error' not in report and report['binary_unchanged']
                            and report['reference_unchanged'] and len(negatives) >= 20
                            and len(negatives) == report.get('negative_controls_planned')
                            and report['counts']['valid'] == 1 and report['counts']['existing_output'] == 1
                            and all(r['passed'] for r in runs))
        report['result_path'] = str(args.out / 'result.json')
        text = json.dumps(report, indent=2, sort_keys=True) + '\n'
        with (args.out / 'result.json').open('x') as stream:
            stream.write(text)
        print(text, end='')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
