#!/usr/bin/env python3
"""Run previously-ignored Rust tests from a recipe table, one process per test.

Every entry produces a row with its original exit code. A test that is not run
keeps an explicit reason; nothing is silently dropped and no failure is retried
into a pass.
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
RUNNABLE = {'RUNNABLE_NOW', 'NEEDS_REDIS'}
RUNNING = re.compile(r'Running (?:unittests|tests) [^\n]+? \((target/[^)]+)\)')


def executed_binary(text):
    """The test binary cargo actually ran, with its bytes hashed.

    A per-crate `cargo test -p X` and a `cargo test --workspace` build of the same
    source produce different binaries, so a row without this is not bound to a
    candidate.
    """
    m = RUNNING.search(text)
    if not m:
        return {}
    path = ROOT / m.group(1)
    if not path.is_file():
        return {'binary_path': str(path), 'binary_sha256': None}
    return {'binary_path': str(path),
            'binary_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def git_identity():
    def run(*args):
        p = subprocess.run(['git', '-C', str(ROOT), *args],
                           capture_output=True, text=True)
        return p.stdout.strip() if p.returncode == 0 else None
    return {'head': run('rev-parse', 'HEAD'),
            'dirty_paths': [l for l in (run('status', '--porcelain') or '').splitlines() if l]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--recipes', required=True, action='append',
                    help='recipe JSON file; repeatable')
    ap.add_argument('--output', required=True)
    ap.add_argument('--only-class', action='append', default=[],
                    help='restrict to these classifications')
    ap.add_argument('--only', action='append', default=[],
                    help='restrict to these fully-qualified test names')
    ap.add_argument('--env-file', action='append', default=[],
                    help='JSON object of extra env vars applied to every test')
    ap.add_argument('--timeout', type=float, default=600.0)
    args = ap.parse_args()

    out = pathlib.Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    extra = {}
    for path in args.env_file:
        extra.update(json.loads(pathlib.Path(path).read_text()))

    entries = []
    for path in args.recipes:
        entries.extend(json.loads(pathlib.Path(path).read_text()))

    rows = []
    for e in entries:
        name = e['test']
        label = name.replace('::', '__').replace('/', '_')
        row = {'test': name, 'crate': e.get('crate'),
               'classification': e.get('classification'),
               'source_file': e.get('source_file'),
               'source_line': e.get('source_line')}
        if args.only and name not in args.only:
            row.update(status='SKIPPED_BY_SELECTOR')
            rows.append(row)
            continue
        allowed = set(args.only_class) if args.only_class else RUNNABLE
        if e.get('classification') not in allowed:
            row.update(status='NOT_RUN', exit_code=None,
                       reason='classification %s excluded from this run'
                              % e.get('classification'))
            rows.append(row)
            continue
        cmd = e.get('cargo_command')
        if not cmd:
            row.update(status='NOT_RUN', exit_code=None,
                       reason='recipe carries no cargo_command')
            rows.append(row)
            continue
        env = dict(os.environ)
        env.update({k: str(v) for k, v in (e.get('env_values') or {}).items()})
        env.update({k: str(v) for k, v in extra.items()})
        log = out / (label + '.log')
        started = time.time()
        argv = cmd if isinstance(cmd, list) else cmd.split()
        with log.open('w') as handle:
            handle.write('CMD %s\n' % ' '.join(argv))
            handle.flush()
            try:
                proc = subprocess.run(argv, cwd=ROOT, env=env, stdout=handle,
                                      stderr=subprocess.STDOUT, timeout=args.timeout)
                code = proc.returncode
            except subprocess.TimeoutExpired:
                code = None
                handle.write('\nTIMEOUT after %ss\n' % args.timeout)
        text = log.read_text(errors='replace')
        if code is None:
            status = 'TIMEOUT'
        elif code == 0 and ' 0 passed; 0 failed' in text and '1 passed' not in text:
            status = 'NO_TEST_MATCHED'
        elif code == 0:
            status = 'PASS'
        else:
            status = 'FAIL'
        row.update(status=status, exit_code=code, log=log.name,
                   seconds=round(time.time() - started, 2), command=' '.join(argv),
                   executed_one='running 1 test' in text and '1 passed; 0 failed' in text)
        row.update(executed_binary(text))
        rows.append(row)
        print('%s %s EXIT=%s' % (status, name, code), flush=True)

    counts = {}
    for r in rows:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    summary = {'identity': git_identity(), 'entries': len(rows),
               'status_counts': counts, 'rows': rows}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    with (out / 'commands.log').open('w') as handle:
        for r in rows:
            handle.write('%s EXIT=%s %s\n' % (r['status'], r.get('exit_code'), r['test']))
        for k in sorted(counts):
            handle.write('%s=%d\n' % (k, counts[k]))
    print('STATUS_COUNTS %s' % json.dumps(counts))
    bad = [r for r in rows if r['status'] in {'FAIL', 'TIMEOUT', 'NO_TEST_MATCHED'}]
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())