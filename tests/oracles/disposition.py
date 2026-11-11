#!/usr/bin/env python3
"""Merge the runtime ignored inventory, recipe tables and run receipts into one
per-item disposition table.

Every runtime entry gets exactly one disposition. A measurement only counts when
the receipt proves the test actually executed (`running 1 test` / `1 passed; 0
failed`) with its original exit code. Later receipts supersede earlier ones by
mtime and the earlier attempt is retained under `superseded_attempts`, so a
failure is never erased by a retry.
"""
import argparse
import json
import pathlib
import sys


def load(path):
    return json.loads(pathlib.Path(path).read_text())


def proved(text):
    return 'running 1 test' in text and '1 passed; 0 failed' in text


def read_log(path):
    return path.read_text(errors='replace') if path.exists() else ''


def exact_name(command):
    """The test name a harness passed after --exact, or None."""
    if not isinstance(command, list):
        return None
    for i, token in enumerate(command):
        if token == '--exact' and i + 1 < len(command):
            return command[i + 1]
    return None


class Merger:
    def __init__(self):
        self.measured = {}

    def upsert(self, name, record):
        if not name:
            return
        previous = self.measured.get(name)
        if previous is None:
            self.measured[name] = record
            return
        older, newer = sorted([previous, record], key=lambda r: r['mtime'])
        newer.setdefault('superseded_attempts', []).extend(
            older.pop('superseded_attempts', []) + [older])
        self.measured[name] = newer

    def paired(self, name, path):
        """A harness that drives one ignored consumer over several real cases."""
        report = load(path)
        path = pathlib.Path(path)
        base = {'receipt': str(path), 'mtime': path.stat().st_mtime}

        if report.get('go_client') and isinstance(report.get('cases'), list):
            cases = report['cases']
            executed = 0
            for case in cases:
                for side in case.get('sides', []):
                    if side.get('side') != 'rust-go-profile':
                        continue
                    log = path.parent / ('%s-%s-rust.stdout'
                                         % (case['id'], case['version']))
                    if proved(read_log(log)) and side.get('exit_code') == 0:
                        executed += 1
            ok = (report.get('status') == 'PASS' and bool(cases)
                  and executed == len(cases)
                  and report.get('binary_identity_unchanged')
                  and report.get('private_inputs_removed'))
            binary = report.get('test_binary') or {}
            self.upsert(name, dict(base, status='PASS' if ok else 'FAIL',
                                   exit_code=0 if ok else 1,
                                   executed_one=executed == len(cases),
                                   paired_cases=len(cases),
                                   rust_executions_proved=executed,
                                   binary_sha256=binary.get('sha256'),
                                   binary_path=binary.get('path')))
            return

        if 'old_public_consumer_exit' in report:
            text = read_log(path.parent / 'paired-run.stdout')
            cases = report.get('cases') or []
            rows = report.get('observer_rows') or []
            ok = (report.get('old_public_consumer_exit') == 0 and proved(text)
                  and report.get('functional_stdout_exit_status') == 'PASS'
                  and bool(cases) and len(cases) == len(rows)
                  and not report.get('cleanup_errors')
                  and (report.get('consumer_process') or {}).get('cleanup_complete'))
            # The ignored test runs in the consumer binary, not the CLI under test;
            # the driver receipt binds the consumer, the report binds the CLI.
            driver = path.parent / 'driver-receipt.json'
            consumer = load(driver).get('consumer_binary_sha256') if driver.is_file() else None
            self.upsert(name, dict(
                base, status='PASS' if ok else 'FAIL',
                exit_code=report.get('old_public_consumer_exit'),
                executed_one=proved(text), paired_status=report.get('status'),
                functional_stdout_exit_status=report.get('functional_stdout_exit_status'),
                whole_cli_stdio_status=report.get('whole_cli_stdio_status'),
                consumer_target_check=report.get('consumer_target_check'),
                paired_cases=len(cases), observer_rows=len(rows),
                refusal_witnesses=len(report.get('refusal_witnesses') or []),
                binary_sha256=consumer or (report.get('binary_sha256') or {}).get('rust'),
                cli_binary_sha256=(report.get('binary_sha256') or {}).get('rust')))
            return

        if report.get('kind') and isinstance(report.get('results'), list):
            cases = report['results']
            executed = True
            derived = None
            for case in cases:
                derived = derived or exact_name(case.get('command'))
                executed = executed and proved(read_log(path.parent / (case['case'] + '.log')))
            ok = (bool(cases) and report.get('binary_unchanged') and executed
                  and all(c.get('accepted') and c.get('exit_code') == 0 for c in cases))
            self.upsert(name or derived, dict(
                base, status='PASS' if ok else 'FAIL', exit_code=0 if ok else 1,
                executed_one=executed, cases=[c['case'] for c in cases],
                binary_sha256=report.get('sha256'), binary_path=report.get('binary')))
            return

        raise SystemExit('unrecognised paired report schema: %s' % path)

    def execution(self, path):
        receipt = load(path)
        path = pathlib.Path(path)
        cases = receipt.get('tests')
        if cases is None and receipt.get('test'):
            cases = [receipt]
        binary = receipt.get('binary') or {}
        for case in cases or []:
            self.upsert(case.get('name') or case.get('test'), {
                'receipt': str(path), 'status': case.get('status'),
                'exit_code': case.get('exit_code'),
                'executed_one': case.get('executed_one', case.get('actual_one_test')),
                'redis_port': case.get('redis_port'),
                'brokers': case.get('brokers'),
                'error': case.get('error') or case.get('failure_reason'),
                'binary_sha256': binary.get('sha256'),
                'binary_path': binary.get('path'),
                'mtime': path.stat().st_mtime,
            })

    def single(self, path):
        receipt = load(path)
        if 'test' not in receipt:
            return
        path = pathlib.Path(path)
        text = read_log(path.parent / 'test.log')
        ok = (receipt.get('test_exit') == 0 and receipt.get('completed') and proved(text))
        self.upsert(receipt['test'], {
            'receipt': str(path), 'status': 'PASS' if ok else 'FAIL',
            'exit_code': receipt.get('test_exit'), 'executed_one': proved(text),
            'redis_port': receipt.get('redis_port'),
            'redis_exits': receipt.get('redis_exits'),
            'mtime': path.stat().st_mtime,
        })

    def runner(self, path, exclude):
        if any(marker in str(path) for marker in exclude):
            return {}
        data = load(path)
        if not isinstance(data, dict):
            return {}
        found = {}
        for row in data.get('rows', []):
            if row.get('status') not in {'PASS', 'FAIL', 'TIMEOUT', 'NO_TEST_MATCHED'}:
                continue
            record = {'receipt': str(path), 'status': row['status'],
                      'exit_code': row.get('exit_code'),
                      'mtime': pathlib.Path(path).stat().st_mtime}
            previous = found.get(row['test'])
            if previous is None or record['mtime'] >= previous['mtime']:
                if previous is not None:
                    record.setdefault('superseded_attempts', []).extend(
                        previous.pop('superseded_attempts', []) + [previous])
                found[row['test']] = record
        return found


def mapping(values, require_name=True):
    out = []
    for value in values:
        name, sep, path = value.partition('=')
        if not sep:
            name, path = None, value
        if not path or (require_name and not name):
            raise SystemExit('expected NAME=PATH, got %r' % value)
        out.append((name, path))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--runtime', required=True)
    ap.add_argument('--recipes', required=True, action='append')
    ap.add_argument('--receipts', required=True, action='append',
                    help='directory scanned recursively for run receipts')
    ap.add_argument('--paired', action='append', default=[], metavar='NAME=REPORT',
                    help='harness report driving one named ignored consumer')
    ap.add_argument('--differential', action='append', default=[], metavar='NAME=REPORT',
                    help='compare.py report gating a write-only observer test')
    ap.add_argument('--exclude-summary', action='append', default=[],
                    help='skip runner summaries whose path contains this marker')
    ap.add_argument('--rebind', action='append', default=[],
                    help='rebind-workspace.py output binding rows to workspace binaries')
    ap.add_argument('--owner-differential', action='append', default=[],
                    help='run-owner-differentials.py summary gating write-only producers')
    ap.add_argument('--output', required=True)
    args = ap.parse_args()

    runtime = load(args.runtime)
    recipes = {}
    for path in args.recipes:
        for row in load(path):
            recipes.setdefault(row['test'], []).append((pathlib.Path(path).name, row))

    merger = Merger()
    runner = {}
    for directory in args.receipts:
        root = pathlib.Path(directory)
        for path in sorted(root.rglob('execution.json')):
            merger.execution(path)
        for path in sorted(root.rglob('result.json')):
            merger.single(path)
        for path in sorted(root.rglob('summary.json')):
            for name, record in merger.runner(path, args.exclude_summary).items():
                previous = runner.get(name)
                if previous is None or record['mtime'] >= previous['mtime']:
                    if previous is not None:
                        record.setdefault('superseded_attempts', []).extend(
                            previous.pop('superseded_attempts', []) + [previous])
                    runner[name] = record
    for name, path in mapping(args.paired, require_name=False):
        merger.paired(name, path)
    # A per-crate build and a workspace build of the same source are different
    # binaries, so the re-bind layer supersedes any earlier per-crate measurement.
    for path in args.rebind:
        path = pathlib.Path(path)
        report = load(path)
        for row in report.get('rows', []):
            record = {'receipt': str(path), 'status': row.get('status'),
                      'exit_code': row.get('exit_code'), 'mode': row.get('mode'),
                      'executed_one': row.get('executed_one'),
                      'binary_path': row.get('binary_path'),
                      'binary_sha256': row.get('binary_sha256'),
                      'reason': row.get('reason'), 'log': row.get('log'),
                      'mtime': path.stat().st_mtime}
            merger.upsert(row['test'], {k: v for k, v in record.items() if v is not None})

    table = []
    for entry in runtime:
        name = entry['test']
        rows = recipes.get(name, [])
        recipe = rows[0][1] if rows else {}
        row = {'test': name, 'binary': entry['binary'],
               'source_file': recipe.get('source_file'),
               'source_line': recipe.get('source_line'),
               'classification': recipe.get('classification', 'UNCLASSIFIED'),
               'recipe_sources': [r[0] for r in rows],
               'justification': recipe.get('justification'),
               'confidence': recipe.get('confidence')}
        if name in merger.measured:
            row['disposition'] = merger.measured[name]['status']
            row['measurement'] = merger.measured[name]
        elif name in runner:
            row['disposition'] = runner[name]['status']
            row['measurement'] = runner[name]
        elif recipe.get('disposition'):
            row['disposition'] = recipe['disposition']
            row['measurement'] = {k: v for k, v in {
                'evidence': recipe.get('evidence'),
                'binary_sha256': recipe.get('binary_sha256'),
                'superseded_evidence': recipe.get('superseded_evidence'),
            }.items() if v is not None}
        else:
            row['disposition'] = 'NOT_RUN'
        table.append(row)

    # A write-only observer test proves nothing by exiting 0; the differential
    # report decides acceptance.
    for name, path in mapping(args.differential):
        summary = load(path).get('summary', {})
        complete = bool(summary.get('cases')) and summary.get('mismatching_cases') == 0 \
            and summary.get('matching_cases') == summary.get('cases') \
            and summary.get('leaves_covered') == summary.get('leaves_go')
        for row in table:
            if row['test'] != name:
                continue
            row['measurement'] = dict(row.get('measurement') or {})
            row['measurement']['differential'] = dict(
                {'report': str(path)},
                **{k: summary.get(k) for k in
                   ('cases', 'matching_cases', 'mismatching_cases',
                    'leaves_go', 'leaves_covered', 'mismatching_paths')})
            row['measurement']['differential_complete'] = complete
            if row['disposition'] == 'PASS' and not complete:
                row['disposition'] = 'FAIL'

    # These Rust tests only WRITE their actual owner rows. The strict reader's
    # comparison decides acceptance, not the producer's exit code.
    for path in args.owner_differential:
        path = pathlib.Path(path)
        summary = load(path)
        for profile, result in (summary.get('results') or {}).items():
            name = result.get('rust_test')
            if not name:
                continue
            complete = (result.get('status') == 'PASS'
                        and result.get('comparison_exit_code') == 0
                        and result.get('go_exit_code') == 0
                        and result.get('rust_exit_code') == 0
                        and bool(result.get('rows')))
            for row in table:
                if row['test'] != name:
                    continue
                row['measurement'] = dict(row.get('measurement') or {})
                row['measurement']['owner_differential'] = dict(
                    {'report': str(path), 'profile': profile},
                    **{k: result.get(k) for k in
                       ('comparison_status', 'rows', 'comparison', 'go_exit_code',
                        'rust_exit_code', 'comparison_exit_code', 'rust_binary_sha256',
                        'go_binary_sha256', 'scope', 'open')})
                row['measurement']['owner_differential_complete'] = complete
                row['measurement']['binary_sha256'] = result.get('rust_binary_sha256')
                row['disposition'] = 'PASS' if complete else 'FAIL'

    counts = {}
    for row in table:
        counts[row['disposition']] = counts.get(row['disposition'], 0) + 1
    missing = [r['test'] for r in table if r['classification'] == 'UNCLASSIFIED']
    out = {'runtime_entries': len(runtime), 'disposition_counts': counts,
           'unclassified': missing, 'table': table}
    pathlib.Path(args.output).write_text(json.dumps(out, indent=2) + '\n')
    print('RUNTIME_ENTRIES', len(runtime))
    print('DISPOSITION_COUNTS', json.dumps(counts))
    print('UNCLASSIFIED', len(missing))
    for name in missing[:20]:
        print('  ', name)
    return 1 if missing or counts.get('FAIL') else 0


if __name__ == '__main__':
    sys.exit(main())