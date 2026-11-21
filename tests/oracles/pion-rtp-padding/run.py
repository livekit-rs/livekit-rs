#!/usr/bin/env python3
"""Actual fixed-Pion padding admission rows paired with the str0m receive path.

Runs the real `rtp.Packet.Unmarshal` observer over the committed vectors,
validates every field against the committed expectations, binds the results to
the ignored Rust differential test, and finally requires that test to fail on a
mutated result file so the comparison cannot pass vacuously.
"""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
TEST = 'rtp::header::test::original_go_rtp_padding_admission_rows_match_native_parser'


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cargo(env, out, label, results, expect_success):
    log = out / (label + '.log')
    command = ['cargo', 'test', '--release', '--locked', '-p', 'str0m', TEST,
               '--', '--ignored', '--exact']
    with log.open('xb') as handle:
        proc = subprocess.run(command, cwd=ROOT,
                              env=dict(env, PION_RTP_PADDING_GO_RESULTS=str(results)),
                              stdout=handle, stderr=subprocess.STDOUT)
    passed = proc.returncode == 0
    if passed != expect_success:
        raise RuntimeError('%s: exit %d, expected %s (see %s)' %
                           (label, proc.returncode,
                            'success' if expect_success else 'failure', log))
    return {'exit_code': proc.returncode, 'log': str(log),
            'log_sha256': sha256(log)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--output', required=True, help='New private output directory')
    ap.add_argument('--go-toolchain', default='go1.26.3')
    args = ap.parse_args()
    out = pathlib.Path(args.output).resolve()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    cases = HERE / 'cases.jsonl'
    results = out / 'pion-rtp-padding.results'
    log = out / 'observer.log'
    env = dict(os.environ, GOTOOLCHAIN=args.go_toolchain, GOPROXY='off')
    report = {
        'scope': 'rtp.Packet.Unmarshal padding admission decision table only; the '
                 'authenticated session-level publication boundary is the str0m '
                 'session test, and no real sender can serialise these vectors',
        'status': 'RUNNING',
        'fixed_go_commit': json.loads((HERE / 'SOURCE.json').read_text())['fixed_go_commit'],
        'cases_sha256': sha256(cases),
        'observer_sha256': sha256(HERE / 'main.go'),
    }
    try:
        vectors = [json.loads(line) for line in cases.read_text().splitlines() if line.strip()]
        with results.open('xb') as stdout, log.open('xb') as stderr, cases.open('rb') as stdin:
            subprocess.run(['go', 'run', '-mod=readonly', '.'], cwd=HERE, env=env,
                           check=True, stdin=stdin, stdout=stdout, stderr=stderr)
        prefix = 'SOURCE_PION_RTP_PADDING '
        rows = [json.loads(line[len(prefix):]) for line in
                results.read_text().splitlines() if line.strip()]
        if len(rows) != len(vectors):
            raise RuntimeError('actual Go row count %d != %d vectors'
                               % (len(rows), len(vectors)))
        if any(not line.startswith(prefix) for line in results.read_text().splitlines()):
            raise RuntimeError('unexpected observer output line')
        by_case = {}
        for row in rows:
            if row['case'] in by_case:
                raise RuntimeError('repeated actual case ' + row['case'])
            by_case[row['case']] = row
        fields = ('accepted', 'payload_hex', 'padding_only', 'padding_bit', 'padding_size')
        compared = 0
        for vector in vectors:
            row = by_case.get(vector['case'])
            if row is None:
                raise RuntimeError('missing actual case ' + vector['case'])
            if row['bytes_hex'] != vector['bytes_hex']:
                raise RuntimeError('actual input hex drifted: ' + vector['case'])
            for field in fields:
                if row[field] != vector['expected'][field]:
                    raise RuntimeError('%s.%s: actual %r != committed %r' %
                                       (vector['case'], field, row[field],
                                        vector['expected'][field]))
                compared += 1
            if bool(row['error_text']) != (not row['accepted']):
                raise RuntimeError('%s: error_text disagrees with accepted' % vector['case'])
        report['rows'] = len(rows)
        report['fields_compared'] = compared
        report['results_sha256'] = sha256(results)
        report['observer_log_sha256'] = sha256(log)
        env['PION_RTP_PADDING_GO_CASES'] = str(cases)
        report['cargo'] = cargo(env, out, 'paired', results, True)

        mutant = out / 'pion-rtp-padding.results.mutant'
        mutated = []
        changed = False
        for row in rows:
            row = dict(row)
            if row['case'] == 'pad_zero_count' and not row['accepted']:
                row.update(accepted=True, error_text='', payload_hex='', padding_only=True)
                changed = True
            mutated.append(row)
        if not changed:
            raise RuntimeError('no vector available for the mutation control')
        with mutant.open('xb') as handle:
            for row in mutated:
                handle.write(prefix + json.dumps(row, sort_keys=True) + '\n')
        report['mutant'] = cargo(env, out, 'mutant', mutant, False)
        report['mutant_field'] = 'pad_zero_count.accepted'
        report['status'] = 'PASS'
    except BaseException:
        report['status'] = 'FAIL'
        raise
    finally:
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
