#!/usr/bin/env python3
"""Coordinator-only real fixed-Go oracles and explicit production comparisons."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, help='New private output directory')
    parser.add_argument('--go-toolchain', default='go1.26.3')
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    server = here.parents[2]
    out = Path(args.output).resolve()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    env = dict(os.environ, GOTOOLCHAIN=args.go_toolchain)
    report = {'scope': 'Actual fixed-Go methods plus explicit Rust comparisons; not HTTP/media acceptance',
              'status': 'RUNNING', 'rows': []}
    cases = [('fragment', 'cases.json', 'WHIP_FRAGMENT', True, 29),
             ('track', 'cases.json', 'WHIP_TRACK', True, 14),
             ('client', 'cases.json', 'WHIP_CLIENT', False, 33),
             ('syntax', 'cases.json', 'WHIP_SDP', False, 45),
             ('syntax', 'optional-field-cases.json', 'WHIP_SDP_OPTIONAL', False, 13),
             ('video', 'cases.json', 'WHIP_VIDEO', False, 9),
             ('metrics', 'cases.tsv', 'WHIP_METRICS', False, 6)]
    try:
        for name, filename, prefix, positional, expected in cases:
            case_file = here / name / filename
            result = out / (prefix.lower() + '.out')
            log = out / (prefix.lower() + '.stderr')
            command = ['go', 'run', '-mod=readonly', '.']
            if positional:
                command.append(str(case_file))
            with result.open('xb') as stdout, log.open('xb') as stderr, case_file.open('rb') as stdin:
                subprocess.run(command, cwd=here / name, env=env, check=True,
                               stdin=None if positional else stdin, stdout=stdout, stderr=stderr)
            rows = result.read_bytes().splitlines()
            if len(rows) != expected:
                raise RuntimeError('Actual Go row count differs: ' + prefix)
            env[prefix + '_GO_CASES'] = str(case_file)
            env[prefix + '_GO_RESULTS'] = str(result)
            report['rows'].append({'name': prefix, 'actual_rows': len(rows),
                                   'case_sha256': hashlib.sha256(case_file.read_bytes()).hexdigest(),
                                   'result_sha256': hashlib.sha256(result.read_bytes()).hexdigest()})
        for package, selection in [('lk-compat', 'whip::'), ('rtc-media', 'actual_go_receiver_bytes')]:
            command = ['cargo', 'test', '--release', '--locked', '-p', package,
                       selection, '--', '--ignored']
            with (out / (package + '.log')).open('xb') as log:
                subprocess.run(command, cwd=server, env=env, check=True,
                               stdout=log, stderr=subprocess.STDOUT)
        report['status'] = 'PASS'
    except BaseException:
        report['status'] = 'FAIL'
        raise
    finally:
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
