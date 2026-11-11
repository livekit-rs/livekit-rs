#!/usr/bin/env python3
"""Compare the dedicated diagnostic HTTP routes on real owned daemons."""
import argparse
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path

SPEC = spec_from_file_location('debug_wire', Path(__file__).with_name('run-wire.py'))
wire = module_from_spec(SPEC)
SPEC.loader.exec_module(wire)

wire.CASES = [
    (f'{name}_{method.lower()}', method, f'/debug/{name}')
    for name in ('rooms', 'goroutine')
    for method in ('GET', 'HEAD', 'POST', 'PUT', 'OPTIONS')
]


def normalize(row, side):
    if row['case'].startswith('rooms_'):
        return row
    normalized = dict(row)
    if row['status'] == 200 and row['method'] != 'HEAD':
        body = row['body']
        expected = 'goroutine ' if side == 'go' else 'Rust native thread stack samples'
        normalized['body'] = {'native_dump': body.startswith(expected)}
    return normalized


def verify_expected(rows):
    for row in rows:
        assert row['status'] == 200, row
        assert row['headers']['content-type'] == 'text/plain; charset=utf-8', row
        if row['method'] == 'HEAD':
            assert row['body'] == '', row
        elif row['case'].startswith('rooms_'):
            assert row['body'] == '[]', row
        else:
            assert row['body'] == {'native_dump': True}, row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', type=Path, required=True)
    parser.add_argument('--rust', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {}
    try:
        for side, binary in [('go', args.go), ('rust', args.rust)]:
            if binary is not None:
                wire.run(str(binary.resolve(strict=True)), output, side, report.setdefault(side, {}))
                assert report[side]['server_exit'] == 0 and not report[side].get('forced_kill'), report[side]
        go = [normalize(row, 'go') for row in report['go']['cases']]
        verify_expected(go)
        if args.rust:
            rust = [normalize(row, 'rust') for row in report['rust']['cases']]
            report['compared'] = [{'case': a['case'], 'match': a == b}
                                  for a, b in zip(go, rust, strict=True)]
            return int(not all(row['match'] for row in report['compared']))
        return 0
    finally:
        (output / 'execution.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
