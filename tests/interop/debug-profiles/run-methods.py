#!/usr/bin/env python3
"""Exercise method-independent pprof routing on an owned debug listener."""
import argparse
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path

SPEC = spec_from_file_location('debug_wire', Path(__file__).with_name('run-wire.py'))
wire = module_from_spec(SPEC)
SPEC.loader.exec_module(wire)

PREFIX = wire.PREFIX
wire.CASES = [
    ('post_redirect', 'POST', PREFIX + '?x=1&y=%3C'),
    ('options_redirect', 'OPTIONS', PREFIX),
    ('index_html', 'POST', PREFIX + '/'),
    ('cmdline_nul_separated', 'PUT', PREFIX + '/cmdline'),
    ('symbol_query', 'OPTIONS', PREFIX + '/symbol?0x0'),
    ('symbol_post', 'POST', PREFIX + '/symbol'),
    ('unknown_profile', 'PATCH', PREFIX + '/unknown-fixture-profile'),
    ('unknown_nested_profile', 'DELETE', PREFIX + '/unknown-fixture-profile/nested'),
    ('invalid_seconds', 'OPTIONS', PREFIX + '/heap?seconds=invalid'),
    ('incompatible_options', 'PUT', PREFIX + '/allocs?seconds=1&debug=1'),
    ('native_binary_heap', 'POST', PREFIX + '/heap'),
]


def verify_go(rows):
    wire.verify_positives(rows)
    by_case = {row['case']: row for row in rows}
    for name, location in [('post_redirect', PREFIX + '/?x=1&y=%3C'),
                           ('options_redirect', PREFIX + '/')]:
        row = by_case[name]
        assert row['status'] == 307 and row['headers']['location'] == location, row
        assert row['headers']['content-type'] is None and row['body'] == '', row
    for name, status, text in [
        ('unknown_profile', 404, 'Unknown profile\n'),
        ('unknown_nested_profile', 404, 'Unknown profile\n'),
        ('invalid_seconds', 400, 'invalid value for "seconds" - must be a positive integer\n'),
        ('incompatible_options', 400, 'seconds and debug params are incompatible\n'),
    ]:
        row = by_case[name]
        assert row['status'] == status and row['body'] == text, row
        assert row['headers']['x-go-pprof'] == '1', row
        assert row['headers']['x-content-type-options'] == 'nosniff', row


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
        go = [wire.normalized(row, report['go'], output / 'go.yaml') for row in report['go']['cases']]
        verify_go(go)
        if args.rust:
            rust = [wire.normalized(row, report['rust'], output / 'rust.yaml') for row in report['rust']['cases']]
            report['compared'] = [{'case': a['case'], 'match': a == b} for a, b in zip(go, rust, strict=True)]
            return int(not all(row['match'] for row in report['compared']))
        return 0
    finally:
        (output / 'execution.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
