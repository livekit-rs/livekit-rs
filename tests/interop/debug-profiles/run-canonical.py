#!/usr/bin/env python3
"""Compare path canonicalization and fallback on the dedicated debug listener."""
import argparse
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path

SPEC = spec_from_file_location('debug_wire', Path(__file__).with_name('run-wire.py'))
wire = module_from_spec(SPEC)
SPEC.loader.exec_module(wire)

CASES = [
    ('duplicate_separators', 'GET', '/debug//pprof/heap?x=1&y=%3C', 307),
    ('dot_segment', 'GET', '/debug/pprof/./heap', 307),
    ('dotdot_segment', 'GET', '/debug/pprof/anything/../heap', 307),
    ('clean_to_slash', 'GET', '/debug//pprof?debug=1', 307),
    ('slash_head', 'HEAD', '/debug/pprof?x=1&y=%3C', 307),
    ('duplicate_head', 'HEAD', '/debug//rooms?x=1&y=%3C', 307),
    ('duplicate_post', 'POST', '/debug//pprof/symbol?x=1', 307),
    ('trailing_slash_preserved', 'GET', '/debug/pprof//heap/', 307),
    ('escaped_dot_with_duplicate', 'GET', '/debug/pprof//%2e%2e/heap', 307),
    ('escaped_separator_with_duplicate', 'GET', '/debug/pprof//a%2fb', 307),
    ('escaped_dot_without_duplicate', 'GET', '/debug/pprof/%2e%2e/heap', 404),
    ('escaped_separator_without_duplicate', 'GET', '/debug/pprof/a%2fb', 404),
    ('clean_unknown', 'GET', '/debug//does-not-exist', 307),
    ('unknown', 'GET', '/debug/does-not-exist', 404),
    ('unknown_head', 'HEAD', '/debug/does-not-exist', 404),
    ('unknown_post', 'POST', '/debug/does-not-exist', 404),
    ('room_trailing_slash', 'GET', '/debug/rooms/', 404),
    ('unknown_pprof_control', 'GET', '/debug/pprof/not-a-profile', 404),
    ('known_room_control', 'GET', '/debug/rooms', 200),
    ('encoded_rooms', 'GET', '/debug/%72ooms', 200),
    ('encoded_debug_and_rooms', 'GET', '/%64ebug/r%6Foms', 200),
    ('encoded_pprof_literal', 'GET', '/debug/%70prof/heap?seconds=0', 400),
    ('literal_symbol_control', 'GET', '/debug/pprof/symbol', 200),
    ('encoded_symbol_literal', 'GET', '/debug/pprof/%73ymbol', 200),
    ('encoded_slash_redirect', 'GET', '/debug/%70prof?x=%2F', 307),
    ('encoded_clean_to_slash', 'HEAD', '/debug//%70prof?x=%2F', 307),
    ('encoded_ordinary_clean_redirect', 'GET', '/debug//%72ooms', 307),
    ('encoded_slash_is_not_separator', 'GET', '/debug%2Frooms', 404),
    ('double_encoded_literal', 'GET', '/debug/%2572ooms', 404),
]
wire.CASES = [(name, method, path) for name, method, path, _ in CASES]
wire.RESPONSE_HEADERS += ['content-length', 'transfer-encoding']


def comparable(row):
    if row['status'] != 200 or row['case'] not in ('literal_symbol_control', 'encoded_symbol_literal'):
        return row
    # Native symbol availability can differ across runtimes.
    assert row['body'] in ('num_symbols: 0\n', 'num_symbols: 1\n'), row
    return {**row, 'body': 'native symbol count line'}


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
                cases = {row['case']: row for row in report[side]['cases']}
                fields = ('status', 'headers', 'body')
                report[side]['encoded_symbol_matches_literal'] = all(
                    cases['encoded_symbol_literal'][key] == cases['literal_symbol_control'][key]
                    for key in fields
                )
        go = report['go']['cases']
        assert [row['status'] for row in go] == [status for _, _, _, status in CASES], go
        for row in go:
            if row['method'] == 'HEAD' and row['status'] == 307:
                assert row['headers']['content-length'] is None, row
        assert report['go']['encoded_symbol_matches_literal']
        if args.rust:
            report['compared'] = [
                {'case': a['case'], 'match': comparable(a) == comparable(b)}
                for a, b in zip(go, report['rust']['cases'], strict=True)
            ]
            return int(not all(row['match'] for row in report['compared'])
                       or not report['rust']['encoded_symbol_matches_literal'])
        return 0
    finally:
        (output / 'execution.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
