#!/usr/bin/env python3
"""Compare Go debug trailing-slash redirects when cleaning escaped paths."""
import argparse
from importlib.util import module_from_spec, spec_from_file_location
import json
from pathlib import Path

SPEC = spec_from_file_location('debug_wire', Path(__file__).with_name('run-wire.py'))
wire = module_from_spec(SPEC)
SPEC.loader.exec_module(wire)

CASES = [
    ('plain_parent', 'GET', '/left/../debug/pprof?x=%2F', '/debug/pprof/?x=%2F'),
    ('escaped_literal', 'GET', '/debug/%70prof', '/debug/pprof/'),
    ('escaped_separator', 'GET', '/left%2Fright/../debug/pprof', '/left/debug/pprof/'),
    ('escaped_separator_head', 'HEAD', '/left%2fright/../debug/%70prof?x=%2F', '/left/debug/pprof/?x=%2F'),
    ('escaped_separator_post', 'POST', '/left%2fright/../debug/pprof', '/left/debug/pprof/'),
    ('escaped_parent', 'GET', '/left%2f%2e%2e%2fright/../debug/pprof', '/debug/pprof/'),
    ('escaped_question', 'GET', '/left%3fq%2fright/../debug/pprof?raw=x%2F', '/left%3Fq/debug/pprof/?raw=x%2F'),
    ('non_utf8_decoded_byte', 'GET', '/%ff%2fright/../debug/pprof', '/%FF/debug/pprof/'),
    ('decoded_target_is_root', 'GET', '/debug/pprof/foo%2F..%2F../..', '/'),
    ('ordinary_redirect', 'GET', '/debug//%72ooms', '/debug/%2572ooms'),
]
wire.CASES = [(name, method, target) for name, method, target, _ in CASES]
wire.RESPONSE_HEADERS += ['content-length', 'transfer-encoding']


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
                assert report[side]['server_exit'] == 0 and not report[side].get('forced_kill')
        go = report['go']['cases']
        for row, (_, method, _, location) in zip(go, CASES, strict=True):
            assert row['status'] == 307 and row['headers']['location'] == location, row
            if method == 'HEAD':
                assert row['body'] == '' and row['headers']['content-length'] is None, row
        if args.rust:
            report['compared'] = [
                {'case': expected['case'], 'match': expected == actual}
                for expected, actual in zip(go, report['rust']['cases'], strict=True)
            ]
            return int(not all(row['match'] for row in report['compared']))
        return 0
    finally:
        (output / 'execution.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
