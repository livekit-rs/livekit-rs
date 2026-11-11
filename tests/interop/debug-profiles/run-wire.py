#!/usr/bin/env python3
"""Compare dedicated debug-listener pprof routing and HTTP metadata."""
import argparse
from contextlib import ExitStack
import hashlib
from http.client import HTTPConnection
import json
from pathlib import Path
import signal
import socket
import subprocess
import time


PREFIX = '/debug/pprof'
RESPONSE_HEADERS = ['content-type', 'location', 'x-content-type-options', 'x-go-pprof',
                    'content-disposition']
CASES = [
    ('slash_redirect', 'GET', PREFIX),
    ('slash_redirect_query', 'GET', PREFIX + '?x=1&y=%3C'),
    ('slash_redirect_head', 'HEAD', PREFIX + '?x=1&y=%3C'),
    ('index_html', 'GET', PREFIX + '/'),
    ('cmdline_nul_separated', 'GET', PREFIX + '/cmdline'),
    ('symbol_query', 'GET', PREFIX + '/symbol?0x0'),
    ('symbol_post', 'POST', PREFIX + '/symbol'),
    ('unknown_profile', 'GET', PREFIX + '/unknown-fixture-profile'),
    ('unknown_nested_profile', 'GET', PREFIX + '/unknown-fixture-profile/nested'),
    ('invalid_seconds', 'GET', PREFIX + '/heap?seconds=invalid'),
    ('incompatible_options', 'GET', PREFIX + '/allocs?seconds=1&debug=1'),
    ('native_binary_heap', 'GET', PREFIX + '/heap'),
]


def request(port, method, path):
    connection = HTTPConnection('127.0.0.1', port, timeout=10)
    try:
        connection.request(method, path, b'0x0' if method == 'POST' else None)
        response = connection.getresponse()
        return response.status, {key.lower(): value for key, value in response.getheaders()}, response.read()
    finally:
        connection.close()


def run(binary, output, side, report):
    with ExitStack() as stack:
        ports = []
        for protocol in (socket.SOCK_STREAM, socket.SOCK_STREAM, socket.SOCK_STREAM, socket.SOCK_DGRAM):
            reserve = stack.enter_context(socket.socket(type=protocol))
            reserve.bind(('127.0.0.1', 0))
            ports.append(reserve.getsockname()[1])
    main, debug, tcp, udp = ports
    config = output / (side + '.yaml')
    config.write_text(f"""port: {main}
bind_addresses: [127.0.0.1]
debug_handler:
  port: {debug}
rtc:
  tcp_port: {tcp}
  udp_port: {udp}
  node_ip: 127.0.0.1
  use_external_ip: false
  enable_loopback_candidate: true
keys:
  devkey: public-profile-fixture-secret-0123456789
turn:
  enabled: false
logging:
  level: error
""")
    with (output / (side + '.server.log')).open('wb') as log:
        process = subprocess.Popen([binary, '--config', str(config)], stdout=log, stderr=subprocess.STDOUT)
        with open(binary, 'rb') as source:
            report.update(binary=binary, sha256=hashlib.file_digest(source, 'sha256').hexdigest(), cases=[])
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                assert process.poll() is None, (side, 'startup failed', process.returncode)
                try:
                    if request(main, 'GET', '/')[0] == 200 and request(debug, 'GET', PREFIX + '/')[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(.05)
            else:
                raise RuntimeError(f'{side} did not open both listeners')
            # Go's signal handler ignores startup interrupts until its running bit flips.
            time.sleep(.2)
            for name, method, path in CASES:
                status, headers, body = request(debug, method, path)
                row = {'case': name, 'method': method, 'path': path, 'status': status,
                       'headers': {key: headers.get(key) for key in RESPONSE_HEADERS}}
                if name == 'native_binary_heap':
                    row['body'] = {'gzip': body.startswith(b'\x1f\x8b'), 'bytes': len(body)}
                else:
                    row['body'] = body.decode('utf-8')
                report['cases'].append(row)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                report['server_exit'] = process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                report['server_exit'] = process.wait(timeout=10)
                report['forced_kill'] = True


def normalized(row, report, config):
    case = row['case']
    out = dict(row)
    if case == 'index_html':
        out['body'] = {name: name in row['body'] for name in ('profile', 'heap', 'allocs', 'cmdline', 'symbol', 'trace')}
    elif case == 'cmdline_nul_separated':
        out['body'] = {'correct_argv': row['body'].split('\0') == [report['binary'], '--config', str(config)]}
    elif case in ('symbol_query', 'symbol_post'):
        out['body'] = {'symbol_count_line': row['body'] in ('num_symbols: 0\n', 'num_symbols: 1\n')}
    elif case == 'native_binary_heap':
        out['body'] = {'gzip': row['body']['gzip'], 'nonempty': row['body']['bytes'] > 2}
    return out


def verify_positives(rows):
    cases = {row['case']: row for row in rows}
    for case in ('index_html', 'cmdline_nul_separated', 'symbol_query', 'symbol_post', 'native_binary_heap'):
        row = cases[case]
        assert row['status'] == 200 and all(row['body'].values()), row


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
                run(str(binary.resolve(strict=True)), output, side, report.setdefault(side, {}))
                assert report[side]['server_exit'] == 0 and not report[side].get('forced_kill'), report[side]
        if args.rust:
            go = [normalized(row, report['go'], output / 'go.yaml') for row in report['go']['cases']]
            rust = [normalized(row, report['rust'], output / 'rust.yaml') for row in report['rust']['cases']]
            verify_positives(go)
            verify_positives(rust)
            report['compared'] = [{'case': a['case'], 'match': a == b} for a, b in zip(go, rust, strict=True)]
            return int(not all(row['match'] for row in report['compared']))
        return 0
    finally:
        (output / 'execution.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
