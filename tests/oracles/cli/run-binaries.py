#!/usr/bin/env python3
"""Real-process CLI differential; requires two prestarted dedicated Redis brokers.
No server/compiler is started by this script. No library store substitutes.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile


def invoke(command, *, env=None, data=None):
    result = subprocess.run(command, input=data, capture_output=True,
                            env=env, timeout=20, check=False)
    return {'exit': result.returncode, 'stdout': result.stdout.decode('utf-8', 'replace'),
            'stderr': result.stderr.decode('utf-8', 'replace')}


def table_equivalent(left, right):
    if left == right:
        return True
    # HVALS order is unspecified. Preserve complete raw stdout; only the order
    # of complete multi-line body row groups can vary between dedicated Redis
    # instances. No cell contents, widths, borders or headers are normalized.
    def split(value):
        lines = value.splitlines(keepends=True)
        boundaries = [i for i, line in enumerate(lines) if line.startswith('├')]
        if not boundaries:
            return (value, ())
        first = boundaries[0]
        groups = []
        for begin, end in zip(boundaries, boundaries[1:] + [len(lines) - 1]):
            groups.append(''.join(lines[begin + 1:end]))
        return (''.join(lines[:first + 1]) + lines[-1], tuple(sorted(groups)))
    return split(left) == split(right)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official', required=True)
    parser.add_argument('--rust', required=True)
    parser.add_argument('--reference-redis-port', type=int, required=True)
    parser.add_argument('--rust-redis-port', type=int, required=True)
    parser.add_argument('--db', type=int, default=3)
    parser.add_argument('--unreachable-port', type=int, required=True)
    parser.add_argument('--wire-fixtures', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--dedicated-brokers', action='store_true', required=True)
    options = parser.parse_args()
    assert options.reference_redis_port != options.rust_redis_port
    assert options.db > 0
    sides = {'official': {'binary': options.official, 'port': options.reference_redis_port,
                         'password': os.environ.get('CLI_REFERENCE_REDIS_PASSWORD', ''),
                         'username': os.environ.get('CLI_REFERENCE_REDIS_USERNAME', '')},
             'rust': {'binary': options.rust, 'port': options.rust_redis_port,
                      'password': os.environ.get('CLI_RUST_REDIS_PASSWORD', ''),
                      'username': os.environ.get('CLI_RUST_REDIS_USERNAME', '')}}
    fixtures = {row['name']: row['wire_base64'] for row in map(json.loads, Path(options.wire_fixtures).read_text().splitlines())}
    rows = []

    def redis(side, *args, data=None, db=None, raw=False):
        env = os.environ.copy()
        if side['password']:
            env['REDISCLI_AUTH'] = side['password']
        else:
            env.pop('REDISCLI_AUTH', None)
        command = ['redis-cli', '--raw', '-h', '127.0.0.1', '-p', str(side['port']), '-n', str(options.db if db is None else db)]
        if side['username']:
            command += ['--user', side['username']]
        if data is not None:
            command += ['-x']
        if raw:
            answer = subprocess.run(command + list(args), input=data, env=env,
                                    capture_output=True, timeout=20, check=False)
            if answer.returncode:
                raise RuntimeError('dedicated Redis snapshot failed; credentials withheld')
            return answer.stdout
        answer = invoke(command + list(args), env=env, data=data)
        # Redis CLI exits zero for server errors unless -e, so command callers
        # assert positive results rather than treating exit alone as success.
        if answer['exit']:
            raise RuntimeError('dedicated Redis operation failed; credentials withheld')
        return answer['stdout'].strip()

    for side in sides.values():
        assert redis(side, 'PING') == 'PONG', 'broker authentication/health prerequisite'
        assert redis(side, 'EXISTS', 'nodes') == '0', 'refuse pre-existing nodes key'
        assert redis(side, 'EXISTS', 'nodes', db=options.db + 1) == '0', 'alternate DB must also be empty'

    with tempfile.TemporaryDirectory(prefix='livekit-rs-f05-cli-') as temporary:
        directory = Path(temporary)

        def call(side, db=None, password=None, endpoint_port=None):
            body = {'rtc': {'node_ip': '127.0.0.1'}, 'redis': {
                'address': '127.0.0.1:' + str(side['port'] if endpoint_port is None else endpoint_port),
                'username': side['username'], 'password': side['password'] if password is None else password,
                'db': options.db if db is None else db}}
            config = directory / ('config-' + str(side['port']) + '.json')
            config.write_text(json.dumps(body))
            config.chmod(0o600)
            env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin')}
            result = invoke([side['binary'], 'list-nodes', '--config', str(config)], env=env)
            # Defensively redact configured credential tokens in evidence only.
            for secret in (side['password'], password):
                if secret:
                    for stream in ('stdout', 'stderr'):
                        result[stream] = result[stream].replace(secret, '[credential]')
            return result

        def compare(name, *, db=None, password=None, endpoint_port=None):
            before = {key: redis(side, 'DUMP', 'nodes', raw=True) for key, side in sides.items()}
            pair = {key: call(side, db=db, password=password, endpoint_port=endpoint_port) for key, side in sides.items()}
            unchanged = all(redis(side, 'DUMP', 'nodes', raw=True) == before[key] for key, side in sides.items())
            ok = unchanged and pair['official']['exit'] == pair['rust']['exit'] and table_equivalent(pair['official']['stdout'], pair['rust']['stdout'])
            rows.append({'name': name, **pair, 'equal_exit_and_stdout': ok, 'raw_stored_value_unchanged': unchanged,
                         'stderr_scope': 'preserved diagnostic output; timestamps/stack frames not declared equal'})

        def clear():
            for side in sides.values():
                redis(side, 'DEL', 'nodes')

        def seed(values):
            for side in sides.values():
                for i, wire in enumerate(values):
                    assert redis(side, 'HSET', 'nodes', 'fixture-' + str(i), data=wire) in ('0', '1')

        try:
            compare('empty-nodes')
            compare('unreachable-endpoint', endpoint_port=options.unreachable_port)
            for name in ('nodes-local-zero', 'nodes-stat-boundaries'):
                seed([base64.b64decode(value, validate=True) for value in fixtures[name]])
                compare(name)
                compare(name + '-other-db-empty', db=options.db + 1)
                for side in sides.values():
                    assert redis(side, 'HLEN', 'nodes') == str(len(fixtures[name])), 'read-only command changed records'
                clear()
            seed([base64.b64decode(fixtures['nodes-local-zero'][0]), base64.b64decode(fixtures['nodes-stat-boundaries'][0])])
            compare('two-nodes-unspecified-HVALS-order')
            clear()
            for name, data in [('nil-stats', b'\x0a\x08ND_empty'), ('corrupt-wire', b'\xff'), ('invalid-node-utf8', b'\x0a\x01\xff')]:
                seed([data]); compare(name); clear()
            for side in sides.values():
                assert redis(side, 'SET', 'nodes', 'f05-owned-wrongtype') == 'OK'
            compare('nodes-wrongtype')
            clear()
            if all(side['password'] for side in sides.values()):
                compare('invalid-credentials', password='f05-deliberately-invalid-password')
            else:
                rows.append({'name': 'invalid-credentials', 'status': 'NOT_RUN', 'reason': 'both dedicated brokers must require auth'})
        finally:
            # Dedicated, initially empty brokers and the exact nodes key alone.
            clear()
            for side in sides.values():
                assert redis(side, 'EXISTS', 'nodes') == '0'
        result = {'status': 'PASS' if all(row.get('equal_exit_and_stdout', False) for row in rows) else 'FAIL_OR_INCOMPLETE',
                  'scope': 'actual CLI processes/Redis, exact exit/stdout except whole HVALS row order, raw stderr retained',
                  'cases': rows}
        Path(options.output).write_text(json.dumps(result, indent=2) + '\n')
        return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
