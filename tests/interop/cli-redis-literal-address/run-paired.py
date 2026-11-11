#!/usr/bin/env python3
"""Paired Go-vs-Rust acceptance of the Redis LITERAL DIAL ADDRESS consumed by the
real `list-nodes` code path in both binaries (SPEC-0076 workline A, item
CONFIG_BEHAVIOR:Redis.LiteralDialAddress).

Why this fixture and not the parse oracle: `livekit-rs-docs/evidence/2026-11-03/c14-redis-literal-dial-address/`
proves `net.ResolveTCPAddr` semantics against a Go oracle program, and the Rust unit
test proves the same table. Neither shows the *shipped CLI* dialing where the literal
value says to dial. This fixture runs the locked official `livekit-server` v1.13.10 and
the Rust `livecloud` with the same argv/ENV, against owned isolated Redis instances, and
compares the externally observable outcome byte for byte.

Endpoint-selection control: two Redis instances hold DIFFERENT seeded nodes
(`owned-node-v4` on port P4, `owned-node-second` on port P4B). An address case must list
exactly its own node id, so a binary that ignored the configured address, defaulted to
6379, or reinterpreted the value as a Redis URL cannot pass.

Input paths covered: `--redis-host` flag, `REDIS_HOST` environment, and flag-over-env
precedence. Failure vectors cover missing port, empty port, refused, unbracketed IPv6,
URL scheme, out-of-range port and unknown named service.

Exit codes and stdout are compared exactly, with no field deleted. stderr is compared on
the `redis:` library diagnostic lines after removing only the wall-clock stamp (the same
rule both sides are subject to); the raw bytes are retained in the output directory.

Usage:
  python3 run-paired.py --official /abs/livekit-server --rust /abs/livecloud --output DIR
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import pathlib
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

ITEM = 'CONFIG_BEHAVIOR:Redis.LiteralDialAddress'
NODE_ID = 'owned-node-v4'
NODE_ID_SECOND = 'owned-node-second'


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def field_varint(number: int, value: int) -> bytes:
    return varint((number << 3) | 0) + varint(value)


def field_fixed32(number: int, raw: bytes) -> bytes:
    return varint((number << 3) | 5) + raw


def field_bytes(number: int, payload: bytes) -> bytes:
    return varint((number << 3) | 2) + varint(len(payload)) + payload


def field_string(number: int, text: str) -> bytes:
    return field_bytes(number, text.encode())


def field_float(number: int, value: float) -> bytes:
    return field_fixed32(number, struct.pack('<f', value))


def node_proto(node_id: str) -> bytes:
    """livekit.Node from references/protocol-stable/protobufs/livekit_internal.proto.

    Node{id=1,ip=2,num_cpus=3,stats=4,type=5,state=6,region=7}
    NodeStats{started_at=1,updated_at=2,num_rooms=3,num_clients=4,num_tracks_in=5,
    num_tracks_out=6,bytes_in=7,bytes_out=8,packets_in=9,packets_out=10,nack_total=11,
    num_cpus=17,load_avg_last1min=18,load_avg_last5min=19,load_avg_last15min=20,
    cpu_load=21,retransmit_bytes_out=22,retransmit_packets_out=23,sys_packets_out=28,
    sys_packets_dropped=29,memory_total=34,memory_used=35,rates=50}
    NodeStatsRate{bytes_in=8,bytes_out=9,packets_in=10,packets_out=11,nack_total=12,
    sys_packets_out=13,sys_packets_dropped=14,retransmit_packets_out=16}
    Fixed values keep the rendered table identical for both binaries.
    """
    rate = (
        field_varint(8, 1_048_576)
        + field_varint(9, 2_097_152)
        + field_varint(10, 3_000)
        + field_varint(11, 4_000)
        + field_varint(12, 5)
        + field_varint(13, 6_000)
        + field_varint(14, 7)
        + field_varint(16, 8)
    )
    stats = (
        field_varint(1, 1_700_000_000)
        + field_varint(2, 1_700_000_060)
        + field_varint(3, 2)
        + field_varint(4, 3)
        + field_varint(5, 4)
        + field_varint(6, 5)
        + field_varint(7, 11_000)
        + field_varint(8, 22_000)
        + field_varint(9, 33)
        + field_varint(10, 44)
        + field_varint(11, 55)
        + field_varint(17, 8)
        + field_float(18, 0.125)
        + field_float(19, 0.25)
        + field_float(20, 0.5)
        + field_float(21, 0.25)
        + field_varint(22, 66)
        + field_varint(23, 77)
        + field_varint(28, 88)
        + field_varint(29, 9)
        + field_varint(34, 34_359_738_368)
        + field_varint(35, 32_212_254_720)
        + field_bytes(50, rate)
    )
    return (
        field_string(1, node_id)
        + field_string(2, '127.0.0.1')
        + field_varint(3, 8)
        + field_bytes(4, stats)
        + field_varint(5, 0)
        + field_varint(6, 1)
        + field_string(7, 'owned')
    )


class Resp:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.file = self.sock.makefile('rb')
        assert self.call(b'PING') == b'PONG'

    def read(self):
        line = self.file.readline(1 << 20)
        assert line.endswith(b'\r\n'), 'bounded RESP line'
        tag, data = line[:1], line[1:-2]
        if tag in (b'+', b'-'):
            return data
        if tag == b':':
            return int(data)
        if tag == b'$':
            n = int(data)
            body = self.file.read(n)
            assert len(body) == n and self.file.read(2) == b'\r\n'
            return body
        raise AssertionError('unexpected RESP type ' + repr(tag))

    def call(self, *args):
        self.sock.sendall(b'*' + str(len(args)).encode() + b'\r\n'
                          + b''.join(b'$' + str(len(v)).encode() + b'\r\n' + v + b'\r\n' for v in args))
        response = self.read()
        if isinstance(response, bytes) and response.startswith(b'ERR'):
            raise AssertionError('owned redis rejected the command: ' + response.decode())
        return response

    def close(self):
        self.file.close()
        self.sock.close()


def free(port: int, bind: str = '127.0.0.1') -> bool:
    family = socket.AF_INET6 if ':' in bind else socket.AF_INET
    with socket.socket(family) as probe:
        return probe.connect_ex((bind, port)) != 0


class Owned:
    """Harness-owned redis-server: private dir, loopback-only, no persistence."""

    def __init__(self, port: int, staging: pathlib.Path, bind: str = '127.0.0.1'):
        self.port, self.bind = port, bind
        self.dir = staging / f'redis-{bind}-{port}'
        self.dir.mkdir(parents=True, exist_ok=True)
        self.log = (self.dir / 'redis.log').open('wb')
        self.proc = subprocess.Popen(
            ['redis-server', '--port', str(port), '--bind', bind,
             '--protected-mode', 'no', '--save', '', '--appendonly', 'no',
             '--dir', str(self.dir), '--logfile', ''],
            stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if not free(port, bind):
                return
            if self.proc.poll() is not None:
                raise AssertionError('redis-server exited during startup; see ' + str(self.dir / 'redis.log'))
            time.sleep(0.05)
        raise AssertionError('owned redis never accepted a connection on ' + bind + ':' + str(port))

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait()
        self.log.close()


def run(binary: pathlib.Path, argv: list[str], env: dict[str, str], out: pathlib.Path, tag: str):
    proc = subprocess.Popen([str(binary), *argv], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env, start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=40)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        stdout, stderr = proc.communicate()
        timed_out = True
    (out / f'{tag}.stdout').write_bytes(stdout)
    (out / f'{tag}.stderr').write_bytes(stderr)
    assert len(stdout) <= (1 << 20) and len(stderr) <= (1 << 20), 'bounded output'
    return {'exit': proc.returncode, 'timed_out': timed_out,
            'stdout_b64': base64.b64encode(stdout).decode(),
            'stderr_b64': base64.b64encode(stderr).decode()}


STAMP = __import__('re').compile(r'^redis: \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ')


def diagnostics(stderr_b64: str) -> list[str]:
    """The `redis:` library lines only, with the wall-clock stamp removed.

    Both binaries are subject to the same rule; the raw stderr stays in the output
    directory so nothing observed here is discarded.
    """
    text = base64.b64decode(stderr_b64).decode('utf-8', 'replace')
    return [STAMP.sub('', line) for line in text.splitlines() if STAMP.match(line)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--official', required=True, type=pathlib.Path)
    ap.add_argument('--rust', required=True, type=pathlib.Path)
    ap.add_argument('--output', required=True, type=pathlib.Path)
    ap.add_argument('--base-port', type=int, default=21679)
    args = ap.parse_args()

    official, rust = args.official.resolve(), args.rust.resolve()
    for binary in (official, rust):
        assert binary.is_file(), binary
    args.output.mkdir(parents=True, exist_ok=True)
    staging = pathlib.Path(tempfile.mkdtemp(prefix='literal-addr-', dir=str(args.output)))

    p4, p4b, p6, p_dead = (args.base_port + i for i in (0, 1, 2, 3))
    for port in (p4, p4b, p6, p_dead):
        assert free(port), f'owned fixture port {port} is already in use'

    report = {
        'status': 'RUNNING',
        'item': ITEM,
        'scope': (f'real paired CLI list-nodes consumption of the literal redis dial address for {ITEM}: '
                  'flag, REDIS_HOST env and flag-over-env precedence on two distinct owned Redis endpoints '
                  '(endpoint selection control), plus missing/empty/refused/IPv6/URL-scheme/out-of-range/'
                  'unknown-service failure vectors. Not the node-stats wire format (tests/interop/'
                  'cli-node-stored-wire), not TLS or Sentinel/Cluster dialing (separate items), '
                  'not the full server lifecycle.'),
        'binaries': {
            'official_go': {'path': str(official), 'sha256': hashlib.sha256(official.read_bytes()).hexdigest()},
            'rust': {'path': str(rust), 'sha256': hashlib.sha256(rust.read_bytes()).hexdigest()},
        },
        # the candidate binary is itself named `livekit-server`, so the paths in
        # `binaries` cannot tell the two sides apart; name each digest with its side
        'go_binary_sha256': hashlib.sha256(official.read_bytes()).hexdigest(),
        'rust_binary_sha256': hashlib.sha256(rust.read_bytes()).hexdigest(),
        'normalization': ('stdout and exit status: none, compared exactly. stderr: only the redis: library '
                          'lines are compared and only their wall-clock stamp is removed; raw bytes retained '
                          'per case in this directory.'),
        'seed': {'node_ids': [NODE_ID, NODE_ID_SECOND],
                 'node_proto_sha256': hashlib.sha256(node_proto(NODE_ID)).hexdigest()},
        'cases': [],
    }
    result = args.output / 'BATCH.json'

    def save():
        result.write_text(json.dumps(report, indent=2) + '\n')

    save()
    instances: list[Owned] = []
    code = 0
    try:
        version = {name: run(binary, ['--version'], dict(os.environ), args.output, f'version-{name}')
                   for name, binary in (('official', official), ('rust', rust))}
        report['version_observations'] = version
        assert base64.b64decode(version['official']['stdout_b64']) == b'livekit-server version 1.13.10\n', \
            'the official binary is not the locked v1.13.10 build'
        assert version['official']['exit'] == 0 and not version['official']['timed_out']
        save()

        instances.append(Owned(p4, staging, '127.0.0.1'))
        instances.append(Owned(p4b, staging, '127.0.0.1'))
        instances.append(Owned(p6, staging, '::1'))
        for bind, port, node in (('127.0.0.1', p4, NODE_ID), ('127.0.0.1', p4b, NODE_ID_SECOND),
                                 ('::1', p6, NODE_ID)):
            conn = Resp(bind, port)
            conn.call(b'DEL', b'nodes')
            assert conn.call(b'HSET', b'nodes', node.encode(), node_proto(node)) == 1
            conn.close()
            report['seed'][f'{bind}:{port}'] = node

        base_env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR') if k in os.environ}
        base_env.update({'TZ': 'UTC', 'LANG': 'C', 'NO_COLOR': '1'})
        for key in ('REDIS_HOST', 'REDIS_PASSWORD', 'LIVEKIT_CONFIG', 'NODE_IP', 'UDP_PORT'):
            base_env.pop(key, None)

        vectors = [
            ('addr-ipv4-first-endpoint', ['--redis-host', f'127.0.0.1:{p4}'], {}, 'success', NODE_ID),
            ('addr-ipv4-second-endpoint', ['--redis-host', f'127.0.0.1:{p4b}'], {}, 'success', NODE_ID_SECOND),
            ('addr-ipv6-bracketed', ['--redis-host', f'[::1]:{p6}'], {}, 'success', NODE_ID),
            ('addr-env-input-path', [], {'REDIS_HOST': f'127.0.0.1:{p4b}'}, 'success', NODE_ID_SECOND),
            ('addr-flag-overrides-env', ['--redis-host', f'127.0.0.1:{p4}'],
             {'REDIS_HOST': f'127.0.0.1:{p4b}'}, 'success', NODE_ID),
            ('addr-missing-port', ['--redis-host', '127.0.0.1'], {}, 'failure', None),
            ('addr-empty-port-zero', ['--redis-host', '127.0.0.1:'], {}, 'failure', None),
            ('addr-connection-refused', ['--redis-host', f'127.0.0.1:{p_dead}'], {}, 'failure', None),
            ('addr-unbracketed-ipv6', ['--redis-host', f'::1:{p4}'], {}, 'failure', None),
            ('addr-unterminated-bracket', ['--redis-host', '[::1:6379'], {}, 'failure', None),
            ('addr-url-scheme-not-reinterpreted', ['--redis-host', f'redis://127.0.0.1:{p4}'], {}, 'failure', None),
            ('addr-port-out-of-range', ['--redis-host', '127.0.0.1:65536'], {}, 'failure', None),
            ('addr-unknown-named-service', ['--redis-host', '127.0.0.1:owned_unknown_service'], {}, 'failure', None),
        ]

        for name, argv, extra_env, expectation, expect_node in vectors:
            env = dict(base_env, **extra_env)
            go = run(official, ['--dev', *argv, 'list-nodes'], env, args.output, f'{name}-official')
            rs = run(rust, ['--dev', *argv, 'list-nodes'], env, args.output, f'{name}-rust')
            rec = {'id': name, 'argv': argv, 'env': extra_env, 'expectation': expectation,
                   'expected_node': expect_node, 'official': go, 'rust': rs,
                   'stdout_exact': go['stdout_b64'] == rs['stdout_b64'],
                   'exit_exact': go['exit'] == rs['exit'],
                   'diagnostics_exact': diagnostics(go['stderr_b64']) == diagnostics(rs['stderr_b64']),
                   'official_diagnostics': diagnostics(go['stderr_b64']),
                   'rust_diagnostics': diagnostics(rs['stderr_b64'])}
            assert rec['stdout_exact'], (name, 'stdout differs', go['stdout_b64'], rs['stdout_b64'])
            assert rec['exit_exact'], (name, 'exit status differs', go['exit'], rs['exit'])
            assert rec['diagnostics_exact'], (name, 'redis: diagnostics differ')
            assert not go['timed_out'] and not rs['timed_out'], name
            go_text = base64.b64decode(go['stdout_b64']).decode()
            if expectation == 'success':
                assert go['exit'] == 0, (name, go_text)
                assert expect_node in go_text, (name, 'seeded node for this endpoint is absent', go_text)
                other = NODE_ID_SECOND if expect_node == NODE_ID else NODE_ID
                assert other not in go_text, (name, 'listed a node that lives on the other endpoint')
                # A CLI that ignored the configured literal address and dialed the
                # ambient 6379 instead would show that instance's node id here.
                assert 'ND_' not in go_text, (name, 'output came from the ambient redis on 6379', go_text)
            else:
                assert go['exit'] != 0, (name, 'failure vector exited 0', go_text)
                assert go_text.startswith('unable to connect to redis:'), (name, go_text)
                assert 'unable to connect to redis' in base64.b64decode(rs['stdout_b64']).decode(), name
            report['cases'].append(rec)
            save()

        go_rows = {c['id'] for c in report['cases']}
        assert len(go_rows) == len(vectors) == 13, 'every declared vector must have a result row'
        failures = {c['id'] for c in report['cases'] if c['expectation'] == 'failure'}
        successes = {c['id'] for c in report['cases'] if c['expectation'] == 'success'}
        assert len(successes) == 5 and len(failures) == 8, 'case coverage must not shrink'
        controls = {c['id']: c['expected_node'] for c in report['cases'] if c['expected_node']}
        assert controls['addr-ipv4-first-endpoint'] != controls['addr-ipv4-second-endpoint'], \
            'endpoint-selection control is degenerate'
        assert controls['addr-flag-overrides-env'] == NODE_ID, \
            'the flag must win over REDIS_HOST, otherwise the precedence vector proves nothing'

        report['aggregate_status'] = ('PASS_CLI_REDIS_LITERAL_DIAL_ADDRESS_PAIRED'
                                      if all(c['stdout_exact'] and c['exit_exact'] and c['diagnostics_exact']
                                             for c in report['cases']) else 'FAIL')
        report['status'] = report['aggregate_status']
    except BaseException as exc:  # retain the failure, never a partial pass
        report['status'] = 'FAIL'
        report['failure'] = f'{type(exc).__name__}: {exc}'
        code = 1
    finally:
        for instance in instances:
            instance.stop()
        shutil.rmtree(staging, ignore_errors=True)
        save()
    print(json.dumps({'status': report['status'], 'cases': len(report['cases']),
                      'failure': report.get('failure')}))
    return code


if __name__ == '__main__':
    sys.exit(main())
