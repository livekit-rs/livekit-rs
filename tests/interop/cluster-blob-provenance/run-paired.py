#!/usr/bin/env python3
"""C45: paired two-node Go/Rust differential for cross-node participant Blob
unknown-field provenance (WIRE:ClusterSignal.ParticipantBlobProvenance,
official_differential_lifecycle tier).

Every server/broker process is owned by this runner; no external Redis or server
is stopped, and no Cargo/Go compilation happens here. The fixed official Go
binary identity is checked before execution.

For each pair: an isolated Redis broker, owner node b (real node registration
witnessed via redis-cli HKEYS nodes) and front node a, BOTH configured with the
top-level `enable_participant_data_blob: true` gate plus the C44 non-default
signal_relay block (psrpc stays at defaults). blob-relay.mjs pins a fresh room to
node b through the owner RoomServiceClient createRoom({nodeId}) and then runs all
26 Go-oracle Blob cases with real writer/reader raw WebSocket actors connected to
FRONT node a, so Join/Store/Get traverse the PSRPC Redis RelaySignal stream. A
MONITOR tap witnesses the actual RelaySignal PUBLISH envelopes. Each response is
compared against the fixed Go codec oracle AND captured raw + normalized
(requestId stripped); after both pure legs run, the Go-leg vs Rust-leg normalized
trees are deepEqual'd per case (paired-comparison.json). That direct equality is
the core differential; oracle conformance alone is not sufficient.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

KEY = 'agentpaired'
SECRET = 'public-agent-paired-test-fixture-secret-0123456'
FIXED_GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'

# Non-default signal_relay in every profile (C44): the launched relay transport
# must consume these, not ignore them. psrpc stays at official defaults.
SIGNAL_RELAY = ('signal_relay: {retry_timeout: 10s, min_retry_interval: 50ms, '
                'max_retry_interval: 200ms, stream_buffer_size: 500, connect_attempts: 5}\n')
# Top-level gate on BOTH nodes (Go config.go EnableParticipantDataBlob,
# Rust official.rs enable_participant_data_blob; default false).
BLOB_GATE = 'enable_participant_data_blob: true\n'

PURE_LEGS = ['go-front-go-owner', 'rust-front-rust-owner']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def free_ports(count):
    # Bind-verify candidates BELOW the OS ephemeral range (macOS
    # net.inet.ip.portrange.first=49152): a bind(0)-then-close reservation inside
    # the ephemeral range can be stolen by the runner's own outbound connections
    # (redis-cli readiness polls, MONITOR tap) before the owned server binds.
    import random
    rng = random.Random()
    picked = []
    seen = set()
    while len(picked) < count:
        port = rng.randint(20000, 48000)
        if port in seen:
            continue
        seen.add(port)
        sock = socket.socket()
        try:
            sock.bind(('127.0.0.1', port))
        except OSError:
            continue
        finally:
            sock.close()
        picked.append(port)
        if len(seen) > 4000:
            raise RuntimeError('cannot reserve enough non-ephemeral ports')
    return picked


def wait_http(proc, port):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f'owned server exited before ready: {proc.returncode}')
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/', timeout=.5) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(.05)
    raise TimeoutError('owned HTTP server readiness')


class RedisError(Exception):
    pass


class Resp:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.file = self.sock.makefile('rb')
        assert self.call(b'PING') == b'PONG'

    def read(self):
        line = self.file.readline(1 << 20)
        tag, data = line[:1], line[1:-2]
        if tag == b'+':
            return data
        if tag == b'-':
            raise RedisError(data.decode(errors='replace'))
        if tag == b':':
            return int(data)
        if tag == b'$':
            size = int(data)
            body = self.file.read(size)
            assert len(body) == size and self.file.read(2) == b'\r\n'
            return body
        if tag == b'*':
            return [self.read() for _ in range(int(data))]
        raise AssertionError('unexpected RESP type ' + repr(tag))

    def call(self, *args):
        self.sock.sendall(b'*' + str(len(args)).encode() + b'\r\n'
                          + b''.join(b'$' + str(len(v)).encode() + b'\r\n' + v + b'\r\n'
                                     for v in args))
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


class Monitor:
    """Redis MONITOR tap capturing the raw command stream while the cross-node
    RelaySignal stream flows. Only the live PUBLISH witness proves the Blob
    Store/Get actually crossed the Redis bus between the two owned nodes."""

    def __init__(self, port):
        self.resp = Resp('127.0.0.1', port)
        self.resp.sock.settimeout(30)
        assert self.resp.call(b'MONITOR') == b'OK', 'MONITOR was not acknowledged'
        self._lines = []
        self._lock = threading.Lock()
        self._stopped = False
        self._thread = threading.Thread(target=self._drain, daemon=True)
        self._thread.start()

    def _drain(self):
        while not self._stopped:
            try:
                raw = self.resp.file.readline(1 << 20)
            except OSError:
                break
            if not raw:
                break
            with self._lock:
                self._lines.append(raw.decode(errors='replace').rstrip('\r\n'))

    def snapshot(self):
        with self._lock:
            return list(self._lines)

    def close(self):
        self._stopped = True
        try:
            self.resp.close()
        except OSError:
            pass
        self._thread.join(timeout=3)


def unescape_monitor(token):
    out = bytearray()
    i = 0
    short = {'n': 10, 'r': 13, 't': 9, 'a': 7, 'b': 8, '\\': 92, '"': 34}
    while i < len(token):
        c = token[i]
        if c == '\\':
            n = token[i + 1]
            if n == 'x':
                out.append(int(token[i + 2:i + 4], 16)); i += 4; continue
            out.append(short[n]); i += 2; continue
        out.append(ord(c)); i += 1
    return bytes(out)


def read_varint(data, i):
    shift = 0
    value = 0
    while True:
        b = data[i]; i += 1
        value |= (b & 0x7f) << shift
        if not b & 0x80:
            return value, i
        shift += 7


def pb_fields(data):
    """Yield (field_number, wire_type, value) for a protobuf message."""
    i = 0
    while i < len(data):
        key, i = read_varint(data, i)
        fn, wt = key >> 3, key & 7
        if wt == 0:
            v, i = read_varint(data, i); yield fn, wt, v
        elif wt == 2:
            ln, i = read_varint(data, i); v = data[i:i + ln]; i += ln; yield fn, wt, v
        elif wt == 5:
            v = data[i:i + 4]; i += 4; yield fn, wt, v
        elif wt == 1:
            v = data[i:i + 8]; i += 8; yield fn, wt, v
        else:
            raise AssertionError(f'unsupported wire type {wt}')


def decode_msg(payload):
    """internal.Msg{type_url=1,value=2,channel=3,compression=4}. Surface the
    nested Any type_url so RelaySignal stream envelopes are identifiable."""
    msg = {'type_url': None, 'channel': None, 'compression': None, 'value_len': 0,
           'inner_type_url': None}
    value = b''
    for fn, wt, v in pb_fields(payload):
        if fn == 1 and wt == 2:
            msg['type_url'] = v.decode(errors='replace')
        elif fn == 2 and wt == 2:
            value = v; msg['value_len'] = len(v)
        elif fn == 3 and wt == 2:
            msg['channel'] = v.decode(errors='replace')
        elif fn == 4 and wt == 0:
            msg['compression'] = v
    try:
        for fn, wt, v in pb_fields(value):
            if wt == 2 and fn in (6, 7, 8):
                for afn, awt, av in pb_fields(v):
                    if afn == 1 and awt == 2:
                        msg['inner_type_url'] = av.decode(errors='replace')
    except (AssertionError, IndexError):
        pass
    return msg


def publish_envelopes(lines):
    out = []
    for line in lines:
        tokens = re.findall(r'"((?:[^"\\]|\\.)*)"', line)
        if len(tokens) < 3 or tokens[0].upper() != 'PUBLISH':
            continue
        channel = tokens[1]
        payload = unescape_monitor(tokens[2])
        try:
            msg = decode_msg(payload)
        except (AssertionError, IndexError):
            continue
        msg['publish_channel'] = channel
        out.append(msg)
    return out


def compare_legs(output):
    """Core differential: deepEqual the Go-leg vs Rust-leg per-case normalized
    trees (requestId stripped). Python == on the parsed JSON is a recursive
    deep equality over dicts/lists/scalars; key order is irrelevant."""
    paths = {leg: output / leg / 'blob.json' for leg in PURE_LEGS}
    receipt = {'status': 'INCOMPLETE', 'legs': {leg: str(p) for leg, p in paths.items()},
               'meaning': 'per-case deepEqual of normalized Store/Get response trees, requestId stripped'}
    if not all(p.exists() for p in paths.values()):
        receipt['missing'] = [leg for leg, p in paths.items() if not p.exists()]
        return receipt
    blobs = {leg: json.loads(p.read_text()) for leg, p in paths.items()}
    g, r = blobs[PURE_LEGS[0]], blobs[PURE_LEGS[1]]
    receipt['leg_status'] = {leg: b.get('status') for leg, b in blobs.items()}
    if len(g.get('cases', [])) != 26 or len(r.get('cases', [])) != 26:
        receipt['status'] = 'FAIL'
        receipt['reason'] = 'a leg did not capture all 26 cases'
        return receipt
    rows = []
    for gc, rc in zip(g['cases'], r['cases']):
        row = {'id': gc['id'], 'ids_match': gc['id'] == rc['id'],
               'go_pass': bool(gc.get('conformance_pass')), 'rust_pass': bool(rc.get('conformance_pass')),
               'store_tree_equal': gc.get('store_tree') is not None and gc.get('store_tree') == rc.get('store_tree'),
               'get_tree_equal': gc.get('get_tree') is not None and gc.get('get_tree') == rc.get('get_tree'),
               'store_wire_equal': gc.get('store_wire_b64') == rc.get('store_wire_b64'),
               'get_wire_equal': gc.get('get_wire_b64') == rc.get('get_wire_b64')}
        rows.append(row)
    receipt['cases'] = rows
    receipt['all_trees_equal'] = all(x['ids_match'] and x['store_tree_equal'] and x['get_tree_equal'] for x in rows)
    receipt['all_wire_equal'] = all(x['store_wire_equal'] and x['get_wire_equal'] for x in rows)
    receipt['legs_pass'] = all(b.get('status') == 'PASS' for b in blobs.values())
    receipt['status'] = 'PASS' if receipt['all_trees_equal'] and receipt['legs_pass'] else 'FAIL'
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go-server', type=Path, required=True)
    parser.add_argument('--rust-server', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--go-cases', type=Path, required=True)
    parser.add_argument('--go-results', type=Path, required=True)
    parser.add_argument('--pair', choices=['go-front-go-owner', 'rust-front-rust-owner',
                                           'go-front-rust-owner', 'rust-front-go-owner'])
    args = parser.parse_args()
    binaries = {'go': args.go_server.resolve(strict=True), 'rust': args.rust_server.resolve(strict=True)}
    if sha(binaries['go']) != FIXED_GO_SHA256:
        raise RuntimeError('fixed source Go binary identity mismatch before execution')
    go_cases = args.go_cases.resolve(strict=True)
    go_results = args.go_results.resolve(strict=True)
    rows = [json.loads(line) for line in go_results.read_text().splitlines() if line.strip()]
    if len(rows) != 26:
        raise RuntimeError(f'Go oracle must produce 26 rows, got {len(rows)}')
    output = args.evidence_dir.resolve(); output.mkdir(parents=True, exist_ok=False)
    redis_server, redis_cli = shutil.which('redis-server'), shutil.which('redis-cli')
    if not redis_server or not redis_cli:
        raise RuntimeError('redis-server and redis-cli required')

    here = Path(__file__).resolve().parent
    probe = here / 'blob-relay.mjs'
    fixture_inputs = [probe, Path(__file__).resolve(),
                      here.parent / 'participant-data-blob' / 'raw-protocol.mjs',
                      go_cases, go_results]
    fixture_hashes = {str(p): sha(p) for p in fixture_inputs}
    summary = {'mode': 'blob-cross-node', 'fixture_sha256': fixture_hashes,
               'fixed_go_source': 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07',
               'binary_sha256': {s: sha(p) for s, p in binaries.items()},
               'pairs': [], 'meaning': 'a=signal WebSocket front (non-owner), b=actual room owner'}
    pairs = [('go', 'go'), ('rust', 'rust')]
    if args.pair:
        all_pairs = [('go', 'go'), ('rust', 'rust'), ('go', 'rust'), ('rust', 'go')]
        pairs = [(a, b) for a, b in all_pairs if f'{a}-front-{b}-owner' == args.pair]

    for a, b in pairs:
        label = f'{a}-front-{b}-owner'
        case = {'pair': label, 'accepted': False, 'commands': [], 'processes': []}
        directory = output / label; directory.mkdir()
        with tempfile.TemporaryDirectory(prefix='c45-blob-pair-') as temp:
            staging = Path(temp); processes = []; monitor = None
            def start(command, name):
                log = (directory / f'{name}.log').open('wb')
                proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                processes.append((name, proc, log)); case['commands'].append(command)
                return proc
            try:
                (redis_port, http_a, tcp_a, udp_a, http_b, tcp_b, udp_b,
                 debug_a, debug_b) = free_ports(9)
                broker = start([redis_server, '--bind', '127.0.0.1', '--port', str(redis_port),
                                '--save', '', '--appendonly', 'no', '--dir', str(staging)], 'redis')
                deadline = time.monotonic() + 10
                while True:
                    if broker.poll() is not None:
                        raise RuntimeError('owned broker exited before ready')
                    ping = subprocess.run([redis_cli, '-h', '127.0.0.1', '-p', str(redis_port), 'PING'],
                                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                    if ping.returncode == 0 and ping.stdout.strip() == b'PONG':
                        break
                    if time.monotonic() > deadline:
                        raise TimeoutError('owned broker readiness')
                    time.sleep(.05)
                monitor = Monitor(redis_port)
                owner_node = None
                for side, kind, http, tcp, udp in [('b', b, http_b, tcp_b, udp_b), ('a', a, http_a, tcp_a, udp_a)]:
                    config = (f'keys: {{{KEY}: {SECRET}}}\nlogging: {{level: info, json: true}}\n'
                              'bind_addresses: [127.0.0.1]\n' + BLOB_GATE +
                              f'rtc: {{node_ip: 127.0.0.1, use_external_ip: false, tcp_port: {tcp}, udp_port: {udp}, enable_loopback_candidate: true}}\n'
                              f'debug_handler: {{port: {debug_b if side == "b" else debug_a}}}\nturn: {{enabled: false}}\n'
                              f'redis: {{address: 127.0.0.1:{redis_port}}}\n' + SIGNAL_RELAY)
                    path = staging / f'{side}.yaml'; path.write_text(config); path.chmod(0o600)
                    (directory / f'{side}.config.yaml').write_text(config)
                    proc = start([str(binaries[kind]), '--dev', '--config', str(path), '--port', str(http)], side)
                    wait_http(proc, http)
                    if side == 'b':
                        end = time.monotonic() + 10
                        while True:
                            rows = subprocess.check_output([redis_cli, '--raw', '-p', str(redis_port), 'HKEYS', 'nodes']).decode().split()
                            if len(rows) == 1:
                                owner_node = rows[0]; break
                            if time.monotonic() > end:
                                raise TimeoutError('owner real node registration')
                            time.sleep(.1)
                env = dict(os.environ, LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET,
                           LIVEKIT_URL=f'ws://127.0.0.1:{http_a}', WHIP_OWNER_URL=f'http://127.0.0.1:{http_b}',
                           WHIP_OWNER_NODE_ID=owner_node, WHIP_REDIS_PORT=str(redis_port),
                           BLOB_GO_CASES=str(go_cases), BLOB_GO_RESULTS=str(go_results),
                           BLOB_OUTPUT=str(directory / 'blob.json'))
                command = ['node', str(probe)]
                case['commands'].append(command)
                completed = subprocess.run(command, env=env, cwd=str(here.parent),
                                           capture_output=True, timeout=300)
                (directory / 'probe.stdout').write_bytes(completed.stdout)
                (directory / 'probe.stderr').write_bytes(completed.stderr)
                case['probe_exit'] = completed.returncode
                # Let the response stream envelopes flush through MONITOR.
                time.sleep(1.0)
                lines = monitor.snapshot()
                (directory / 'monitor.log').write_text('\n'.join(lines) + '\n')
                envelopes = publish_envelopes(lines)
                (directory / 'envelopes.json').write_text(json.dumps(envelopes, indent=2) + '\n')
                # Cross-node witness: the RelaySignal stream server channel is
                # Signal|RelaySignal|<ownerNode>|STR on both implementations.
                relay = [e for e in envelopes
                         if 'RelaySignal' in (e.get('channel') or '')
                         or 'RelaySignal' in (e.get('publish_channel') or '')]
                case['relay_envelope_count'] = len(relay)
                case['relay_envelopes'] = relay[:8]
                observation = json.loads((directory / 'blob.json').read_text())
                case['observation_status'] = observation.get('status')
                case['case_count'] = len(observation.get('cases', []))
                case['conformance_pass_count'] = sum(1 for c in observation.get('cases', []) if c.get('conformance_pass'))
                case['validStoreCases'] = observation.get('validStoreCases')
                case['invalidStoreCases'] = observation.get('invalidStoreCases')
                case['accepted'] = (completed.returncode == 0 and observation.get('status') == 'PASS'
                                    and case['case_count'] == 26 and case['conformance_pass_count'] == 26
                                    and len(relay) >= 1)
            except Exception as error:
                case['runner_error'] = str(error)
            finally:
                if monitor:
                    monitor.close()
                for name, proc, log in reversed(processes):
                    forced = False
                    if proc.poll() is None:
                        proc.send_signal(signal.SIGTERM)
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            forced = True; proc.kill(); proc.wait()
                    case['processes'].append({'name': name, 'pid': proc.pid, 'exit': proc.returncode, 'forced': forced})
                    log.close()
                if any(p['forced'] or p['exit'] != 0 for p in case['processes']):
                    case['accepted'] = False
                    case['shutdown_error'] = 'owned process did not release resources and exit cleanly'
                (directory / 'result.json').write_text(json.dumps(case, indent=2) + '\n')
                (directory / 'execution.json').write_text(json.dumps(
                    {'pair': label, 'commands': case['commands'], 'probe_exit': case.get('probe_exit'),
                     'processes': case['processes'], 'accepted': case['accepted']}, indent=2) + '\n')
        summary['pairs'].append(case)
        (output / 'result.json').write_text(json.dumps(summary, indent=2) + '\n')

    comparison = compare_legs(output)
    (output / 'paired-comparison.json').write_text(json.dumps(comparison, indent=2) + '\n')
    summary['paired_comparison'] = comparison['status']
    summary['final_binary_sha256'] = {s: sha(p) for s, p in binaries.items()}
    summary['final_fixture_sha256'] = {str(p): sha(p) for p in fixture_inputs}
    summary['input_identity_stable'] = (summary['final_fixture_sha256'] == fixture_hashes
                                        and summary['final_binary_sha256'] == summary['binary_sha256'])
    ran_pure = {c['pair'] for c in summary['pairs']} >= set(PURE_LEGS)
    summary['accepted'] = (summary['input_identity_stable'] and len(summary['pairs']) == len(pairs)
                           and all(c['accepted'] for c in summary['pairs'])
                           and (comparison['status'] == 'PASS' if ran_pure else True))
    summary['complete_paired'] = ran_pure and summary['accepted']
    (output / 'result.json').write_text(json.dumps(summary, indent=2) + '\n')
    (output / 'execution.json').write_text(json.dumps(
        {'pairs': [{'pair': c['pair'], 'probe_exit': c.get('probe_exit'),
                    'processes': c['processes'], 'accepted': c['accepted']} for c in summary['pairs']],
         'paired_comparison': comparison['status'], 'accepted': summary['accepted']}, indent=2) + '\n')
    raise SystemExit(0 if summary['accepted'] else 1)


if __name__ == '__main__':
    main()