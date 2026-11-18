#!/usr/bin/env python3
"""C44: paired two-node Go/Rust differential for the cluster config consumers
CONFIG_CLUSTER_CONSUMER:signal_relay and CONFIG_CLUSTER_CONSUMER:psrpc.

Every server/broker process is owned by this runner; no external Redis or server
is stopped, and no Cargo/Go compilation happens here. The fixed official Go
binary identity is checked before execution.

compression mode (light, official server-sdk, no browser):
  createRoom pins the room to owner node b; a large highly-compressible
  UpdateRoomMetadata is then sent through FRONT node a. roomservice.go:344-351
  routes it to the owner over the PSRPC Redis bus, so a MONITOR tap witnesses the
  internal.Msg.compression flag (field 4: NONE=0/GZIP=1) of the actual cross-node
  envelope. Varying psrpc.compression.quality/threshold/max_decompressed_size must
  change that flag identically on Go and Rust. The exact deflate bytes are NOT a
  parity contract (Go uses klauspost/compress/gzip, Rust uses flate2 gzip; both are
  standard gzip and interoperate), so the oracle is the gating decision + envelope
  flag + owner round-trip, not byte-exact compressed output.

relay mode (official browser SDK, reuses ../cluster-room-api/probe.mjs unchanged):
  a real participant joins through FRONT node a while node b owns the room, so the
  cross-node signal relay (routing/signal.go) carries the join. Non-default
  signal_relay.{connect_attempts,retry_timeout,min/max_retry_interval,
  stream_buffer_size} must drive the same successful cross-node relay on Go/Rust.
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

# Non-default signal_relay in every profile: the launched relay transport must
# consume these (server.rs:672-682 -> cluster_signal/mod.rs), not ignore them.
SIGNAL_RELAY = ('signal_relay: {retry_timeout: 10s, min_retry_interval: 50ms, '
                'max_retry_interval: 200ms, stream_buffer_size: 500, connect_attempts: 5}\n')

PROFILES = {
    'compression-off': {
        'psrpc': 'psrpc: {max_attempts: 5, timeout: 5s, backoff: 1s, buffer_size: 500, compression: {quality: 0, threshold: 1024}}\n',
        'expect_flag': 0, 'expect_outcome': 'applied', 'metadata_size': 8192},
    'compression-on': {
        'psrpc': 'psrpc: {max_attempts: 5, timeout: 5s, backoff: 1s, buffer_size: 500, compression: {quality: 6, threshold: 1024}}\n',
        'expect_flag': 1, 'expect_outcome': 'applied', 'metadata_size': 8192},
    'compression-below-threshold': {
        'psrpc': 'psrpc: {max_attempts: 5, timeout: 5s, backoff: 1s, buffer_size: 500, compression: {quality: 6, threshold: 1000000}}\n',
        'expect_flag': 0, 'expect_outcome': 'applied', 'metadata_size': 8192},
    'compression-cap': {
        'psrpc': 'psrpc: {max_attempts: 5, timeout: 5s, backoff: 1s, buffer_size: 500, compression: {quality: 6, threshold: 1024, max_decompressed_size: 64}}\n',
        'expect_flag': 1, 'expect_outcome': 'rejected', 'metadata_size': 8192},
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(); sock.bind(('127.0.0.1', 0)); sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


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
    RPC flows. The post-hoc keyspace cannot reveal the envelope compression flag;
    only the live PUBLISH witness can."""

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
    """internal.Msg{type_url=1,value=2,channel=3,compression=4}. For an RPC
    envelope the value is internal.Request/Response whose Any (field 6/7) carries
    the business type_url; surface it so the UpdateRoomMetadata RPC is identifiable
    regardless of channel naming."""
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
    # internal.Request.request = 6 (Any), internal.Response.response = 6 (Any),
    # Any{type_url=1,value=2}. Best-effort: find a nested Any type_url.
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go-server', type=Path, required=True)
    parser.add_argument('--rust-server', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--mode', choices=['compression', 'relay'], default='compression')
    parser.add_argument('--profile', choices=list(PROFILES), default='compression-on')
    parser.add_argument('--pair', choices=['go-front-go-owner', 'rust-front-rust-owner',
                                           'go-front-rust-owner', 'rust-front-go-owner'])
    args = parser.parse_args()
    binaries = {'go': args.go_server.resolve(strict=True), 'rust': args.rust_server.resolve(strict=True)}
    if sha(binaries['go']) != FIXED_GO_SHA256:
        raise RuntimeError('fixed source Go binary identity mismatch before execution')
    output = args.evidence_dir.resolve(); output.mkdir(parents=True, exist_ok=False)
    redis_server, redis_cli = shutil.which('redis-server'), shutil.which('redis-cli')
    if not redis_server or not redis_cli:
        raise RuntimeError('redis-server and redis-cli required')

    here = Path(__file__).resolve().parent
    if args.mode == 'compression':
        probe = here / 'probe-rpc.mjs'
        profile = PROFILES[args.profile]
        extra_yaml = profile['psrpc'] + SIGNAL_RELAY
        probe_timeout = 90
    else:
        probe = here.parent / 'cluster-room-api' / 'probe.mjs'
        profile = PROFILES['compression-on']
        extra_yaml = profile['psrpc'] + SIGNAL_RELAY
        probe_timeout = 240
    fixture_inputs = [probe, Path(__file__).resolve()]
    if args.mode == 'relay':
        fixture_inputs += [probe.parent / 'client.html', here.parent / 'owned-browser-exit.mjs']
    fixture_hashes = {str(p): sha(p) for p in fixture_inputs}
    summary = {'mode': args.mode, 'profile': args.profile, 'fixture_sha256': fixture_hashes,
               'fixed_go_source': 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07',
               'binary_sha256': {s: sha(p) for s, p in binaries.items()},
               'expect_flag': profile['expect_flag'], 'expect_outcome': profile['expect_outcome'],
               'pairs': [], 'meaning': 'a=HTTP front (sender), b=actual room owner'}
    pairs = [('go', 'go'), ('rust', 'rust'), ('go', 'rust'), ('rust', 'go')]
    if args.pair:
        pairs = [(a, b) for a, b in pairs if f'{a}-front-{b}-owner' == args.pair]

    for a, b in pairs:
        label = f'{a}-front-{b}-owner'
        case = {'pair': label, 'accepted': False, 'commands': [], 'processes': []}
        directory = output / label; directory.mkdir()
        with tempfile.TemporaryDirectory(prefix='cfg-owned-pair-') as temp:
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
                              'bind_addresses: [127.0.0.1]\nroom: {enable_remote_unmute: true}\n'
                              f'rtc: {{node_ip: 127.0.0.1, use_external_ip: false, tcp_port: {tcp}, udp_port: {udp}, enable_loopback_candidate: true}}\n'
                              f'debug_handler: {{port: {debug_b if side == "b" else debug_a}}}\nturn: {{enabled: false}}\n'
                              f'redis: {{address: 127.0.0.1:{redis_port}}}\n' + extra_yaml)
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
                           WHIP_OWNER_NODE_ID=owner_node, WHIP_REDIS_PORT=str(redis_port))
                if args.mode == 'compression':
                    env.update(CONFIG_PROBE_OUTPUT=str(directory / 'probe.json'),
                               METADATA_SIZE=str(profile['metadata_size']))
                    command = ['node', str(probe)]
                else:
                    env.update(DEBUG='pw:browser', WHIP_OWNER_DEBUG_URL=f'http://127.0.0.1:{debug_b}',
                               WHIP_FRONT_DEBUG_URL=f'http://127.0.0.1:{debug_a}',
                               WHIP_MEDIA_OUTPUT=str(directory / 'media.json'),
                               WHIP_BROWSER_STDERR=str(directory / 'browser.stderr'),
                               WHIP_BROWSER_OWNERSHIP=str(directory / 'browser-owner.json'))
                    command = ['node', str(probe)]
                case['commands'].append(command)
                completed = subprocess.run(command, env=env, capture_output=True, timeout=probe_timeout)
                (directory / 'probe.stdout').write_bytes(completed.stdout)
                (directory / 'probe.stderr').write_bytes(completed.stderr)
                case['probe_exit'] = completed.returncode
                # Let the response envelope flush through MONITOR before snapshot.
                time.sleep(1.0)
                lines = monitor.snapshot()
                (directory / 'monitor.log').write_text('\n'.join(lines) + '\n')
                envelopes = publish_envelopes(lines)
                (directory / 'envelopes.json').write_text(json.dumps(envelopes, indent=2) + '\n')
                if args.mode == 'compression':
                    observation = json.loads((directory / 'probe.json').read_text())
                    case['observation'] = observation
                    room = observation.get('roomName', '')
                    # The cross-node UpdateRoomMetadata REQUEST envelope: the front
                    # node compresses the large metadata before publishing to the
                    # owner. The outer Msg channel (field 3, always plaintext) is
                    # 'UpdateRoomMetadata.REQ'; the Redis topic is
                    # Room|UpdateRoomMetadata|<room>|REQ (room name Redis-escaped).
                    # RCLAIM/RES are excluded: small payloads whose flag is not the
                    # gating observable. The compressed value is gzip, so the inner
                    # Any type_url is not parseable -- channel is the robust signal.
                    def is_update_req(e):
                        ch = e.get('channel') or ''
                        pc = e.get('publish_channel') or ''
                        return (('UpdateRoomMetadata' in ch and ch.endswith('.REQ'))
                                or ('|UpdateRoomMetadata|' in pc and pc.endswith('|REQ')))
                    rpc = [e for e in envelopes if is_update_req(e)]
                    case['rpc_envelopes'] = rpc
                    # field 4 absent == COMPRESSION_NONE(0); present 1 == GZIP.
                    flags = sorted({(e['compression'] if e['compression'] is not None else 0)
                                    for e in rpc})
                    case['observed_flags'] = flags
                    outcome = observation.get('outcome')
                    case['observed_outcome'] = outcome
                    flag_ok = flags == [profile['expect_flag']]
                    outcome_ok = outcome == profile['expect_outcome']
                    case['flag_ok'] = flag_ok; case['outcome_ok'] = outcome_ok
                    case['accepted'] = (completed.returncode == 0 and observation.get('status') == 'PASS'
                                        and flag_ok and outcome_ok and len(rpc) >= 1)
                else:
                    observation = json.loads((directory / 'media.json').read_text())
                    case['observation'] = observation
                    case['accepted'] = completed.returncode == 0 and observation.get('status') == 'PASS'
            except Exception as error:
                case['runner_error'] = str(error)
            finally:
                if monitor:
                    monitor.close()
                if args.mode == 'relay':
                    ownership = directory / 'browser-owner.json'
                    if ownership.exists():
                        browser_pid = json.loads(ownership.read_text())['browserPid']
                        try:
                            os.kill(browser_pid, 0)
                        except ProcessLookupError:
                            case['browser_exited'] = True
                        else:
                            case['browser_exited'] = False; case['accepted'] = False
                            os.kill(browser_pid, signal.SIGTERM)
                            end = time.monotonic() + 5
                            while time.monotonic() < end:
                                try:
                                    os.kill(browser_pid, 0)
                                except ProcessLookupError:
                                    break
                                time.sleep(.05)
                            else:
                                os.kill(browser_pid, signal.SIGKILL)
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
        summary['pairs'].append(case)
        (output / 'result.json').write_text(json.dumps(summary, indent=2) + '\n')
    summary['final_binary_sha256'] = {s: sha(p) for s, p in binaries.items()}
    summary['final_fixture_sha256'] = {str(p): sha(p) for p in fixture_inputs}
    summary['input_identity_stable'] = (summary['final_fixture_sha256'] == fixture_hashes
                                        and summary['final_binary_sha256'] == summary['binary_sha256'])
    summary['accepted'] = (summary['input_identity_stable'] and len(summary['pairs']) == len(pairs)
                           and all(c['accepted'] for c in summary['pairs']))
    summary['complete_four_way'] = len(pairs) == 4 and summary['accepted']
    (output / 'result.json').write_text(json.dumps(summary, indent=2) + '\n')
    raise SystemExit(0 if summary['accepted'] else 1)


if __name__ == '__main__':
    main()