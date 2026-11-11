#!/usr/bin/env python3
"""One transient WATCH admission refusal in front of a real three-primary Cluster.

Every master gets its own loopback proxy and CLUSTER SLOTS/SHARDS announcements
are rewritten to those proxies, so the slot owner cannot be reached around the
fault. Exactly one WATCH for the test key is refused with a transient code; the
receipt requires a second, forwarded WATCH plus the committed SET/GET/DEL.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import socketserver
import subprocess
import tempfile
import threading
import time

TEST = 'redis_endpoint::cluster_watch::tests::real_cluster_watch_retries_one_transient_admission_before_actual_exec'
KEY = '{ingress}_watch_fault_test'


def identity(path):
    path = Path(path).resolve()
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def read_frame(stream):
    """Return (raw bytes, parsed value) so untouched replies stay byte-exact."""
    line = stream.readline()
    if not line:
        raise EOFError
    mark, body = line[:1], line[1:-2]
    if mark in (b'$', b'!', b'='):
        size = int(body)
        if size < 0:
            return line, None
        data = stream.read(size + 2)
        if len(data) != size + 2:
            raise EOFError
        return line + data, data[:-2]
    if mark in (b'*', b'~', b'>'):
        count = int(body)
        if count < 0:
            return line, None
        parts, values = [], []
        for _ in range(count):
            raw, value = read_frame(stream)
            parts.append(raw)
            values.append(value)
        return line + b''.join(parts), values
    if mark in (b'%', b'|'):
        count = int(body)
        if count < 0:
            return line, None
        parts, values = [], {}
        for _ in range(count):
            raw_key, key = read_frame(stream)
            raw_value, value = read_frame(stream)
            parts.append(raw_key + raw_value)
            values[key] = value
        return line + b''.join(parts), values
    if mark == b':':
        return line, int(body)
    if mark == b'_':
        return line, None
    if mark in (b'+', b'-', b',', b'#', b'('):
        return line, body
    raise ValueError('unsupported RESP mark %r' % mark)


def encode(value):
    if value is None:
        return b'*-1\r\n'
    if isinstance(value, int):
        return b':%d\r\n' % value
    if isinstance(value, bytes):
        return b'$%d\r\n%s\r\n' % (len(value), value)
    if isinstance(value, list):
        return b'*%d\r\n' % len(value) + b''.join(encode(v) for v in value)
    if isinstance(value, dict):
        return b'%%%d\r\n' % len(value) + b''.join(encode(k) + encode(v) for k, v in value.items())
    raise TypeError('cannot encode %r' % type(value))


def rewrite(value, mapping):
    """Point every announced real master at its own proxy."""
    if isinstance(value, list):
        if (len(value) >= 2 and value[0] == b'127.0.0.1'
                and isinstance(value[1], int) and value[1] in mapping):
            return [value[0], mapping[value[1]]] + [rewrite(v, mapping) for v in value[2:]]
        return [rewrite(v, mapping) for v in value]
    if isinstance(value, dict):
        out = {k: rewrite(v, mapping) for k, v in value.items()}
        host = out.get(b'ip', out.get(b'host'))
        if host == b'127.0.0.1' and isinstance(out.get(b'port'), int) and out[b'port'] in mapping:
            out[b'port'] = mapping[out[b'port']]
        return out
    return value


class Proxy(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, backend, state):
        self.backend = backend
        self.state = state
        super().__init__(('127.0.0.1', 0), Handler)


class State:
    def __init__(self, owner_port, code):
        self.owner_port = owner_port
        self.code = code
        self.remaining = 1
        self.events = []
        self.lock = threading.Lock()

    def watch(self, backend, key, connection_id):
        with self.lock:
            fault = (backend == self.owner_port and key == KEY
                     and self.remaining > 0)
            if fault:
                self.remaining -= 1
            self.events.append({'command': 'WATCH', 'key': key, 'backend': backend,
                                'socket': connection_id,
                                'fault': self.code if fault else None,
                                'monotonic_ns': time.monotonic_ns()})
            return fault

    def observe(self, command, key, backend, connection_id):
        with self.lock:
            self.events.append({'command': command, 'key': key, 'backend': backend,
                                'socket': connection_id, 'fault': None,
                                'monotonic_ns': time.monotonic_ns()})


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        proxy = self.server
        state = proxy.state
        with state.lock:
            connection_id = len({e['socket'] for e in state.events}) + 1
        try:
            upstream = socket.create_connection(('127.0.0.1', proxy.backend), timeout=10)
        except OSError:
            return
        with upstream:
            self.request.settimeout(30)
            with self.request.makefile('rb') as client, upstream.makefile('rb') as server:
                while True:
                    try:
                        raw_command, command = read_frame(client)
                    except (EOFError, ConnectionError, OSError, ValueError):
                        return
                    if not isinstance(command, list) or not command:
                        return
                    name = command[0].upper() if isinstance(command[0], bytes) else b''
                    key = command[1].decode() if len(command) > 1 and isinstance(command[1], bytes) else None
                    if name == b'WATCH' and key is not None:
                        if state.watch(proxy.backend, key, connection_id):
                            reply = ('-%s injected transient WATCH admission refusal\r\n'
                                     % state.code).encode()
                            try:
                                self.request.sendall(reply)
                            except ConnectionError:
                                return
                            continue
                    elif name in (b'SET', b'GET', b'DEL', b'EXEC', b'MULTI', b'UNWATCH'):
                        state.observe(name.decode(), key, proxy.backend, connection_id)
                    try:
                        upstream.sendall(raw_command)
                        raw_reply, reply = read_frame(server)
                    except (EOFError, ConnectionError, OSError, ValueError):
                        return
                    if name == b'CLUSTER' and len(command) > 1 and isinstance(command[1], bytes) \
                            and command[1].upper() in (b'SLOTS', b'SHARDS'):
                        with state.lock:
                            mapping = dict(state.mapping)
                        raw_reply = encode(rewrite(reply, mapping))
                        state.observe('CLUSTER ' + command[1].upper().decode(), None,
                                      proxy.backend, connection_id)
                    try:
                        self.request.sendall(raw_reply)
                    except ConnectionError:
                        return


def cluster_slots(port):
    with socket.create_connection(('127.0.0.1', port), timeout=5) as sock:
        sock.sendall(encode([b'CLUSTER', b'SLOTS']))
        with sock.makefile('rb') as stream:
            return read_frame(stream)[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--code', default='TRYAGAIN',
                        choices=('TRYAGAIN', 'CLUSTERDOWN', 'MASTERDOWN', 'LOADING'))
    parser.add_argument('--redis-server', type=Path, default=Path(shutil.which('redis-server')))
    parser.add_argument('--redis-cli', type=Path, default=Path(shutil.which('redis-cli')))
    args = parser.parse_args()
    assert __debug__, 'assertions must stay enabled'
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = {'status': 'RUNNING', 'runner': identity(__file__),
               'binary': identity(args.test_binary), 'redis': identity(args.redis_server),
               'cli': identity(args.redis_cli), 'test': TEST, 'fault_code': args.code,
               'key': KEY, 'brokers': [], 'proxies': []}

    def save():
        (args.output / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')

    save()
    handles, reservations, servers, workers = [], [], [], []
    child = None
    with tempfile.TemporaryDirectory(prefix='livekit-watch-fault-') as work:
        root = Path(work)
        try:
            ports = []
            for _ in range(3):
                for _attempt in range(500):
                    number = random.SystemRandom().randrange(27000, 44000)
                    if number in ports or number + 10000 in ports or number - 10000 in ports:
                        continue
                    sockets = []
                    try:
                        for port in (number, number + 10000):
                            sock = socket.socket()
                            sockets.append(sock)
                            sock.bind(('127.0.0.1', port))
                        reservations.extend(sockets)
                        ports.append(number)
                        break
                    except OSError:
                        for sock in sockets:
                            sock.close()
                else:
                    raise RuntimeError('owned client/bus port pair unavailable')
            for slot, port in enumerate(ports):
                directory = root / str(slot)
                directory.mkdir()
                for sock in reservations[slot * 2:slot * 2 + 2]:
                    sock.close()
                log = (args.output / f'master-{slot}.log').open('wb')
                proc = subprocess.Popen(
                    [args.redis_server, '--bind', '127.0.0.1', '--port', str(port),
                     '--cluster-enabled', 'yes', '--cluster-config-file', str(directory / 'nodes.conf'),
                     '--cluster-node-timeout', '1000', '--cluster-announce-ip', '127.0.0.1',
                     '--save', '', '--appendonly', 'no', '--dir', str(directory)],
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                row = {'pid': proc.pid, 'port': port}
                receipt['brokers'].append(row)
                handles.append((proc, row, log))
            deadline = time.monotonic() + 15
            for proc, row, _ in handles:
                while True:
                    assert proc.poll() is None, 'owned broker exited before readiness'
                    result = subprocess.run([args.redis_cli, '-p', str(row['port']), 'PING'],
                                            capture_output=True, timeout=2)
                    if result.returncode == 0 and result.stdout.strip() == b'PONG':
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError('actual master readiness deadline')
                    time.sleep(.05)
            command = [str(args.redis_cli), '--cluster', 'create'] + \
                [f'127.0.0.1:{port}' for port in ports] + ['--cluster-replicas', '0', '--cluster-yes']
            with (args.output / 'create.stdout').open('wb') as stdout, \
                    (args.output / 'create.stderr').open('wb') as stderr:
                result = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=30)
            receipt['cluster_create_exit'] = result.returncode
            assert result.returncode == 0, 'actual cluster formation failed'
            deadline = time.monotonic() + 15
            for port in ports:
                while True:
                    result = subprocess.run([args.redis_cli, '-p', str(port), 'CLUSTER', 'INFO'],
                                            capture_output=True, timeout=2)
                    if result.returncode == 0 and b'cluster_state:ok' in result.stdout \
                            and b'cluster_slots_assigned:16384' in result.stdout:
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError('all real cluster slots not ready')
                    time.sleep(.05)

            slot = int(subprocess.run([args.redis_cli, '-p', str(ports[0]), 'CLUSTER', 'KEYSLOT', KEY],
                                      capture_output=True, timeout=5, check=True).stdout.strip())
            owner = None
            for row in cluster_slots(ports[0]):
                if row[0] <= slot <= row[1]:
                    owner = row[2][1]
                    break
            assert owner in ports, 'could not locate the real slot owner for %s' % KEY
            receipt.update(key_slot=slot, slot_owner_port=owner)

            state = State(owner, args.code)
            mapping = {}
            for port in ports:
                proxy = Proxy(port, state)
                mapping[port] = proxy.server_address[1]
                servers.append(proxy)
                receipt['proxies'].append({'backend': port, 'listen': proxy.server_address[1]})
            state.mapping = mapping
            for proxy in servers:
                worker = threading.Thread(target=proxy.serve_forever, daemon=True)
                worker.start()
                workers.append((proxy, worker))
            seed = mapping[ports[0]]
            receipt['seed_url'] = f'redis://127.0.0.1:{seed}'
            save()

            env = dict(os.environ)
            env['WATCH_FAULT_REDIS_URL'] = f'redis://127.0.0.1:{seed}'
            env['WATCH_FAULT_CLUSTER_ISOLATED'] = '1'
            with (args.output / 'test.stdout').open('wb') as stdout, \
                    (args.output / 'test.stderr').open('wb') as stderr:
                child = subprocess.Popen(
                    [args.test_binary, '--ignored', '--exact', TEST, '--nocapture', '--test-threads=1'],
                    env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                receipt['exit_code'] = child.wait(timeout=180)
            text = (args.output / 'test.stdout').read_text()
            receipt['executed_one'] = 'running 1 test' in text and '1 passed; 0 failed' in text
            receipt['wire_events'] = state.events
            watches = [e for e in state.events if e['command'] == 'WATCH' and e['key'] == KEY]
            refused = [e for e in watches if e['fault'] == args.code]
            forwarded = [e for e in watches if e['fault'] is None]
            receipt['watch_attempts'] = len(watches)
            receipt['watch_refused'] = len(refused)
            receipt['watch_forwarded'] = len(forwarded)
            receipt['committed_commands'] = [e['command'] for e in state.events
                                             if e['command'] in ('SET', 'GET', 'DEL') and e['key'] == KEY]
            ok = (receipt['exit_code'] == 0 and receipt['executed_one']
                  and len(refused) == 1 and len(forwarded) >= 1
                  and refused[0]['monotonic_ns'] < forwarded[0]['monotonic_ns']
                  and receipt['committed_commands'] == ['GET', 'SET', 'GET', 'DEL'])
            receipt['status'] = 'PASS' if ok else 'FAIL'
            if not ok:
                receipt['failure_reason'] = 'fault injection or committed command sequence not observed'
        except Exception as error:
            receipt.update(status='FAIL', error=type(error).__name__ + ': ' + str(error))
        finally:
            for sock in reservations:
                sock.close()
            if child and child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
                receipt.update(status='FAIL', forced_test_cleanup=True)
            for proxy, worker in workers:
                proxy.shutdown()
                proxy.server_close()
                worker.join(timeout=5)
                if worker.is_alive():
                    receipt.update(status='FAIL', forced_proxy_cleanup=True)
            for proc, row, log in reversed(handles):
                if proc.poll() is None:
                    proc.terminate()
                try:
                    row['exit'] = proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    row.update(exit=proc.wait(), forced=True)
                log.close()
                if row['exit'] != 0 or row.get('forced'):
                    receipt['status'] = 'FAIL'
            save()
    receipt['temporary_inputs_removed'] = True
    receipt['binary_unchanged'] = identity(args.test_binary) == receipt['binary']
    if not receipt['binary_unchanged']:
        receipt['status'] = 'FAIL'
    save()
    print(json.dumps({'status': receipt['status'], 'exit_code': receipt.get('exit_code'),
                      'watch_refused': receipt.get('watch_refused'),
                      'watch_forwarded': receipt.get('watch_forwarded')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())