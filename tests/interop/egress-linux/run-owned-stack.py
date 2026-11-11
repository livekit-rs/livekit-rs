#!/usr/bin/env python3
"""Coordinator-owned, guest-only original Egress / Go and Rust media campaign."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import time
import threading
import urllib.request


def identity(path):
    path = Path(path).resolve()
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return {'path': str(path), 'sha256': digest.hexdigest()}


def free(port, udp=False):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM if udp else socket.SOCK_STREAM) as sock:
        if not udp:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', port))


def process_identity(pid):
    try:
        value = Path('/proc') / str(pid) / 'stat'
        fields = value.read_text().rsplit(')', 1)[1].split()
        return {'pid': pid, 'ppid': int(fields[1]), 'start': fields[19], 'state': fields[0]}
    except (FileNotFoundError, ProcessLookupError):
        return None


class OwnedProcessTree:
    """Observe only descendants of exact owned PIDs/start-times; never names."""
    def __init__(self):
        self.known = {}; self.roots = set(); self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._watch, daemon=True)
        self.thread.start()
    def add(self, proc):
        value = process_identity(proc.pid)
        if value:
            with self.lock: self.known[proc.pid] = value; self.roots.add(proc.pid)
    def scan(self):
        rows = [process_identity(int(p.name)) for p in Path('/proc').iterdir() if p.name.isdigit()]
        rows = [p for p in rows if p]
        with self.lock:
            parents = {pid for pid, old in self.known.items() if any(row['pid'] == pid and row['start'] == old['start'] for row in rows)}
            while True:
                added = [row for row in rows if row['ppid'] in parents and row['pid'] not in parents]
                if not added: break
                for row in added: self.known[row['pid']] = row; parents.add(row['pid'])
    def _watch(self):
        while not self.stop_event.wait(.2): self.scan()
    def finish(self):
        self.scan(); self.stop_event.set(); self.thread.join(timeout=2)
        with self.lock: known = dict(self.known); roots = set(self.roots)
        def alive(pid, original):
            now = process_identity(pid)
            return now and now['start'] == original['start'] and now['state'] != 'Z'
        survivors = [row for pid, row in known.items() if pid not in roots and alive(pid, row)]
        # A surviving owned child is always a failed release gate. Cleanup does
        # not erase that failure and never signals a reused PID or another tree.
        for row in survivors:
            try: os.kill(row['pid'], signal.SIGTERM)
            except ProcessLookupError: pass
        deadline = time.monotonic() + 5
        while any(alive(row['pid'], row) for row in survivors) and time.monotonic() < deadline: time.sleep(.05)
        for row in survivors:
            if alive(row['pid'], row):
                try: os.kill(row['pid'], signal.SIGKILL)
                except ProcessLookupError: pass
        return {'observed_descendant_count': len(set(known) - roots), 'survivors_reclaimed': survivors,
                'released_without_reclamation': not survivors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source-root', 'go', 'rust', 'worker', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--profile', choices=('explicit-track', 'auto-track', 'auto-room'), required=True)
    args = parser.parse_args()
    assert platform.system() == 'Linux' and platform.machine() == 'aarch64'
    assert os.getuid() != 0 and Path('/etc/cloud/cloud.cfg').is_file()
    args.go=args.go.resolve(strict=True);args.rust=args.rust.resolve(strict=True);args.worker=args.worker.resolve(strict=True)
    assert args.worker.name == 'egress', 'original handler requires basename egress'
    base_env={key:value for key,value in os.environ.items() if not key.startswith('LIVEKIT_') and key not in ('REDIS_HOST','REDIS_PASSWORD','NODE_IP','UDP_PORT')}
    base_env['PATH']=str(args.worker.parent)+os.pathsep+base_env.get('PATH','')
    base_env.update(LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret',LIVEKIT_WS_URL='ws://127.0.0.1:17880')
    root = args.source_root.resolve()
    output = args.output.resolve()
    assert str(output).startswith('/home/fixture/fixture/output/')
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    here = root / 'tests/interop'
    script = here / 'egress-linux/run-track-file.mjs'
    if args.profile == 'auto-track':
        script = here / 'egress-linux/run-auto-track-file.mjs'
    elif args.profile == 'auto-room':
        script = here / 'automatic-egress/run-auto-audio.mjs'
    input_paths=[Path(__file__).resolve(),script,here/'egress-linux/worker.yaml.template',here/'package.json',here/'package-lock.json',root/'tests/agent-sdk/package.json',root/'tests/agent-sdk/package-lock.json']
    input_before={str(path):identity(path) for path in input_paths}
    receipt = {'inputs':input_before,'worker_path_first':str(args.worker.parent),'livekit_overrides_cleared':True,'status': 'RUNNING', 'profile': args.profile,
               'runner': identity(__file__), 'probe': identity(script),
               'binaries': {key: identity(getattr(args, key)) for key in ('go', 'rust', 'worker')},
               'cases': [], 'scope': 'Actual original Egress child and decoded media; not all worker outputs'}

    def save():
        (output / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')

    save()
    for kind in ('go', 'rust'):
        case = {'server': kind, 'status': 'RUNNING', 'processes': []}
        receipt['cases'].append(case)
        directory = output / kind
        directory.mkdir(mode=0o700)
        processes, logs = [], []
        owned_tree=OwnedProcessTree()

        def launch(name, command):
            log = (directory / (name + '.log')).open('wb')
            logs.append(log)
            proc = subprocess.Popen(command, env=base_env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            record = {'name': name, 'pid': proc.pid, 'command': list(map(str, command))}
            processes.append((proc, record))
            owned_tree.add(proc)
            case['processes'].append(record)
            save()
            return proc

        def ready(check, label, seconds=45):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                assert all(proc.poll() is None for proc, _ in processes), 'owned process exited during ' + label
                try:
                    if check():
                        return
                except OSError:
                    pass
                time.sleep(.1)
            raise RuntimeError(label + ' readiness deadline')

        def http(port):
            with urllib.request.urlopen('http://127.0.0.1:' + str(port) + '/', timeout=1) as response:
                return response.status == 200

        def ping():
            with socket.create_connection(('127.0.0.1', 17842), timeout=1) as sock:
                sock.sendall(b'*1\r\n$4\r\nPING\r\n')
                return sock.recv(64) == b'+PONG\r\n'

        try:
            for port in (17842, 17880, 17881, 19080, 17980, 17981):
                free(port)
            free(17882, True)
            config = {'port': 17880, 'bind_addresses': ['127.0.0.1'],
                      'keys': {'devkey': 'secret'}, 'redis': {'address': '127.0.0.1:17842'},
                      'rtc': {'tcp_port': 17881, 'udp_port': 17882, 'node_ip': '127.0.0.1',
                              'use_external_ip': False, 'enable_loopback_candidate': True,
                              'ips': {'includes': ['127.0.0.1/32']}},
                      'logging': {'level': 'info'}}
            if args.profile == 'auto-room':
                config['webhook'] = {'api_key': 'devkey', 'urls': ['http://127.0.0.1:17981/egress']}
            main_config = directory / 'main.yaml'
            main_config.write_text(json.dumps(config))
            worker_config = directory / 'worker.yaml'
            worker_config.write_bytes((here / 'egress-linux/worker.yaml.template').read_bytes())
            case['configs'] = {name: identity(path) for name, path in [('main', main_config), ('worker', worker_config)]}
            launch('redis', ['redis-server', '--bind', '127.0.0.1', '--port', '17842', '--save', '', '--appendonly', 'no', '--dir', str(directory)])
            ready(ping, 'Redis')
            launch('main', [str(getattr(args, kind)), '--dev', '--config', str(main_config)])
            ready(lambda: http(17880), 'main')
            worker = launch('worker', [str(args.worker), '--config', str(worker_config)])
            ready(lambda: http(19080), 'original worker health')
            env = dict(base_env, INTEROP_PROJECT=str(here), AGENT_SDK_ROOT=str(root / 'tests/agent-sdk'),
                       LIVEKIT_URL='http://127.0.0.1:17880', LIVEKIT_API_KEY='devkey', LIVEKIT_API_SECRET='secret',
                       EGRESS_WORKER_PID=str(worker.pid), EGRESS_WEBHOOK_PORT='17981',
                       EGRESS_OUTPUT_FILE=str(directory / 'recording.ogg'), EGRESS_MEDIA_REPORT=str(directory / 'media.json'),
                       EGRESS_AUTO_DIR=str(output.parent / ('auto-' + output.name + '-' + kind)),
                       AUTO_RECORDING_MODE='room', AUTO_CONFIG_SOURCE='explicit-api')
            with (directory / 'probe.stdout').open('wb') as stdout, (directory / 'probe.stderr').open('wb') as stderr:
                probe = subprocess.Popen(['node', str(script)], env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                owned_tree.add(probe)
                try:
                    case['probe_exit'] = probe.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    os.killpg(probe.pid, signal.SIGTERM)
                    try:
                        probe.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(probe.pid, signal.SIGKILL)
                        probe.wait()
                    case['probe_forced'] = True
                    raise
            report = json.loads((directory / 'media.json').read_text())
            case['media'] = report
            assert case['probe_exit'] == 0 and report['status'] == 'PASS', 'actual original worker/media acceptance failed'
            case['status'] = 'PASS'
        except Exception as error:
            case['status'] = 'FAIL'
            case['error'] = type(error).__name__ + ': ' + str(error)
        finally:
            for proc, record in reversed(processes):
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM if record['name'] != 'main' else signal.SIGINT)
                try:
                    record['exit'] = proc.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    record['exit'] = proc.wait()
                    record['forced'] = True
                if record.get('forced') or record['exit'] != 0:
                    case['status'] = 'FAIL'
            case['child_release']=owned_tree.finish()
            if not case['child_release']['released_without_reclamation']:case['status']='FAIL'
            for log in logs:
                log.close()
            try:
                for port in (17842, 17880, 17881, 19080, 17980, 17981):
                    free(port)
                free(17882, True)
                case['ports_released'] = True
            except OSError as error:
                case['status'] = 'FAIL'
                case['release_error'] = str(error)
            save()
    receipt['inputs_after']={str(path):identity(path) for path in input_paths}
    receipt['input_identity_stable'] = receipt['inputs_after']==input_before and all(identity(getattr(args, key)) == receipt['binaries'][key] for key in ('go', 'rust', 'worker')) and identity(script) == receipt['probe']
    receipt['status'] = 'PASS_SCOPED_BOTH_SERVERS' if receipt['input_identity_stable'] and len(receipt['cases']) == 2 and all(case['status'] == 'PASS' for case in receipt['cases']) else 'FAIL'
    save()
    return 0 if receipt['status'] == 'PASS_SCOPED_BOTH_SERVERS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
