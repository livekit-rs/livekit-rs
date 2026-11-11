#!/usr/bin/env python3
"""Each selected native test gets three fresh, owned, actual Redis masters."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time


def identity(path):
    path = Path(path).resolve()
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-binary', type=Path, required=True)
    parser.add_argument('--filter', required=True)
    parser.add_argument('--env', required=True)
    parser.add_argument('--urls-format', choices=('csv', 'json'), default='csv')
    parser.add_argument('--isolated-env', help='explicit dedicated fixture ownership variable set to 1')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--redis-server', type=Path, default=Path(shutil.which('redis-server')))
    parser.add_argument('--redis-cli', type=Path, default=Path(shutil.which('redis-cli')))
    args = parser.parse_args()
    assert __debug__, 'assertions must stay enabled'
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = {'status': 'RUNNING', 'runner': identity(__file__), 'binary': identity(args.test_binary),
               'redis': identity(args.redis_server), 'cli': identity(args.redis_cli), 'tests': []}

    def save():
        (args.output / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')

    listing = subprocess.run([args.test_binary, '--ignored', '--list', args.filter], capture_output=True, timeout=30)
    (args.output / 'inventory.stdout').write_bytes(listing.stdout)
    names = re.findall(r'^(.+): test$', listing.stdout.decode(), re.M)
    assert listing.returncode == 0 and names, 'nonempty actual test inventory required'
    save()
    for index, name in enumerate(names):
        case = {'name': name, 'status': 'RUNNING', 'brokers': []}
        receipt['tests'].append(case)
        save()
        handles, reservations = [], []
        child = None
        with tempfile.TemporaryDirectory(prefix='livekit-cluster-owned-') as work:
            root = Path(work)
            try:
                ports = []
                for _ in range(3):
                    for attempt in range(500):
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
                    log = (args.output / f'{index:02d}-master-{slot}.log').open('wb')
                    proc = subprocess.Popen([args.redis_server, '--bind', '127.0.0.1', '--port', str(port),
                                             '--cluster-enabled', 'yes', '--cluster-config-file', str(directory / 'nodes.conf'),
                                             '--cluster-node-timeout', '1000', '--cluster-announce-ip', '127.0.0.1',
                                             '--save', '', '--appendonly', 'no', '--dir', str(directory)],
                                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                    row = {'pid': proc.pid, 'port': port}
                    case['brokers'].append(row)
                    handles.append((proc, row, log))
                deadline = time.monotonic() + 15
                for proc, row, _ in handles:
                    while True:
                        assert proc.poll() is None, 'owned broker exited before readiness'
                        result = subprocess.run([args.redis_cli, '-h', '127.0.0.1', '-p', str(row['port']), 'PING'], capture_output=True, timeout=2)
                        if result.returncode == 0 and result.stdout.strip() == b'PONG':
                            break
                        if time.monotonic() > deadline:
                            raise RuntimeError('actual master readiness deadline')
                        time.sleep(.05)
                command = [str(args.redis_cli), '--cluster', 'create'] + [f'127.0.0.1:{port}' for port in ports] + ['--cluster-replicas', '0', '--cluster-yes']
                with (args.output / f'{index:02d}-create.stdout').open('wb') as stdout, (args.output / f'{index:02d}-create.stderr').open('wb') as stderr:
                    result = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=30)
                case['cluster_create_exit'] = result.returncode
                assert result.returncode == 0, 'actual cluster formation failed'
                deadline = time.monotonic() + 15
                for port in ports:
                    while True:
                        result = subprocess.run([args.redis_cli, '-p', str(port), 'CLUSTER', 'INFO'], capture_output=True, timeout=2)
                        if result.returncode == 0 and b'cluster_state:ok' in result.stdout and b'cluster_slots_assigned:16384' in result.stdout:
                            break
                        if time.monotonic() > deadline:
                            raise RuntimeError('all real cluster slots not ready')
                        time.sleep(.05)
                env = dict(os.environ)
                urls = [f'redis://127.0.0.1:{port}' for port in ports]
                env[args.env] = json.dumps(urls) if args.urls_format == 'json' else ','.join(urls)
                if args.isolated_env:
                    env[args.isolated_env] = '1'
                case['urls_format'] = args.urls_format
                case['isolated_environment_name'] = args.isolated_env
                case['environment_name'] = args.env
                with (args.output / f'{index:02d}-test.stdout').open('wb') as stdout, (args.output / f'{index:02d}-test.stderr').open('wb') as stderr:
                    child = subprocess.Popen([args.test_binary, '--ignored', '--exact', name, '--nocapture', '--test-threads=1'],
                                             env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                    case['exit_code'] = child.wait(timeout=180)
                text = (args.output / f'{index:02d}-test.stdout').read_text()
                case['executed_one'] = 'running 1 test' in text and '1 passed; 0 failed' in text
                case['status'] = 'PASS' if case['exit_code'] == 0 and case['executed_one'] else 'FAIL'
            except Exception as error:
                case.update(status='FAIL', error=type(error).__name__ + ': ' + str(error))
            finally:
                for sock in reservations:
                    sock.close()
                if child and child.poll() is None:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                    case.update(status='FAIL', forced_test_cleanup=True)
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
                        case['status'] = 'FAIL'
                save()
        case['temporary_inputs_removed'] = True
        save()
    receipt['binary_unchanged'] = identity(args.test_binary) == receipt['binary']
    receipt['status'] = 'PASS_FRESH_THREE_MASTER_INVENTORY' if receipt['binary_unchanged'] and all(case['status'] == 'PASS' for case in receipt['tests']) else 'FAIL'
    save()
    print(json.dumps({'status': receipt['status'], 'tests': len(names)}))
    return 0 if receipt['status'] != 'FAIL' else 1


if __name__ == '__main__':
    raise SystemExit(main())
