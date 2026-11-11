#!/usr/bin/env python3
"""Real HTTP profile bodies parsed by the unchanged Go pprof tool.

This verifies the standard profile/tool contract, not Go runtime/GC equivalence.
Owns and joins only its two sequential server processes. Never edits references.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import urllib.request


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go', type=Path, required=True)
    parser.add_argument('--rust', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = {'status': 'INCOMPLETE', 'scope': 'HTTP gzip pprof parsed by actual go tool pprof; no runtime equivalence claim', 'runs': []}
    save = lambda: (args.output / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')
    try:
        for side, binary in [('go', args.go.resolve()), ('rust', args.rust.resolve())]:
            row = {'side': side, 'binary': str(binary), 'sha256': sha(binary), 'profiles': []}
            receipt['runs'].append(row)
            save()
            for port in [18080, 18081, 18082, 18085]:
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(('127.0.0.1', port))
            config = args.output / (side + '.yaml')
            config.write_text('port: 18080\nbind_addresses: [127.0.0.1]\nrtc:\n  tcp_port: 18081\n  udp_port: 18082\n  node_ip: 127.0.0.1\n  use_external_ip: false\n  enable_loopback_candidate: true\nkeys:\n  devkey: public-profile-fixture-secret-0123456789\nturn:\n  enabled: false\nlogging:\n  level: error\ndebug_handler:\n  port: 18085\n')
            row['config_sha256'] = sha(config)
            stop = threading.Event()
            pool = ThreadPoolExecutor(max_workers=2)
            with (args.output / (side + '.server.log')).open('wb') as log:
                process = subprocess.Popen([str(binary), '--dev', '--config', str(config.resolve())], stdout=log, stderr=subprocess.STDOUT)
                row['pid'] = process.pid
                save()
                try:
                    deadline = time.monotonic() + 30
                    while True:
                        assert process.poll() is None, (side, 'startup exit', process.returncode)
                        try:
                            with urllib.request.urlopen('http://127.0.0.1:18080/', timeout=1) as reply:
                                assert reply.status == 200
                            break
                        except OSError:
                            assert time.monotonic() < deadline, 'startup deadline'
                            time.sleep(.05)

                    def traffic():
                        count = 0
                        while not stop.is_set() and count < 20000:
                            with urllib.request.urlopen('http://127.0.0.1:18080/', timeout=3) as reply:
                                reply.read()
                                assert reply.status == 200
                            count += 1
                        return count

                    workers = [pool.submit(traffic) for _ in range(2)]
                    for name, query in [('profile', '?seconds=2'), ('heap', ''), ('allocs', '?seconds=1')]:
                        target = args.output / (side + '.' + name + '.pb.gz')
                        with urllib.request.urlopen('http://127.0.0.1:18085/debug/pprof/' + name + query, timeout=30) as reply:
                            body = reply.read()
                            assert reply.status == 200
                        assert body.startswith(b'\x1f\x8b'), 'real gzip profile'
                        target.write_bytes(body)
                        env = dict(os.environ, GOTOOLCHAIN='go1.26.3', GOMAXPROCS='2')
                        sample_index = {'profile': 'cpu', 'heap': 'inuse_space', 'allocs': 'alloc_space'}[name]
                        result = subprocess.run(['go', 'tool', 'pprof', '-top', '-sample_index=' + sample_index, '-nodecount=8', str(binary), str(target.resolve())], env=env, capture_output=True, timeout=45)
                        (args.output / (side + '.' + name + '.pprof.stdout')).write_bytes(result.stdout)
                        (args.output / (side + '.' + name + '.pprof.stderr')).write_bytes(result.stderr)
                        row['profiles'].append({'name': name, 'bytes': len(body), 'sha256': sha(target), 'pprof_exit': result.returncode, 'sample_index': sample_index})
                        save()
                        assert result.returncode == 0, (side, name, result.stderr.decode(errors='replace'))
                        assert b'Type:' in result.stdout and b'Total samples = 0 ' not in result.stdout, 'actual nonempty samples required'
                    stop.set()
                    row['actual_health_requests'] = sum(worker.result(timeout=5) for worker in workers)
                    assert row['actual_health_requests'] > 0
                finally:
                    stop.set()
                    pool.shutdown(wait=True, cancel_futures=True)
                    if process.poll() is None:
                        process.send_signal(signal.SIGINT)
                    try:
                        row['server_exit'] = process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        row['server_exit'] = process.wait()
                        row['forced_kill'] = True
                    save()
                assert row['server_exit'] == 0 and not row.get('forced_kill')
                assert sha(binary) == row['sha256'], 'candidate mutated during run'
        receipt['status'] = 'PASS_SCOPED_PROFILE_TOOL_CONTRACT'
    finally:
        save()


if __name__ == '__main__':
    main()
