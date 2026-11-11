#!/usr/bin/env python3
"""Run one compiled Rust test across two real, owned Redis outages.

Requires --test-binary (lk_compat unit test executable), redis-server and
redis-cli. Never points at or stops an external broker. Saves exit statuses,
binary SHA-256, test stdout and Redis logs in the selected evidence directory.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

TEST = 'agent::cluster::tests::actual_redis_restart_recovers_existing_agent_claims_cache_and_termination'


def wait_until(condition, seconds, label, test=None):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if condition():
            return
        if test is not None and test.poll() is not None:
            raise RuntimeError(f'test exited before {label}: {test.returncode}')
        time.sleep(.025)
    raise TimeoutError(label)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--test-binary', type=Path, required=True)
    p.add_argument('--evidence-dir', type=Path, required=True)
    args = p.parse_args()
    binary = args.test_binary.resolve(strict=True)
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    redis_server = shutil.which('redis-server')
    redis_cli = shutil.which('redis-cli')
    if not redis_server or not redis_cli:
        raise RuntimeError('redis-server and redis-cli are required')
    with socket.socket() as port_socket:
        port_socket.bind(('127.0.0.1', 0))
        port = port_socket.getsockname()[1]
    processes = []
    result = {'test': TEST, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'redis_port': port, 'commands': [], 'completed': False}
    with tempfile.TemporaryDirectory(prefix='agent-owned-redis-restart-') as staging:
        staging = Path(staging)
        env = dict(os.environ, LIVEKIT_AGENT_REDIS_SINGLE_URL=f'redis://127.0.0.1:{port}/0',
                   LIVEKIT_AGENT_RESTART_BARRIER_DIR=str(staging))
        broker = None
        test = None
        def start_broker(number):
            cmd = [redis_server, '--bind', '127.0.0.1', '--port', str(port),
                   '--save', '', '--appendonly', 'no', '--dir', str(staging)]
            result['commands'].append(cmd)
            log = (evidence / f'redis-{number}.log').open('wb')
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
            processes.append((proc, log))
            def ping():
                if proc.poll() is not None:
                    raise RuntimeError(f'owned Redis exited before readiness: {proc.returncode}')
                return subprocess.run([redis_cli, '-h', '127.0.0.1', '-p', str(port), 'PING'],
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout.strip() == b'PONG'
            wait_until(ping, 10, 'owned Redis ready')
            return proc
        def stop_broker(proc):
            # Only this exact process was launched by this runner.
            proc.terminate()
            proc.wait(timeout=10)
        try:
            broker = start_broker(1)
            cmd = [str(binary), TEST, '--exact', '--ignored', '--nocapture', '--test-threads=1']
            result['commands'].append(cmd)
            test_log = (evidence / 'test.log').open('wb')
            test = subprocess.Popen(cmd, env=env, stdout=test_log, stderr=subprocess.STDOUT)
            processes.append((test, test_log))
            wait_until(lambda: (staging / 'restart-ready').exists(), 40, 'pre-restart state', test)
            stop_broker(broker)
            # A real outage long enough for live pubsub disconnect notification.
            time.sleep(.5)
            broker = start_broker(2)
            (staging / 'restarted').write_text('real owned process restarted\n')
            wait_until(lambda: (staging / 'stop-again-ready').exists(), 40, 'post-restart lifecycle', test)
            stop_broker(broker)
            (staging / 'stopped-again').write_text('owned process stopped for cancellation check\n')
            code = test.wait(timeout=15)
            test_log.flush()
            text = (evidence / 'test.log').read_text()
            result.update(test_exit=code, redis_exits=[p.returncode for p, _ in processes if p is not test])
            if code != 0 or '1 passed' not in text or '0 failed' not in text:
                raise RuntimeError(f'actual restart test failed or did not execute exactly once: {code}')
            result['completed'] = True
        finally:
            for proc, log in reversed(processes):
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                log.close()
            (evidence / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    if not result['completed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
