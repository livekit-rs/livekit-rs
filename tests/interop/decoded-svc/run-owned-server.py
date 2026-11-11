#!/usr/bin/env python3
"""Owned server wrapper for an immutable decoded-SDK calibration."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument('--binary', type=Path, required=True)
p.add_argument('--sha256', required=True)
p.add_argument('--harness', type=Path, required=True)
p.add_argument('--dependencies', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--codec', choices=['vp9', 'av1'], required=True)
p.add_argument('--mode', required=True)
p.add_argument('--single', choices=['true', 'false'], required=True)
p.add_argument('--c21', choices=['original', 'enabled', 'disabled'], required=True)
a = p.parse_args()
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
assert sha(a.binary) == a.sha256
a.output.mkdir(parents=True, exist_ok=False)
temporary = Path(tempfile.mkdtemp(prefix='owned-decoded-server-'))
held, ports = [], []
for kind in [socket.SOCK_STREAM, socket.SOCK_STREAM, socket.SOCK_DGRAM]:
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind(('127.0.0.1', 0))
    ports.append(sock.getsockname()[1])
    held.append(sock)
cfg = {'port': ports[0], 'bind_addresses': ['127.0.0.1'],
       'keys': {'devkey': 'secret'}, 'rtc': {
           'node_ip': '127.0.0.1', 'use_external_ip': False,
           'enable_loopback_candidate': True,
           'ips': {'includes': ['127.0.0.1/32']},
           'tcp_port': ports[1], 'udp_port': ports[2]}}
if a.c21 == 'enabled':
    cfg['rtc']['forward_stats'] = {'summary_interval': '100ms',
                                   'report_interval': '100ms', 'report_window': '1s'}
config = temporary / 'server.yaml'
config.write_text(json.dumps(cfg))
report = {'status': 'RUNNING', 'binary': str(a.binary), 'binary_sha256': sha(a.binary),
          'public_config': cfg, 'config_sha256': sha(config),
          'harness_sha256': sha(a.harness / 'run.mjs'), 'ports': ports}
server = child = None
for sock in held:
    sock.close()
with (a.output / 'server.log').open('wb') as log:
    try:
        server = subprocess.Popen([str(a.binary), '--config', str(config), '--dev'],
                                  stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        report['server_pid'] = server.pid
        report['server_birth'] = subprocess.check_output(
            ['ps', '-p', str(server.pid), '-o', 'lstart='], text=True).strip()
        deadline = time.monotonic() + 30
        while True:
            assert server.poll() is None, 'owned server exited during startup'
            try:
                with socket.create_connection(('127.0.0.1', ports[0]), timeout=.5):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.1)
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('LIVEKIT_', 'INTEROP_', 'C21_'))}
        env.update(INTEROP_DEPENDENCY_ROOT=str(a.dependencies.resolve()),
                   INTEROP_OUTPUT_DIR=str(a.output.resolve() / 'sdk'),
                   LIVEKIT_URL='ws://127.0.0.1:' + str(ports[0]),
                   VIDEO_CODEC=a.codec, SCALABILITY_MODE=a.mode, SINGLE_PC=a.single,
                   C21_STATE=a.c21, SERVER_LABEL=a.binary.name,
                   SERVER_BINARY=str(a.binary.resolve()), SERVER_CONFIG_FILE=str(config),
                   BROWSER_CHANNEL='chromium')
        with (a.output / 'probe.log').open('wb') as probe_log:
            child = subprocess.Popen(['node', str(a.harness / 'run.mjs')], env=env,
                                     stdout=probe_log, stderr=subprocess.STDOUT,
                                     start_new_session=True)
            report['probe_pid'] = child.pid
            try:
                report['probe_exit'] = child.wait(timeout=300)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGTERM)
                report['probe_exit'] = child.wait(timeout=30)
                report['forced_owned_timeout'] = True
        report['status'] = ('PASS' if report['probe_exit'] == 0 else
                            'BLOCKED_SCOPED' if report['probe_exit'] == 2 else 'FAIL')
    except BaseException as error:
        report.update(status='FAIL', error=str(error))
    finally:
        if child and child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            child.wait(timeout=30)
        if server and server.poll() is None:
            server.send_signal(signal.SIGINT)
        if server:
            report['server_exit'] = server.wait(timeout=30)
        shutil.rmtree(temporary)
        report['temporary_config_removed'] = not temporary.exists()
        report['runner_sha256'] = sha(Path(__file__))
        (a.output / 'execution.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({'status': report['status'], 'probe_exit': report.get('probe_exit')}))
raise SystemExit(report.get('probe_exit', 1))
