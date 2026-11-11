#!/usr/bin/env python3
"""Own fresh real servers; retain failed runs and never count partial pairs."""
import argparse
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import time
import urllib.request

p = argparse.ArgumentParser()
p.add_argument('--go', required=True, type=pathlib.Path)
p.add_argument('--rust', required=True, type=pathlib.Path)
p.add_argument('--node', default='node')
p.add_argument('--output', required=True, type=pathlib.Path)
p.add_argument('--reference-only', action='store_true', help='Validate all official fixture expectations; never produces paired acceptance')
a = p.parse_args()
here = pathlib.Path(__file__).resolve().parent
plan = json.loads((here / 'PLAN.json').read_text())
a.output.mkdir(parents=True, exist_ok=False)
manifest = {'status': 'INCOMPLETE', 'runs': [], 'binaries': {}}
for side in ['go', 'rust']:
    binary = getattr(a, side).resolve()
    manifest['binaries'][side] = {'path': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}

def save():
    (a.output / 'execution.json').write_text(json.dumps(manifest, indent=2) + '\n')

save()
try:
    for profile in plan['required_profiles']:
        for side in (['go'] if a.reference_only else ['go', 'rust']):
            conf = next(c for c in plan['configs'] if c['side'] == side and c['profile'] == profile)
            config = here / conf['config']
            assert hashlib.sha256(config.read_bytes()).hexdigest() == conf['sha256']
            port = conf['http_ws_port']
            url = 'http://127.0.0.1:' + str(port)
            # An occupied port is a failure, not permission to test a different process.
            import socket
            with socket.socket() as sock:
                assert sock.connect_ex(('127.0.0.1', port)) != 0, ('occupied', port)
            record = {'side': side, 'profile': profile, 'config_sha256': conf['sha256'], 'started_unix': time.time()}
            manifest['runs'].append(record)
            log = (a.output / (side + '-' + profile + '.server.log')).open('wb')
            server = subprocess.Popen([str(getattr(a, side).resolve()), '--dev', '--config', str(config)], stdout=log, stderr=subprocess.STDOUT)
            record['pid'] = server.pid
            save()
            try:
                deadline = time.monotonic() + 30
                while True:
                    assert server.poll() is None, ('server exited', server.returncode)
                    try:
                        with urllib.request.urlopen(url + '/', timeout=1) as response:
                            assert response.status == 200
                        break
                    except (OSError, TimeoutError):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(.1)
                env = dict(os.environ, LIVEKIT_URL=url.replace('http:', 'ws:'), LIVEKIT_API_KEY='devkey', LIVEKIT_API_SECRET='secret', SIGNED_LIMIT_PROFILE=profile, SIGNED_LIMIT_OUTPUT=str((a.output / (side + '-' + profile + '.json')).resolve()))
                with (a.output / (side + '-' + profile + '.runner.log')).open('wb') as out:
                    result = subprocess.run([a.node, str(here / 'run.mjs')], env=env, stdout=out, stderr=subprocess.STDOUT, timeout=180)
                record['runner_exit'] = result.returncode
                assert result.returncode == 0, (side, profile, 'runner failed')
            finally:
                if server.poll() is None:
                    server.send_signal(signal.SIGINT)
                try:
                    record['server_exit'] = server.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    server.kill()
                    record['server_exit'] = server.wait()
                    record['forced_kill'] = True
                log.close()
                record['finished_unix'] = time.time()
                save()
            assert record['server_exit'] == 0, record
        if not a.reference_only:
            subprocess.run([a.node, str(here / 'compare.mjs'), str(a.output / ('go-' + profile + '.json')), str(a.output / ('rust-' + profile + '.json')), str(a.output / ('pair-' + profile + '.json'))], check=True, timeout=30)
    if a.reference_only:
        manifest['status'] = 'REFERENCE_FIXTURE_ONLY_ALL9_COMPLETED_NOT_PAIRED'
    else:
        result = subprocess.run(['/usr/bin/python3', str(here / 'check-reports.py'), str(a.output)], check=True, capture_output=True, text=True, timeout=30)
        (a.output / 'aggregate.json').write_text(result.stdout)
        manifest['status'] = 'PASS_SCOPED_ACTUAL_9_PAIRS'
finally:
    save()
