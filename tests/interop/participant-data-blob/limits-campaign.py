#!/usr/bin/env python3
"""Run actual fixed-Go/Rust Blob limit profiles; owns only its child servers."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official', required=True, type=Path)
    parser.add_argument('--rust', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--official-port', type=int, default=19880)
    parser.add_argument('--rust-port', type=int, default=19980)
    parser.add_argument('--origin', choices=['yaml', 'env', 'cli'], default='yaml')
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    workspace = here.parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    profiles = json.loads((here / 'limit-profiles.json').read_text())
    binaries = {'official': args.official.resolve(), 'rust': args.rust.resolve()}
    report = {'status': 'RUNNING', 'input_origin': args.origin, 'profiles': [], 'binary_sha256': {
        name: digest(path) for name, path in binaries.items()}}
    try:
        with tempfile.TemporaryDirectory(prefix='livekit-rs-blob-limit-config-') as private:
            os.chmod(private, 0o700)
            for name, functional in profiles.items():
                row = {'profile': name, 'declaredConfig': functional, 'servers': {}}
                report['profiles'].append(row)
                for implementation, binary in binaries.items():
                    port = args.official_port if implementation == 'official' else args.rust_port
                    config = {'port': port, 'bind_addresses': ['127.0.0.1'],
                              'rtc': {'tcp_port': port + 1, 'udp_port': port + 2,
                                      'node_ip': '127.0.0.1', 'use_external_ip': False,
                                      'enable_loopback_candidate': True,
                                      'ips': {'includes': ['127.0.0.1/32']}},
                              'logging': {'level': 'info'}}
                    if args.origin == 'yaml':
                        config.update(functional)
                    def leaves(value, prefix=''):
                        for key, child in value.items():
                            path = prefix + key
                            if isinstance(child, dict):
                                yield from leaves(child, path + '.')
                            else:
                                yield path, str(child).lower() if isinstance(child, bool) else str(child)
                    arguments = []
                    server_env = {key: value for key, value in os.environ.items()
                                  if not key.startswith(('LIVEKIT_', 'REDIS_'))
                                  and key not in ('NODE_IP', 'UDP_PORT')}
                    public_environment = {}
                    for path, value in leaves(functional):
                        if args.origin == 'cli':
                            arguments.append('--' + path + '=' + value)
                        elif args.origin == 'env':
                            public_environment['LIVEKIT_' + path.replace('.', '_').upper()] = value
                    server_env.update(public_environment)
                    config_path = Path(private) / (implementation + '.yaml')
                    config_path.write_text(json.dumps(config, indent=2) + '\n')
                    os.chmod(config_path, 0o600)
                    stem = name + '-' + implementation
                    result_path = output / (stem + '.json')
                    record = {'config_sha256': digest(config_path),
                              'public_functional_cli': arguments,
                              'public_functional_env': public_environment}
                    row['servers'][implementation] = record
                    with (output / (stem + '-server.log')).open('wb') as server_log:
                        process = subprocess.Popen(
                            [str(binary), '--dev', '--config', str(config_path)] + arguments,
                            cwd=workspace, env=server_env, stdout=server_log,
                            stderr=subprocess.STDOUT)
                        try:
                            deadline = time.monotonic() + 10
                            while True:
                                if process.poll() is not None:
                                    raise RuntimeError('owned server exited during startup: ' + stem)
                                try:
                                    urllib.request.urlopen('http://127.0.0.1:' + str(port), timeout=1).close()
                                    break
                                except urllib.error.HTTPError:
                                    break
                                except urllib.error.URLError:
                                    if time.monotonic() >= deadline:
                                        raise RuntimeError('owned server startup deadline: ' + stem)
                                    time.sleep(0.05)
                            env = dict(os.environ, LIVEKIT_URL='ws://127.0.0.1:' + str(port),
                                       LIVEKIT_API_KEY='devkey', LIVEKIT_API_SECRET='secret',
                                       BLOB_LIMIT_PROFILE=name, BLOB_LIMIT_OUTPUT=str(result_path),
                                       BLOB_SERVER_CONFIG_SHA256=record['config_sha256'])
                            with (output / (stem + '-fixture.log')).open('wb') as log:
                                result = subprocess.run(['node', str(here / 'limits.mjs')],
                                                        cwd=here.parent, env=env, stdout=log,
                                                        stderr=subprocess.STDOUT, timeout=60)
                            record['fixture_exit'] = result.returncode
                            if result.returncode:
                                raise RuntimeError('actual configured-limit fixture failed: ' + stem)
                        finally:
                            if process.poll() is None:
                                process.send_signal(signal.SIGINT)
                            try:
                                record['server_exit'] = process.wait(timeout=15)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                record['server_exit'] = process.wait(timeout=5)
                                raise RuntimeError('owned server graceful shutdown deadline: ' + stem)
                            if record['server_exit'] != 0:
                                raise RuntimeError('owned server did not exit cleanly: ' + stem)
                    record['result_sha256'] = digest(result_path)
                    record['evidence'] = json.loads(result_path.read_text())
                comparison_path = output / (name + '-comparison.json')
                result = subprocess.run(['node', str(here / 'compare-limits.mjs'),
                                         str(output / (name + '-official.json')),
                                         str(output / (name + '-rust.json')),
                                         str(comparison_path)], cwd=here.parent, timeout=10)
                row['comparison_exit'] = result.returncode
                if result.returncode:
                    raise RuntimeError('actual Go/Rust configured-limit mismatch: ' + name)
                row['comparison'] = json.loads(comparison_path.read_text())
                row['status'] = 'PASS'
        report['status'] = 'PASS_SCOPED'
    except Exception as error:
        report['status'] = 'FAIL'
        report['error'] = str(error)
    finally:
        (output / 'campaign.json').write_text(json.dumps(report, indent=2) + '\n')
    return 0 if report['status'] == 'PASS_SCOPED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
