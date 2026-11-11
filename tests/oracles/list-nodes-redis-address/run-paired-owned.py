#!/usr/bin/env python3
"""Coordinator-owned paired list-nodes raw Redis address acceptance.

Stages and builds the fixed Go observer, starts one exclusively owned Redis with
IPv4, IPv6 and a punctuation UNIX socket, then runs the paired run.py which
executes the ignored public consumer while those exact fixtures are still live.
The owned broker and its private config are always torn down afterwards.

The consumer is a prebuilt test binary, not `cargo test`, so the receipt binds to
the same workspace-unified candidate as the rest of the acceptance set.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[3]
TEST = 'cli_redis_address_tests::actual_original_redis_address_public_consumer'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--work', required=True, help='fresh output directory')
    ap.add_argument('--owned-dir', required=True, help='must not exist; holds redis.conf')
    ap.add_argument('--consumer-binary', required=True, help='prebuilt livecloud lib test binary')
    ap.add_argument('--rust-cli', required=True, help='prebuilt target/debug/livecloud')
    ap.add_argument('--official', required=True, help='locked official Go server binary')
    ap.add_argument('--go', required=True, help='go1.26.3 toolchain binary')
    ap.add_argument('--redis-server', default='/opt/homebrew/bin/redis-server')
    ap.add_argument('--redis-cli', default='/opt/homebrew/bin/redis-cli')
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--password', required=True, help='synthetic printable-ASCII password')
    args = ap.parse_args()

    work = pathlib.Path(args.work).resolve()
    owned = pathlib.Path(args.owned_dir).resolve()
    work.mkdir(parents=True, exist_ok=False)
    steps = []

    def record(label, code, **extra):
        steps.append(dict({'step': label, 'exit_code': code}, **extra))
        print('%s EXIT=%s' % (label, code), flush=True)

    def run(label, argv, cwd=ROOT, env=None, timeout=2400):
        proc = subprocess.run([str(a) for a in argv], cwd=str(cwd), env=env,
                              capture_output=True, timeout=timeout)
        (work / (label + '.stdout')).write_bytes(proc.stdout)
        (work / (label + '.stderr')).write_bytes(proc.stderr)
        record(label, proc.returncode)
        return proc

    def finish(code):
        receipt = work / 'actual-v2.json'
        status = None
        if receipt.exists():
            try:
                status = json.loads(receipt.read_text()).get('status')
            except ValueError:
                status = 'UNPARSEABLE'
        summary = {'status': 'PASS' if code == 0 else 'FAIL', 'test': TEST,
                   'steps': steps, 'paired_report_status': status,
                   'receipt': str(receipt), 'consumer_binary': str(args.consumer_binary),
                   'consumer_binary_sha256': hashlib.sha256(
                       pathlib.Path(args.consumer_binary).read_bytes()).hexdigest(),
                   'rust_cli': str(args.rust_cli), 'official_binary': str(args.official),
                   'owned_dir': str(owned), 'owned_broker_port': args.port}
        (work / 'driver-receipt.json').write_text(json.dumps(summary, indent=2) + '\n')
        print('PAIRED_REPORT_STATUS=%s' % status, flush=True)
        print('REDIS_ADDRESS_DRIVER_EXIT=%d' % code, flush=True)
        return code

    goenv = dict(os.environ, GOTOOLCHAIN='local', GOPROXY='off', GOWORK='off')
    observer = work / 'observer'
    here = pathlib.Path(__file__).resolve().parent
    proc = run('prepare-observer',
               [sys.executable, here / 'oracle/prepare.py', '--root', ROOT, '--out', observer])
    if proc.returncode != 0:
        return finish(1)
    proc = run('build-observer',
               [args.go, 'build', '-mod=readonly', '-o', 'address-observer', '.'],
               cwd=observer, env=goenv)
    if proc.returncode != 0:
        return finish(1)
    if owned.exists():
        print('owned dir already exists; refusing to reuse', flush=True)
        return finish(1)
    proc = run('prepare-runtime',
               [sys.executable, here / 'prepare-runtime.py',
                '--owned-dir', owned, '--port', args.port],
               env=dict(os.environ, NODE_ADDRESS_REDIS_PASSWORD=args.password))
    if proc.returncode != 0:
        return finish(1)

    log = (work / 'redis.log').open('wb')
    broker = subprocess.Popen([args.redis_server, str(owned / 'redis.conf')],
                              stdout=log, stderr=subprocess.STDOUT,
                              start_new_session=True)
    record('start-owned-redis', 0, pid=broker.pid, port=args.port)
    code = 1
    try:
        deadline = time.monotonic() + 15
        ready = False
        while time.monotonic() < deadline:
            if broker.poll() is not None:
                break
            probe = subprocess.run(
                [args.redis_cli, '-h', '127.0.0.1', '-p', str(args.port),
                 '-a', args.password, '--no-auth-warning', 'PING'],
                capture_output=True, text=True)
            if probe.stdout.strip() == 'PONG':
                ready = True
                break
            time.sleep(0.1)
        print('owned-redis %s' % ('ready' if ready else 'not-ready'), flush=True)
        if ready:
            consumer = json.dumps([str(args.consumer_binary), '--ignored', '--exact',
                                   TEST, '--nocapture', '--test-threads=1'])
            proc = run('paired-run',
                       [sys.executable, here / 'run.py',
                        '--official', args.official, '--rust', args.rust_cli,
                        '--go-observer', observer / 'address-observer',
                        '--owned-dir', owned, '--owned-isolated-redis',
                        '--output', work / 'actual-v2.json',
                        '--consumer-command-json', consumer],
                       env=dict(os.environ, NODE_ADDRESS_REDIS_PASSWORD=args.password))
            code = proc.returncode
    finally:
        if broker.poll() is None:
            os.killpg(broker.pid, signal.SIGTERM)
            try:
                broker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(broker.pid, signal.SIGKILL)
                broker.wait()
        record('stop-owned-redis', broker.returncode)
        log.close()
        # redis.conf and the per-case YAML hold the synthetic password; the report
        # keeps only their digests, so the private inputs must not outlive the run.
        shutil.rmtree(owned, ignore_errors=True)
        record('remove-owned-private-inputs', 0, removed=not owned.exists())
    return finish(code)


if __name__ == '__main__':
    sys.exit(main())