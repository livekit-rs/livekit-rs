#!/usr/bin/env python3
"""Exact production Rust / fixed Go / Linux qdisc actual-counter comparison.

Coordinator executes in the disposable guest only. No Cargo, online Go modules,
lo/eth0 mutation, host network changes, synthetic netlink or fake counter input.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import pwd
import shutil
import signal
import subprocess
import time
import uuid


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as file:
        for block in iter(lambda: file.read(1024 * 1024), b''): digest.update(block)
    return digest.hexdigest()


def inventory(root):
    excluded = {'.git', 'node_modules', 'target', 'output', '__pycache__'}
    result = {}
    for path in sorted(Path(root).rglob('*')):
        if set(path.relative_to(root).parts) & excluded: continue
        assert not path.is_symlink(), str(path)
        if path.is_file(): result[str(path.relative_to(root))] = sha(path)
    return result


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['rust-source', 'libc-rlib', 'rustc', 'go-source', 'go', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    assert platform.system() == 'Linux' and platform.machine() == 'aarch64'
    assert os.getuid() != 0 and Path('/etc/cloud/cloud.cfg').is_file()
    out = args.output.resolve()
    assert str(out).startswith('/home/fixture/fixture/output/')
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    here = Path(__file__).resolve().parent
    expected = json.loads((here / 'SOURCE.json').read_text())
    rust_source, go_root = args.rust_source.resolve(strict=True), args.go_source.resolve(strict=True)
    assert sha(rust_source) == expected['rust_source_sha256'], 'exact frozen production node_tc.rs required'
    for name, want in expected['fixed_go_files_sha256'].items():
        relative = name.removeprefix('references/livekit-stable/')
        assert sha(go_root / relative) == want, relative
    go_before = inventory(go_root)
    inputs = {str(path): sha(path) for path in [Path(__file__).resolve(), here / 'SOURCE.json', here / 'probe.rs', here / 'parity_observer_test.go', rust_source, args.libc_rlib, args.rustc, args.go]}
    receipt = {'status': 'RUNNING', 'inputs': inputs, 'commands': [], 'phases': [], 'cleanup_errors': [],
               'network_scope': 'Only two newly created task netns and their veth pair; no lo/eth0 or host network mutation',
               'go_original_source_inventory': go_before}
    def save(): (out / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')
    def command(argv, label, env=None, cwd=None):
        result = subprocess.run(list(map(str, argv)), env=env, cwd=cwd, capture_output=True, timeout=120)
        (out / (label + '.stdout')).write_bytes(result.stdout); (out / (label + '.stderr')).write_bytes(result.stderr)
        receipt['commands'].append({'label': label, 'argv': list(map(str, argv)), 'exit': result.returncode}); save()
        if result.returncode != 0: raise RuntimeError(label + ' exit ' + str(result.returncode))
        return result.stdout.decode()
    sudo = ['sudo', '-n']
    username = pwd.getpwuid(os.getuid()).pw_name
    token = uuid.uuid4().hex[:8]
    ns_a, ns_b = 'lkq-' + token + '-a', 'lkq-' + token + '-b'
    dev_a, dev_b = 'lq' + token + 'a', 'lq' + token + 'b'
    created_ns = []; created_veth = False; receiver = None; stage = None; staged_expected = None
    def ns(namespace, argv): return sudo + ['ip', 'netns', 'exec', namespace, 'runuser', '-u', username, '--'] + list(map(str, argv))
    def kernel(label):
        raw = command(sudo + ['ip', 'netns', 'exec', ns_a, 'tc', '-s', '-j', 'qdisc', 'show', 'dev', dev_a], label)
        qdiscs = json.loads(raw); assert len(qdiscs) == 1 and qdiscs[0]['kind'] == 'netem', qdiscs
        return {'packets': int(qdiscs[0]['packets']), 'drops': int(qdiscs[0]['drops'])}
    def triad(phase):
        result = {'phase': phase, 'kernel_before': kernel(phase + '-kernel-before')}
        rust = command(ns(ns_a, [out / 'rust-probe']), phase + '-rust')
        result['rust'] = json.loads(rust)
        go_file = out / (phase + '-go.json')
        command(ns(ns_a, ['/usr/bin/env', 'QDISC_RESULT_PATH=' + str(go_file), out / 'go-probe', '-test.run=^TestOwnedQdiscKernelCounters$', '-test.v']), phase + '-go')
        result['go'] = json.loads(go_file.read_text()); result['kernel_after'] = kernel(phase + '-kernel-after')
        assert result['kernel_before'] == result['kernel_after'], 'actual qdisc changed during observation'
        for kind in ['rust', 'go']:
            assert {key: result[kind][key] for key in ['packets', 'drops']} == result['kernel_before'], (phase, result)
            assert result[kind]['samples'] == 257 and result[kind]['fd_before'] == result[kind]['fd_after']
        receipt['phases'].append(result); save()
        return result['kernel_after']
    try:
        # Staging is additive: exact original source is copied, then one test
        # observer is appended. Never modify the original checkout or algorithm.
        stage = out / 'go-stage'
        shutil.copytree(go_root, stage, ignore=shutil.ignore_patterns('.git', 'node_modules', 'target', 'output', '__pycache__'))
        assert inventory(stage) == go_before
        observer = stage / 'pkg/telemetry/prometheus/owned_qdisc_parity_test.go'
        assert not observer.exists(); shutil.copyfile(here / 'parity_observer_test.go', observer)
        staged_expected=dict(go_before);staged_expected['pkg/telemetry/prometheus/owned_qdisc_parity_test.go']=sha(observer)
        shutil.copyfile(rust_source, out / 'production.rs'); shutil.copyfile(here / 'probe.rs', out / 'probe.rs')
        command([args.rustc, '--edition=2024', '--crate-name', 'owned_qdisc_production', out / 'probe.rs', '--extern', 'libc=' + str(args.libc_rlib.resolve()), '-L', 'dependency=' + str(args.libc_rlib.resolve().parent), '-o', out / 'rust-probe'], 'rustc-production')
        env = dict(os.environ, GOWORK='off', GOPROXY='off', GOSUMDB='off', GOTOOLCHAIN='local', GOMAXPROCS='2')
        command([args.go, 'test', '-mod=readonly', '-p', '1', '-c', '-o', out / 'go-probe', './pkg/telemetry/prometheus'], 'go-original-offline-build', env, stage)
        assert inventory(stage)==staged_expected, 'Go staging must only add the observer, never alter original algorithm/module graph'
        receipt['probes'] = {name: sha(out / name) for name in ['rust-probe', 'go-probe']}
        for namespace in [ns_a, ns_b]:
            command(sudo + ['ip', 'netns', 'add', namespace], 'add-' + namespace); created_ns.append(namespace)
        command(sudo + ['ip', 'link', 'add', dev_a, 'type', 'veth', 'peer', 'name', dev_b], 'add-veth'); created_veth = True
        command(sudo + ['ip', 'link', 'set', dev_a, 'netns', ns_a], 'move-a')
        command(sudo + ['ip', 'link', 'set', dev_b, 'netns', ns_b], 'move-b')
        for namespace, device, address in [(ns_a, dev_a, '10.211.0.1/30'), (ns_b, dev_b, '10.211.0.2/30')]:
            command(sudo + ['ip', '-n', namespace, 'addr', 'add', address, 'dev', device], 'addr-' + device)
            command(sudo + ['ip', '-n', namespace, 'link', 'set', device, 'up'], 'up-' + device)
        command(sudo + ['ip', 'netns', 'exec', ns_a, 'tc', 'qdisc', 'add', 'dev', dev_a, 'root', 'netem', 'loss', '0%'], 'netem-success')
        ready_file, received_file = out / 'udp-ready', out / 'udp-received.json'
        receive_code = 'import socket,pathlib,json; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.bind(("10.211.0.2",19755));s.settimeout(5);pathlib.Path(__import__("sys").argv[1]).write_text("ready"); values=set();' + '\nwhile len(values)<100:\n data,_=s.recvfrom(1024)\n assert data.startswith(b"qdisc-success-")\n values.add(int.from_bytes(data[14:18],"big"))\n' + 's.close();assert values==set(range(100));pathlib.Path(__import__("sys").argv[2]).write_text(json.dumps({"received":len(values)}))'
        with (out / 'receiver.stdout').open('wb') as stdout, (out / 'receiver.stderr').open('wb') as stderr:
            receiver = subprocess.Popen(ns(ns_b, ['/usr/bin/python3', '-c', receive_code, ready_file, received_file]), stdout=stdout, stderr=stderr, start_new_session=True)
            deadline = time.monotonic() + 10
            while not ready_file.exists():
                assert receiver.poll() is None, 'actual UDP receiver exited before bind'
                if time.monotonic() > deadline: raise RuntimeError('actual UDP bind readiness')
                time.sleep(.05)
            send_code = 'import socket,time; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);' + '\nfor i in range(100):\n s.sendto(b"qdisc-success-"+i.to_bytes(4,"big")+b"x"*64,("10.211.0.2",19755));time.sleep(.002)\ns.close()'
            command(ns(ns_a, ['/usr/bin/python3', '-c', send_code]), 'actual-success-udp')
            receipt['receiver_exit'] = receiver.wait(timeout=10); assert receipt['receiver_exit'] == 0
        receipt['received'] = json.loads(received_file.read_text()); assert receipt['received']['received'] == 100
        successful = triad('successful-traffic'); assert successful['packets'] > 0 and successful['drops'] == 0
        command(sudo + ['ip', 'netns', 'exec', ns_a, 'tc', 'qdisc', 'change', 'dev', dev_a, 'root', 'netem', 'loss', '100%'], 'netem-drop')
        command(ns(ns_a, ['/usr/bin/python3', '-c', send_code]), 'actual-dropped-udp')
        dropped = triad('successful-plus-drops'); assert dropped['packets'] > 0 and dropped['drops'] > successful['drops']
        receipt['status'] = 'PASS_REAL_QDISC_AND_PRODUCTION_COUNTERS'
    except Exception as error:
        receipt['status'] = 'FAIL'; receipt['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        if receiver is not None and receiver.poll() is None:
            os.killpg(receiver.pid, signal.SIGTERM)
            try: receiver.wait(timeout=5)
            except subprocess.TimeoutExpired: os.killpg(receiver.pid, signal.SIGKILL); receiver.wait()
            receipt['cleanup_errors'].append('UDP receiver required termination')
        # Delete only names this invocation actually created. Each namespace
        # owns its one veth end; deleting both removes the pair and qdisc.
        for namespace in reversed(created_ns):
            try: command(sudo + ['ip', 'netns', 'delete', namespace], 'delete-' + namespace)
            except Exception as error: receipt['cleanup_errors'].append(str(error))
        if created_veth:
            # If setup failed before move, either end may still be in the guest
            # root namespace. A precise random-name check authorizes deletion.
            for device in [dev_a, dev_b]:
                check = subprocess.run(sudo + ['ip', 'link', 'show', 'dev', device], capture_output=True)
                if check.returncode == 0:
                    try: command(sudo + ['ip', 'link', 'delete', device], 'delete-root-' + device)
                    except Exception as error: receipt['cleanup_errors'].append(str(error))
        check = subprocess.run(sudo + ['ip', 'netns', 'list'], capture_output=True, text=True)
        if check.returncode != 0 or any(line.split()[0] in created_ns for line in check.stdout.splitlines() if line.strip()): receipt['cleanup_errors'].append('owned namespaces not released')
        receipt['go_staging_unchanged_except_observer'] = staged_expected is not None and inventory(stage)==staged_expected
        receipt['source_identity_stable'] = receipt['go_staging_unchanged_except_observer'] and inventory(go_root) == go_before and all(sha(path) == want for path, want in inputs.items())
        if not receipt['source_identity_stable'] or receipt['cleanup_errors']: receipt['status'] = 'FAIL'
        save()
    return 0 if receipt['status'] == 'PASS_REAL_QDISC_AND_PRODUCTION_COUNTERS' else 1

if __name__ == '__main__': raise SystemExit(main())
