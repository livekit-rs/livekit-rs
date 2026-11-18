#!/usr/bin/env python3
"""Pair supplied MAIN executables with the official SDK VP8 lifecycle probe."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

HERE = Path(__file__).resolve().parent
FIXED_GO_SHA = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'
ITEMS = ['SIGNAL:VideoLayer.OpenQualityConsumers']
SCOPE = ('Launched MAIN official SDK VP8 simulcast lifecycle on controlled loopback; '
         'not full open-quality, signed -1, concurrent, network or cloud coverage.')
CASE_IDS = ['low-high', 'high-low', 'medium-low', 'low-low', 'a-paused-b-low',
            'a-low-b-paused', 'both-paused', 'resume-high-low', 'publisher-pause',
            'publisher-resume', 'unpublish', 'republish']
PINS = {'@livekit/protocol': '1.52.1', 'livekit-client': '2.23.0',
        'livekit-server-sdk': '2.19.2', 'playwright': '1.64.1'}
PROBE_TIMEOUT = 600
CANCELLED = threading.Event()


class CheckFailure(Exception):
    pass


def require(condition, label):
    if not condition:
        raise CheckFailure(label)


def failure(error, stage):
    return {'stage': stage, 'kind': type(error).__name__,
            'check': str(error) if isinstance(error, CheckFailure) else 'operation_failed',
            'errno': getattr(error, 'errno', None)}


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            digest.update(chunk)
    return digest.hexdigest()


def redact_log(raw, secret):
    text = raw.decode('utf-8', errors='replace').replace(secret, '<redacted-secret>')
    text = re.sub(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', '<redacted-jwt>', text)
    return re.sub(r'\b(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})\b', '<redacted-private-ip>', text)


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def strict_json(text):
    def pairs(entries):
        result = {}
        for key, value in entries:
            require(key not in result, 'duplicate_json_key')
            result[key] = value
        return result

    def constant(_):
        raise CheckFailure('nonfinite_json_number')

    result = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    require(finite_json(result), 'nonfinite_json_number')
    return result


def finite_json(value):
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and finite_json(item) for key, item in value.items())
    if isinstance(value, list):
        return all(finite_json(item) for item in value)
    return value is None or type(value) in (str, int, bool)


def deep_equal(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(deep_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(deep_equal(a, b) for a, b in zip(left, right))
    return left == right


def check_cancelled():
    require(not CANCELLED.is_set(), 'cancelled')


def server_config(ports, key, secret):
    return {
        'port': ports['http'], 'bind_addresses': ['127.0.0.1'],
        'keys': {key: secret}, 'turn': {'enabled': False}, 'logging': {'level': 'error'},
        'rtc': {'tcp_port': 0, 'udp_port': ports['udp'],
                'node_ip': '127.0.0.1', 'use_external_ip': False,
                'enable_loopback_candidate': True, 'stun_servers': [],
                'ips': {'includes': ['127.0.0.1/32']}},
        'video': {'dynacast_pause_delay': '1s'},
        'api': {'enable_psrpc_for_get_list_participants': True},
        'room': {'enabled_codecs': [{'mime': 'audio/opus'}, {'mime': 'video/vp8'}]},
    }


def dependency_snapshot(node):
    script = r'''
import { createRequire } from 'node:module';
import { readFileSync, realpathSync } from 'node:fs';
import { dirname, resolve, sep } from 'node:path';
import { createHash } from 'node:crypto';
const root = process.argv[1], pins = JSON.parse(process.argv[2]);
const loader = createRequire(resolve(root, 'package.json'));
const hash = p => createHash('sha256').update(readFileSync(p)).digest('hex');
const modules = realpathSync(resolve(root, 'node_modules'));
const manifest = JSON.parse(readFileSync(resolve(root, 'package.json')));
const lock = JSON.parse(readFileSync(resolve(root, 'package-lock.json')));
const pinPath = resolve(root, 'client-browser-capabilities/sdk-pins.json');
const pinned = JSON.parse(readFileSync(pinPath));
const packages = [];
if (JSON.stringify(Object.keys(pinned.packages).sort()) !== JSON.stringify(Object.keys(pins).sort())) throw new Error('pin_names');
for (const [name, expected] of Object.entries(pinned.packages)) {
  const version = pins[name];
  const entry = realpathSync(loader.resolve(name));
  if (!entry.startsWith(modules + sep)) throw new Error('dependency_not_in_interop');
  let dir = dirname(entry);
  for (;;) {
    let pkg;
    try { pkg = JSON.parse(readFileSync(resolve(dir, 'package.json'))); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (pkg?.name === name) {
      if (pkg.version !== version || manifest.dependencies[name] !== version ||
          lock.packages[''].dependencies[name] !== version ||
          lock.packages[`node_modules/${name}`].version !== version) throw new Error('pin_mismatch');
      const identity = { name, version, package_sha256: hash(resolve(dir, 'package.json')), entry_sha256: hash(entry) };
      if (version !== expected.version || identity.package_sha256 !== expected.package_sha256 ||
          identity.entry_sha256 !== expected.entry_sha256) throw new Error('pinned_identity_mismatch');
      if (name === 'livekit-client' && entry !== realpathSync(resolve(root, 'node_modules/livekit-client/dist/livekit-client.umd.js'))) throw new Error('umd_entry_mismatch');
      packages.push(identity);
      break;
    }
    if (dirname(dir) === dir) throw new Error('package_missing');
    dir = dirname(dir);
  }
}
const umd = realpathSync(resolve(root, 'node_modules/livekit-client/dist/livekit-client.umd.js'));
if (!umd.startsWith(modules + sep)) throw new Error('umd_not_in_interop');
const lockHash = hash(resolve(root, 'package-lock.json'));
if (lockHash !== pinned.package_lock_sha256) throw new Error('pinned_lock_mismatch');
console.log(JSON.stringify({ packages, package_lock_sha256: lockHash, pins_sha256: hash(pinPath), umd_entry: 'livekit-client/dist/livekit-client.umd.js' }));
'''
    text = subprocess.check_output([node, '--input-type=module', '-e', script,
                                    str(HERE.parent), json.dumps(PINS)],
                                   stderr=subprocess.DEVNULL, text=True, timeout=20,
                                   env={'PATH': os.environ.get('PATH', '/usr/bin:/bin')})
    result = strict_json(text)
    require(dependencies_ok(result), 'dependency_identity_invalid')
    return result


def digest_ok(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def dependencies_ok(value):
    if not isinstance(value, dict) or not isinstance(value.get('packages'), list):
        return False
    packages = value['packages']
    if not all(isinstance(item, dict) and isinstance(item.get('name'), str) for item in packages):
        return False
    return (len(packages) == len(PINS) and {item['name'] for item in packages} == set(PINS)
            and digest_ok(value.get('package_lock_sha256')) and digest_ok(value.get('pins_sha256'))
            and value.get('umd_entry') == 'livekit-client/dist/livekit-client.umd.js'
            and all(item.get('version') == PINS[item['name']]
                    and digest_ok(item.get('package_sha256')) and digest_ok(item.get('entry_sha256'))
                    for item in packages))


def validate_probe_result(node, result):
    script = r'''
const { validateReport } = await import(process.argv[1]);
let text = '';
for await (const chunk of process.stdin) text += chunk;
const result = validateReport(JSON.parse(text));
console.log(JSON.stringify(result));
process.exitCode = result.ok === true && result.errors.length === 0 ? 0 : 1;
'''
    completed = subprocess.run([node, '--input-type=module', '-e', script, (HERE / 'probe.mjs').as_uri()],
                               input=json.dumps(result, allow_nan=False), capture_output=True,
                               text=True, timeout=30, env={'PATH': os.environ.get('PATH', '/usr/bin:/bin')})
    return {'exit': completed.returncode, 'result': strict_json(completed.stdout)}


def bind_port(kind, port):
    sock = socket.socket(socket.AF_INET, kind)
    try:
        if kind == socket.SOCK_STREAM:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', port))
        if kind == socket.SOCK_STREAM:
            sock.listen(1)
        return sock
    except BaseException:
        sock.close()
        raise


def reserve_ports():
    reservations, ports = [], {}
    try:
        for name, kind in [('http', socket.SOCK_STREAM), ('udp', socket.SOCK_DGRAM)]:
            for _ in range(500):
                port = 18000 + secrets.randbelow(10000)
                if port in ports.values():
                    continue
                try:
                    sock = bind_port(kind, port)
                except OSError:
                    continue
                reservations.append(sock)
                ports[name] = port
                break
            else:
                raise CheckFailure('low_port_reservation_failed')
        return ports, reservations
    except BaseException:
        for sock in reservations:
            sock.close()
        raise


def rebind_ports(ports):
    result, sockets = {}, []
    try:
        for name, kind in [('http', socket.SOCK_STREAM), ('udp', socket.SOCK_DGRAM)]:
            try:
                sockets.append(bind_port(kind, ports[name]))
                result[name] = True
            except OSError:
                result[name] = False
        return result
    finally:
        for sock in sockets:
            sock.close()


def stop_owned(proc):
    result = {'exit': None, 'forced_kill': False, 'process_group_gone': False}
    if proc is None:
        return result
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        result['exit'] = proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        result['forced_kill'] = True
        os.killpg(proc.pid, signal.SIGKILL)
        result['exit'] = proc.wait(timeout=5)
    try:
        os.killpg(proc.pid, 0)
    except ProcessLookupError:
        result['process_group_gone'] = True
    else:
        result['forced_kill'] = True
        os.killpg(proc.pid, signal.SIGKILL)
        try:
            os.killpg(proc.pid, 0)
        except ProcessLookupError:
            result['process_group_gone'] = True
    return result


def live_identity(proc, binary, expected_sha, config, config_sha, ports, lsof):
    require(proc.poll() is None, 'main_not_alive')
    binary_sha, observed_config_sha = sha(binary), sha(config)
    require(binary_sha == expected_sha, 'binary_changed_while_running')
    require(observed_config_sha == config_sha, 'config_changed_while_running')
    executable = Path(f'/proc/{proc.pid}/exe')
    if executable.exists():
        require(executable.resolve() == binary, 'launched_executable_path_mismatch')
        require(sha(executable) == expected_sha, 'launched_executable_sha_mismatch')
        method = 'proc_exe_sha256'
    else:
        text = subprocess.check_output([lsof, '-nP', '-a', '-p', str(proc.pid), '-d', 'txt', '-Ffin'],
                                       stderr=subprocess.DEVNULL, timeout=5, text=True)
        inode, found = None, False
        for line in text.splitlines():
            if line.startswith('f'):
                inode = None
            elif line.startswith('i'):
                inode = line[1:]
            elif line.startswith('n') and line[1:] == str(binary):
                found |= inode == str(binary.stat().st_ino)
        require(found, 'mapped_executable_identity_mismatch')
        method = 'lsof_text_path_inode'
    for name, protocol in [('http', 'TCP'), ('udp', 'UDP')]:
        flags = ['-sTCP:LISTEN'] if protocol == 'TCP' else []
        listeners = subprocess.check_output(
            [lsof, '-nP', '-a', '-p', str(proc.pid), f'-i{protocol}:{ports[name]}', *flags, '-Fpn'],
            stderr=subprocess.DEVNULL, timeout=5, text=True).splitlines()
        addresses = {line[1:] for line in listeners if line.startswith('n')}
        require(f'p{proc.pid}' in listeners and addresses == {f'127.0.0.1:{ports[name]}'},
                f'owned_loopback_{name}_listener_missing')
    require(proc.poll() is None, 'main_exited_during_identity')
    return {'pid': proc.pid, 'method': method, 'binary_sha256': binary_sha,
            'config_sha256': observed_config_sha, 'executable_matches': True, 'main_alive': True,
            'owned_loopback_listener': True, 'owned_loopback_udp': True}


def wait_ready(proc, port):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        check_cancelled()
        require(proc.poll() is None, 'main_exited_before_ready')
        try:
            with opener.open(f'http://127.0.0.1:{port}/', timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        CANCELLED.wait(0.05)
    raise CheckFailure('main_ready_timeout')


def run_side(side, binary, expected_sha, output, node, lsof, chromium, record):
    server = probe = None
    reservations, ports = [], {}
    stage = 'setup'
    secret = secrets.token_hex(32)
    probe_path = output / f'{side}.json'
    try:
        require(not probe_path.exists(), 'probe_output_already_exists')
        record['dependencies_before'] = dependency_snapshot(node)
        record['chromium_sha256_before'] = sha(chromium)
        with tempfile.TemporaryDirectory(prefix='quality-lifecycle-') as work, tempfile.TemporaryFile() as log:
            work = Path(work).resolve()
            try:
                ports, reservations = reserve_ports()
                record['ports'] = ports
                key = 'quality-fixture'
                config = work / 'server.yaml'
                config.write_text(json.dumps(server_config(ports, key, secret)))
                config.chmod(0o600)
                config_sha = sha(config)
                record['config_sha256'] = config_sha
                env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': str(work),
                       'TMPDIR': str(work), 'LANG': 'C', 'LC_ALL': 'C'}
                stage = 'launch'
                check_cancelled()
                record['binary_sha256_before'] = sha(binary)
                require(record['binary_sha256_before'] == expected_sha, 'prelaunch_binary_changed')
                for sock in reservations:
                    sock.close()
                reservations.clear()
                server = subprocess.Popen([str(binary), '--config', str(config)], cwd=work, env=env,
                                          stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                record['pid'] = server.pid
                stage = 'ready'
                wait_ready(server, ports['http'])
                record['launch_before'] = live_identity(server, binary, expected_sha, config, config_sha, ports, lsof)
                stage = 'probe'
                probe = subprocess.Popen([node, str(HERE / 'probe.mjs'), '--output', str(probe_path)],
                                         cwd=work, env=env, stdin=subprocess.PIPE,
                                         stdout=subprocess.DEVNULL, stderr=log, start_new_session=True)
                probe.stdin.write(json.dumps({'side': side, 'url': f'http://127.0.0.1:{ports["http"]}',
                                             'key': key, 'secret': secret, 'chromium': str(chromium)}).encode())
                probe.stdin.close()
                deadline = time.monotonic() + PROBE_TIMEOUT
                while probe.poll() is None:
                    check_cancelled()
                    require(server.poll() is None, 'main_exited_during_probe')
                    require(time.monotonic() < deadline, 'probe_timeout')
                    CANCELLED.wait(0.05)
                record['probe_exit'] = probe.returncode
                stage = 'launch_after'
                record['launch_after'] = live_identity(server, binary, expected_sha, config, config_sha, ports, lsof)
            finally:
                if probe is not None and 'probe_exit' not in record:
                    record['probe_exit'] = probe.poll()
                for name, proc in [('probe_cleanup', probe), ('server_cleanup', server)]:
                    if proc is not None:
                        try:
                            record[name] = stop_owned(proc)
                        except BaseException as error:
                            record.setdefault('cleanup_errors', []).append(failure(error, name))
                for sock in reservations:
                    sock.close()
                if ports:
                    record['ports_rebound'] = rebind_ports(ports)
                log.seek(0)
                raw_log = log.read()
                log_path = output / f'{side}.log'
                log_path.write_text(redact_log(raw_log, secret))
                log_path.chmod(0o600)
                record['log'] = {'raw_sha256': hashlib.sha256(raw_log).hexdigest(),
                                 'raw_bytes': len(raw_log), 'redacted_sha256': sha(log_path)}
    except BaseException as error:
        record['failure'] = failure(error, stage)
    finally:
        for name, operation in [('binary_sha256_after', lambda: sha(binary)),
                                ('dependencies_after', lambda: dependency_snapshot(node)),
                                ('chromium_sha256_after', lambda: sha(chromium))]:
            try:
                record[name] = operation()
            except BaseException as error:
                record.setdefault('identity_failures', []).append(failure(error, name))
        try:
            if probe_path.exists():
                record['result'] = strict_json(probe_path.read_text())
                require(isinstance(record['result'], dict), 'malformed_probe_result')
                record['probe_validation'] = validate_probe_result(node, record['result'])
        except (OSError, ValueError, CheckFailure, subprocess.SubprocessError) as error:
            record['result_failure'] = failure(error, 'probe_result_read')


def has_number(value):
    if type(value) in (int, float):
        return True
    if isinstance(value, dict):
        return any(has_number(item) for item in value.values())
    if isinstance(value, list):
        return any(has_number(item) for item in value)
    return False


def independent_case_ok(row):
    if not isinstance(row, dict):
        return False
    observed, samples = row.get('observed'), row.get('samples')
    checks = observed.get('contract') if isinstance(observed, dict) else None
    return (row.get('status') == 'PASS' and 'failure' not in row
            and isinstance(observed, dict) and bool(observed)
            and isinstance(checks, dict) and bool(checks) and all(value is True for value in checks.values())
            and isinstance(samples, (dict, list)) and bool(samples) and has_number(samples)
            and finite_json(row))


def browser_cleanup_ok(value):
    if not isinstance(value, dict) or not isinstance(value.get('exit'), dict):
        return False
    exit_record = value['exit']
    return (value.get('failures') == [] and type(value.get('contextsAfter')) is int
            and value['contextsAfter'] == 0 and type(exit_record.get('code')) is int
            and exit_record['code'] == 0 and 'signal' in exit_record and exit_record['signal'] is None
            and exit_record.get('observedBy') in ('child-exit-event', 'child-state')
            and 'timeout' not in exit_record)


def lifecycle_ok(record, expected_sha):
    cleanup = {'exit': 0, 'forced_kill': False, 'process_group_gone': True}
    dependencies = record.get('dependencies_before')
    return (digest_ok(expected_sha) and digest_ok(record.get('config_sha256'))
            and not any(key in record for key in ('failure', 'identity_failures', 'result_failure', 'cleanup_errors'))
            and type(record.get('probe_exit')) is int and record['probe_exit'] == 0
            and all(record.get(key) == expected_sha for key in ('binary_sha256_before', 'binary_sha256_after'))
            and all(isinstance(record.get(key), dict)
                    and record[key].get('pid') == record.get('pid') and type(record.get('pid')) is int
                    and record[key].get('binary_sha256') == expected_sha
                    and record[key].get('config_sha256') == record['config_sha256']
                    and record[key].get('executable_matches') is True
                    and record[key].get('main_alive') is True
                    and record[key].get('owned_loopback_listener') is True
                    and record[key].get('owned_loopback_udp') is True
                    for key in ('launch_before', 'launch_after'))
            and deep_equal(record.get('ports_rebound'), {'http': True, 'udp': True})
            and all(deep_equal(record.get(key), cleanup) for key in ('probe_cleanup', 'server_cleanup'))
            and dependencies_ok(dependencies) and deep_equal(dependencies, record.get('dependencies_after'))
            and digest_ok(record.get('chromium_sha256_before'))
            and record['chromium_sha256_before'] == record.get('chromium_sha256_after'))


def paired_cases(records):
    indices, valid = {}, {}
    for side in ('go', 'rust'):
        result = records.get(side, {}).get('result', {})
        result = result if isinstance(result, dict) else {}
        rows = result.get('cases', [])
        rows = rows if isinstance(rows, list) else []
        well_formed = all(isinstance(row, dict) and isinstance(row.get('id'), str) for row in rows)
        ids = [row['id'] for row in rows] if well_formed else []
        dependencies = result.get('dependencies')
        dependencies = dependencies if isinstance(dependencies, dict) else {}
        record = records.get(side, {})
        valid[side] = (well_formed and ids == CASE_IDS and len(set(ids)) == len(ids)
                       and type(result.get('case_count')) is int and result['case_count'] == len(CASE_IDS)
                       and result.get('side') == side and result.get('status') == 'PASS_SCOPED'
                       and 'failure' not in result and result.get('cleanup_errors') == []
                       and browser_cleanup_ok(result.get('browser_cleanup'))
                       and deep_equal(record.get('probe_validation'), {'exit': 0, 'result': {'ok': True, 'errors': []}})
                       and dependencies_ok(dependencies.get('before'))
                       and deep_equal(dependencies['before'], record.get('dependencies_before'))
                       and deep_equal(dependencies.get('after'), record.get('dependencies_after'))
                       and dependencies.get('chromium_sha256') == record.get('chromium_sha256_before')
                       and isinstance(record.get('fixture_sha256'), dict)
                       and all(digest_ok(record['fixture_sha256'].get(name)) for name in ('client', 'probe'))
                       and deep_equal(dependencies.get('fixture_sha256'), record['fixture_sha256']))
        indices[side] = {row['id']: row for row in rows if isinstance(row, dict)
                         and isinstance(row.get('id'), str) and ids.count(row['id']) == 1}
    cases = []
    for case_id in CASE_IDS:
        go, rust = (indices[side].get(case_id) for side in ('go', 'rust'))
        go_ok, rust_ok = (independent_case_ok(row) for row in (go, rust))
        equal = (go is not None and rust is not None and isinstance(go.get('observed'), dict)
                 and bool(go['observed']) and deep_equal(go['observed'], rust.get('observed')))
        status = 'NOT_RUN' if go is None and rust is None else ('PASS' if go_ok and rust_ok and equal else 'FAIL')
        cases.append({'id': case_id, 'status': status, 'go_contract_ok': go_ok,
                      'rust_contract_ok': rust_ok, 'cross_side_equal': equal, 'go': go, 'rust': rust})
    return cases, valid


def finalize(manifest):
    manifest['cases'], manifests_ok = paired_cases(manifest['sides'])
    manifest['side_manifests_ok'] = manifests_ok
    manifest['observed_case_count'] = {side: sum(case[side] is not None and bool(case[side].get('observed'))
                                              for case in manifest['cases']) for side in ('go', 'rust')}
    manifest['side_lifecycle_ok'] = {side: lifecycle_ok(manifest['sides'].get(side, {}),
        manifest['binaries'].get(side, {}).get('sha256')) for side in ('go', 'rust')}
    deps = [manifest['sides'].get(side, {}).get('dependencies_before') for side in ('go', 'rust')]
    manifest['dependencies_equal'] = all(dependencies_ok(dep) for dep in deps) and deep_equal(*deps)
    passed = (not CANCELLED.is_set() and 'failure' not in manifest
              and manifest.get('inputs_unchanged') is True and manifest.get('binary_identities_stable') is True
              and manifest.get('chromium_identity_stable') is True
              and manifest['binaries'].get('go', {}).get('sha256') == FIXED_GO_SHA
              and manifest['binaries'].get('rust', {}).get('sha256') != FIXED_GO_SHA
              and manifest['dependencies_equal'] and all(manifests_ok.values())
              and all(manifest['side_lifecycle_ok'].values())
              and all(case['status'] == 'PASS' for case in manifest['cases']))
    manifest['status'] = 'PASS_SCOPED' if passed else 'FAIL'
    manifest['failed_cases'] = [case['id'] for case in manifest['cases'] if case['status'] != 'PASS']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('go', 'rust', 'chromium'):
        parser.add_argument(f'--{name}', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path, help='fresh directory; parent must exist')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(exist_ok=False, mode=0o700)
    manifest = {'status': 'FAIL', 'covers': ITEMS, 'items': ITEMS, 'scope': SCOPE,
                'case_count': len(CASE_IDS), 'binaries': {}, 'sides': {}, 'cases': []}
    previous_handlers = {sig: signal.signal(sig, lambda *_: CANCELLED.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    previous_umask = os.umask(0o077)
    stage = 'inputs'
    try:
        require(all(getattr(args, name).is_absolute() for name in ('go', 'rust', 'chromium')), 'absolute_executable_paths_required')
        binaries = {side: getattr(args, side).resolve(strict=True) for side in ('go', 'rust')}
        for side, binary in binaries.items():
            require(binary.is_file() and os.access(binary, os.X_OK), 'executable_required')
            with binary.open('rb') as stream:
                require(stream.read(4) in (b'\x7fELF', b'\xcf\xfa\xed\xfe', b'\xfe\xed\xfa\xcf',
                                           b'\xca\xfe\xba\xbe', b'\xbe\xba\xfe\xca'), 'native_main_required')
            manifest['binaries'][side] = {'path': str(binary), 'sha256': sha(binary)}
        require(manifest['binaries']['go']['sha256'] == FIXED_GO_SHA, 'fixed_go_sha_mismatch')
        require(manifest['binaries']['rust']['sha256'] != FIXED_GO_SHA, 'rust_is_reference_binary')
        chromium = args.chromium.resolve(strict=True)
        require(chromium.is_file() and os.access(chromium, os.X_OK), 'chromium_executable_required')
        manifest['chromium'] = {'path': str(chromium), 'sha256': sha(chromium)}
        node, lsof = shutil.which('node'), shutil.which('lsof')
        require(node is not None and lsof is not None, 'node_and_lsof_required')
        inputs = [HERE / 'probe.mjs', HERE / 'client.html', HERE / 'run-paired.py',
                  HERE.parent / 'owned-browser-exit.mjs', HERE.parent / 'package.json', HERE.parent / 'package-lock.json',
                  HERE.parent / 'client-browser-capabilities/sdk-pins.json']
        manifest['input_sha256'] = {str(path): sha(path) for path in inputs}
        for side in ('go', 'rust'):
            check_cancelled()
            stage = side
            record = manifest['sides'].setdefault(side, {'fixture_sha256': {
                'client': manifest['input_sha256'][str(HERE / 'client.html')],
                'probe': manifest['input_sha256'][str(HERE / 'probe.mjs')]}})
            run_side(side, binaries[side], manifest['binaries'][side]['sha256'], output, node, lsof, chromium, record)
            write_json(output / 'execution.json', manifest)
        manifest['inputs_unchanged'] = all(sha(path) == manifest['input_sha256'][str(path)] for path in inputs)
        manifest['binary_identities_stable'] = all(sha(binary) == manifest['binaries'][side]['sha256'] for side, binary in binaries.items())
        manifest['chromium_identity_stable'] = (sha(chromium) == manifest['chromium']['sha256']
            and all(record.get('chromium_sha256_before') == manifest['chromium']['sha256']
                    and record.get('chromium_sha256_after') == manifest['chromium']['sha256']
                    for record in manifest['sides'].values()))
    except BaseException as error:
        manifest['failure'] = failure(error, stage)
    finally:
        try:
            finalize(manifest)
            write_json(output / 'execution.json', manifest)
            batch = {key: value for key, value in manifest.items() if key != 'sides'}
            batch['lifecycle'] = {side: {key: value for key, value in record.items() if key != 'result'}
                                  for side, record in manifest['sides'].items()}
            write_json(output / 'BATCH.json', batch)
        finally:
            os.umask(previous_umask)
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
    print(json.dumps({'status': manifest['status'], 'case_count': len(CASE_IDS), 'failed_cases': manifest['failed_cases']}))
    return 0 if manifest['status'] == 'PASS_SCOPED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
