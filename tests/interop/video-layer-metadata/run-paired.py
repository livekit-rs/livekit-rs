#!/usr/bin/env python3
"""Run supplied MAIN executables; binary WS metadata only, never build or send media."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
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
SCOPE = 'Launched MAIN authenticated binary /rtc AddTrack metadata only; no SDP exchange, RTP, media, SRTP or dynacast coverage.'
CASE_IDS = [
    'codec-less-common-unordered', 'vp8-common-fallback', 'vp8-specific-unordered',
    'dual-codec-distinct', 'codec-layers-without-common', 'signed-codec-less-common',
    'signed-common-fallback', 'signed-codec-specific', 'empty-layers',
    'codec-less-empty', 'regular-low-medium-high',
]
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
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def check_cancelled():
    require(not CANCELLED.is_set(), 'cancelled')


def server_config(ports, key, secret):
    # Go's TCP media mux binds wildcard; this signal-only probe does not need it.
    return {
        'port': ports['http'], 'bind_addresses': ['127.0.0.1'],
        'keys': {key: secret}, 'turn': {'enabled': False}, 'logging': {'level': 'error'},
        'rtc': {'tcp_port': 0, 'udp_port': ports['udp'],
                'node_ip': '127.0.0.1', 'use_external_ip': False,
                'enable_loopback_candidate': True, 'stun_servers': [],
                'ips': {'includes': ['127.0.0.1/32']}},
        'api': {'enable_psrpc_for_get_list_participants': True},
        'room': {'enabled_codecs': [{'mime': 'audio/opus'}, {'mime': 'video/vp8'}, {'mime': 'video/h264'}]},
    }


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
        # Darwin: bind the mapped text file's inode and path to the hashed binary.
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
    return {'pid': proc.pid, 'method': method, 'binary_sha256': binary_sha,
            'config_sha256': observed_config_sha, 'executable_matches': True,
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


def run_side(side, binary, expected_sha, output, node, lsof, record):
    server = probe = None
    reservations, ports = [], {}
    stage = 'setup'
    secret = secrets.token_hex(32)
    probe_path = output / f'{side}.json'
    try:
        with tempfile.TemporaryDirectory(prefix='layer-metadata-') as work, tempfile.TemporaryFile() as log:
            work = Path(work).resolve()
            try:
                ports, reservations = reserve_ports()
                record['ports'] = ports
                key = 'layer-fixture'
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
                                             'key': key, 'secret': secret}).encode())
                probe.stdin.close()
                deadline = time.monotonic() + 240
                while probe.poll() is None:
                    check_cancelled()
                    require(server.poll() is None, 'main_exited_during_probe')
                    require(time.monotonic() < deadline, 'probe_timeout')
                    CANCELLED.wait(0.05)
                record['probe_exit'] = probe.returncode
                stage = 'launch_after'
                record['launch_after'] = live_identity(server, binary, expected_sha, config, config_sha, ports, lsof)
            finally:
                # The probe closes its WS/room while MAIN is still alive, including cancellation.
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
                record['log'] = {'raw_sha256': hashlib.sha256(raw_log).hexdigest(),
                                 'raw_bytes': len(raw_log), 'redacted_sha256': sha(log_path)}
    except BaseException as error:
        record['failure'] = failure(error, stage)
    finally:
        try:
            record['binary_sha256_after'] = sha(binary)
        except OSError as error:
            record['identity_failure'] = failure(error, 'postlaunch_hash')
        if probe is not None:
            record['probe_exit'] = probe.returncode
        try:
            if probe_path.exists():
                result = json.loads(probe_path.read_text())
                require(isinstance(result, dict) and isinstance(result.get('cases'), list)
                        and all(isinstance(row, dict) and isinstance(row.get('id'), str)
                                for row in result['cases']), 'malformed_probe_result')
                record['result'] = result
        except (OSError, ValueError, CheckFailure) as error:
            record['result_failure'] = failure(error, 'probe_result_read')


def independent_case_ok(row):
    return (row.get('status') == 'PASS' and isinstance(row.get('observed'), dict)
            and isinstance(row.get('expected'), dict) and row['observed'] == row['expected']
            and row.get('differences') == [] and row.get('join_owner_ok') is True
            and row.get('response', {}).get('type') == 'trackPublished'
            and row.get('response', {}).get('track_sid_present') is True
            and row.get('track_published_count') == 1 and row.get('ws_healthy') is True
            and row.get('ws_close_code') == 1000 and row.get('barrier', {}).get('reason') == 0
            and row.get('barrier', {}).get('owner_metadata_matches') is True)


def lifecycle_ok(record, expected_sha):
    return (not any(key in record for key in ('failure', 'identity_failure', 'result_failure', 'cleanup_errors'))
            and record.get('probe_exit') == 0
            and all(record.get(key) == expected_sha for key in ('binary_sha256_before', 'binary_sha256_after'))
            and all(record.get(key, {}).get('binary_sha256') == expected_sha
                    and record.get(key, {}).get('executable_matches') is True
                    and record.get(key, {}).get('owned_loopback_listener') is True
                    and record.get(key, {}).get('owned_loopback_udp') is True
                    for key in ('launch_before', 'launch_after'))
            and record.get('ports_rebound') == {'http': True, 'udp': True}
            and all(record.get(key) == {'exit': 0, 'forced_kill': False, 'process_group_gone': True}
                    for key in ('probe_cleanup', 'server_cleanup')))


def paired_cases(records):
    indices, valid = {}, {}
    for side in ('go', 'rust'):
        result = records.get(side, {}).get('result', {})
        rows = result.get('cases', [])
        ids = [row.get('id') for row in rows]
        valid[side] = (ids == CASE_IDS and len(set(ids)) == len(ids)
                       and result.get('case_count') == len(CASE_IDS) and result.get('side') == side
                       and result.get('status') == 'PASS_SCOPED' and result.get('cleanup_errors') == [])
        # A duplicate is an integrity failure, never silently overwrite a case.
        indices[side] = {row['id']: row for row in rows if ids.count(row.get('id')) == 1}
    cases = []
    for case_id in CASE_IDS:
        go, rust = (indices[side].get(case_id) for side in ('go', 'rust'))
        go_ok, rust_ok = (independent_case_ok(row) if row else False for row in (go, rust))
        equal = (go is not None and rust is not None and go.get('observed') is not None
                 and rust.get('observed') is not None and go['observed'] == rust['observed'])
        status = 'NOT_RUN' if go is None and rust is None else ('PASS' if go_ok and rust_ok and equal else 'FAIL')
        cases.append({'id': case_id, 'status': status, 'go_contract_ok': go_ok,
                      'rust_contract_ok': rust_ok, 'cross_side_equal': equal, 'go': go, 'rust': rust})
    return cases, valid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', required=True, type=Path)
    parser.add_argument('--rust', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path, help='must not already exist')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    manifest = {'status': 'FAIL', 'covers': ITEMS, 'items': ITEMS, 'scope': SCOPE,
                'case_count': len(CASE_IDS), 'binaries': {}, 'sides': {}, 'cases': []}
    previous_handlers = {sig: signal.signal(sig, lambda *_: CANCELLED.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    os.umask(0o077)
    stage = 'inputs'
    try:
        binaries = {side: getattr(args, side).resolve(strict=True) for side in ('go', 'rust')}
        for side, binary in binaries.items():
            require(binary.is_file() and os.access(binary, os.X_OK), 'executable_required')
            with binary.open('rb') as stream:
                require(stream.read(4) in (b'\x7fELF', b'\xcf\xfa\xed\xfe', b'\xfe\xed\xfa\xcf',
                                           b'\xca\xfe\xba\xbe', b'\xbe\xba\xfe\xca'), 'native_main_required')
            manifest['binaries'][side] = {'path': str(binary), 'sha256': sha(binary)}
        require(manifest['binaries']['go']['sha256'] == FIXED_GO_SHA, 'fixed_go_sha_mismatch')
        require(manifest['binaries']['rust']['sha256'] != FIXED_GO_SHA, 'rust_is_reference_binary')
        node, lsof = shutil.which('node'), shutil.which('lsof')
        require(node is not None and lsof is not None, 'node_and_lsof_required')
        inputs = [HERE / 'probe.mjs', HERE / 'run-paired.py', HERE.parent / 'package-lock.json']
        manifest['input_sha256'] = {path.name: sha(path) for path in inputs}
        for side in ('go', 'rust'):
            check_cancelled()
            stage = side
            record = manifest['sides'].setdefault(side, {})
            run_side(side, binaries[side], manifest['binaries'][side]['sha256'], output, node, lsof, record)
            write_json(output / 'execution.json', manifest)
        manifest['inputs_unchanged'] = all(sha(path) == manifest['input_sha256'][path.name] for path in inputs)
        manifest['binary_identities_stable'] = all(sha(binary) == manifest['binaries'][side]['sha256'] for side, binary in binaries.items())
    except BaseException as error:
        manifest['failure'] = failure(error, stage)
    finally:
        manifest['cases'], manifests_ok = paired_cases(manifest['sides'])
        manifest['side_manifests_ok'] = manifests_ok
        manifest['observed_case_count'] = {side: sum(case[side] is not None and case[side].get('observed') is not None
                                                  for case in manifest['cases']) for side in ('go', 'rust')}
        manifest['side_lifecycle_ok'] = {side: lifecycle_ok(manifest['sides'].get(side, {}),
            manifest['binaries'].get(side, {}).get('sha256')) for side in ('go', 'rust')}
        deps = [manifest['sides'].get(side, {}).get('result', {}).get('dependencies') for side in ('go', 'rust')]
        manifest['dependencies_equal'] = deps[0] is not None and deps[0] == deps[1]
        passed = (not CANCELLED.is_set() and 'failure' not in manifest
                  and manifest.get('inputs_unchanged') and manifest.get('binary_identities_stable')
                  and manifest['dependencies_equal'] and all(manifests_ok.values())
                  and all(manifest['side_lifecycle_ok'].values())
                  and all(case['status'] == 'PASS' for case in manifest['cases']))
        manifest['status'] = 'PASS_SCOPED' if passed else 'FAIL'
        manifest['failed_cases'] = [case['id'] for case in manifest['cases'] if case['status'] != 'PASS']
        write_json(output / 'execution.json', manifest)
        batch = {key: value for key, value in manifest.items() if key != 'sides'}
        batch['lifecycle'] = {side: {key: value for key, value in record.items() if key != 'result'}
                              for side, record in manifest['sides'].items()}
        write_json(output / 'BATCH.json', batch)
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
    print(json.dumps({'status': manifest['status'], 'case_count': len(CASE_IDS), 'failed_cases': manifest['failed_cases']}))
    return 0 if manifest['status'] == 'PASS_SCOPED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
