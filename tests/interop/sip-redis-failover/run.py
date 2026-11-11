#!/usr/bin/env python3
"""Fixed Go/Rust with the original SIP worker across owned Redis promotions."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import socket
import ssl
import subprocess
import tempfile
import time
from urllib.request import urlopen
from urllib.error import HTTPError

HOST = '127.0.0.1'
KEY = 'sip-topology-test-key'
SECRET = 'sip-topology-public-fixture-secret-00001'
PROBE = Path(__file__).resolve().parent.parent / 'sip-media' / 'run.mjs'


def sha(path):
    with path.open('rb') as reader:
        return hashlib.file_digest(reader, 'sha256').hexdigest()


def ports(count):
    sockets = []
    selected = []
    try:
        # Stay below the OS ephemeral range so the driver Redis clients do
        # not take the just-released ports before their owners bind.
        for port in random.sample(range(20000, 30000), count * 5):
            tcp = socket.socket()
            udp = socket.socket(type=socket.SOCK_DGRAM)
            try:
                tcp.bind(('', port))
                udp.bind(('', port))
            except OSError:
                tcp.close()
                udp.close()
                continue
            sockets.extend((tcp, udp))
            selected.append(port)
            if len(selected) == count:
                return selected
        raise RuntimeError('insufficient available fixture ports')
    finally:
        for sock in sockets:
            sock.close()


def released(port, kind=socket.SOCK_STREAM):
    with socket.socket(type=kind) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((HOST, port))
            return True
        except OSError:
            return False


def wait(check, timeout, label, process=None):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'{label}: process exited {process.returncode}')
        if value := check():
            return value
        time.sleep(.1)
    raise TimeoutError(label)


def redis_cli(port, *cmd, tls_options=()):
    result = subprocess.run(
        ['redis-cli', '--raw', '-h', HOST, '-p', str(port), *tls_options, *map(str, cmd)],
        capture_output=True, text=True, timeout=3,
    )
    return result.stdout.strip()


def generate_certificates(stage):
    """Private P-256 CA and distinct service and client identities for this case."""
    def openssl(*args):
        subprocess.run(['openssl', *args], cwd=stage, capture_output=True, check=True, timeout=20)

    key_args = ('-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes')
    openssl('req', '-x509', *key_args, '-keyout', 'ca.key',
            '-out', 'ca.crt', '-days', '2', '-subj', '/CN=Owned SIP Redis CA',
            '-addext', 'basicConstraints=critical,CA:TRUE',
            '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
            '-addext', 'subjectKeyIdentifier=hash')
    for name, subject, extensions in [
        ('server', 'redis.fixture.test', 'subjectAltName=DNS:redis.fixture.test\nextendedKeyUsage=serverAuth,clientAuth\n'),
        ('client', 'owned-sip-redis-client', 'extendedKeyUsage=clientAuth\n'),
    ]:
        openssl('req', '-new', *key_args, '-keyout', f'{name}.key',
                '-out', f'{name}.csr', '-subj', f'/CN={subject}')
        (stage / f'{name}.ext').write_text(
            'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n'
            'subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n' + extensions
        )
        openssl('x509', '-req', '-in', f'{name}.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key',
                '-CAcreateserial', '-out', f'{name}.crt', '-days', '2', '-extfile', f'{name}.ext')
    for key in stage.glob('*.key'):
        key.chmod(0o600)


def verify_tls_fixture(port, stage):
    def ping(name, certificate):
        context = ssl.create_default_context(cafile=stage / 'ca.crt')
        if certificate:
            context.load_cert_chain(stage / 'client.crt', stage / 'client.key')
        with socket.create_connection((HOST, port), timeout=3) as connection:
            with context.wrap_socket(connection, server_hostname=name) as peer:
                peer.sendall(b'*1\r\n$4\r\nPING\r\n')
                return peer.recv(32), peer.version()

    response, version = ping('redis.fixture.test', True)
    if response != b'+PONG\r\n':
        raise RuntimeError('verified TLS positive control did not reach Redis')
    controls = dict(verified_ping=True, negotiated_version=version)
    for label, name, certificate in [
        ('wrong_name', 'wrong.fixture.test', True),
        ('missing_client_certificate', 'redis.fixture.test', False),
    ]:
        try:
            ping(name, certificate)
        except ssl.SSLError as error:
            controls[label] = str(error)
        else:
            raise RuntimeError(f'TLS negative control unexpectedly succeeded: {label}')
    return controls


def run(binary, worker, topology, out, tls=False, hard_stop=False, worker_cpu_ceiling=None,
        slot_profile=None, settle_seconds=0):
    if slot_profile:
        from hard_stop_trace import cleanup_readback, redis_slot, slot_owners, trace_summary, transfer_stages
    out.mkdir()
    evidence = dict(topology=topology, binaries={p.name: sha(p) for p in (binary, worker)},
                    driver_sha256=sha(Path(__file__).resolve()),
                    probe_sha256=sha(PROBE), lock_sha256=sha(PROBE.parent.parent / 'package-lock.json'),
                    redis_transport='verified-mtls' if tls else 'plaintext',
                    cluster_fault='hard-primary-stop' if hard_stop else 'promotion-then-close-pubsub',
                    worker_cpu_ceiling=worker_cpu_ceiling, slot_profile=slot_profile,
                    settle_seconds=settle_seconds,
                    transitions=[], processes={}, status='FAIL')
    processes = {}
    monitors = []
    killed_label = None
    key_owner = {}

    def start(label, cmd, env=None):
        with (out / f'{label}.log').open('wb') as log:
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
        processes[label] = proc
        evidence['processes'][label] = {'pid': proc.pid}
        return proc

    def stop(label):
        proc = processes[label]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        evidence['processes'][label]['exit'] = proc.returncode
        return proc.returncode

    with tempfile.TemporaryDirectory(prefix='sip-redis-failover-') as tmp:
        stage = Path(tmp)
        tcp_ports, udp_ports = [], []
        try:
            tls_options = []
            redis_tls_config = ''
            tls_args = []
            if tls:
                generate_certificates(stage)
                tls_options = ['--tls', '--sni', 'redis.fixture.test', '--cacert', str(stage / 'ca.crt'),
                               '--cert', str(stage / 'client.crt'), '--key', str(stage / 'client.key')]
                redis_tls_config = (
                    '  tls:\n    enabled: true\n    server_name: redis.fixture.test\n'
                    f'    ca_cert_file: {stage / "ca.crt"}\n'
                    f'    client_cert_file: {stage / "client.crt"}\n'
                    f'    client_key_file: {stage / "client.key"}\n'
                )
                tls_args = ['--tls-cert-file', str(stage / 'server.crt'), '--tls-key-file', str(stage / 'server.key'),
                            '--tls-ca-cert-file', str(stage / 'ca.crt'), '--tls-auth-clients', 'yes',
                            '--tls-replication', 'yes']
                evidence['redis_tls'] = dict(server_name='redis.fixture.test', verification_configured=True,
                                             client_authentication_required=True, plaintext_port=0,
                                             certificates={name: sha(stage / name) for name in (
                                                 'ca.crt', 'server.crt', 'client.crt')})

            def redis(port, *cmd):
                return redis_cli(port, *cmd, tls_options=tls_options)

            probe_room = ''
            selected = ports(17 if topology == 'cluster' else 8)
            http, rtc_tcp, rtc_udp, sip_port, health = selected[:5]
            master_ports = selected[5:11] if topology == 'cluster' else selected[5:7]
            broker_labels = {}
            cluster_bus_ports = selected[11:] if topology == 'cluster' else []
            tcp_ports = [http, rtc_tcp, sip_port, health, *master_ports, *cluster_bus_ports]
            udp_ports = [rtc_udp, sip_port, *range(32768, 32868)]
            for index, port in enumerate(master_ports):
                label = f'redis-{index}'
                broker_labels[port] = label
                work = stage / label
                work.mkdir()
                port_args = ['--port', '0', '--tls-port', str(port), *tls_args] if tls else ['--port', str(port)]
                cmd = ['redis-server', '--bind', HOST, *port_args, '--save', '',
                       '--appendonly', 'no', '--dir', str(work)]
                if topology == 'cluster':
                    cmd += ['--cluster-enabled', 'yes', '--cluster-config-file', 'nodes.conf',
                            '--cluster-port', str(cluster_bus_ports[index]), '--cluster-node-timeout', '1000']
                    if tls:
                        cmd += ['--tls-cluster', 'yes', '--cluster-announce-tls-port', str(port)]
                elif index == 1:
                    cmd += ['--replicaof', HOST, str(master_ports[0])]
                proc = start(label, cmd)
                wait(lambda: redis(port, 'PING') == 'PONG', 5, f'{label} ready', proc)
            if topology == 'cluster':
                cluster = subprocess.run(
                    ['redis-cli', *tls_options, '--cluster', 'create',
                     *[f'{HOST}:{p}' for p in master_ports], '--cluster-replicas', '1', '--cluster-yes'],
                    capture_output=True, text=True, timeout=25,
                )
                (out / 'cluster-create.log').write_text(cluster.stdout + cluster.stderr)
                if cluster.returncode != 0 or '[OK] All 16384 slots covered' not in cluster.stdout:
                    raise RuntimeError('owned cluster allocation failed')
                wait(lambda: all('cluster_state:ok' in redis(p, 'CLUSTER', 'INFO') for p in master_ports),
                     10, 'all cluster nodes ready')
                if slot_profile:
                    key_owner = slot_owners(redis, master_ports[0])
                    if len(key_owner) != 16384:
                        raise RuntimeError('initial slot coverage incomplete')
                    rooms_owner = key_owner[redis_slot(b'rooms')]
                    # Force the catalog and participant keys onto different
                    # primaries, regardless of fixture addresses or call IDs.
                    room_prefix = f'f07-sip-{os.getpid()}'
                    for suffix in range(100):
                        candidate = f'{room_prefix}-{suffix}'
                        slot = redis_slot(f'room_participants:{candidate}'.encode())
                        if key_owner[slot] != rooms_owner:
                            probe_room = candidate
                            break
                    if not probe_room:
                        raise RuntimeError('could not place room and participant keys on separate primaries')
                redis_cfg = 'redis:\n  cluster_addresses: [' + ', '.join(f'"{HOST}:{p}"' for p in master_ports) + ']\n'
            else:
                sentinel = selected[7]
                tcp_ports.append(sentinel)
                config = stage / 'sentinel.conf'
                sentinel_tls = (
                    f'port 0\ntls-port {sentinel}\n'
                    + ''.join(f'{name[2:]} {value}\n' for name, value in zip(tls_args[::2], tls_args[1::2]))
                ) if tls else f'port {sentinel}\n'
                config.write_text(
                    f'bind {HOST}\n{sentinel_tls}dir {stage}\n'
                    f'sentinel monitor worker-test {HOST} {master_ports[0]} 1\n'
                    'sentinel down-after-milliseconds worker-test 1000\n'
                    'sentinel failover-timeout worker-test 8000\n'
                )
                proc = start('sentinel', ['redis-server', str(config), '--sentinel'])
                wait(lambda: redis(sentinel, 'PING') == 'PONG', 5, 'sentinel ready', proc)
                wait(lambda: 'master_link_status:up' in redis(master_ports[1], 'INFO', 'replication'),
                     10, 'redis replica synchronized')
                redis_cfg = (
                    f'redis:\n  sentinel_master_name: worker-test\n'
                    f'  sentinel_addresses: ["{HOST}:{sentinel}"]\n'
                )
            redis_cfg += redis_tls_config
            if tls:
                control_ports = [master_ports[0], sentinel] if topology == 'sentinel' else [master_ports[0]]
                evidence['redis_tls']['controls'] = {
                    port: verify_tls_fixture(port, stage) for port in control_ports
                }

            server_config = stage / 'server.yaml'
            server_config.write_text(
                f'port: {http}\nbind_addresses: ["{HOST}"]\n'
                f'rtc:\n  node_ip: {HOST}\n  use_external_ip: false\n'
                f'  tcp_port: {rtc_tcp}\n  udp_port: {rtc_udp}\n'
                f'  enable_loopback_candidate: true\n  ips:\n    includes: [{HOST}/32]\n'
                f'keys:\n  {KEY}: {SECRET}\n' + redis_cfg
            )
            worker_config = stage / 'worker.yaml'
            worker_capacity = f'max_cpu_utilization: {worker_cpu_ceiling}\n' if worker_cpu_ceiling is not None else ''
            worker_config.write_text(
                f'api_key: {KEY}\napi_secret: {SECRET}\nws_url: ws://{HOST}:{http}\n'
                f'listen_ip: {HOST}\nsip_hostname: {HOST}\nsip_port: {sip_port}\n'
                f'sip_port_listen: {sip_port}\nuse_external_ip: false\nnat_1_to_1_ip: {HOST}\n'
                f'media_use_external_ip: false\nmedia_nat_1_to_1_ip: {HOST}\n'
                f'rtp_port: 32768-32867\nhealth_port: {health}\n' + worker_capacity + redis_cfg
            )
            env = {key: value for key, value in os.environ.items() if not key.startswith(('LIVEKIT_', 'SIP_'))}
            main = start('server', [str(binary), '--config', str(server_config)], env)

            def listening(port, *, worker=False):
                try:
                    with urlopen(f'http://{HOST}:{port}/', timeout=1) as response:
                        return response.status == 200
                except HTTPError as error:
                    # An original worker already listening may report load
                    # while the browser starts. The admission gate runs again
                    # immediately before CreateSIPParticipant.
                    return worker and error.code == 429
                except OSError:
                    return False

            wait(lambda: listening(http), 45, 'LiveKit ready', main)
            sip = start('worker', [str(worker)], dict(env, SIP_CONFIG_FILE=str(worker_config)))
            wait(lambda: listening(health, worker=True), 20, 'SIP worker health listening', sip)
            probe = start('probe', ['node', str(PROBE)], dict(
                env, LIVEKIT_URL=f'ws://{HOST}:{http}', LIVEKIT_API_KEY=KEY,
                LIVEKIT_API_SECRET=SECRET, SIP_MEDIA_OUTPUT=str(out / 'probe.json'),
                SIP_MEDIA_TOPOLOGY_BARRIER=str(stage), SIP_MEDIA_WORKER_HEALTH_URL=f'http://{HOST}:{health}/',
                SIP_MEDIA_ROOM_NAME=probe_room, SIP_MEDIA_SETTLE_SECONDS=str(settle_seconds),
            ))
            ready = stage / 'call-ready.json'
            wait(ready.exists, 80, 'real worker published media and reached transfer barrier', probe)
            call = json.loads(ready.read_text())
            call_id = call['callId']
            if slot_profile:
                for port in master_ports:
                    path = stage / f'monitor-{port}.raw'
                    with path.open('wb') as output:
                        monitor = subprocess.Popen(
                            ['redis-cli', '--raw', '-h', HOST, '-p', str(port), *tls_options, 'MONITOR'],
                            stdout=output, stderr=subprocess.DEVNULL)
                    monitors.append(monitor)
                    wait(lambda: path.read_bytes().startswith(b'OK'), 5, 'Redis MONITOR ready', monitor)

            def transfer_channels(port):
                return redis(port, 'PUBSUB', 'CHANNELS', f'*{call_id}*').splitlines()

            subscribed = [(port, transfer_channels(port)) for port in master_ports]
            subscribed = [(p, topics) for p, topics in subscribed if topics]
            if len(subscribed) != 1:
                raise RuntimeError(f'expected one node hosting the per-call worker subscription: {subscribed}')
            old, channels = subscribed[0]
            if topology == 'cluster':
                members = [line.split() for line in redis(old, 'CLUSTER', 'NODES').splitlines()]
                subscriber = next(row for row in members if 'myself' in row[2].split(','))
                if 'master' in subscriber[2].split(','):
                    owner = subscriber
                    replica = next(row for row in members if row[3] == owner[0])
                else:
                    replica = subscriber
                    owner = next(row for row in members if row[0] == replica[3])

                if slot_profile:
                    target_key = 'rooms' if slot_profile == 'room' else f'room_participants:{call["room"]}'
                    target_slot = redis_slot(target_key.encode())
                    target_port = key_owner[target_slot]
                    target_field = call['room'] if slot_profile == 'room' else call['participant']
                    if redis(target_port, 'HEXISTS', target_key, target_field) != '1':
                        raise RuntimeError('targeted live catalog field missing before fault')
                    owner = next(row for row in members if int(row[1].split('@')[0].rsplit(':', 1)[1]) == target_port)
                    replica = next(row for row in members if row[3] == owner[0])
                    evidence['fault_slot'] = dict(profile=slot_profile, slot=target_slot, old_primary=target_port)
                    evidence['catalog_slots'] = {
                        bucket: {'slot': slot, 'old_primary': key_owner[slot]}
                        for bucket, slot in [('rooms', redis_slot(b'rooms')),
                                             ('room_participants', redis_slot(f'room_participants:{call["room"]}'.encode()))]
                    }

                def advertised_port(node):
                    return int(node[1].split('@')[0].rsplit(':', 1)[1])

                previous_primary = advertised_port(owner)
                promoted = advertised_port(replica)
                if hard_stop:
                    wait(lambda: 'master_link_status:up' in redis(promoted, 'INFO', 'replication'),
                         10, 'cluster replica synchronized', probe)
                    if slot_profile:
                        killed_label = broker_labels[previous_primary]
                        processes[killed_label].kill()
                    expected_exit = -9 if slot_profile else 0
                    if stop(broker_labels[previous_primary]) != expected_exit or not released(previous_primary):
                        raise RuntimeError('old cluster primary did not exit and release its listener')
                    surviving_ports = [port for port in master_ports if port != previous_primary]
                    evidence['hard_stop_unix'] = time.time()
                    fault = dict(previous_primary=previous_primary, stopped_primary=True,
                                 stopped_before_election=True, old_primary_listener_released=True)
                    wait(lambda: redis(promoted, 'ROLE').startswith('master') and all(
                        'cluster_state:ok' in redis(p, 'CLUSTER', 'INFO')
                        for p in surviving_ports), 14, 'automatic cluster election and surviving coverage', probe)
                else:
                    if redis(promoted, 'CLUSTER', 'FAILOVER') != 'OK':
                        raise RuntimeError('owned Redis Cluster failover command failed')
                    wait(lambda: redis(promoted, 'ROLE').startswith('master'), 10,
                         'cluster replica promotion', probe)
                    wait(lambda: redis(previous_primary, 'ROLE').startswith('slave') and all(
                        'cluster_state:ok' in redis(p, 'CLUSTER', 'INFO')
                        for p in master_ports), 10, 'old primary demoted and all cluster nodes ready', probe)
                    surviving_ports = master_ports
                    fault = dict(previous_primary=previous_primary)
                # If Pub/Sub selected a surviving replica, primary loss alone
                # does not force that separate socket to reconnect. Explicitly
                # close it too, and record which mechanism caused recovery.
                killed = int(redis(old, 'CLIENT', 'KILL', 'TYPE', 'PUBSUB')) if old in surviving_ports else 0
                if old in surviving_ports and killed < 1:
                    raise RuntimeError('no existing subscription connection was killed')
                fault['closed_surviving_pubsub_sockets'] = killed
            else:
                if old != master_ports[0] or redis(sentinel, 'SENTINEL', 'FAILOVER', 'worker-test') != 'OK':
                    raise RuntimeError('owned Redis Sentinel failover command failed')
                promoted = master_ports[1]
                wait(lambda: redis(sentinel, 'SENTINEL', 'get-master-addr-by-name', 'worker-test').splitlines()
                     == [HOST, str(promoted)], 12, 'sentinel promoted replica', probe)
                if stop(broker_labels[old]) != 0:
                    raise RuntimeError('former primary failed orderly shutdown')
                surviving_ports = [promoted]
                fault = dict(previous_primary=old, stopped_primary=True)
            evidence['transitions'].append(dict(
                previous_subscription_node=old, promoted=promoted, prior_channels=channels, **fault
            ))
            wait(lambda: any(set(transfer_channels(p)) >= set(channels) for p in surviving_ports),
                 12, 'same original SIP worker per-call subscriptions restored', probe)
            evidence['transitions'][0]['restored_ports'] = [
                p for p in surviving_ports if set(transfer_channels(p)) >= set(channels)
            ]
            if slot_profile:
                target_slot = evidence['fault_slot']['slot']
                wait(lambda: all(slot_owners(redis, port).get(target_slot) == promoted
                                 for port in surviving_ports), 10, 'survivors agree on promoted slot owner', probe)
                if redis(promoted, 'HEXISTS', target_key, target_field) != '1':
                    raise RuntimeError('promoted owner lost targeted live catalog field')
                evidence['fault_slot']['live_field_survived_promotion'] = True
            evidence['probe_released_unix'] = time.time()
            (stage / 'promoted').touch()
            if probe.wait(timeout=50 + settle_seconds) != 0 or json.loads((out / 'probe.json').read_text())['status'] != 'PASS':
                raise RuntimeError('real post-promotion transfer and media probe failed')
            if sip.poll() is not None or main.poll() is not None:
                raise RuntimeError('original worker and media server must stay alive throughout fault')
            evidence['status'] = 'PASS'
        except (OSError, RuntimeError, ValueError, StopIteration, subprocess.SubprocessError) as error:
            evidence['failure'] = f'{type(error).__name__}: {error}'
        finally:
            for monitor in monitors:
                if monitor.poll() is None:
                    monitor.terminate()
                    try:
                        monitor.wait(timeout=4)
                    except subprocess.TimeoutExpired:
                        monitor.kill()
                        monitor.wait(timeout=4)
            if slot_profile and evidence['transitions'] and (out / 'probe.json').is_file():
                try:
                    result = json.loads((out / 'probe.json').read_text())
                    details = result['fixtureResources']
                    rows = trace_summary(stage, call_id, key_owner, details)
                    (out / 'redis-wire.json').write_text(json.dumps(rows, indent=2) + '\n')
                    owners = slot_owners(redis, surviving_ports[0])
                    readback = cleanup_readback(redis, owners, details)
                    transition = evidence['transitions'][0]
                    stages = transfer_stages(rows, result, transition, readback)
                    evidence['post_cleanup_redis'] = readback
                    evidence['transfer_stages'] = stages
                    evidence['old_primary_still_down'] = released(transition['previous_primary'])
                    if not (stages['healthy_transfer'] and stages['stores_empty']
                            and evidence['old_primary_still_down']):
                        evidence['status'] = 'FAIL'
                except (KeyError, ValueError, OSError, subprocess.SubprocessError) as error:
                    evidence['trace_failure'] = f'{type(error).__name__}: {error}'
                    evidence['status'] = 'FAIL'
            for label in reversed(processes):
                if stop(label) != (-9 if label == killed_label else 0):
                    evidence['status'] = 'FAIL'
            evidence['ports_released'] = all(released(p) for p in tcp_ports) and all(
                released(p, socket.SOCK_DGRAM) for p in udp_ports
            )
            if not evidence['ports_released']:
                evidence['status'] = 'FAIL'
            (out / 'execution.json').write_text(json.dumps(evidence, indent=2) + '\n')
    return evidence['status'] == 'PASS'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ['go', 'rust', 'worker', 'output']:
        parser.add_argument('--' + arg, type=Path, required=True)
    parser.add_argument('--only', choices=['go', 'rust'])
    parser.add_argument('--topology', choices=['sentinel', 'cluster'])
    parser.add_argument('--tls', action='store_true', help='require verified TLS with a Redis client certificate')
    parser.add_argument('--hard-stop', action='store_true', help='stop the old Cluster primary before automatic election')
    parser.add_argument('--slot-profile', choices=['participant', 'room'],
                        help='hard-stop the exact participant or room catalog slot owner and trace claim/cleanup')
    parser.add_argument('--settle-seconds', type=int, default=0,
                        help='hold the same real call after promotion before submitting the SDK transfer')
    parser.add_argument('--worker-cpu-ceiling', type=float, help='explicit fixture CPU admission ceiling for the original worker')
    args = parser.parse_args()
    if args.hard_stop and args.topology != 'cluster':
        parser.error('--hard-stop requires --topology cluster')
    if args.slot_profile and not (args.hard_stop and args.tls):
        parser.error('--slot-profile requires --hard-stop and --tls')
    if not 0 <= args.settle_seconds <= 90:
        parser.error('--settle-seconds must be between 0 and 90')
    if args.worker_cpu_ceiling is not None and not 0 < args.worker_cpu_ceiling <= 1:
        parser.error('--worker-cpu-ceiling must be greater than 0 and at most 1')
    args.output.mkdir(parents=True)
    results = []
    for topology in ([args.topology] if args.topology else ['sentinel', 'cluster']):
        for label in ([args.only] if args.only else ['go', 'rust']):
            name = f'{topology}-{label}'
            passed = run(getattr(args, label).resolve(strict=True), args.worker.resolve(strict=True),
                         topology, args.output.resolve() / name, tls=args.tls, hard_stop=args.hard_stop,
                         worker_cpu_ceiling=args.worker_cpu_ceiling,
                         slot_profile=args.slot_profile, settle_seconds=args.settle_seconds)
            print(f'{name}: {"PASS" if passed else "FAIL"}', flush=True)
            results.append(passed)
    return 0 if all(results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
