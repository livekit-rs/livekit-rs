#!/usr/bin/env python3
"""Run an actual pinned Linux Egress child through Redis promotion and stop it via SDK."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
PROBE = HERE / 'run-track-file.mjs'
INTEROP = HERE.parent
SIBLING = INTEROP / 'sip-redis-failover/run.py'
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('redis_fixture_helpers', SIBLING)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
HOST = helpers.HOST
ports, redis, released, sha, wait = (helpers.ports, helpers.redis_cli, helpers.released, helpers.sha, helpers.wait)
KEY = 'egress-topology-test-key'
SECRET = 'egress-topology-public-fixture-secret-00001'


def healthy(port):
    try:
        with urlopen(f'http://{HOST}:{port}/', timeout=1) as response:
            return response.status == 200
    except OSError:
        return False


def node_port(member):
    return int(member[1].split('@')[0].rsplit(':', 1)[1])


def run(binary, worker, topology, out, agent_sdk, sdk_project, hard_stop):
    out.mkdir(mode=0o700)
    receipt = {
        'status': 'FAIL', 'topology': topology, 'hard_stop_requested': hard_stop,
        'kernel': platform.release(),
        'inputs': {
            'server_sha256': sha(binary), 'worker_sha256': sha(worker),
            'driver_sha256': sha(Path(__file__)), 'probe_sha256': sha(PROBE),
            'redis_helpers_sha256': sha(SIBLING),
            'server_sdk_lock_sha256': sha(sdk_project / 'package-lock.json'),
            'agent_sdk_lock_sha256': sha(agent_sdk / 'package-lock.json'),
            'native_sdk_version': json.loads(
                (agent_sdk / 'node_modules/@livekit/rtc-node/package.json').read_text()
            )['version'],
        },
        'processes': {},
    }
    processes = {}
    tracked_child = None

    def start(label, cmd, env=None):
        with (out / f'{label}.log').open('wb') as log:
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
        processes[label] = proc
        receipt['processes'][label] = {'pid': proc.pid}
        return proc

    def stop(label):
        proc = processes[label]
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT if label == 'server' else signal.SIGTERM)
            try:
                proc.wait(timeout=25 if label == 'worker' else 10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
                receipt['processes'][label]['forced'] = True
        receipt['processes'][label]['exit'] = proc.returncode
        return proc.returncode

    with tempfile.TemporaryDirectory(prefix='egress-redis-failover-') as tmp:
        stage = Path(tmp)
        tcp_ports, udp_ports = [], []
        try:
            selected = ports(17 if topology == 'cluster' else 8)
            http, rtc_tcp, rtc_udp, health, template = selected[:5]
            broker_ports = selected[5:11] if topology == 'cluster' else selected[5:7]
            labels = {}
            bus_ports = selected[11:] if topology == 'cluster' else []
            tcp_ports = [http, rtc_tcp, health, template, *broker_ports, *bus_ports]
            udp_ports = [rtc_udp]
            for index, port in enumerate(broker_ports):
                label = f'redis-{index}'
                labels[port] = label
                work = stage / label
                work.mkdir()
                command = ['redis-server', '--bind', HOST, '--port', str(port),
                           '--save', '', '--appendonly', 'no', '--dir', str(work)]
                if topology == 'cluster':
                    command += ['--cluster-enabled', 'yes', '--cluster-config-file', 'nodes.conf',
                                '--cluster-port', str(bus_ports[index]), '--cluster-node-timeout', '1000']
                elif index == 1:
                    command += ['--replicaof', HOST, str(broker_ports[0])]
                broker = start(label, command)
                wait(lambda: redis(port, 'PING') == 'PONG', 8, f'{label} ready', broker)

            if topology == 'cluster':
                cluster = subprocess.run(
                    ['redis-cli', '--cluster', 'create', *[f'{HOST}:{p}' for p in broker_ports],
                     '--cluster-replicas', '1', '--cluster-yes'],
                    capture_output=True, text=True, timeout=25,
                )
                (out / 'cluster-create.log').write_text(cluster.stdout + cluster.stderr)
                assert cluster.returncode == 0 and '[OK] All 16384 slots covered' in cluster.stdout
                wait(lambda: all('cluster_state:ok' in redis(p, 'CLUSTER', 'INFO') for p in broker_ports),
                     12, 'all cluster nodes ready')
                redis_cfg = {'cluster_addresses': [f'{HOST}:{p}' for p in broker_ports]}
            else:
                sentinel = selected[7]
                tcp_ports.append(sentinel)
                conf = stage / 'sentinel.conf'
                conf.write_text(
                    f'bind {HOST}\nport {sentinel}\ndir {stage}\n'
                    f'sentinel monitor egress-test {HOST} {broker_ports[0]} 1\n'
                    'sentinel down-after-milliseconds egress-test 1000\n'
                    'sentinel failover-timeout egress-test 8000\n'
                )
                sentinel_proc = start('sentinel', ['redis-server', str(conf), '--sentinel'])
                wait(lambda: redis(sentinel, 'PING') == 'PONG', 5, 'sentinel ready', sentinel_proc)
                wait(lambda: 'master_link_status:up' in redis(broker_ports[1], 'INFO', 'replication'),
                     12, 'redis replica synchronized')
                redis_cfg = {'sentinel_master_name': 'egress-test',
                             'sentinel_addresses': [f'{HOST}:{sentinel}']}

            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(('LIVEKIT_', 'EGRESS_', 'REDIS_'))}
            env['PATH'] = str(worker.parent) + os.pathsep + env['PATH']
            conf = stage / 'server.yaml'
            conf.write_text(json.dumps({
                'port': http, 'bind_addresses': [HOST], 'keys': {KEY: SECRET}, 'redis': redis_cfg,
                'rtc': {'tcp_port': rtc_tcp, 'udp_port': rtc_udp, 'node_ip': HOST,
                        'use_external_ip': False, 'enable_loopback_candidate': True,
                        'ips': {'includes': [f'{HOST}/32']}},
            }))
            main = start('server', [str(binary), '--config', str(conf)], env)
            wait(lambda: healthy(http), 20, 'LiveKit ready', main)
            conf = stage / 'worker.yaml'
            conf.write_text(json.dumps({
                'api_key': KEY, 'api_secret': SECRET, 'ws_url': f'ws://{HOST}:{http}',
                'redis': redis_cfg, 'health_port': health, 'template_port': template,
                'logging': {'level': 'info', 'json': True},
                'session_limits': {'file_output_max_duration': '1m'},
            }))
            original = start('worker', [str(worker), '--config', str(conf)], env)
            wait(lambda: healthy(health), 20, 'original Egress health', original)
            probe = start('probe', ['node', str(PROBE)], dict(
                env, LIVEKIT_URL=f'http://{HOST}:{http}', LIVEKIT_API_KEY=KEY, LIVEKIT_API_SECRET=SECRET,
                INTEROP_PROJECT=str(sdk_project), AGENT_SDK_ROOT=str(agent_sdk),
                EGRESS_WORKER_PID=str(original.pid), EGRESS_OUTPUT_FILE=str(out / 'recording.ogg'),
                EGRESS_MEDIA_REPORT=str(out / 'media.json'), EGRESS_TOPOLOGY_BARRIER=str(stage),
            ))
            ready = stage / 'recording-ready.json'
            wait(ready.exists, 60, 'actual ACTIVE Egress with a native child', probe)
            recording = json.loads(ready.read_text())
            egress_id, tracked_child = recording['egressId'], recording['child']

            def channels(port):
                return set(redis(port, 'PUBSUB', 'CHANNELS', f'*|{egress_id}|*').splitlines())

            active = [(port, channels(port)) for port in broker_ports]
            [(old_subscription, prior)] = [(port, topics) for port, topics in active if topics]
            stop_topics = {topic.rsplit('|', 1)[1] for topic in prior if '|StopEgress|' in topic}
            assert stop_topics == {'REQ', 'RCLAIM'}, prior
            assert any('|UpdateStream|' in topic for topic in prior), prior
            fault = receipt['fault'] = {
                'recording': recording, 'old_subscription_node': old_subscription,
                'pre_fault_topics': sorted(prior), 'old_primary_stopped': False,
                'started_at_unix_ms': time.time_ns() // 1_000_000,
            }
            if topology == 'cluster':
                members = [line.split() for line in redis(old_subscription, 'CLUSTER', 'NODES').splitlines()]
                subscriber = next(row for row in members if 'myself' in row[2].split(','))
                if 'master' in subscriber[2].split(','):
                    owner = subscriber
                    replica = next(row for row in members if row[3] == owner[0])
                else:
                    replica = subscriber
                    owner = next(row for row in members if row[0] == replica[3])
                old_primary, promoted = node_port(owner), node_port(replica)
                assert redis(promoted, 'CLUSTER', 'FAILOVER') == 'OK'
                wait(lambda: redis(promoted, 'ROLE').startswith('master'), 12, 'Cluster promotion', probe)
                wait(lambda: redis(old_primary, 'ROLE').startswith('slave') and all(
                    'cluster_state:ok' in redis(p, 'CLUSTER', 'INFO') for p in broker_ports
                ), 10, 'former primary demoted and Cluster ready', probe)
                surviving = broker_ports
                if hard_stop:
                    assert stop(labels[old_primary]) == 0
                    assert released(old_primary), 'former primary is still listening'
                    surviving = [p for p in broker_ports if p != old_primary]
                    fault['old_primary_stopped'] = True
                if old_subscription in surviving:
                    killed = int(redis(old_subscription, 'CLIENT', 'KILL', 'TYPE', 'PUBSUB'))
                    assert killed >= 1, 'existing Egress Pub/Sub connection was not closed'
                    fault['closed_pubsub_sockets'] = killed
            else:
                old_primary, promoted = broker_ports
                assert old_subscription == old_primary, active
                assert redis(sentinel, 'SENTINEL', 'FAILOVER', 'egress-test') == 'OK'
                wait(lambda: redis(sentinel, 'SENTINEL', 'get-master-addr-by-name', 'egress-test').splitlines()
                     == [HOST, str(promoted)], 12, 'Sentinel promotion', probe)
                assert stop(labels[old_primary]) == 0
                assert released(old_primary), 'former primary is still listening'
                fault['old_primary_stopped'] = True
                surviving = [promoted]

            fault.update(old_primary=old_primary, promoted=promoted)
            restored = wait(lambda: [p for p in surviving if channels(p) >= prior], 15,
                            'original Egress StopEgress and UpdateStream subscriptions restored', probe)
            fault['restored_ports'] = restored
            fault['subscriptions_restored_at_unix_ms'] = time.time_ns() // 1_000_000
            (stage / 'promoted').touch()
            assert probe.wait(timeout=75) == 0, 'actual Egress media/stop probe failed'
            media = json.loads((out / 'media.json').read_text())
            assert media['status'] == 'PASS'
            assert media['topology']['child_before_stop'] == tracked_child
            assert all(proc.poll() is None for proc in (main, original))
            receipt['status'] = 'PASS'
        except (OSError, RuntimeError, AssertionError, StopIteration, ValueError, subprocess.SubprocessError) as error:
            receipt['failure'] = f'{type(error).__name__}: {error}'
        finally:
            for label in reversed(processes):
                if stop(label) != 0:
                    receipt['status'] = 'FAIL'
            receipt['ports_released'] = all(released(p) for p in tcp_ports) and all(
                released(p, helpers.socket.SOCK_DGRAM) for p in udp_ports
            )
            receipt['handler_released'] = tracked_child is not None and not Path(f'/proc/{tracked_child}').exists()
            if not (receipt['ports_released'] and receipt['handler_released']):
                receipt['status'] = 'FAIL'
            (out / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt['status'] == 'PASS'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('go', 'rust', 'worker', 'output', 'agent-sdk', 'sdk-project'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--only', choices=('go', 'rust'))
    parser.add_argument('--topology', choices=('sentinel', 'cluster'))
    parser.add_argument('--cluster-hard-stop', action='store_true')
    args = parser.parse_args()
    assert platform.system() == 'Linux' and platform.machine() == 'aarch64'
    assert os.getuid() != 0 and Path('/etc/cloud/cloud.cfg').is_file()
    out = args.output.resolve()
    assert str(out).startswith('/home/fixture/fixture/output/')
    worker = args.worker.resolve(strict=True)
    assert worker.name == 'egress', 'native child lookup requires original binary basename'
    out.mkdir(mode=0o700, parents=True)
    results = []
    for topology in ([args.topology] if args.topology else ('sentinel', 'cluster')):
        for kind in ([args.only] if args.only else ('go', 'rust')):
            case = f'{topology}-{kind}'
            passed = run(getattr(args, kind).resolve(strict=True), worker, topology, out / case,
                         args.agent_sdk.resolve(strict=True), args.sdk_project.resolve(strict=True),
                         args.cluster_hard_stop)
            print(f'{case}: {"PASS" if passed else "FAIL"}', flush=True)
            results.append(passed)
    return 0 if all(results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
