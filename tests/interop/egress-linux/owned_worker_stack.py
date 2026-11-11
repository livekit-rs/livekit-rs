#!/usr/bin/env python3
"""Actual Linux-only original Ingress/SIP worker orchestration, no emulation."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import signal
import shutil
import socket
import subprocess
import time
import urllib.request

BASE = Path(__file__).resolve().parent / 'run-owned-stack.py'
spec = importlib.util.spec_from_file_location('owned_egress_process_helpers', BASE)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
identity, free, OwnedProcessTree = helpers.identity, helpers.free, helpers.OwnedProcessTree


def main(mode):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source-root', 'go', 'rust', 'worker', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    if mode == 'ingress':
        parser.add_argument('--profile', choices=('all', 'url', 'rtmp'), default='all')
    parser.add_argument('--trace-ioinfo',action='store_true',help='SIP only: require actual original worker→backend IOInfo OTLP chain as well as unchanged media/Digest/REFER gates')
    parser.add_argument('--trace-config-mode',choices=['yaml','cli','env'],default='yaml')
    args = parser.parse_args()
    assert not args.trace_ioinfo or mode=='sip', 'trace profile belongs to the actual SIP worker constructor'

    assert platform.system() == 'Linux' and platform.machine() == 'aarch64'
    assert os.getuid() != 0 and Path('/etc/cloud/cloud.cfg').is_file()
    root = args.source_root.resolve(strict=True)
    for name in ('go', 'rust', 'worker'):
        setattr(args, name, getattr(args, name).resolve(strict=True))
    if mode == 'ingress':
        assert args.worker.name == 'ingress', 'original handler requires basename ingress'
    # SIP has no exec(self-basename) handler path; its original source-built
    # binary may legitimately have a task-specific output filename.
    out = args.output.resolve()
    assert str(out).startswith('/home/fixture/fixture/output/')
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    here = root / 'tests/interop'
    profiles = ('url', 'rtmp') if mode == 'ingress' and args.profile == 'all' else ((args.profile,) if mode == 'ingress' else ('sip-media',))
    trace_module=None
    collector_path=here/'trace-psrpc/collector.py'
    if args.trace_ioinfo:
        trace_spec=importlib.util.spec_from_file_location('owned_sip_trace_collector',collector_path)
        trace_module=importlib.util.module_from_spec(trace_spec);trace_spec.loader.exec_module(trace_module)
    probes = {'url': here / 'ingress-url/run-url.mjs', 'rtmp': here / 'ingress-rtmp/run-rtmp.mjs', 'sip-media': here / 'sip-media/run.mjs'}
    inputs = [Path(__file__).resolve(), BASE, Path(__file__).resolve().parent / ('run-owned-' + mode + '.py'), here / 'package.json', here / 'package-lock.json']
    inputs.extend(probes[profile] for profile in profiles)
    if mode == 'ingress':
        inputs += [here / 'ingress-tone-window.mjs', root / 'tests/agent-sdk/package.json', root / 'tests/agent-sdk/package-lock.json']
    else:
        inputs += [here / 'sip-media/client.html', here / 'sip-media/peer.mjs']
    if args.trace_ioinfo:inputs.append(collector_path)
    before = {str(path): identity(path) for path in inputs}
    binaries = {key: identity(getattr(args, key)) for key in ('go', 'rust', 'worker')}
    env_base = {key: value for key, value in os.environ.items() if not key.startswith(('LIVEKIT_','OTEL_')) and key not in ('REDIS_HOST', 'REDIS_PASSWORD', 'NODE_IP', 'UDP_PORT')}
    env_base['PATH'] = str(args.worker.parent) + os.pathsep + env_base.get('PATH', '')
    env_base.update(LIVEKIT_API_KEY='devkey', LIVEKIT_API_SECRET='secret', LIVEKIT_WS_URL='ws://127.0.0.1:17880')
    tool_names=['redis-server','node']+(['ffmpeg'] if mode=='ingress' else [])
    tools={name:identity(Path(shutil.which(name,path=env_base['PATH']) or '/missing-required-'+name)) for name in tool_names}
    receipt = {'tools':tools,'status': 'RUNNING', 'mode': mode, 'profiles': list(profiles), 'inputs': before, 'binaries': binaries,
               'worker_path_first': str(args.worker.parent), 'livekit_overrides_cleared': True, 'trace_ioinfo_required':args.trace_ioinfo,'trace_config_mode':args.trace_config_mode,'cases': [],
               'network_scope': 'Fresh guest-private broker/server/worker; every fixture endpoint uses loopback; source worker health/relay may bind wildcard behind coordinator guest nft lock',
               'acceptance': 'Original probe final PASS, actual decoded content, real handler or SIP dialog lifetime, process exit0 and owned descendants released. STARTING is not acceptance.'}

    def save():
        (out / 'execution.json').write_text(json.dumps(receipt, indent=2) + '\n')

    save()
    for profile in profiles:
        for server_kind in ('go', 'rust'):
            directory = out / (server_kind + '-' + profile)
            directory.mkdir(mode=0o700)
            case = {'server': server_kind, 'profile': profile, 'status': 'RUNNING', 'processes': []}
            receipt['cases'].append(case)
            processes, logs = [], []
            tree = OwnedProcessTree()
            collector=trace_module.Collector(directory/'collector') if args.trace_ioinfo else None
            tcp_ports = [17842, 17880, 17881] + ([18090, 18091, 19090, 19355] if mode == 'ingress' else [15060, 15061])
            udp_ports = [17882] + ([] if mode == 'ingress' else [15060] + list(range(15100, 15200)))

            def launch(name, command):
                log = (directory / (name + '.log')).open('wb'); logs.append(log)
                process_env=dict(env_base)
                if collector and name in ('main','worker'):
                    side='b' if name=='main' else 'a';kind=server_kind if name=='main' else 'go-sip-worker'
                    process_env['OTEL_RESOURCE_ATTRIBUTES']=f'livekit.parity.side={side},livekit.parity.kind={kind}'
                    if name=='main' and args.trace_config_mode=='env':process_env['LIVEKIT_TRACE_JAEGER_URL']=collector.url
                proc = subprocess.Popen(command, env=process_env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                record = {'name': name, 'pid': proc.pid, 'command': list(map(str, command))}
                processes.append((proc, record)); case['processes'].append(record); tree.add(proc); save()
                return proc

            def ready(check, label):
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    assert all(proc.poll() is None for proc, _ in processes), 'owned process exited during ' + label
                    try:
                        if check(): return
                    except OSError: pass
                    time.sleep(.1)
                raise RuntimeError(label + ' readiness deadline')

            def http(port):
                with urllib.request.urlopen('http://127.0.0.1:' + str(port) + '/', timeout=1) as response:
                    return response.status == 200

            def ping():
                with socket.create_connection(('127.0.0.1', 17842), timeout=1) as sock:
                    sock.sendall(b'*1\r\n$4\r\nPING\r\n'); return sock.recv(64) == b'+PONG\r\n'

            try:
                for port in tcp_ports: free(port)
                for port in udp_ports: free(port, True)
                config = {'port': 17880, 'bind_addresses': ['127.0.0.1'], 'keys': {'devkey': 'secret'},
                          'redis': {'address': '127.0.0.1:17842'},
                          'rtc': {'tcp_port': 17881, 'udp_port': 17882, 'node_ip': '127.0.0.1', 'use_external_ip': False,
                                  'enable_loopback_candidate': True, 'ips': {'includes': ['127.0.0.1/32']}},
                          'psrpc': {'compression': {'quality': 0}}, 'logging': {'level': 'info'}}
                worker_config = {'redis': {'address': '127.0.0.1:17842'}, 'api_key': 'devkey', 'api_secret': 'secret', 'ws_url': 'ws://127.0.0.1:17880', 'logging': {'level': 'info'}}
                if mode == 'ingress':
                    config['ingress'] = {'rtmp_base_url': 'rtmp://127.0.0.1:19355/live'}
                    worker_config.update(health_port=18090, prometheus_port=18091, http_relay_port=19090, rtmp_port=19355, whip_port=-1)
                    worker_health = 18090
                else:
                    worker_config.update(listen_ip='127.0.0.1', sip_hostname='127.0.0.1', sip_port=15060, sip_port_listen=15060,
                                         use_external_ip=False, nat_1_to_1_ip='127.0.0.1', media_use_external_ip=False, media_nat_1_to_1_ip='127.0.0.1',
                                         rtp_port='15100-15199', health_port=15061, prometheus_port=0, pprof_port=0,
                                         psrpc={'compression': {'quality': 0}})
                    worker_health = 15061
                if collector:
                    if args.trace_config_mode=='yaml':config['trace']={'jaeger_url':collector.url}
                    worker_config['jaeger_url']=collector.url
                main_path, worker_path = directory / 'main.yaml', directory / 'worker.yaml'
                main_path.write_text(json.dumps(config)); worker_path.write_text(json.dumps(worker_config))
                case['configs'] = {'main': identity(main_path), 'worker': identity(worker_path)}
                launch('redis', ['redis-server', '--bind', '127.0.0.1', '--port', '17842', '--save', '', '--appendonly', 'no', '--dir', str(directory)])
                ready(ping, 'private Redis')
                main_command=[str(getattr(args, server_kind)), '--dev', '--config', str(main_path)]
                if collector and args.trace_config_mode=='cli':main_command+=['--trace.jaeger_url',collector.url]
                launch('main',main_command)
                ready(lambda: http(17880), 'actual main health')
                worker = launch('worker', [str(args.worker), '--config', str(worker_path)])
                ready(lambda: http(worker_health), 'original worker health')
                if mode == 'ingress':
                    # The original health endpoint proves listener readiness,
                    # not a populated CPU sample. Observe the original monitor's
                    # real CanAccept gauge; never override its admission costs.
                    admission = {'source': 'original livekit_ingress_available CanAccept gauge', 'polls': 0}
                    case['worker_admission_readiness'] = admission
                    def available():
                        admission['polls'] += 1
                        with urllib.request.urlopen('http://127.0.0.1:18091/metrics', timeout=1) as response:
                            data = response.read(1024 * 1024).decode()
                        values = re.findall(r'^livekit_ingress_available(?:\{[^}]*\})?\s+([^\s]+)$', data, re.M)
                        assert len(values) == 1, 'one actual original worker admission gauge required'
                        admission['value'] = float(values[0])
                        return admission['value'] == 1.0
                    ready(available, 'original worker sampled CPU admission')
                env = dict(env_base, INTEROP_PROJECT=str(here), AGENT_SDK_ROOT=str(root / 'tests/agent-sdk'),
                           INTEROP_PACKAGE=str(here / 'package.json'), LIVEKIT_URL='http://127.0.0.1:17880',
                           INGRESS_WORKER_PID=str(worker.pid), INGRESS_MEDIA_OUTPUT=str(directory / 'media.json'),
                           INGRESS_URL_OUTPUT=str(directory / 'media.json'), SIP_MEDIA_OUTPUT=str(directory / 'media.json'))
                if mode == 'sip': env.update(LIVEKIT_URL='ws://127.0.0.1:17880', BROWSER_CHANNEL='chromium', DEBUG='pw:browser')
                with (directory / 'probe.stdout').open('wb') as stdout, (directory / 'probe.stderr').open('wb') as stderr:
                    probe = subprocess.Popen(['node', str(probes[profile])], env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                    tree.add(probe)
                    try: case['probe_exit'] = probe.wait(timeout=180)
                    except subprocess.TimeoutExpired:
                        os.killpg(probe.pid, signal.SIGTERM)
                        try: probe.wait(timeout=10)
                        except subprocess.TimeoutExpired: os.killpg(probe.pid, signal.SIGKILL); probe.wait()
                        case['probe_forced'] = True; raise
                report = json.loads((directory / 'media.json').read_text()); case['media'] = report
                assert case['probe_exit'] == 0 and report['status'] == 'PASS', 'original probe actual media/lifecycle acceptance failed'
                assert not report.get('cleanup_errors') and not report.get('cleanupFailures'), 'original probe cleanup failed'
                if mode == 'sip':
                    text = (directory / 'probe.stderr').read_text(errors='replace')
                    exits = re.findall(r'\[pid=(\d+)\] <process did exit: exitCode=([^,]+), signal=([^>]+)>', text)
                    case['browser_process_exits'] = [{'pid': int(pid), 'code': code.strip(), 'signal': sig.strip()} for pid, code, sig in exits]
                    assert len(exits) == 1 and all(code.strip() == '0' and sig.strip() == 'null' for _, code, sig in exits), 'actual Playwright browser must have one real exit0 lifecycle log'
                if collector:
                    observed=trace_module.validate_sip_ioinfo_trace(collector.snapshot());deadline=time.monotonic()+15
                    while not observed['accepted'] and time.monotonic()<deadline:
                        time.sleep(.05);observed=trace_module.validate_sip_ioinfo_trace(collector.snapshot())
                    case['trace_before_shutdown']=observed
                    assert observed['accepted'], 'actual original SIP IOInfo trace profile failed: '+repr(observed['failures'])
                case['status'] = 'PASS'
            except Exception as error:
                case['status'] = 'FAIL'; case['error'] = type(error).__name__ + ': ' + str(error)
            finally:
                for proc, record in reversed(processes):
                    if proc.poll() is None: proc.send_signal(signal.SIGINT if record['name'] == 'main' else signal.SIGTERM)
                    try: record['exit'] = proc.wait(timeout=45)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL); record['exit'] = proc.wait(); record['forced'] = True
                    if record.get('forced') or record['exit'] != 0: case['status'] = 'FAIL'
                case['child_release'] = tree.finish()
                if not case['child_release']['released_without_reclamation']: case['status'] = 'FAIL'
                for log in logs: log.close()
                if collector:
                    snapshot=collector.close();case['trace_after_shutdown']=trace_module.validate_sip_ioinfo_trace(snapshot)
                    case['collector_thread_exited']=snapshot['thread_exited']
                    if not snapshot['thread_exited'] or not case['trace_after_shutdown']['accepted']:case['status']='FAIL'

                try:
                    for port in tcp_ports: free(port)
                    for port in udp_ports: free(port, True)
                    case['ports_released'] = True
                except OSError as error: case['status'] = 'FAIL'; case['release_error'] = str(error)
                save()
    receipt['inputs_after'] = {str(path): identity(path) for path in inputs}
    receipt['tools_after']={name:identity(Path(shutil.which(name,path=env_base['PATH']) or '/missing-required-'+name)) for name in tool_names}
    receipt['input_identity_stable'] = receipt['tools_after']==tools and receipt['inputs_after'] == before and all(identity(getattr(args, key)) == binaries[key] for key in binaries)
    receipt['status'] = 'PASS_SCOPED_ALL_CASES' if receipt['input_identity_stable'] and len(receipt['cases']) == len(profiles) * 2 and all(case['status'] == 'PASS' for case in receipt['cases']) else 'FAIL'
    save(); return 0 if receipt['status'] == 'PASS_SCOPED_ALL_CASES' else 1
