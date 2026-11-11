#!/usr/bin/env python3
"""Four owned Go/Rust AgentInternal API/worker pairs on real Redis.

The separately built probe uses the fixed official Go protocol/auth/Twirp
packages. This runner owns every server/broker process; no external Redis or
server process is stopped. No Cargo or Go compilation happens here.
"""
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
import urllib.error
import urllib.request

KEY = 'agentpaired'
SECRET = 'public-agent-paired-test-fixture-secret-0123456'
FIXED_GO_SHA256 = '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def free_ports(count):
    sockets=[]
    try:
        for _ in range(count):
            sock=socket.socket();sock.bind(('127.0.0.1',0));sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:sock.close()

def wait_http(proc,port):
    deadline=time.monotonic()+40
    while time.monotonic()<deadline:
        if proc.poll() is not None:raise RuntimeError(f'owned server exited before ready: {proc.returncode}')
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/',timeout=.5) as response:
                if response.status==200:return
        except (OSError,urllib.error.URLError):pass
        time.sleep(.05)
    raise TimeoutError('owned HTTP server readiness')

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--go-server',type=Path,required=True)
    parser.add_argument('--rust-server',type=Path,required=True)
    parser.add_argument('--probe',type=Path,required=True)
    parser.add_argument('--evidence-dir',type=Path,required=True)
    parser.add_argument('--pair', choices=['go-owner-go-worker','rust-owner-rust-worker','go-owner-rust-worker','rust-owner-go-worker'])
    args=parser.parse_args()
    binaries={'go':args.go_server.resolve(strict=True),'rust':args.rust_server.resolve(strict=True)}
    probe=args.probe.resolve(strict=True)
    if sha(binaries['go']) != FIXED_GO_SHA256:
        raise RuntimeError('fixed source Go binary identity mismatch before execution')
    output=args.evidence_dir.resolve();output.mkdir(parents=True,exist_ok=False)
    redis_server,redis_cli=shutil.which('redis-server'),shutil.which('redis-cli')
    if not redis_server or not redis_cli:raise RuntimeError('redis-server and redis-cli required')
    summary={'fixed_go_source':'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07',
             'fixed_protocol_source':'9bb708cdb81918a6c63dd25e915b4644310b45b6',
             'binary_sha256':{side:sha(path) for side,path in binaries.items()},'probe_sha256':sha(probe),'pairs':[]}
    pairs=[('go','go'),('rust','rust'),('go','rust'),('rust','go')]
    if args.pair:
        pairs=[(a,b) for a,b in pairs if f'{a}-owner-{b}-worker'==args.pair]
    for a,b in pairs:
        label=f'{a}-owner-{b}-worker'
        case={'pair':label,'accepted':False,'commands':[],'processes':[]}
        directory=output/label;directory.mkdir()
        with tempfile.TemporaryDirectory(prefix='agent-owned-pair-') as temp:
            staging=Path(temp);processes=[]
            def start(command,name):
                log=(directory/f'{name}.log').open('wb')
                proc=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
                processes.append((name,proc,log));case['commands'].append(command)
                return proc
            try:
                redis_port,http_a,tcp_a,udp_a,http_b,tcp_b,udp_b=free_ports(7)
                broker=start([redis_server,'--bind','127.0.0.1','--port',str(redis_port),
                    '--save','','--appendonly','no','--dir',str(staging)],'redis')
                deadline=time.monotonic()+10
                while True:
                    if broker.poll() is not None:raise RuntimeError('owned broker exited before ready')
                    ping=subprocess.run([redis_cli,'-h','127.0.0.1','-p',str(redis_port),'PING'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
                    if ping.returncode==0 and ping.stdout.strip()==b'PONG':break
                    if time.monotonic()>deadline:raise TimeoutError('owned broker readiness')
                    time.sleep(.05)
                for side,kind,http,tcp,udp in [('a',a,http_a,tcp_a,udp_a),('b',b,http_b,tcp_b,udp_b)]:
                    config=(f'keys: {{{KEY}: {SECRET}}}\nlogging: {{level: info, json: true}}\n'
                      'bind_addresses: [127.0.0.1]\n'
                      f'rtc: {{node_ip: 127.0.0.1, use_external_ip: false, tcp_port: {tcp}, udp_port: {udp}, enable_loopback_candidate: true}}\n'
                      f'turn: {{enabled: false}}\nredis: {{address: 127.0.0.1:{redis_port}}}\n')
                    path=staging/f'{side}.yaml';path.write_text(config);path.chmod(0o600)
                    (directory/f'{side}.config.yaml').write_text(config)
                    proc=start([str(binaries[kind]),'--dev','--config',str(path),'--port',str(http)],side)
                    wait_http(proc,http)
                env=dict(os.environ,LIVEKIT_AGENT_API_KEY=KEY,LIVEKIT_AGENT_API_SECRET=SECRET)
                command=[str(probe),'--node-a',f'http://127.0.0.1:{http_a}','--node-b',f'http://127.0.0.1:{http_b}','--label',label]
                case['commands'].append(command)
                completed=subprocess.run(command,env=env,capture_output=True,timeout=60)
                (directory/'probe.stdout').write_bytes(completed.stdout)
                (directory/'probe.stderr').write_bytes(completed.stderr)
                case['probe_exit']=completed.returncode
                try:observation=json.loads(completed.stdout)
                except (ValueError,TypeError):observation={'accepted':False,'error':'probe output must be one JSON observation'}
                case['observation']=observation
                case['accepted']=completed.returncode==0 and observation.get('accepted') is True
            except Exception as error:
                case['runner_error']=str(error)
            finally:
                for name,proc,log in reversed(processes):
                    forced=False
                    if proc.poll() is None:
                        proc.send_signal(signal.SIGTERM)
                        try:proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            forced=True;proc.kill();proc.wait()
                    case['processes'].append({'name':name,'pid':proc.pid,'exit':proc.returncode,'forced':forced})
                    log.close()
                if any(proc['forced'] or proc['exit'] != 0 for proc in case['processes']):
                    case['accepted']=False
                    case['shutdown_error']='owned process did not release resources and exit cleanly'
                (directory/'result.json').write_text(json.dumps(case,indent=2)+'\n')
        summary['pairs'].append(case)
        (output/'result.json').write_text(json.dumps(summary,indent=2)+'\n')
    summary['final_binary_sha256']={side:sha(path) for side,path in binaries.items()}
    summary['final_probe_sha256']=sha(probe)
    summary['input_identity_stable']=(summary['final_binary_sha256']==summary['binary_sha256'] and summary['final_probe_sha256']==summary['probe_sha256'])
    summary['accepted']=len(summary['pairs'])==len(pairs) and all(case['accepted'] for case in summary['pairs'])
    summary['complete_four_way']=len(pairs)==4 and summary['accepted'] and summary['input_identity_stable']
    (output/'result.json').write_text(json.dumps(summary,indent=2)+'\n')
    raise SystemExit(0 if summary['accepted'] else 1)

if __name__=='__main__':main()
