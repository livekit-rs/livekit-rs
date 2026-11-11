#!/usr/bin/env python3
"""Owned single-node/no-Redis Go/Rust actual SDK node-capacity campaign.

Runs two invalid-selector startup verdicts plus both real track and byte-rate
quota/release lifecycles against each server. No Cargo/Go/npm build is invoked.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

HERE=Path(__file__).resolve().parent
INTEROP=HERE.parent

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def ports():
    held=[]
    try:
        for kind in [socket.SOCK_STREAM,socket.SOCK_STREAM,socket.SOCK_DGRAM]:
            sock=socket.socket(socket.AF_INET,kind);sock.bind(('127.0.0.1',0));held.append(sock)
        return [sock.getsockname()[1] for sock in held]
    finally:
        for sock in held:sock.close()
def ready(proc,port):
    until=time.monotonic()+40
    while time.monotonic()<until:
        if proc.poll() is not None:raise RuntimeError(f'owned server exited during startup: {proc.returncode}')
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/',timeout=.5) as response:
                if response.status==200:return
        except OSError:pass
        time.sleep(.05)
    raise TimeoutError('owned server HTTP readiness')
def stop(proc):
    forced=False
    if proc.poll() is None:proc.send_signal(signal.SIGINT)
    try:code=proc.wait(timeout=15)
    except subprocess.TimeoutExpired:forced=True;proc.kill();code=proc.wait(timeout=5)
    return {'pid':proc.pid,'exit':code,'forced':forced}
def freed(http,tcp,udp):
    for kind,port in [(socket.SOCK_STREAM,http),(socket.SOCK_STREAM,tcp),(socket.SOCK_DGRAM,udp)]:
        with socket.socket(socket.AF_INET,kind) as sock:
            if kind==socket.SOCK_STREAM:sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            sock.bind(('127.0.0.1',port))
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go',type=Path,required=True);parser.add_argument('--rust',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--only',choices=[f'{side}-{case}' for side in ['go','rust'] for case in ['invalid-selector','num-tracks','bytes-per-sec']])
    args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    binaries={side:getattr(args,side).resolve(strict=True) for side in ['go','rust']}
    result={'status':'INCOMPLETE','selection':args.only or 'all6','scope':'actual single-node/no Redis configuration effects, official SDK2.23.0 native RTP publication and sampled admission/release; startup verdict only, not byte-identical diagnostics',
        'binaries':{side:{'path':str(binary),'sha256':sha(binary)} for side,binary in binaries.items()},
        'inputs':{str(path.relative_to(INTEROP)):sha(path) for path in [HERE/'run.py',HERE/'probe.mjs',HERE/'client.html',INTEROP/'package.json',INTEROP/'package-lock.json']},'cases':[]}
    def save():(out/'execution.json').write_text(json.dumps(result,indent=2)+'\n')
    save()
    for side,binary in binaries.items():
        for case in ['invalid-selector','num-tracks','bytes-per-sec']:
            label=f'{side}-{case}'
            if args.only and args.only!=label:continue
            evidence=out/label;evidence.mkdir();row={'case':label,'status':'RUNNING','no_redis':True};result['cases'].append(row);save()
            proc=None;browser=None
            with tempfile.TemporaryDirectory(prefix='owned-local-node-quota-') as staging:
                http,tcp,udp=ports()
                config={'port':http,'bind_addresses':['127.0.0.1'],'keys':{'devkey':'secret'},
                    'rtc':{'node_ip':'127.0.0.1','tcp_port':tcp,'udp_port':udp,'use_external_ip':False,'enable_loopback_candidate':True},
                    'turn':{'enabled':False},'logging':{'level':'error'},'room':{'empty_timeout':10,'departure_timeout':2},
                    'node_selector':{'kind':'not-a-selector' if case=='invalid-selector' else 'any'},
                    'limit':{'num_tracks':1 if case=='num-tracks' else 0,'bytes_per_sec':128 if case=='bytes-per-sec' else 0}}
                path=Path(staging)/'config.json';path.write_text(json.dumps(config));path.chmod(0o600)
                (evidence/'config.json').write_text(json.dumps(config,indent=2)+'\n')
                env={k:v for k,v in os.environ.items() if not k.startswith('LIVEKIT_') and k not in ['REDIS_HOST','REDIS_PASSWORD','NODE_IP','UDP_PORT']}
                command=[str(binary),'--dev','--config',str(path),'--port',str(http)];row['command']=command
                server_log=(evidence/'server.log').open('wb')
                try:
                    proc=subprocess.Popen(command,env=env,stdout=server_log,stderr=subprocess.STDOUT)
                    row['server_pid']=proc.pid
                    if case=='invalid-selector':
                        code=proc.wait(timeout=25);server_log.flush();text=(evidence/'server.log').read_text(errors='replace')
                        assert code!=0, 'invalid node selector must fail startup without Redis'
                        assert 'unsupported node selector' in text,text
                        row['startup_exit']=code;freed(http,tcp,udp);row['ports_released']=True;row['status']='PASS'
                    else:
                        ready(proc,http)
                        browser_env=dict(env,LOCAL_QUOTA_URL=f'ws://127.0.0.1:{http}',LOCAL_QUOTA_OUTPUT=str(evidence/'browser-output'),LOCAL_QUOTA_CASE=label,LIVEKIT_API_KEY='devkey',LIVEKIT_API_SECRET='secret')
                        browser_log=(evidence/'browser.log').open('wb')
                        browser=subprocess.Popen(['node',str(HERE/'probe.mjs')],cwd=INTEROP,env=browser_env,stdout=browser_log,stderr=subprocess.STDOUT,start_new_session=True)
                        row['node_pid']=browser.pid
                        try:code=browser.wait(timeout=160)
                        except subprocess.TimeoutExpired:
                            os.killpg(browser.pid,signal.SIGTERM)
                            try:browser.wait(timeout=10)
                            except subprocess.TimeoutExpired:os.killpg(browser.pid,signal.SIGKILL);browser.wait(timeout=5)
                            raise RuntimeError('owned browser capacity campaign deadline')
                        finally:browser_log.close()
                        row['probe_exit']=code
                        observation=json.loads((evidence/'browser-output'/'result.json').read_text());row['observation']=observation
                        assert code==0 and observation['status']=='PASS',observation.get('error')
                        assert observation['senderStats'] and any(s['bytesSent']>4096 and s['packetsSent']>50 for s in observation['senderStats'])
                        assert observation['unpublished'] and observation['publisherLeft'] and observation['rtcRecovered']
                        row['status']='PASS'
                except Exception as error:
                    row['status']='FAIL';row['error']=str(error)
                finally:
                    if browser is not None and browser.poll() is None:
                        os.killpg(browser.pid,signal.SIGTERM)
                        try:browser.wait(timeout=10)
                        except subprocess.TimeoutExpired:os.killpg(browser.pid,signal.SIGKILL);browser.wait(timeout=5)
                    ownership=evidence/'browser-output'/'ownership.json'
                    if ownership.exists():
                        browser_pid=json.loads(ownership.read_text())['browserPid']
                        try:
                            os.kill(browser_pid,0)
                        except ProcessLookupError:pass
                        else:
                            # This exact PID is the public BrowserServer.process
                            # handle written by this runner's own probe. Any
                            # survivor is a failed resource gate, never a pass.
                            row['status']='FAIL';row['browser_resource_error']='owned browser survived Node probe exit'
                            try:
                                if os.getpgid(browser_pid)==browser_pid:os.killpg(browser_pid,signal.SIGTERM)
                                else:os.kill(browser_pid,signal.SIGTERM)
                            except ProcessLookupError:pass
                            until=time.monotonic()+5
                            while time.monotonic()<until:
                                try:os.kill(browser_pid,0)
                                except ProcessLookupError:break
                                time.sleep(.05)
                            else:
                                try:
                                    if os.getpgid(browser_pid)==browser_pid:os.killpg(browser_pid,signal.SIGKILL)
                                    else:os.kill(browser_pid,signal.SIGKILL)
                                except ProcessLookupError:pass
                    if proc is not None:
                        ended=stop(proc);row['server_shutdown']=ended
                        if case!='invalid-selector' and (ended['forced'] or ended['exit']!=0):row['status']='FAIL';row['shutdown_error']='owned server failed to release gracefully'
                    server_log.close()
                    try:freed(http,tcp,udp);row['ports_released']=True
                    except OSError as error:row['status']='FAIL';row['resource_error']=str(error)
                    save()
    result['status']='PASS' if result['cases'] and all(row['status']=='PASS' for row in result['cases']) else 'FAIL'
    result['complete_matrix']=not args.only and len(result['cases'])==6 and result['status']=='PASS';save()
    raise SystemExit(0 if result['status']=='PASS' else 1)
if __name__=='__main__':main()
