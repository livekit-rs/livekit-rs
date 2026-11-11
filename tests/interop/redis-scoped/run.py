#!/usr/bin/env python3
"""Run selected named Redis integration tests against one owned fresh broker each."""
import argparse, hashlib, json, os, re, signal, socket, subprocess, tempfile, time
from pathlib import Path

def identity(path):
    path=Path(path).resolve()
    return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

def save(path,data):
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(data,indent=2)+'\n');tmp.replace(path)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--test-binary',type=Path,required=True)
    p.add_argument('--redis-server',type=Path,default=Path('/opt/homebrew/bin/redis-server'))
    p.add_argument('--filter',required=True)
    p.add_argument('--env',required=True,help='test-owned Redis URL environment variable')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();a.test_binary=a.test_binary.resolve();a.output=a.output.resolve()
    if not __debug__:p.error('assertions must stay enabled')
    a.output.mkdir(parents=True,exist_ok=False)
    receipt={'status':'RUNNING','runner':identity(__file__),'binary':identity(a.test_binary),'redis':identity(a.redis_server),'filter':a.filter,'environment_name':a.env,'tests':[]}
    path=a.output/'execution.json';save(path,receipt)
    listing=subprocess.run([a.test_binary,'--ignored','--list',a.filter],capture_output=True,timeout=30)
    (a.output/'inventory.stdout').write_bytes(listing.stdout);(a.output/'inventory.stderr').write_bytes(listing.stderr)
    names=re.findall(r'^(.+): test$',listing.stdout.decode(),re.M)
    if listing.returncode or not names:raise RuntimeError('nonempty actual test inventory required')
    for index,name in enumerate(names):
        row={'name':name,'status':'RUNNING'};receipt['tests'].append(row);save(path,receipt)
        with tempfile.TemporaryDirectory(prefix='livekit-redis-scoped-') as work:
            with socket.socket() as probe:probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
            redis_log=open(a.output/f'{index:02d}-redis.log','wb')
            broker=subprocess.Popen([a.redis_server,'--bind','127.0.0.1','--port',str(port),'--save','','--appendonly','no','--dir',work],stdout=redis_log,stderr=subprocess.STDOUT,start_new_session=True)
            row.update(redis_pid=broker.pid,redis_port=port)
            test=None
            try:
                deadline=time.monotonic()+10
                while True:
                    if broker.poll() is not None:raise RuntimeError('owned Redis exited before readiness')
                    try:
                        with socket.create_connection(('127.0.0.1',port),timeout=.5) as connection:
                            connection.sendall(b'*1\r\n$4\r\nPING\r\n')
                            if connection.recv(64)==b'+PONG\r\n':break
                    except (OSError,TimeoutError):pass
                    if time.monotonic()>=deadline:raise RuntimeError('owned Redis readiness timed out')
                    time.sleep(.05)
                environment={**os.environ,a.env:f'redis://127.0.0.1:{port}'}
                with open(a.output/f'{index:02d}-test.stdout','wb') as stdout,open(a.output/f'{index:02d}-test.stderr','wb') as stderr:
                    test=subprocess.Popen([a.test_binary,'--ignored','--exact',name,'--nocapture','--test-threads=1'],stdout=stdout,stderr=stderr,env=environment,start_new_session=True)
                    code=test.wait(timeout=120)
                text=(a.output/f'{index:02d}-test.stdout').read_text()
                row['exit_code']=code
                row['executed_one']='running 1 test' in text
                row['status']='PASS' if code==0 and row['executed_one'] and '1 passed; 0 failed' in text else 'FAIL'
            except BaseException as error:
                row['status']='FAIL';row['error']=f'{type(error).__name__}: {error}'
                if test is not None and test.poll() is None:
                    os.killpg(test.pid,signal.SIGKILL);test.wait();row['forced_test_cleanup']=True
            finally:
                if broker.poll() is None:broker.terminate()
                try:row['redis_exit']=broker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    broker.kill();row['redis_exit']=broker.wait();row['forced_redis_cleanup']=True
                redis_log.close()
                if row.get('redis_exit')!=0 or row.get('forced_test_cleanup') or row.get('forced_redis_cleanup'):row['status']='FAIL'
                save(path,receipt)
        row['temporary_inputs_removed']=True;save(path,receipt)
    receipt['binary_unchanged']=identity(a.test_binary)==receipt['binary']
    receipt['status']='PASS_SELECTED_INVENTORY' if receipt['binary_unchanged'] and all(r['status']=='PASS' for r in receipt['tests']) else 'FAIL'
    save(path,receipt);print(json.dumps({'status':receipt['status'],'tests':len(names),'receipt':str(path)}))
    return 0 if receipt['status']=='PASS_SELECTED_INVENTORY' else 1
if __name__=='__main__':raise SystemExit(main())
