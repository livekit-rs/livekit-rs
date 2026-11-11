#!/usr/bin/env python3
"""Parent-executed actual POSIX profile open modes. No sampler/profile mocks.
Original compare-profiles.py and its corpus are unchanged; this is additive.
"""
import argparse, concurrent.futures, hashlib, importlib.util, json, os
from pathlib import Path
import select, signal, subprocess, time
spec=importlib.util.spec_from_file_location('base_profiles',Path(__file__).with_name('compare-profiles.py'))
base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)

def environment():
    return {k:v for k,v in os.environ.items() if not k.startswith(('LIVEKIT_','LIVECLOUD_')) and k not in ['NODE_IP','REDIS_HOST','REDIS_PASSWORD','UDP_PORT']}

def args(binary,path,kind,port):
    return [binary,'--dev','--node-ip=127.0.0.1','--bind=127.0.0.1',f'--port={port}','--rtc.tcp_port=0',f'--udp-port={base.free_port(base.socket.SOCK_DGRAM)}','--cpuprofile' if kind=='cpu' else '--memprofile',str(path)]

def stop(process):
    if process.poll() is None:
        os.killpg(process.pid,signal.SIGTERM)
        try: process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
    assert process.poll() is not None,'owned process must be reaped'

def write_only(binary,directory,kind):
    directory.mkdir();path=directory/'write-only.pprof';path.write_bytes(b'owned original content');path.chmod(0o222)
    process=None
    try:
        process=subprocess.Popen(args(binary,path,kind,base.free_port()),stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=environment(),start_new_session=True)
        stdout,stderr=process.communicate(timeout=15)
        assert process.returncode==1,('write-only profile must fail opening before service starts',process.returncode)
        expected=f'open {path}: permission denied\n'.encode()
        assert stdout==expected,('write-only exact source diagnostic',stdout)
        # Failure must not truncate a file that could not be opened RDWR.
        path.chmod(0o600);assert path.read_bytes()==b'owned original content'
        return {'kind':kind,'exit':process.returncode,'stdout':stdout.decode().replace(str(directory),'$TMP'),'originalContentPreserved':True,'stderrSha256':hashlib.sha256(stderr).hexdigest()}
    finally:
        if process is not None: stop(process)
        path.chmod(0o600)

def read_fifo(path,process):
    fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK);data=bytearray();deadline=time.monotonic()+45
    try:
        while time.monotonic()<deadline:
            ready,_,_=select.select([fd],[],[],.1)
            # Drain final nonblocking bytes after the owned producer exits,
            # independently of readiness notifications. The fixed producer has
            # closed its writer; the unchanged decoder still requires full data.
            exited = process.poll() is not None
            if not ready and not exited: continue
            try:
                chunk=os.read(fd,65536)
            except BlockingIOError:
                if exited: return bytes(data)
                continue
            if chunk:
                data.extend(chunk);assert len(data)<=128*1024*1024,'owned profile resource bound exceeded'
            elif process.poll() is not None:
                return bytes(data)
        raise AssertionError('owned FIFO producer did not finish after shutdown')
    finally: os.close(fd)

def fifo(binary,go,directory,kind):
    directory.mkdir();path=directory/'profile.fifo';os.mkfifo(path,0o600)
    process=None;port=base.free_port();log=directory/'server.log'
    try:
        with log.open('wb') as stream:
            process=subprocess.Popen(args(binary,path,kind,port),stdout=stream,stderr=subprocess.STDOUT,env=environment(),start_new_session=True)
            # Intentionally NO external reader until genuine HTTP readiness:
            # O_RDWR can open the FIFO; old write-only create would block here.
            deadline=time.monotonic()+30
            while True:
                assert process.poll() is None,('FIFO startup exited',process.returncode)
                try:base.request(f'http://127.0.0.1:{port}/');break
                except (OSError,base.urllib.error.URLError):
                    assert time.monotonic()<deadline,'FIFO open blocked before HTTP readiness with no external reader'
                    time.sleep(.05)
            readers=concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                captured=readers.submit(read_fifo,path,process)
                for index in range(256):base.request(f'http://127.0.0.1:{port}/twirp/livekit.RoomService/CreateRoom',{'name':f'profile-{index}','metadata':'m'*60000,'emptyTimeout':600})
                snapshot=json.loads(base.request(f'http://127.0.0.1:{port}/twirp/livekit.RoomService/ListRooms',{}))
                rooms=[r for r in snapshot.get('rooms',[]) if r.get('name','').startswith('profile-')]
                assert len(rooms)==256 and {r.get('name')for r in rooms}=={f'profile-{i}'for i in range(256)}
                assert all(r.get('metadata')=='m'*60000 for r in rooms)
                deadline=time.monotonic()+3;count=0
                while time.monotonic()<deadline:
                    base.request(f'http://127.0.0.1:{port}/twirp/livekit.RoomService/ListRooms',{});count+=1
                assert count>0
                process.send_signal(signal.SIGTERM);process.wait(timeout=30)
                assert process.returncode==0,('FIFO graceful server exit',process.returncode)
                wire=captured.result(timeout=10)
            finally:
                stop(process)
                readers.shutdown(wait=True)
        capture=directory/'captured.pprof';capture.write_bytes(wire)
        profile=base.validate_profile(go,capture,kind,True)
        return {'kind':kind,'serverReadyBeforeExternalReader':True,'roomsCreatedAndReadBack':256,'requests':count,'exit':0,'profile':profile,'logSha256':hashlib.sha256(log.read_bytes()).hexdigest()}
    finally:
        if process is not None:stop(process)

def main():
    p=argparse.ArgumentParser();p.add_argument('--official',required=True);p.add_argument('--rust',required=True);p.add_argument('--go',required=True);p.add_argument('--artifacts',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
    assert os.name=='posix' and hasattr(os,'getuid') and os.geteuid()!=0,'requires a real non-root POSIX process; do not report skipped PASS'
    assert not a.artifacts.exists(),'new owned directory required';a.artifacts.mkdir(parents=True)
    result={'status':'RUNNING','scope':'additive regular write-only rejection and real FIFO profiling positive; no profile sample equality across languages','binaries':{n:hashlib.sha256(Path(b).read_bytes()).hexdigest()for n,b in [('official',a.official),('rust',a.rust)]},'rows':{}}
    try:
        go_path=Path(a.go).resolve();assert go_path.is_file(),'explicit actual Go tool path required'
        version=subprocess.run([str(go_path),'version'],capture_output=True,text=True,timeout=10)
        assert version.returncode==0,('Go tool failed',version.stderr)
        result['profileTool']={'sha256':hashlib.sha256(go_path.read_bytes()).hexdigest(),'actualVersion':version.stdout.strip()}
        for name,binary in [('official',a.official),('rust',a.rust)]:
            result['rows'][name]={'writeOnly':[write_only(binary,a.artifacts/(name+'-writeonly-'+k),k)for k in ['cpu','heap']],'fifo':[fifo(binary,a.go,a.artifacts/(name+'-fifo-'+k),k)for k in ['cpu','heap']]}
        for expected,actual in zip(result['rows']['official']['writeOnly'],result['rows']['rust']['writeOnly']):
            # Only stderr runtime logger bytes are recorded, not equality claims.
            assert {k:v for k,v in expected.items()if k!='stderrSha256'}=={k:v for k,v in actual.items()if k!='stderrSha256'}
        result['status']='PASS_SCOPED'
    except Exception as error:
        result['status']='FAIL';result['error']=str(error);raise
    finally:
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(result,indent=2)+'\n')
if __name__=='__main__': main()
