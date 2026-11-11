#!/usr/bin/env python3
"""Actual fixed-Go/Rust binaries; owns processes/files. NOT_RUN draft.
No server is mocked, no samples injected, and no profile content is waived.
"""
import argparse, base64, concurrent.futures, gzip, hashlib, hmac, json, os
from pathlib import Path
import signal, socket, subprocess, tempfile, time, urllib.error, urllib.request, shutil


def token():
    enc = lambda obj: base64.urlsafe_b64encode(json.dumps(obj, separators=(',', ':')).encode()).rstrip(b'=')
    a, b = enc({'alg':'HS256','typ':'JWT'}), enc({'iss':'devkey','sub':'cli-profile-oracle','exp':int(time.time())+600,'video':{'roomCreate':True,'roomList':True}})
    c=base64.urlsafe_b64encode(hmac.new(b'secret',a+b'.'+b,hashlib.sha256).digest()).rstrip(b'=')
    return (a+b'.'+b+b'.'+c).decode()


def free_port(kind=socket.SOCK_STREAM):
    with socket.socket(socket.AF_INET,kind) as sock:
        sock.bind(('127.0.0.1',0)); return sock.getsockname()[1]


def request(url, body=None):
    headers={'authorization':'Bearer '+token(),'content-type':'application/json'}
    req=urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),headers=headers)
    with urllib.request.urlopen(req,timeout=5) as response:return response.read()


def validate_profile(go, path, kind, require_samples=True):
    # Both Go and Rust files must be genuine standard gzip+protobuf profiles,
    # accepted by the unchanged Go tool rather than a homemade JSON parser.
    assert path.is_file() and path.stat().st_size>0, (kind,'missing/empty')
    payload=gzip.decompress(path.read_bytes()); assert payload, (kind,'empty protobuf')
    result=subprocess.run([go,'tool','pprof','-raw',str(path)],capture_output=True,text=True,timeout=30)
    assert result.returncode==0,(kind,'Go pprof rejected',result.stderr)
    lines=result.stdout.splitlines(); sample_types=None; values=[]; stack_samples=0; in_samples=False
    for line in lines:
        if line.startswith('Samples:'):
            in_samples=True;continue
        if in_samples and sample_types is None:
            sample_types=line.split();continue
        if in_samples:
            before=line.split(':',1)[0].split()
            if before and all(v.lstrip('-').isdigit() for v in before):
                values.append([int(v) for v in before])
                if ':' in line and any(part.isdigit() and int(part)>0 for part in line.split(':',1)[1].split()):stack_samples+=1
            elif line and not line[0].isspace():in_samples=False
    assert sample_types,(kind,'missing sample types',result.stdout[:500])
    if not require_samples:
        return {'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size,'sampleTypes':sample_types,'sampleCount':len(values),'scope':'valid real lifecycle profile; short startup error need not produce positive samples'}
    assert values and stack_samples>0,(kind,'no real stack samples',result.stdout[:500])
    if kind=='cpu':
        index=sample_types.index('cpu/nanoseconds');assert sum(v[index] for v in values)>0
    else:
        for field in ['alloc_objects/count','alloc_space/bytes','inuse_objects/count','inuse_space/bytes']:assert field in sample_types
        index=sample_types.index('alloc_space/bytes');assert sum(v[index] for v in values)>0
    return {'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size,'sampleTypes':sample_types,'sampleCount':len(values),'stackSamples':stack_samples,'sumValues':[sum(row[i] for row in values) for i in range(len(sample_types))]}


def execute(binary,go,directory,selection):
    directory.mkdir();port,udp=free_port(),free_port(socket.SOCK_DGRAM)
    cpu,heap=directory/'cpu.pprof',directory/'heap.pprof'
    argv=[binary,'--dev','--node-ip=127.0.0.1','--bind=127.0.0.1',f'--port={port}','--rtc.tcp_port=0',f'--udp-port={udp}']
    for kind in selection:argv += ['--cpuprofile' if kind=='cpu' else '--memprofile',str(cpu if kind=='cpu' else heap)]
    env={k:v for k,v in os.environ.items() if not k.startswith(('LIVEKIT_','LIVECLOUD_')) and k not in ['NODE_IP','REDIS_HOST','REDIS_PASSWORD','UDP_PORT']}
    log=directory/'server.log';process=None
    try:
        with log.open('wb') as stream:
            process=subprocess.Popen(argv,stdout=stream,stderr=subprocess.STDOUT,env=env,start_new_session=True)
            deadline=time.monotonic()+30
            while True:
                if process.poll() is not None:raise AssertionError(('startup exit',process.returncode,log))
                try:request(f'http://127.0.0.1:{port}/');break
                except (OSError,urllib.error.URLError):
                    if time.monotonic()>=deadline:raise AssertionError(('startup timeout',log))
                    time.sleep(.05)
            # Substantial retained real service allocations, not a fake allocator
            # fixture. Valid room metadata stays below the actual64KiB limit.
            for index in range(256):
                request(f'http://127.0.0.1:{port}/twirp/livekit.RoomService/CreateRoom',{'name':f'profile-{index}','metadata':'m'*60000,'emptyTimeout':600})
            # Prove this is retained real service state, not merely HTTP200
            # request parsing or transient serialized metadata buffers.
            snapshot=json.loads(request(f'http://127.0.0.1:{port}/twirp/livekit.RoomService/ListRooms',{}))
            retained=[room for room in snapshot.get('rooms',[]) if room.get('name','').startswith('profile-')]
            assert len(retained)==256,('real retained room workload missing',len(retained))
            assert {room.get('name') for room in retained}=={f'profile-{index}' for index in range(256)}
            assert all(room.get('metadata')=='m'*60000 for room in retained),'actual room metadata not retained'
            deadline=time.monotonic()+3
            def work(_):
                count=0
                while time.monotonic()<deadline:
                    request(f'http://127.0.0.1:{port}/twirp/livekit.RoomService/ListRooms',{})
                    count+=1
                return count
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:requests=sum(pool.map(work,range(4)))
            assert requests>0
            process.send_signal(signal.SIGTERM);process.wait(timeout=30)
            assert process.returncode==0,('graceful exit',process.returncode,log)
        profiles={kind:validate_profile(go,cpu if kind=='cpu' else heap,kind) for kind in selection}
        return {'status':'PASS','requests':requests,'roomsCreatedAndReadBack':256,'retainedMetadataBytes':256*60000,'profiles':profiles,'logSha256':hashlib.sha256(log.read_bytes()).hexdigest()}
    finally:
        if process and process.poll() is None:
            os.killpg(process.pid,signal.SIGTERM)
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()


def command_cases(binary,directory):
    directory.mkdir();env={k:v for k,v in os.environ.items() if not k.startswith(('LIVEKIT_','LIVECLOUD_')) and k not in ['NODE_IP','REDIS_HOST','REDIS_PASSWORD','UDP_PORT']}
    output=[]
    for name,args in [
        ('ports-ignore',['ports','--dev','--node-ip=127.0.0.1','--cpuprofile',str(directory/'unused-cpu'),'--memprofile',str(directory/'unused-heap')]),
        ('cpu-missing-parent',['--dev','--node-ip=127.0.0.1','--cpuprofile',str(directory/'missing/cpu')]),
        ('heap-missing-parent',['--dev','--node-ip=127.0.0.1','--memprofile',str(directory/'missing/heap')]),
        ('cpu-directory',['--dev','--node-ip=127.0.0.1','--cpuprofile',str(directory)]),
        ('heap-directory',['--dev','--node-ip=127.0.0.1','--memprofile',str(directory)]),
    ]:
        result=subprocess.run([binary]+args,capture_output=True,text=True,env=env,timeout=15)
        # Actual CLI error is stdout; runtime development logs are separately
        # stderr and may differ by implementation. Never normalize error text.
        output.append({'case':name,'exit':result.returncode,'stdout':result.stdout.replace(str(directory),'$TMP'),'stderrSha256':hashlib.sha256(result.stderr.encode()).hexdigest()})
    assert not (directory/'unused-cpu').exists() and not (directory/'unused-heap').exists()
    return output


def startup_failure_profiles(binary, go, directory):
    """Actual owned TCP collision after profile creation; no fake server failure."""
    directory.mkdir(); cpu,heap=directory/'cpu.pprof',directory/'heap.pprof'
    env={k:v for k,v in os.environ.items() if not k.startswith(('LIVEKIT_','LIVECLOUD_')) and k not in ['NODE_IP','REDIS_HOST','REDIS_PASSWORD','UDP_PORT']}
    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as occupied:
        occupied.bind(('127.0.0.1',0));occupied.listen(1);port=occupied.getsockname()[1]
        argv=[binary,'--dev','--node-ip=127.0.0.1','--bind=127.0.0.1',f'--port={port}','--rtc.tcp_port=0',f'--udp-port={free_port(socket.SOCK_DGRAM)}','--cpuprofile',str(cpu),'--memprofile',str(heap)]
        process=subprocess.run(argv,capture_output=True,env=env,timeout=30)
        assert process.returncode!=0, 'real occupied port must fail startup'
    return {'exit':process.returncode,'stdoutSha256':hashlib.sha256(process.stdout).hexdigest(),'stderrSha256':hashlib.sha256(process.stderr).hexdigest(),'profiles':{kind:validate_profile(go,path,kind,False) for kind,path in [('cpu',cpu),('heap',heap)]}}

def main():
    p=argparse.ArgumentParser();p.add_argument('--official',required=True);p.add_argument('--rust',required=True);p.add_argument('--go',default='go');p.add_argument('--output',type=Path,required=True);p.add_argument('--artifacts',type=Path,required=True);a=p.parse_args()
    assert not a.artifacts.exists(),'explicit new task-owned artifacts directory required';a.artifacts.mkdir(parents=True)
    result={'status':'RUNNING','scope':'real CLI lifecycle + unchanged Go pprof decoder; profiles contain language-specific actual stacks/allocations, not identical bytes/GC semantics','binaries':{k:hashlib.sha256(Path(v).read_bytes()).hexdigest() for k,v in [('official',a.official),('rust',a.rust)]},'runs':{},'commandCases':{},'startupFailures':{}}
    try:
        go_path=shutil.which(a.go) if not Path(a.go).is_file() else str(Path(a.go).resolve())
        assert go_path,'actual Go tool executable required'
        version=subprocess.run([go_path,'version'],capture_output=True,text=True,timeout=10)
        assert version.returncode==0,('actual Go version failed',version.stderr)
        result['profileTool']={'sha256':hashlib.sha256(Path(go_path).read_bytes()).hexdigest(),'actualVersion':version.stdout.strip(),'scope':'test oracle only, not production runtime'}
        for kind,binary in [('official',a.official),('rust',a.rust)]:
            result['commandCases'][kind]=command_cases(binary,a.artifacts/(kind+'-commands'))
            result['runs'][kind]={}
            result['startupFailures'][kind]=startup_failure_profiles(binary,a.go,a.artifacts/(kind+'-late-startup-error'))
            for selection in [('cpu',),('heap',),('cpu','heap')]:
                result['runs'][kind]['+'.join(selection)]=execute(binary,a.go,a.artifacts/(kind+'-'+'-'.join(selection)),selection)
        for reference,actual in zip(result['commandCases']['official'],result['commandCases']['rust']):
            assert (reference['case'],reference['exit'],reference['stdout'])==(actual['case'],actual['exit'],actual['stdout']),(reference,actual)
        result['status']='PASS'
    except Exception as error:
        result['status']='FAIL';result['error']=str(error);raise
    finally:
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(result,indent=2)+'\n')
if __name__=='__main__':main()
