#!/usr/bin/env python3
"""Physical POSIX envp vector, preserving duplicate byte names. Never dict→execve.
Only a child of this wrapper is replaced; bounded capture belongs to its parent.
"""
import ctypes, json, os, signal, subprocess, sys, tempfile
from pathlib import Path

def replace(spec):
    binary=bytes.fromhex(spec['binary_hex']); argv=[bytes.fromhex(v)for v in spec['argv_hex']]; entries=[bytes.fromhex(v)for v in spec['envp_hex']]
    if not argv or any(b'\0'in v for v in [binary,*argv,*entries]):raise ValueError('invalid NUL or argv')
    os.chdir(bytes.fromhex(spec['cwd_hex']))
    arr=ctypes.c_char_p*(len(argv)+1); av=arr(*argv,None)
    env=ctypes.c_char_p*(len(entries)+1); ev=env(*entries,None)
    libc=ctypes.CDLL(None,use_errno=True); fn=libc.execve
    fn.argtypes=[ctypes.c_char_p,ctypes.POINTER(ctypes.c_char_p),ctypes.POINTER(ctypes.c_char_p)]; fn.restype=ctypes.c_int
    fn(binary,av,ev); number=ctypes.get_errno();raise OSError(number,os.strerror(number))

def execute(binary,args,entries,cwd,timeout=10):
    binary=os.fsencode(Path(binary).resolve());spec={'binary_hex':binary.hex(),'argv_hex':[v.hex()for v in [binary,*args]],'envp_hex':[v.hex()for v in entries],'cwd_hex':cwd.hex()}
    # Named temporary is mode0600, containing only owned crafted test ENV.
    with tempfile.NamedTemporaryFile(mode='w',prefix='f92-envp-',suffix='.json')as f:
        json.dump(spec,f);f.flush()
        p=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--exec-child',f.name],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        timed_out=False
        try:out,err=p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out=True;os.killpg(p.pid,signal.SIGKILL);out,err=p.communicate()
    return {'exit':p.returncode,'timed_out':timed_out,'stdout':out,'stderr':err}
if __name__=='__main__':
    if len(sys.argv)!=3 or sys.argv[1]!='--exec-child':raise SystemExit('internal --exec-child SPEC only')
    replace(json.loads(Path(sys.argv[2]).read_text()))
