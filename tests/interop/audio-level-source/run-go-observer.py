#!/usr/bin/env python3
"""Call fixed server's actual exported audio detector, without copying it."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess

def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def inventory(root):
    return {p.relative_to(root).as_posix():digest(p) for p in root.rglob('*') if p.is_file() and '.git' not in p.parts}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixed-server-source',type=Path,required=True)
    parser.add_argument('--go-binary',type=Path,required=True)
    parser.add_argument('--evidence-dir',type=Path,required=True)
    args=parser.parse_args();source=args.fixed_server_source.resolve(strict=True);go=args.go_binary.resolve(strict=True);observer=Path(__file__).with_name('original_observer.go').resolve();output=args.evidence_dir.resolve();output.mkdir(parents=True,exist_ok=False)
    before=inventory(source);result={'status':'FAILED','go_binary':str(go),'go_binary_sha256':digest(go),'runner_sha256':digest(Path(__file__)),'observer_sha256':digest(observer),'fixed_source_files_before':before}
    try:
        env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='local',GOPROXY='off',GOSUMDB='off',AUDIO_LEVEL_GO_OBSERVATION=str(output/'observation.json'))
        version=subprocess.run([str(go),'version'],cwd=source,env=env,capture_output=True,timeout=15)
        result['go_version_exit']=version.returncode;result['go_version']=version.stdout.decode().strip();(output/'go-version.stderr').write_bytes(version.stderr)
        if version.returncode!=0 or not result['go_version'].startswith('go version go1.26.3 '):raise ValueError('direct fixed Go1.26.3 toolchain required')
        command=[str(go),'run','-mod=readonly',str(observer)];result['command']=command
        proc=subprocess.Popen(command,cwd=source,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True);result['owned_process_group']=proc.pid
        try:stdout,stderr=proc.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            result['timeout']=True
            try:os.killpg(proc.pid,signal.SIGTERM)
            except ProcessLookupError:pass
            try:stdout,stderr=proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                try:os.killpg(proc.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                stdout,stderr=proc.communicate()
        (output/'stdout.log').write_bytes(stdout);(output/'stderr.log').write_bytes(stderr);result['exit']=proc.returncode
        if proc.returncode!=0 or result.get('timeout'):raise ValueError('actual exported Go observer failed')
        observation=json.loads((output/'observation.json').read_text());result['actual_rows']=len(observation['rows'])
        if result['actual_rows']!=17 or observation['row_count']!=17:raise ValueError('observer did not execute every required original-consumer stage')
        result['status']='PASS_ORIGINAL_EXPORTED_CONSUMER'
    except Exception as error:result['error']=str(error)
    finally:
        result['source_identity_unchanged']=inventory(source)==before
        result['observer_identity_unchanged']=digest(observer)==result['observer_sha256']
        result['go_binary_identity_unchanged']=digest(go)==result['go_binary_sha256']
        result['runner_identity_unchanged']=digest(Path(__file__))==result['runner_sha256']
        result['accepted']=result['status']=='PASS_ORIGINAL_EXPORTED_CONSUMER' and all(result[k] for k in ['source_identity_unchanged','observer_identity_unchanged','go_binary_identity_unchanged','runner_identity_unchanged'])
        (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    raise SystemExit(0 if result['accepted'] else 1)

if __name__=='__main__':main()
