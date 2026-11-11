#!/usr/bin/env python3
"""New, explicitly initialized empty-map profile; original CLI32/37 unmodified.
Loads the frozen original runner functions; changes only JWT-mode global argv.
Actual private bytes are compared by original implementation before safe reporting.
"""
import argparse, hashlib, importlib.util, json, os, pathlib, sys, tempfile
DEFAULT_SOURCE=str(pathlib.Path(__file__).with_name('run-expanded.py'))
EXPECTED_SOURCE_SHA256='2695d5951c85735b3c61321c78f99d9408bf30629b0443ef9327960397c893de'

def digest(path):
    obj=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda:f.read(131072),b''):obj.update(chunk)
    return obj.hexdigest()

def main():
    parser=argparse.ArgumentParser(add_help=False);parser.add_argument('--source',default=DEFAULT_SOURCE)
    opt,remaining=parser.parse_known_args()
    source=pathlib.Path(opt.source).resolve()
    if digest(source)!=EXPECTED_SOURCE_SHA256:raise SystemExit('original frozen runner source guard mismatch')
    # Parse only to locate report; retain original parsing/validation for all args.
    locate=argparse.ArgumentParser(add_help=False);locate.add_argument('--output',required=True)
    target,unused=locate.parse_known_args(remaining)
    spec=importlib.util.spec_from_file_location('f72_original_cli37',source);original=importlib.util.module_from_spec(spec);spec.loader.exec_module(original)
    original_cases=original.f61_cases;changed=[]
    def initialized_cases(*args,**kwargs):
        rows=original_cases(*args,**kwargs)
        assert len(rows)==37,'original37 inventory changed'
        result=[]
        for name,argv,env,mode in rows:
            if mode=='jwt':
                changed.append(name)
                argv=[b'--config-body',b'keys: {}\n']+argv
            result.append((name,argv,env,mode))
        return result
    original.f61_cases=initialized_cases
    code=1
    with tempfile.TemporaryDirectory(prefix='f72-private-cli-report-') as temp:
        private=pathlib.Path(temp)/'report.json';rewritten=[];index=0
        while index<len(remaining):
            if remaining[index]=='--output':rewritten.extend(['--output',str(private)]);index+=2
            elif remaining[index].startswith('--output='):rewritten.append('--output='+str(private));index+=1
            else:rewritten.append(remaining[index]);index+=1
        saved=sys.argv
        try:
            sys.argv=[str(source)]+rewritten
            try:original.main()
            except SystemExit as exit:code=exit.code
        finally:sys.argv=saved
        if not private.exists():raise SystemExit(code)
        report=json.loads(private.read_text())
        report['scope']='F72 initialized empty-map profile: same original37 comparisons, only JWT global argv gets explicit keys: {}; NOT original F61 PASS'
        report['source_runner']={'path':str(source),'sha256':EXPECTED_SOURCE_SHA256,'functions_reused_without_changes':['execute','jwt_semantics','original_24_cases','cases','f61_cases original row construction','canonical_identity','create_owned_file','main actual comparisons and setup/cleanup']}
        report['profile_precondition']={'global_argv_prefix_for_jwt_only':['--config-body','keys: {}\n'],'credential_entries_in_config':0,'actual_file_read_required_by_len_zero':True,'changed_case_names':changed,'non_jwt_cases_unchanged':True,'historical_cli32_cli37_results_modified':False}
        # Preserve computed byte comparisons and cryptographic validation results;
        # discard only credential-bearing raw output serialization in safe report.
        for row in report['cases']:
            for raw in row.get('raw',{}).values():
                for stream in ['stdout','stderr']:
                    if isinstance(raw.get(stream),dict):
                        data=raw[stream];raw[stream]={k:data[k] for k in ['bytes','sha256'] if k in data}
        report['privacy']='Actual full outputs compared in original runner; safe report stores stdout/stderr lengths/hashes only, never generated JWT output encodings. Synthetic argv/ENV byte encodings remain reconstructable. Temporary private report removed.'
        if report.get('profile')=='linux' and (report.get('configured_cases')!=37 or report.get('executed_cases')!=37 or report.get('not_run_platform') or report.get('not_run_setup')):
            report['status']='FAIL_REQUIRED_37';code=1
        pathlib.Path(target.output).write_text(json.dumps(report,indent=2)+'\n')
    return code if isinstance(code,int) else 1
if __name__=='__main__':raise SystemExit(main())
