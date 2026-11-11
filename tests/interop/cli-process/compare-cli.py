#!/usr/bin/env python3
"""Actual binary CLI differential; never a library-parser substitute.
Requires fixed official binary, Rust binary and independently run Go generated
flag inventory. Invokes only bounded early-exit CLI commands, no test server.
"""
import argparse, collections, hashlib, json, os, re, signal, subprocess, tempfile, sys
ACTIVE_REPORT=None
from pathlib import Path

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def digest(data): return hashlib.sha256(data).hexdigest()
def invoke(binary,args,env):
    process=subprocess.Popen([str(binary),*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,start_new_session=True)
    try: out,err=process.communicate(timeout=8); timeout=False
    except subprocess.TimeoutExpired:
        os.killpg(process.pid,signal.SIGKILL);out,err=process.communicate();timeout=True
    if len(out)>2**20 or len(err)>2**20: raise RuntimeError('unexpected CLI output exceeded fixed resource bound')
    return {'exit':process.returncode,'stdout':out.decode('utf8'),'stderr':err.decode('utf8'),'timed_out':timeout}
def first_diff(a,b):
    aa=a.splitlines();bb=b.splitlines()
    for i in range(max(len(aa),len(bb))):
        if (aa[i] if i<len(aa) else None)!=(bb[i] if i<len(bb) else None):
            return {'line':i+1,'official':aa[i] if i<len(aa) else None,'rust':bb[i] if i<len(bb) else None}
    return None

def identity(text,product):
    # SPEC-0074 C31 decided exact parity: the Rust binary prints the upstream
    # version, so no product-identity substitution remains permitted.
    return text,0

def generated_order(text,names):
    lines=text.splitlines(keepends=True);rows=[];positions=[]
    for i,line in enumerate(lines):
        match=re.match(r'^   --([a-zA-Z0-9_.-]+)(?:[ =,]|$)',line)
        if match and match.group(1) in names:
            # Content is retained byte-for-byte; fail closed if row is not the
            # actual generated flag usage row or wraps into another line.
            if not re.search(r'\s+generated(?:\s|\(|$)',line):raise ValueError('generated flag row lacks exact generated usage')
            rows.append((match.group(1),line));positions.append(i)
    if len(rows)!=len(names) or collections.Counter(x for x,_ in rows)!=collections.Counter(names):
        raise ValueError('missing/duplicated generated flag names')
    if positions!=list(range(positions[0],positions[0]+len(positions))):
        raise ValueError('generated rows are not one contiguous source flag cohort')
    for position,(_,line) in zip(positions,sorted(rows,key=lambda row:row[0])):lines[position]=line
    return ''.join(lines),{'rows':len(rows),'original_order_sha256':digest('\n'.join(x for x,_ in rows).encode()),'complete_rows_multiset_sha256':digest(''.join(sorted(line for _,line in rows)).encode())}

def main():
    global ACTIVE_REPORT
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--official',required=True);parser.add_argument('--rust',required=True);parser.add_argument('--generated-inventory',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    official=Path(args.official).resolve();rust=Path(args.rust).resolve();inventory=json.loads(Path(args.generated_inventory).read_text())
    assert inventory['source_commit']=='e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
    names=[row['name'] for row in inventory['flags']];assert len(names)>250 and len(set(names))==len(names)
    corpus=json.loads((Path(__file__).parent/'cases.json').read_text());assert len(corpus)==158
    result={'status':'RUNNING','scope':'158 actual early-exit binary CLI rows; no consumer/profile/node/drain completeness claim','normalization':{'text_values':'None; product identity must equal upstream exactly (C31)','ordering':'Only actual generated flag complete-row contiguous cohort; all contents/multiplicity and non-generated order exact'},'binary_sha256':{'official':sha(official),'rust':sha(rust)},'inventory_sha256':sha(args.generated_inventory),'cases_sha256':sha(Path(__file__).parent/'cases.json'),'cases':[]}
    ACTIVE_REPORT=result
    with tempfile.TemporaryDirectory(prefix='livekit-rs-f33-cli-') as owned:
        owned=Path(owned);bins={}
        for side,path in [('official',official),('rust',rust)]:
            directory=owned/side;directory.mkdir();link=directory/'livekit-server';link.symlink_to(path);bins[side]=link
        env={'PATH':'/usr/bin:/bin','LC_ALL':'C','TZ':'UTC'}
        versions={side:invoke(path,['--version'],env) for side,path in bins.items()}
        result['bootstrap_versions']=versions
        assert versions['official']=={'exit':0,'stdout':'livekit-server version 1.13.10\n','stderr':'','timed_out':False},'wrong fixed reference version identity'
        assert versions['rust']==versions['official'],'rust --version must equal official exactly (C31)'
        product='1.13.10';result['product_version']=product
        # Preserve actual official self-repeat raw evidence before permitting
        # source-limited map-order normalization. No raw-byte equality claim.
        repeats=[invoke(bins['official'],['help-verbose'],env) for _ in range(3)]
        result['official_verbose_self_repeat']={'observations':repeats,'status':'RUNNING'}
        normalized=[]
        for row in repeats:
            value,meta=generated_order(row['stdout'],names);normalized.append(value);row['generated_order']=meta
        result['official_verbose_self_repeat']={'observations':repeats,'raw_stdout_identical':all(r['stdout']==repeats[0]['stdout'] for r in repeats),'normalized_stdout_identical':len(set(normalized))==1,'exact_exit_stderr_identical':all((r['exit'],r['stderr'],r['timed_out'])==(repeats[0]['exit'],repeats[0]['stderr'],False) for r in repeats)}
        for case in corpus:
            argv=[arg.replace('{OWNED_MISSING}',str(owned/'missing-config.yaml')) for arg in case['args']]
            pair={side:invoke(path,argv,{**env,**case.get('env',{})}) for side,path in bins.items()}
            current={'name':case['name'],'args':argv,'coverage':case['coverage'],'raw':pair,'status':'RUNNING'};result['cases'].append(current)
            raw_equal=pair['official']==pair['rust'];compare={side:dict(obs) for side,obs in pair.items()};normalizations={}
            for side in compare:
                for stream in ['stdout','stderr']:
                    value,count=identity(compare[side][stream],product) if side=='rust' else (compare[side][stream],0)
                    normalizations[f'{side}_{stream}_identity_replacements']=count
                    if case.get('generated_rows') and stream=='stdout':
                        value,meta=generated_order(value,names);normalizations[f'{side}_generated']=meta
                    compare[side][stream]=value
            equal=compare['official']==compare['rust'] and not any(row['timed_out'] for row in pair.values())
            current.update(status='PASS' if equal else 'FAIL',raw_exact_equal=raw_equal,normalized_equal=equal,normalizations=normalizations,first_normalized_difference={stream:first_diff(compare['official'][stream],compare['rust'][stream]) for stream in ['stdout','stderr']})
        selfcheck=result['official_verbose_self_repeat'];allpass=all(row['normalized_equal'] for row in result['cases'])
        result['status']='PASS_SCOPED' if allpass and selfcheck['normalized_stdout_identical'] and selfcheck['exact_exit_stderr_identical'] else 'FAIL'
        result['official_map_order_observed_unstable']=not selfcheck['raw_stdout_identical']
    Path(args.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'status':result['status'],'rows':len(result['cases']),'raw_map_order_unstable':result['official_map_order_observed_unstable']}));return 0 if result['status']=='PASS_SCOPED' else 1
if __name__=='__main__':
    try: code=main()
    except Exception as error:
        result=ACTIVE_REPORT or {'scope':'actual process fixture failed before first observation'}
        result.update(status='FAIL',fixture_failure=f'{type(error).__name__}: {error}')
        if '--output' in sys.argv:
            Path(sys.argv[sys.argv.index('--output')+1]).write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps({'status':'FAIL','fixture_failure':result['fixture_failure']}));code=1
    raise SystemExit(code)
