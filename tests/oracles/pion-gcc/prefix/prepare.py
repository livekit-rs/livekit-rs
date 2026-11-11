#!/usr/bin/env python3
from pathlib import Path
import argparse,hashlib,json,shutil,re
p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--repo',type=Path,default=Path('/Users/zoujiaqing/projects/livekit-rs'));a=p.parse_args();here=Path(__file__).parent;source=a.repo/'references/go-modules/pion-interceptor-v0.1.49';server=a.repo/'references/livekit-stable';assert not a.out.exists()
lock=json.loads((here/'interceptor-source-lock.json').read_text())
for name,digest in lock['files_sha256'].items():
 assert hashlib.sha256((source/name).read_bytes()).hexdigest()==digest,'SOURCE_DRIFT '+name
manifest=json.loads((here/'SOURCE.json').read_text())
for name,digest in manifest['fixed_server_inputs'].items():
 assert hashlib.sha256((server/name).read_bytes()).hexdigest()==digest,'SERVER_DRIFT '+name
shutil.copytree(source,a.out)
mod=(server/'go.mod').read_text().replace('module github.com/livekit/livekit-server\n','module github.com/pion/interceptor\n',1);mod,n=re.subn(r'(?m)^\s*github.com/pion/interceptor v0\.1\.49\s*\n','\n',mod);assert n==1;(a.out/'go.mod').write_text(mod);(a.out/'go.sum').write_text('\n'.join(sorted(set((server/'go.sum').read_text().splitlines())|set((source/'go.sum').read_text().splitlines())))+'\n')
changes=[]
def edit(name,fn):
 f=a.out/'pkg/gcc'/name;before=f.read_bytes();s=fn(before.decode());f.write_text(s);changes.append({'path':str(f.relative_to(a.out)),'before':hashlib.sha256(before).hexdigest(),'after':hashlib.sha256(f.read_bytes()).hexdigest()})
for name in ['adaptive_threshold.go','overuse_detector.go','rate_controller.go','loss_based_bwe.go','send_side_bwe.go']:
 edit(name,lambda s:s.replace('time.Now','prefixNow').replace('time.Since','prefixSince'))
# A barrier at batch end and group start chooses one actual possible source
# scheduling order; locals/history and every source calculation remain untouched.
def barrier(s,begin,end):
 marker='for acks := range in {';assert s.count(marker)==1;start=s.index(marker)+len(marker);depth=1;i=start
 while depth:
  if s[i]=='{':depth+=1
  elif s[i]=='}':depth-=1
  i+=1
 return s[:start]+begin+s[start:i-1]+end+s[i-1:]
edit('arrival_group_accumulator.go',lambda s:barrier(s,'\n        <-prefixRateDone\n','\n        prefixGroupDone <- struct{}{}\n'))
edit('rate_calculator.go',lambda s:barrier(s,'','\n        prefixRateDone <- struct{}{}\n'))
def sync(s):
 old='d.ackRatePipe <- acks';assert s.count(old)==1;return s.replace(old,old+'\n    <-prefixGroupDone')
edit('delay_based_bwe.go',sync);shutil.copyfile(here/'oracle_test.go',a.out/'pkg/gcc/prefix_wire_source_test.go');(a.out/'PREFIX_SOURCE_STAGE.json').write_text(json.dumps({'status':'STAGED_NOT_RUN','changes':changes,'schedule':'source goroutines rates before groups, complete each feedback before next packet','no_algorithm_replacement':True},indent=2)+'\n')
print('STAGED_NOT_RUN')
