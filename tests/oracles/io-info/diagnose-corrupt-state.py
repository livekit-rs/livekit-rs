#!/usr/bin/env python3
"""Actual fixed Go/Rust IOInfo corrupt-state diagnostic; no error normalization.
Only the known synthetic 0xff parser error text is retained. The coordinator owns
execution and dedicated brokers; first cross failure is never overwritten.
"""
import argparse,importlib.util,json,os,pathlib,subprocess,tempfile,time
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('cross',HERE/'run-cross.py');cross=importlib.util.module_from_spec(spec);spec.loader.exec_module(cross)

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--go',required=True);ap.add_argument('--rust',required=True);ap.add_argument('--output',required=True);ap.add_argument('--dedicated-brokers',action='store_true');args=ap.parse_args()
 if not args.dedicated_brokers:raise RuntimeError('dedicated brokers required')
 report={'status':'RUNNING','scope':'actual IOInfo corrupt external ingress state diagnostic only','results':[],'cleanup_errors':[]}
 urls=[os.environ['IO_REFERENCE_REDIS_URL'],os.environ['IO_RUST_REDIS_URL']]
 with tempfile.TemporaryDirectory(prefix='livekit-rs-io-corrupt-') as directory:
  for label,binary,url in [('go',args.go,urls[0]),('rust',args.rust,urls[1])]:
   redis=cross.Redis(url);child=None
   with open(pathlib.Path(directory)/(label+'.log'),'wb') as log:
    try:
     if redis.execute('DBSIZE') or redis.execute('PUBSUB','CHANNELS','*IOInfo*'):raise RuntimeError('dedicated Redis preflight not empty')
     ready=pathlib.Path(directory)/(label+'.ready');events=pathlib.Path(directory)/(label+'.events')
     child=subprocess.Popen([binary,'--mode','serve','--ready',str(ready),'--events',str(events)],env={**os.environ,'IO_REDIS_URL':url},stdout=log,stderr=log)
     deadline=time.monotonic()+20
     while not ready.exists():
      if child.poll() is not None or time.monotonic()>=deadline:raise RuntimeError('actual readiness failure')
      time.sleep(.02)
     if ready.read_text()!='ACK_READY\n':raise RuntimeError('invalid readiness')
     create=cross.communicate(binary,'call',url,[{'case':'create-synthetic-ingress','op':'CreateIngress','message':{'ingressId':'IN_F06_probe','streamKey':'public-synthetic-F06-stream','roomName':'F06_room_A'}}])
     if create[0]['status']!='OK':raise RuntimeError('actual CreateIngress failed')
     redis.execute('SET','{ingress}_state:IN_F06_probe',b'\xff')
     result=cross.communicate(binary,'call',url,[{'case':'fault-corrupt-ingress-state','op':'GetIngressInfo','message':{'ingressId':'IN_F06_probe'}}])[0]
     report['results'].append({'implementation':label,**cross.controlled_error(result)})
    except Exception:
     report['status']='FAIL';report['failure_stage']=label
    finally:
     if child is not None:
      try:cross.stop(child)
      except Exception:report['cleanup_errors'].append(label+' owned server cleanup failed')
     try:cross.clean(redis)
     except Exception:report['cleanup_errors'].append(label+' owned state cleanup failed')
  if report['status']=='RUNNING':report['status']='DIAGNOSTIC_COMPLETE'
  report['exact_error_equal']=len(report['results'])==2 and all(report['results'][0][k]==report['results'][1][k] for k in ('code','message'))
  report['binary_sha256']={name:cross.sha(pathlib.Path(binary).read_bytes()) for name,binary in [('go',args.go),('rust',args.rust)]}
  report['input_sha256']={p.name:cross.sha(p.read_bytes()) for p in [pathlib.Path(__file__),HERE/'run-cross.py']}
  report['oracle_processes']=cross.PROCESS_AUDIT
  if report['cleanup_errors']:report['status']='FAIL'
  cross.private_write(pathlib.Path(args.output),report)
 print(report['status'])
 return 0 if report['status']=='DIAGNOSTIC_COMPLETE' else 1
if __name__=='__main__':raise SystemExit(main())
