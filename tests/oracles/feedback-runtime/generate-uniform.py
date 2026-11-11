#!/usr/bin/env python3
"""Prepare the fixed actual Go uniform cluster methods; no compiler execution.
Changes are package/import/logger removal and injected monotonic clock only.
"""
import argparse,hashlib,json,re
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source-root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
f=a.source_root/'pkg/sfu/ccutils/prober.go';raw=f.read_text();source=raw[raw.index('type ProbeClusterId uint32'):]
source=re.sub(r'(?ms)^func [^\n]*MarshalLogObject\([^\n]*\n.*?^}\n','',source)
assert 'zapcore.' not in source
source=source.replace('mono.Now()','oracleNow').replace('time.Since(c.startTime)','oracleNow.Sub(c.startTime)')
head='''// Copyright2023 LiveKit, Inc. Apache-2.0. Actual fixed Cluster methods.
package main
import("fmt";"sync";"math";"time";"encoding/json";"os")
var oracleNow=time.Unix(1000,0)
type ProberListener interface{OnProbeClusterSwitch(ProbeClusterInfo);OnSendProbe(int)}
type listener struct{Requested int}
func(*listener)OnProbeClusterSwitch(ProbeClusterInfo){}
func(l *listener)OnSendProbe(bytes int){l.Requested=bytes}
'''
main='''
func main(){var cases []struct{Name string;Desired,Expected int;DurationNS int64;Ticks []struct{AtNS int64;ActualBytes int}};if err:=json.NewDecoder(os.Stdin).Decode(&cases);err!=nil{panic(err)};for _,row:=range cases {oracleNow=time.Unix(1000,0);l:=new(listener);c:=newCluster(1,ProbeClusterModeUniform,ProbeClusterGoal{DesiredBps:row.Desired,ExpectedUsageBps:row.Expected,Duration:time.Duration(row.DurationNS)},l);c.Start();actual:=0;for _,tick:=range row.Ticks{oracleNow=time.Unix(1000,tick.AtNS);l.Requested=0;sleep:=c.Process();json.NewEncoder(os.Stdout).Encode(map[string]any{"case":row.Name,"at_ns":tick.AtNS,"actual_before":actual,"requested":l.Requested,"desired_bytes":c.info.Goal.DesiredBytes,"count":len(c.buckets),"base_ns":int64(c.baseSleepDuration),"index":c.bucketIdx,"bytes_per_bucket":c.buckets[0].expectedProbeBytesSent,"sleep_ns":int64(sleep)});c.ProbesSent(tick.ActualBytes);actual+=tick.ActualBytes}}}
'''
a.out.mkdir(parents=True,exist_ok=True);(a.out/'main.go').write_text(head+source+main);(a.out/'go.mod').write_text('module livekit-rs-tests/remote-feedback-uniform\n\ngo 1.26.0\n')
(a.out/'SOURCE.json').write_text(json.dumps({'commit':'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07','source':str(f),'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'transformations':['actual Cluster and supporting source declarations from type ProbeClusterId onward','remove diagnostic MarshalLogObject methods/imports','mono.Now/time.Since ->explicit oracleNow'],'execution':'NOT_RUN'},indent=2)+'\n')
