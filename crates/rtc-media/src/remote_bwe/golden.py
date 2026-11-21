#!/usr/bin/env python3
"""Execute pinned actual Go policy bodies with injected deterministic clocks.

Coordinator-owned compiler invocation. Originals are only read; temporary source
is self-cleaning. No Rust results are used to generate expected output.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
FILES = [
    'pkg/sfu/bwe/remotebwe/remote_bwe.go',
    'pkg/sfu/bwe/remotebwe/channel_observer.go',
    'pkg/sfu/bwe/remotebwe/nack_tracker.go',
    'pkg/sfu/bwe/remotebwe/probe_controller.go',
    'pkg/sfu/ccutils/trenddetector.go',
    'pkg/sfu/ccutils/probe_regulator.go',
    'pkg/sfu/ccutils/probe_signal.go',
]
COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'

def transformed(source):
    source = re.sub(r'^package \w+$', 'package main', source, flags=re.M)
    source = re.sub(r'^\s*"(?:github.com/[^\"]+|go.uber.org/[^\"]+)"\n', '', source, flags=re.M)
    source = re.sub(r'^var _ bwe.BWE = .*\n', '', source, flags=re.M)
    source = source.replace('logger.Logger', 'goldenLogger').replace('logger.ObjectSlice', 'goldenSlice')
    source = source.replace('zapcore.ObjectEncoder', 'goldenEncoder').replace('ccutils.', '').replace('bwe.', '')
    source = source.replace('mono.Now()', 'goldenNow()').replace('time.Now()', 'goldenNow()').replace('time.Since(', 'goldenSince(')
    return source

STUBS = r'''package main
import "time"
var currentGoldenTime = time.Unix(1000, 0)
func goldenNow() time.Time { return currentGoldenTime }
func goldenSince(t time.Time) time.Duration { return currentGoldenTime.Sub(t) }
type goldenLogger struct{}
func (goldenLogger) Infow(string, ...any) {}
func (goldenLogger) Debugw(string, ...any) {}
func (goldenLogger) Warnw(string, ...any) {}
func goldenSlice[T any](v []T) any { return v }
type goldenEncoder interface {
 AddString(string,string); AddDuration(string,time.Duration); AddFloat64(string,float64)
 AddTime(string,time.Time); AddUint32(string,uint32); AddInt(string,int)
 AddArray(string,any); AddObject(string,any)
}
type BWEType int
const BWETypeRemote BWEType = 0
const DefaultRTT = float64(0.070)
const RTTSmoothingFactor = float64(0.5)
type NullBWE struct{}
type CongestionState int
const (CongestionStateNone CongestionState=iota; CongestionStateEarlyWarning; CongestionStateCongested)
type BWEListener interface { OnCongestionStateChange(CongestionState,CongestionState,int64) }
type ProbeClusterId uint32
const ProbeClusterIdInvalid ProbeClusterId=0
type ProbeClusterGoal struct { AvailableBandwidthBps, ExpectedUsageBps, DesiredBps, DesiredBytes int; Duration time.Duration }
type ProbeClusterResult struct {
 StartTime, EndTime int64
 PacketsProbe,BytesProbe,PacketsNonProbePrimary,BytesNonProbePrimary,PacketsNonProbeRTX,BytesNonProbeRTX int
 IsCompleted bool
}
type ProbeClusterInfo struct { Id ProbeClusterId; CreatedAt time.Time; Goal ProbeClusterGoal; Result ProbeClusterResult }
var ProbeClusterInfoInvalid = ProbeClusterInfo{Id: ProbeClusterIdInvalid}
'''
MAIN = r'''package main
import ("bufio";"encoding/csv";"fmt";"os";"strconv";"time")
func preset(name string) RemoteBWEConfig {
 c:=DefaultRemoteBWEConfig
 if name=="fast" { c.ChannelObserverNonProbe.Estimate.RequiredSamples=3;c.ChannelObserverNonProbe.Estimate.RequiredSamplesMin=3;c.ChannelObserverNonProbe.Estimate.CollapseThreshold=0 }
 if name=="negative" {
 c.ChannelObserverNonProbe.Nack.WindowMinDuration=-1;c.ChannelObserverNonProbe.Nack.WindowMaxDuration=-1
 c.ChannelObserverProbe.Estimate.CollapseThreshold=-1;c.ChannelObserverProbe.Estimate.ValidityWindow=-1
 c.ProbeController.SettleWaitMin=-2*time.Millisecond;c.ProbeController.SettleWaitMax=-time.Millisecond
 c.ProbeController.ProbeRegulator.BaseInterval=-3*time.Millisecond;c.ProbeController.ProbeRegulator.MinDuration=-200*time.Millisecond
 }
 // C71 single-leaf overrides: each preset changes exactly one remote_bwe leaf unless
 // the name carries an explicit pair, so a baseline/variant case pair isolates that leaf.
 switch name {
 case "att0": c.NackRatioAttenuator=0.0
 case "eut05": c.ExpectedUsageThreshold=0.5
 case "npTh0": c.ChannelObserverNonProbe.Estimate.DownwardTrendThreshold=0.0
 case "npMw0": c.ChannelObserverNonProbe.Estimate.DownwardTrendMaxWait=0
 case "npVw0": c.ChannelObserverNonProbe.Estimate.ValidityWindow=0
 case "npNrt09": c.ChannelObserverNonProbe.Nack.RatioThreshold=0.9
 case "prbRs5": c.ChannelObserverProbe.Estimate.RequiredSamples=5
 case "prbRsm4": c.ChannelObserverProbe.Estimate.RequiredSamplesMin=4
 case "prbThM1": c.ChannelObserverProbe.Estimate.DownwardTrendThreshold=-1.0
 case "prbCt10s": c.ChannelObserverProbe.Estimate.CollapseThreshold=10*time.Second
 case "prbVw0": c.ChannelObserverProbe.Estimate.ValidityWindow=0
 case "prbRs5Mw5": c.ChannelObserverProbe.Estimate.RequiredSamples=5;c.ChannelObserverProbe.Estimate.DownwardTrendMaxWait=5*time.Second
 case "prbRs5Mw0": c.ChannelObserverProbe.Estimate.RequiredSamples=5;c.ChannelObserverProbe.Estimate.DownwardTrendMaxWait=0
 case "prbNm0": c.ChannelObserverProbe.Nack.WindowMinDuration=0
 case "prbNx0": c.ChannelObserverProbe.Nack.WindowMaxDuration=0
 case "prbNrt09": c.ChannelObserverProbe.Nack.RatioThreshold=0.9
 case "npRs9": c.ChannelObserverNonProbe.Estimate.RequiredSamples=9
 case "npRsm6": c.ChannelObserverNonProbe.Estimate.RequiredSamplesMin=6
 case "npCt0": c.ChannelObserverNonProbe.Estimate.CollapseThreshold=0
 case "npNm0": c.ChannelObserverNonProbe.Nack.WindowMinDuration=0
 case "npX0": c.ChannelObserverNonProbe.Nack.WindowMaxDuration=0
 case "swNr0": c.ProbeController.SettleWaitNumRTT=0
 case "swMin1": c.ProbeController.SettleWaitMin=1*time.Second
 case "swMax100ms": c.ProbeController.SettleWaitMax=100*time.Millisecond
 case "bi1": c.ProbeController.ProbeRegulator.BaseInterval=1*time.Second
 case "mind1": c.ProbeController.ProbeRegulator.MinDuration=1*time.Second
 case "bf3": c.ProbeController.ProbeRegulator.BackoffFactor=3.0
 case "mi4": c.ProbeController.ProbeRegulator.MaxInterval=4*time.Second
 case "md1": c.ProbeController.ProbeRegulator.MaxDuration=1*time.Second
 case "dif3": c.ProbeController.ProbeRegulator.DurationIncreaseFactor=3.0
 }
 return c
}
func number(s string) int64 { n,e:=strconv.ParseInt(s,10,64);if e!=nil {panic(e)};return n }
func main() {
 file,e:=os.Open(os.Args[1]);if e!=nil {panic(e)};defer file.Close()
 input:=csv.NewReader(bufio.NewReader(file));input.Comment='#';rows,e:=input.ReadAll();if e!=nil {panic(e)}
 origin:=time.Unix(1000,0);var r *RemoteBWE;previous:=""
 for _,f:=range rows {
 if f[0]!=previous { currentGoldenTime=origin;r=NewRemoteBWE(RemoteBWEParams{Config:preset(f[1]),Logger:goldenLogger{}});previous=f[0] }
 at:=number(f[2]);currentGoldenTime=origin.Add(time.Duration(at));a,b,c,d:=number(f[4]),number(f[5]),number(f[6]),number(f[7])
 oldState,oldCapacity,oldSwitched:=r.CongestionState(),r.committedChannelCapacity,r.congestionStateSwitchedAt
 finalized:=false;signal:=ProbeSignal(-1);resultBytes:=0
 switch f[3] {
 case "remb":r.HandleREMB(a,b,uint32(c),uint32(d))
 case "start":r.ProbeClusterStarting(ProbeClusterInfo{Id:ProbeClusterId(a),CreatedAt:currentGoldenTime,Goal:ProbeClusterGoal{DesiredBps:int(b),ExpectedUsageBps:int(c),Duration:r.ProbeDuration()}})
 case "done":r.ProbeClusterDone(ProbeClusterInfo{Id:ProbeClusterId(a),Result:ProbeClusterResult{BytesProbe:int(b),IsCompleted:true}})
 case "rtt":r.UpdateRTT(float64(a)/1e9)
 case "reset":r.Reset()
 case "finalize":var value ProbeSignal;value,_,finalized=r.ProbeClusterFinalize();if finalized {signal=value;resultBytes=r.probeController.pci.Result.BytesProbe}
 case "poll":
 default:panic(f[3])
 }
 changed:=f[3]=="remb" && (oldState!=r.CongestionState() || oldCapacity!=r.committedChannelCapacity || !oldSwitched.Equal(r.congestionStateSwitchedAt))
 fmt.Printf("%s\t%d\t%d\t%d\t%t\t%t\t%d\t%t\t%t\t%d\t%d\n",f[0],at,r.committedChannelCapacity,r.CongestionState(),r.ProbeClusterIsGoalReached(),r.CanProbe(),r.ProbeDuration().Nanoseconds(),changed,finalized,signal,resultBytes)
 }
}
'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path, default=ROOT/'references/livekit-stable')
    parser.add_argument('--go', default='go')
    parser.add_argument('--output', type=Path, default=HERE/'golden.tsv')
    parser.add_argument('--evidence', type=Path, required=True)
    args=parser.parse_args()
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=args.reference,text=True).strip()
    if commit!=COMMIT: raise SystemExit('reference commit mismatch')
    original_hashes={}; transformed_hashes={}
    with tempfile.TemporaryDirectory(prefix='livekit-rs-remote-bwe-golden-') as temporary:
        work=Path(temporary)
        (work/'go.mod').write_text('module golden\n\ngo 1.26.0\n')
        for i,name in enumerate(FILES):
            raw=(args.reference/name).read_bytes();original_hashes[name]=hashlib.sha256(raw).hexdigest()
            code=transformed(raw.decode());transformed_hashes[name]=hashlib.sha256(code.encode()).hexdigest()
            (work/f'policy{i}.go').write_text(code)
        (work/'support.go').write_text(STUBS);(work/'main.go').write_text(MAIN)
        run=subprocess.run([args.go,'run','.',str(HERE/'operations.csv')],cwd=work,text=True,capture_output=True)
        if run.returncode: raise SystemExit(run.stderr)
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text('# Actual pinned Go policy output; generated independently of Rust.\n'+run.stdout)
    evidence={'status':'PASS','reference_commit':COMMIT,'original_sha256':original_hashes,
        'clock_adapted_sha256':transformed_hashes,'operations_sha256':hashlib.sha256((HERE/'operations.csv').read_bytes()).hexdigest(),
        'output_sha256':hashlib.sha256(args.output.read_bytes()).hexdigest(),'rows':len(run.stdout.splitlines()),
        'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'transforms':['original policy bodies retained','package/import namespaces unified','time.Now/mono.Now/time.Since replaced by supplied test clock','logging and collaborator boundary types stubbed; no algorithm substituted'],
        'scope':'Actual Go policy golden, not Rust execution or network/media acceptance.','temporary_directory_removed':True}
    args.evidence.parent.mkdir(parents=True,exist_ok=True);args.evidence.write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps(evidence,indent=2))
if __name__=='__main__':main()
