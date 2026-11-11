#!/usr/bin/env python3
"""Extract the actual pinned PublicationMonitor with only an explicit clock seam."""
from pathlib import Path
import argparse,hashlib,json
p=argparse.ArgumentParser();p.add_argument('--repository',required=True);p.add_argument('--out',required=True);a=p.parse_args();root=Path(a.repository).resolve();out=Path(a.out).resolve();out.mkdir(parents=True,exist_ok=True)
source=root/'references/livekit-stable/pkg/rtc/supervisor/publication_monitor.go';raw=source.read_text();code=raw.replace('package supervisor','package main',1)
assert code.count('time.Now()')==1 and code.count('time.Since(unmutedAt)')==1
code=code.replace('time.Now()','clockNow()').replace('time.Since(unmutedAt)','clockSince(unmutedAt)')
code=code.replace('import (','import (\n "encoding/json"\n "fmt"\n "os"\n "github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"',1)
code+='''
var currentTime=time.Unix(100,0)
func clockNow()time.Time{return currentTime}
func clockSince(at time.Time)time.Duration{return currentTime.Sub(at)}
type action struct {Op string `json:"op"`;At int64 `json:"at_ns"`}
type vector struct {Name string `json:"name"`;Connected bool `json:"connected"`;Actions []action `json:"actions"`}
func main(){var rows []vector;if err:=json.NewDecoder(os.Stdin).Decode(&rows);err!=nil{panic(err)};enc:=json.NewEncoder(os.Stdout);for _,row:=range rows{currentTime=time.Unix(100,0);m:=NewPublicationMonitor(PublicationMonitorParams{TrackID:livekit.TrackID("fixture"),IsPeerConnectionConnected:row.Connected});track:=&typesfakes.FakeLocalMediaTrack{};for i,a:=range row.Actions{currentTime=time.Unix(100,a.At);switch a.Op{case "pending":m.PostEvent(types.OperationMonitorEventAddPendingPublication,nil);case "connect":m.PostEvent(types.OperationMonitorEventPublisherPeerConnectionConnected,true);case "disconnect":m.PostEvent(types.OperationMonitorEventPublisherPeerConnectionConnected,false);case "mute":m.PostEvent(types.OperationMonitorEventSetPublicationMute,true);case "unmute":m.PostEvent(types.OperationMonitorEventSetPublicationMute,false);case "publish":m.PostEvent(types.OperationMonitorEventSetPublishedTrack,track);case "clear":m.PostEvent(types.OperationMonitorEventClearPublishedTrack,track);case "check":default:panic(fmt.Sprintf("unknown action %q",a.Op))};if err:=enc.Encode(map[string]any{"case":row.Name,"index":i,"timeout":m.Check()!=nil,"idle":m.IsIdle(),"has_published":m.hasPublishedTrack()});err!=nil{panic(err)}}}}
'''
(out/'main.go').write_text(code)
# Mirror the fixed server module graph, including its explicit Pion fork
# replacements. Dependency-module replace directives are not inherited by Go.
# Retain its checksums so the coordinator can run with -mod=readonly.
module_source=root/'references/livekit-stable/go.mod'
module_text=module_source.read_text().replace('module github.com/livekit/livekit-server','module source.local/reconnect-supervision',1)
module_text+=f'\nrequire github.com/livekit/livekit-server v0.0.0\nreplace github.com/livekit/livekit-server => {root}/references/livekit-stable\nreplace github.com/livekit/protocol => {root}/references/protocol-stable\n'
(out/'go.mod').write_text(module_text)
(out/'go.sum').write_bytes((root/'references/livekit-stable/go.sum').read_bytes())
(out/'SOURCE.json').write_text(json.dumps({'source':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'generated_sha256':hashlib.sha256(code.encode()).hexdigest(),'seam':'Only package main, extra imports/driver and replacing time.Now()/time.Since with explicit sameclock; all monitor methods/body/error remain fixed source. Real source generated test-fake track is a marker, not physical media proof.','runtime':'NOT_RUN by generator'},indent=2)+'\n')
