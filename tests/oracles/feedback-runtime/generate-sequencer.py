#!/usr/bin/env python3
"""Prepare an executable *actual source* sequencer eligibility oracle (does not run Go).
Only method dependencies and the explicit clock are adapted; no Rust output is read.
Controlled domain: non-sparse valid metadata slots; expired/missing slots explicit.
"""
import argparse,hashlib,json,re
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source-root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
source=a.source_root/'pkg/sfu/sequencer.go';text=source.read_text()
start=text.index('func (s *sequencer) getExtPacketMetas(')
end=text.index('\nfunc ',start+1)
method=text[start:end].replace('time.Now().UnixNano()','oracleTimeNS')
# Types only supply fields/method dependencies used by the unchanged real method.
head='''// Copyright2023 LiveKit, Inc. Apache-2.0; actual fixed sequencer eligibility method.
package main
import("sync";"math";"encoding/json";"os";"fmt")
const(ignoreRetransmission=100;maxAck=3)
var oracleTimeNS int64
// No range mapping in this controlled non-sparse fixture.
type rangeMap struct{}
func(*rangeMap)GetValue(uint64)(uint64,error){return 0,nil}
type packetMeta struct{targetSeqNo uint16;timestamp uint32;lastNack uint32;nacked uint8;codecBytesSlice,ddBytesSlice,actBytes []byte}
type extPacketMeta struct{packetMeta;extSequenceNumber uint64;extTimestamp uint64}
type sequencer struct{sync.Mutex;initialized bool;extHighestSN,extHighestTS,snOffset uint64;size int;meta []packetMeta;snRangeMap *rangeMap;rtt uint32;valid map[int]bool}
func(s *sequencer)getRefTime(t int64)uint32{return uint32(t/1000000)}
func(s *sequencer)isInvalidSlot(slot int)bool{return !s.valid[slot]}
'''
main='''
func main(){ var cases []struct{Name string;Capacity int;RTT uint32;Events []struct{Kind string;MS int64;Seq uint64}};if err:=json.NewDecoder(os.Stdin).Decode(&cases);err!=nil{panic(err)};for _,c:=range cases{s:=sequencer{size:c.Capacity,rtt:c.RTT,meta:make([]packetMeta,c.Capacity),valid:map[int]bool{}};repeated:=uint32(0);for _,e:=range c.Events{oracleTimeNS=e.MS*1000000;accepted:=0;switch e.Kind{case "send":s.initialized=true;s.extHighestSN=e.Seq;slot:=int(e.Seq%uint64(s.size));s.valid[slot]=true;s.meta[slot]=packetMeta{targetSeqNo:uint16(e.Seq),lastNack:s.getRefTime(oracleTimeNS)};case "nack":for _,m:=range s.getExtPacketMetas([]uint16{uint16(e.Seq)}){accepted++;if m.nacked>1{repeated++}};default:panic(fmt.Sprintf("unknown operation%s",e.Kind))};json.NewEncoder(os.Stdout).Encode(map[string]any{"case":c.Name,"kind":e.Kind,"ms":e.MS,"seq":e.Seq,"accepted":accepted,"repeated":repeated})}}}
'''
a.out.mkdir(parents=True,exist_ok=True);(a.out/'main.go').write_text(head+method+main)
(a.out/'go.mod').write_text('module livekit-rs-tests/remote-feedback-sequencer\n\ngo 1.26.0\n')
(a.out/'SOURCE.json').write_text(json.dumps({'commit':'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07','source_path':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'transformations':['literal actual getExtPacketMetas body','time.Now().UnixNano replaced explicit oracleTimeNS','dependency type adapters; non-sparse controlled valid metadata slots'],'execution':'NOT_RUN'},indent=2)+'\n')
