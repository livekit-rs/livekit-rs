#!/usr/bin/env python3
"""Assemble exact fixed-Go signed predicates, no compiler/process execution."""
import argparse,json,hashlib,re
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--repository',required=True);p.add_argument('--out',required=True);a=p.parse_args()
repo=Path(a.repository);source=repo/'references/livekit-stable/pkg/config/config.go';text=source.read_text();out=Path(a.out);out.mkdir(parents=True,exist_ok=False)
fields=['MaxRoomNameLength','MaxParticipantIdentityLength','MaxParticipantNameLength'];methods=['CheckRoomNameLength','CheckParticipantIdentityLength','CheckParticipantNameLength']
field_lines=[];method_text=[]
for field in fields:
 line=next(l for l in text[text.index('type LimitConfig struct {'):].splitlines() if re.match(r'\s*'+field+r'\s+int\s',l));field_lines.append(line)
for method in methods:
 start=text.index('func (l LimitConfig) '+method+'(');end=text.index('\n}',start)+2;method_text.append(text[start:end])
(out/'source.go').write_text('// Source-derived from fixed LiveKit config.go, Apache-2.0; original methods unchanged.\npackage main\ntype LimitConfig struct {\n'+'\n'.join(field_lines)+'\n}\n'+'\n\n'.join(method_text)+'\n')
(out/'main.go').write_text(r'''package main
import("encoding/json";"os";"fmt";"strconv")
type Case struct{ID string `json:"id"`;Limit int64 `json:"limit"`;Value string `json:"value"`}
func main(){if strconv.IntSize!=64{panic("declared source profile requires64bit Go int")};var cases []Case;if e:=json.NewDecoder(os.Stdin).Decode(&cases);e!=nil{panic(e)};for _,c:=range cases{l:=LimitConfig{MaxRoomNameLength:int(c.Limit),MaxParticipantIdentityLength:int(c.Limit),MaxParticipantNameLength:int(c.Limit)};b,e:=json.Marshal(map[string]any{"id":c.ID,"bytes":len(c.Value),"room":l.CheckRoomNameLength(c.Value),"identity":l.CheckParticipantIdentityLength(c.Value),"name":l.CheckParticipantNameLength(c.Value)});if e!=nil{panic(e)};fmt.Println(string(b))}}
''')
(out/'go.mod').write_text('module local.test/livekit-signed-name-predicates\n\ngo 1.26.0\n')
(out/'SOURCE.json').write_text(json.dumps({'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'methods':methods,'field_declarations':field_lines,'profile':'fixedGo64bitint','status':'ASSEMBLED_NOT_EXECUTED'},indent=2)+'\n')
