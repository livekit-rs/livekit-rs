#!/usr/bin/env python3
"""Build a source-exact package-private Pion receiver eligibility oracle.
Only assembles files. Coordinator runs pinned Go; no precomputed expectations.
"""
import argparse,hashlib,json,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--repository',required=True);p.add_argument('--pion-source',required=True);p.add_argument('--out',required=True);a=p.parse_args()
repo=Path(a.repository).resolve();source=Path(a.pion_source).resolve();out=Path(a.out)
assert source.is_relative_to(repo/'references'),'borrowed source must be retained in references'
assert not out.exists(),'use an owned new oracle directory'
expected={'peerconnection.go':'ed16faf84a4bc813d68a8f119c38f56c50dc5e826d9970ff1d727ad2634fb708','track_remote.go':'a96cec031ddd83689b337f252850f4137fb738e38ac90286cd5495231ebb63d5'}
for name,h in expected.items():assert hashlib.sha256((source/name).read_bytes()).hexdigest()==h,name
shutil.copytree(source,out)
# Module cache/reference directories may be read-only. Only the owned scratch
# copy is made writable; retained source bytes remain untouched.
for d in [out,*out.rglob('*')]:
 if d.is_dir():d.chmod(d.stat().st_mode | 0o300)
# Do not compile unrelated upstream test suites. Production Go files remain exact.
for f in out.rglob('*_test.go'):f.unlink()
(out/'f58_receiver_source_test.go').write_text(r'''package webrtc
import("encoding/json";"os";"fmt";"testing";"github.com/pion/logging";"github.com/pion/sdp/v3")
func TestF58SourceReceiverEligibility(t *testing.T){
 raw,e:=os.ReadFile(os.Getenv("RECONNECT_RECEIVER_CASES"));if e!=nil{t.Fatal(e)}
 var cases []struct{Name string `json:"name"`;SDP string `json:"sdp"`};if e=json.Unmarshal(raw,&cases);e!=nil{t.Fatal(e)}
 log:=logging.NewDefaultLoggerFactory().NewLogger("f58-source")
 for _,c:=range cases{
  var desc sdp.SessionDescription;if e=desc.Unmarshal([]byte(c.SDP));e!=nil{t.Fatal(c.Name,e)}
  tracks:=trackDetailsFromSDP(log,&desc)
  // These are actual source trackDetails and actual source conversion, not
  // hand-built Pion receiver expectations. startReceiver's first guard follows.
  eligible:=false
  if len(tracks)>0{params:=trackDetailsToRTPReceiveParameters(&tracks[0]);if len(params.Encodings)>0{first:=params.Encodings[0];eligible=first.SSRC!=0&&first.RID==""}}
  b,_:=json.Marshal(map[string]any{"name":c.Name,"sdp_receiver_eligible":eligible,"incoming_tracks":len(tracks)})
  fmt.Println("F58ROW "+string(b))
 }
}
''')
(out/'F58-INPUTS.json').write_text(json.dumps({'source':str(source),'files':{n:hashlib.sha256((source/n).read_bytes()).hexdigest() for n in ['peerconnection.go','track_remote.go','sdp.go','LICENSE','go.mod']},'execution':'NOT_RUN'},indent=2)+'\n')
print(json.dumps({'status':'ASSEMBLED_NOT_RUN','out':str(out)}))
