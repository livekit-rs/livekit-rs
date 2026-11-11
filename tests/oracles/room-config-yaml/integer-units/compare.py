#!/usr/bin/env python3
"""Exact JSON-value comparison only; no message/float/whitespace normalization."""
import argparse,json,pathlib
p=argparse.ArgumentParser();p.add_argument('go');p.add_argument('rust');p.add_argument('--out',required=True);a=p.parse_args()
def read(path):
 rows=[json.loads(x) for x in pathlib.Path(path).read_text().splitlines() if x.strip()]
 if len({r['name'] for r in rows})!=len(rows):raise ValueError('duplicate name')
 return rows
left,right=read(a.go),read(a.rust)
if [r['name'] for r in left]!=[r['name'] for r in right]:raise ValueError('missing/reordered row')
diff=[{'name':g['name'],'go':g,'rust':r} for g,r in zip(left,right) if g!=r]
report={'status':'PASS' if not diff else 'FAIL','count':len(left),'differences':diff}
pathlib.Path(a.out).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
raise SystemExit(bool(diff))
