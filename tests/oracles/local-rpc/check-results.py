#!/usr/bin/env python3
"""Identity/integrity only. Does not certify business values or run a service."""
from pathlib import Path
import argparse,hashlib,json

def checked(text,ids):
    rows=[json.loads(line) for line in text.splitlines()]
    if len(rows)!=len(ids):raise ValueError('incomplete/extra source rows')
    seen=set()
    for row,want in zip(rows,ids):
        if not isinstance(row,dict) or not isinstance(row.get('id'),str):raise ValueError('missing/string ID required')
        if row['id'] in seen:raise ValueError('duplicate source ID')
        seen.add(row['id'])
        if row['id']!=want:raise ValueError('unknown/missing/reordered source ID')
    return rows

def self_test(ids):
    valid=[{'id':v} for v in ids]
    encode=lambda rows:'\n'.join(json.dumps(v) for v in rows)
    assert len(checked(encode(valid),ids))==12
    variants=[valid[:-1],valid+[valid[0]],valid[:1]+[valid[0]]+valid[2:],[{'id':'unexpected'}]+valid[1:],[{}]+valid[1:],list(reversed(valid))]
    for rows in variants:
        try:checked(encode(rows),ids)
        except (ValueError,TypeError):pass
        else:raise AssertionError('integrity negative accepted')
    return {'scope':'synthetic ID integrity tests only, not source/runtime compatibility','valid_inventory':1,'negative_inventories':len(variants),'result':'PASS'}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('results',type=Path,nargs='?');parser.add_argument('--self-test',action='store_true');args=parser.parse_args()
    ids=json.loads((Path(__file__).with_name('case-ids.json')).read_text())['ordered_ids']
    if args.self_test:print(json.dumps(self_test(ids),sort_keys=True))
    else:
        if args.results is None:parser.error('actual results path required')
        raw=args.results.read_bytes();rows=checked(raw.decode('utf-8'),ids)
        print(json.dumps({'scope':'actual output case integrity only; consumers must compare values','rows':len(rows),'sha256':hashlib.sha256(raw).hexdigest(),'ids':ids},sort_keys=True))
