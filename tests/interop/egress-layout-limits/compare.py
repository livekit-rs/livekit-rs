import argparse,json,hashlib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--official',type=Path,required=True);p.add_argument('--rust',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();go=json.loads(a.official.read_text());rs=json.loads(a.rust.read_text())
assert go['profile']==rs['profile']and go['declaredLimits']==rs['declaredLimits'];assert go['status']==rs['status']=='PASS_SCOPED';assert not go['cleanupErrors']and not rs['cleanupErrors']
def compare_projection(x):
 rows=[]
 for row in x['observations']:
  value=json.loads(json.dumps(row));value['http'].pop('rawBodySha256');rows.append(value)
 return rows
left,right=compare_projection(go),compare_projection(rs);equal=left==right
report={'status':'PASS_SCOPED'if equal else'FAIL','profile':go['profile'],'scope':'all public HTTP statuses/content-type and JSON fields, actual participant metadata/name/attributes, with only exact response roomId mapped to verified owned room SID. No raw byte-equality or recording claim.','exact_semantic_equal':equal,'actual_source_sha256':hashlib.sha256(a.official.read_bytes()).hexdigest(),'actual_rust_sha256':hashlib.sha256(a.rust.read_bytes()).hexdigest(),'official':left,'rust':right,'raw_body_sha_equal':[g['http']['rawBodySha256']==r['http']['rawBodySha256']for g,r in zip(go['observations'],rs['observations'])]}
a.output.write_text(json.dumps(report,indent=2)+'\n');assert equal
