"""Coordinator-run stdlib tests; no service/compiler required. Not executed by author."""
import json,unittest,copy,os
from pathlib import Path
import normalize
WORKSPACE = Path(__file__).resolve().parents[3]
ROOT = Path(os.environ.get('LIVEKIT_DOCS_ROOT', WORKSPACE.parent / 'livekit-docs')) / 'server/evidence/2026-10-28'
class StrictSourceSchema(unittest.TestCase):
 def actual(self,kind):return (ROOT/('f170-fixed-go-actual-observations.jsonl' if kind=='permission' else 'f171-fixed-go-actual-observations.jsonl')).read_bytes()
 def test_actual20_and55(self):
  for kind,count in [('permission',20),('settings',55)]:self.assertEqual(len(normalize.validate(kind,self.actual(kind))),count)
 def bad(self,kind,change):
  rows=self.actual(kind).splitlines();change(rows)
  with self.assertRaises((AssertionError,ValueError,KeyError,TypeError)):normalize.validate(kind,b'\n'.join(rows))
 def test_order_count_and_action(self):
  self.bad('permission',lambda r:r.reverse());self.bad('settings',lambda r:r.pop())
  def wrong(r):x=json.loads(r[0]);x['op']='tick';r[0]=json.dumps(x).encode()
  self.bad('settings',wrong)
 def test_duplicate_extra_and_wrong_boolean(self):
  self.bad('settings',lambda r:r.__setitem__(0,r[0][:-1]+b',"entry":true}'))
  def extra(r):x=json.loads(r[0]);x['invented']=True;r[0]=json.dumps(x).encode()
  def wrong(r):x=json.loads(r[0]);x['entry']=1;r[0]=json.dumps(x).encode()
  self.bad('settings',extra);self.bad('settings',wrong)
 def test_nested_complete_fields(self):
  def wrong(r):i=next(i for i,b in enumerate(r)if json.loads(b)['errors']);x=json.loads(r[i]);x['errors'][0]['fatal']='true';r[i]=json.dumps(x).encode()
  self.bad('permission',wrong)
  def extra(r):i=next(i for i,b in enumerate(r)if json.loads(b)['actual_downtrack_setting_calls']);x=json.loads(r[i]);x['actual_downtrack_setting_calls'][0]['extra']=False;r[i]=json.dumps(x).encode()
  self.bad('settings',extra)
if __name__=='__main__':unittest.main()
