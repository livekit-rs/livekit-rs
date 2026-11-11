"""Pinned protobuf's exact two-form error contract; no general normalization."""
import importlib.util,pathlib,unittest
spec=importlib.util.spec_from_file_location('cross',pathlib.Path(__file__).with_name('run-cross.py'))
cross=importlib.util.module_from_spec(spec);spec.loader.exec_module(cross)

def row(message,**kw):return {'case':'fault-corrupt-ingress-state','status':'ERROR','code':'unknown','message':message,**kw}

class ErrorComparison(unittest.TestCase):
 def test_both_source_forms_match_and_original_bytes_remain(self):
  for expected in cross.CORRUPT_STATE_ERROR_FORMS:
   for actual in cross.CORRUPT_STATE_ERROR_FORMS:
    left,right=[row(expected)],[row(actual)]
    result=cross.safe_comparison('go-to-rust-faults',left,right)
    self.assertTrue(result['equal'])
    self.assertEqual(result['controlled_corrupt_state_errors'][1]['message'],actual)
    self.assertEqual(result['controlled_corrupt_state_errors'][1]['message_utf8_hex'],actual.encode().hex())
    self.assertEqual(left,[row(expected)]);self.assertEqual(right,[row(actual)])
 def test_other_whitespace_suffix_wrappers_and_text_remain_exact(self):
  exact=cross.CORRUPT_STATE_ERROR_FORMS[0]
  for changed in ('proto:  cannot parse invalid wire-format data','proto:\t cannot parse invalid wire-format data','proto: cannot parse invalid wire-format data!','wrapped: '+exact,'proto:\u00a0other failure'):
   self.assertFalse(cross.safe_comparison('rust-to-go-faults',[row(exact)],[row(changed)])['equal'])
 def test_code_and_other_properties_remain_exact(self):
  a,b=cross.CORRUPT_STATE_ERROR_FORMS
  self.assertFalse(cross.safe_comparison('rust-to-go-faults',[row(a)],[row(b,code='invalid_argument')])['equal'])
  self.assertFalse(cross.safe_comparison('rust-to-go-faults',[row(a,detail='same')],[row(b,detail='different')])['equal'])
 def test_other_case_status_and_comparison_scope_do_not_normalize(self):
  a,b=cross.CORRUPT_STATE_ERROR_FORMS
  for override in ({'case':'other-corrupt-state'},{'status':'OK'}):
   left,_=cross.canonical_comparison_rows('rust-to-go-faults',[row(a,**override)])
   right,records=cross.canonical_comparison_rows('rust-to-go-faults',[row(b,**override)])
   self.assertNotEqual(left,right);self.assertEqual(records,[])
  self.assertFalse(cross.safe_comparison('rust-to-go-callbacks',[row(a)],[row(b)])['equal'])
if __name__=='__main__':unittest.main()
