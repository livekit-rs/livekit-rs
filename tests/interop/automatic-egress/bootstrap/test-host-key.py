"""Pure framing/fingerprint negatives; no SSH/server/VM or key generation."""
import base64,hashlib,importlib.util,pathlib,struct,unittest
p=pathlib.Path(__file__).with_name('verify-host-key.py');spec=importlib.util.spec_from_file_location('hostverify',p);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
class HostKey(unittest.TestCase):
 def setUp(self):
  self.wire=struct.pack('>I',11)+b'ssh-ed25519'+struct.pack('>I',32)+bytes(range(32));self.key=base64.b64encode(self.wire).decode();self.fp=base64.b64encode(hashlib.sha256(self.wire).digest()).decode().rstrip('=');self.serial=f'256 SHA256:{self.fp} root@fixture (ED25519)\n';self.scan=f'[127.0.0.1]:2222 ssh-ed25519 {self.key}\n'
 def test_actual_framing_and_fingerprint_match(self):
  self.assertEqual(mod.verify(self.serial,self.scan,2222),(self.scan,self.fp))
 def test_mismatch_wrongport_and_missing_serial_fail(self):
  for serial,scan,port in [(self.serial,self.scan,2223),('',self.scan,2222),(self.serial.replace(self.fp,'A'*43),self.scan,2222)]:
   with self.assertRaises(ValueError):mod.verify(serial,scan,port)
 def test_multiple_keys_and_invalid_framing_fail(self):
  for scan in [self.scan+self.scan,self.scan.replace('ssh-ed25519','ssh-rsa'),f'[127.0.0.1]:2222 ssh-ed25519 {base64.b64encode(b"bad").decode()}\n']:
   with self.assertRaises(ValueError):mod.verify(self.serial,scan,2222)
if __name__=='__main__':unittest.main()
