"""Pure parser/fault-selection tests; no socket, Redis, server or child starts."""
import gzip,io,json,threading,unittest
from redis_signal_gate import fields,stream_info,read_resp,RedisSignalGate
def vi(n):
 out=b''
 while n>=128:out+=bytes([(n&127)|128]);n>>=7
 return out+bytes([n])
def blob(n,v):return vi(n*8+2)+vi(len(v))+v
def envelope(stream,compressed=False):
 return blob(1,b'type.googleapis.com/internal.Stream')+blob(2,gzip.compress(stream)if compressed else stream)+(b'\x20\x01'if compressed else b'')
def join(stream_id,room):
 proto_room=blob(1,b'RM_actual_fixture')+blob(2,room.encode())
 signal=blob(1,blob(1,proto_room))
 relay=blob(2,signal)
 message=blob(1,blob(1,b'type.googleapis.com/rpc.RelaySignalResponse')+blob(2,relay))+blob(2,relay)
 return envelope(blob(1,stream_id.encode())+blob(7,message))
class Parser(unittest.TestCase):
 def test_actual_schema_layers_distinguish_first_join_and_new_stream(self):
  value=stream_info(join('ST_one','fixture'))
  self.assertEqual(value['stream_id'],'ST_one');self.assertEqual(value['room'],'fixture');self.assertEqual(value['kind'],'join')
  wire=fields(join('ST_two','fixture'))[2][0]
  self.assertEqual(stream_info(envelope(wire,True))['stream_id'],'ST_two')
 def test_original_resp_binary_bytes_remain_exact(self):
  raw=b'*3\r\n$7\r\nmessage\r\n$11\r\nSignal|x|ST\r\n$5\r\n\x00\r\n\xffx\r\n'
  value,encoded=read_resp(io.BytesIO(raw));self.assertEqual(encoded,raw);self.assertEqual(value[-1],b'\x00\r\n\xffx')
 def test_redis3_map_and_push_are_transparently_framed(self):
  raw=b'%1\r\n+proto\r\n:3\r\n';_,encoded=read_resp(io.BytesIO(raw));self.assertEqual(encoded,raw)
  raw=b'>2\r\n+hello\r\n_\r\n';_,encoded=read_resp(io.BytesIO(raw));self.assertEqual(encoded,raw)
 def test_unknown_non_signal_message_is_not_parsed_or_changed(self):
  gate=object.__new__(RedisSignalGate)
  self.assertFalse(gate.inspect([b'message',b'not-related',b'arbitrary not protobuf'],'response'))
 def test_fault_discards_only_first_target_join_stream_without_success_reply(self):
  gate=object.__new__(RedisSignalGate);gate.lock=threading.Lock();gate.rows=[];gate.blocked=None;gate.target_room=lambda:'target';gate.fault_enabled=lambda:True
  self.assertFalse(gate.inspect([b'message',b'Signal|x|STR',join('ST_other','other')],'response'))
  self.assertTrue(gate.inspect([b'message',b'Signal|x|STR',join('ST_one','target')],'response'))
  self.assertFalse(gate.inspect([b'message',b'Signal|x|STR',join('ST_two','target')],'response'))
  close=envelope(blob(1,b'ST_one')+blob(9,b''))
  self.assertFalse(gate.inspect([b'message',b'Signal|x|STR',close],'response'))
  self.assertEqual(gate.blocked,'ST_one')
 def test_inflate_limit_does_not_expand_arbitrary_gzip_unbounded(self):
  with self.assertRaises(AssertionError):stream_info(envelope(b'a'*(4*1024*1024+1),True))
if __name__=='__main__':unittest.main()
