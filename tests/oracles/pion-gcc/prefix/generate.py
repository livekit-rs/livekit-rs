from pathlib import Path
import json,struct
out=Path(__file__).parent
# Packet is independently specified wire, then both runtimes parse it. Symbol 3
# is admitted by Pion Unmarshal but lacks a delta for a genuinely matched history.
def twcc(seq,ns,symbol=1):
 micros=(ns+1000000000)//1000;ref=micros//64000;delta=(micros%64000)//250
 b=bytes([0x8f,205,0,5])+struct.pack('!IIHH',55,44,seq,1)+ref.to_bytes(3,'big')+b'\0'+struct.pack('!H',(symbol<<13)|1)
 return b+(bytes([delta,0]) if symbol==1 else b'\0\0')
def sent(seq,ns):return {'kind':'sent','at_ns':ns,'sequence':seq,'ssrc':44,'header_bytes':20,'payload_bytes':1200,'transport_sequence':seq}
def report(ns,b):return {'kind':'report','at_ns':ns,'wire_hex':b.hex()}
cases=[]
for kind in ['good-bad-good','bad-good','good-malformed-tail','good-only']:
 steps=[]
 for i in range(80):
  ns=i*20000000;steps +=[sent(i,ns),report(ns+50000000,twcc(i,ns))]
 ns=2000000000
 for i in range(3):
  step=sent(80+i,ns+i*20000000);step['payload_bytes']=10000;steps.append(step)
 good=twcc(80,ns);bad=twcc(81,ns+20000000,3);later=twcc(82,ns+40000000)
 packet={'good-bad-good':good+bad+later,'bad-good':bad+good,'good-malformed-tail':good+b'\0','good-only':good}[kind]
 steps.append(report(ns+90000000,packet))
 # Exact followup exposes retained source history/controller prefix; not a reset.
 steps.append(report(ns+110000000,later))
 cases.append({'name':kind,'steps':steps,'challenge_index':len(steps)-2})
(out/'cases.jsonl').write_text(''.join(json.dumps(c,separators=(',',':'))+'\n' for c in cases))
