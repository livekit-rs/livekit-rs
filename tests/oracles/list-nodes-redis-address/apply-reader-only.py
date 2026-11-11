from pathlib import Path
import argparse,hashlib,json
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--apply',action='store_true');a=p.parse_args();here=Path(__file__).resolve().parent;rel="livekit-rs-server/crates/livecloud/src/redis_address.rs";f=a.root/rel;before=f.read_bytes();assert hashlib.sha256(before).hexdigest()=="89580d5d71b89768cfb57bfa659770a3fade03dd0e765f3298010ce9e15d99cc",'not exact immutable v1 helper';payload=(here/'redis_address.rs').read_bytes();manifest=json.loads((here/'SOURCE.json').read_text());row=next(t for t in manifest['targets']if t['path']==rel);assert hashlib.sha256(payload).hexdigest()==row['payload_sha256'];assert before[:before.index(b'#[cfg(test)]')]==payload[:payload.index(b'#[cfg(test)]')],'runtime source changed'
if a.apply:f.write_bytes(payload)
print('READER_ONLY_APPLIED'if a.apply else'READER_ONLY_PREFLIGHT')
