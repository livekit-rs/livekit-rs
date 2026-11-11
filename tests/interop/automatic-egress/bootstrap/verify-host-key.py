#!/usr/bin/env python3
"""Verify actual scanned guest key against tracked QEMU serial, not trust-on-first-use."""
import argparse,base64,hashlib,json,os,pathlib,re,struct

def verify(serial, keyscan, port):
    candidates=set(re.findall(r'\bSHA256:([A-Za-z0-9+/]{43})=?[^\r\n]*(?:ED25519|ed25519)',serial))
    if len(candidates)!=1:raise ValueError('expected one actual ED25519 fingerprint in owned serial')
    lines=[s.split() for s in keyscan.splitlines() if s.strip() and not s.lstrip().startswith('#')]
    if len(lines)!=1 or len(lines[0])!=3:raise ValueError('expected exactly one ED25519 keyscan row')
    host,kind,value=lines[0]
    if host!=f'[127.0.0.1]:{port}' or kind!='ssh-ed25519':raise ValueError('wrong keyscan endpoint/type')
    wire=base64.b64decode(value,validate=True)
    if len(wire)!=51 or wire[:4]!=struct.pack('>I',11) or wire[4:15]!=b'ssh-ed25519' or wire[15:19]!=struct.pack('>I',32):raise ValueError('invalid ED25519 public key framing')
    fingerprint=base64.b64encode(hashlib.sha256(wire).digest()).decode().rstrip('=')
    if fingerprint not in candidates:raise ValueError('actual serial/keyscan fingerprint mismatch')
    return f'{host} {kind} {value}\n',fingerprint

def main():
    p=argparse.ArgumentParser();p.add_argument('--vm-dir',type=pathlib.Path,required=True);p.add_argument('--serial',type=pathlib.Path,required=True);p.add_argument('--keyscan',type=pathlib.Path,required=True);p.add_argument('--port',type=int,required=True);a=p.parse_args()
    d=a.vm_dir.resolve();assert 1024<=a.port<=65535
    assert d.is_dir() and d.stat().st_uid==os.getuid() and d.stat().st_mode&0o077==0
    assert (d/'.livekit-egress-vm-owner').read_text().strip()=='livekit-rs-f11:noble-20261017-arm64:50e24e1fb79e89116e939d90aa7738f7af48c442fa1bc2510131e0b39c50348e'
    assert a.serial.is_file() and not a.serial.is_symlink() and a.keyscan.is_file() and not a.keyscan.is_symlink()
    line,fingerprint=verify(a.serial.read_text(errors='strict'),a.keyscan.read_text(errors='strict'),a.port)
    dest=d/'known_hosts';receipt=d/'host-key-verification.json'
    assert not dest.exists() and not dest.is_symlink() and not receipt.exists() and not receipt.is_symlink()
    fd=os.open(dest,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:f.write(line)
    receipt.write_text(json.dumps({'status':'ACTUAL_GUEST_SERIAL_AND_SCANNED_HOST_KEY_MATCH','algorithm':'ssh-ed25519','fingerprint':'SHA256:'+fingerprint,'serial_sha256':hashlib.sha256(a.serial.read_bytes()).hexdigest(),'keyscan_sha256':hashlib.sha256(a.keyscan.read_bytes()).hexdigest(),'known_hosts_sha256':hashlib.sha256(dest.read_bytes()).hexdigest()},indent=2)+'\n');receipt.chmod(0o600)
if __name__=='__main__':main()
