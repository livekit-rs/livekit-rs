#!/usr/bin/env python3
"""Run only by coordinator: fresh owned profile, never an existing user guest."""
import os,pathlib,subprocess,json,hashlib,argparse
p=argparse.ArgumentParser();p.add_argument('--runner',type=pathlib.Path,required=True);a=p.parse_args()
r=pathlib.Path(os.environ['VM_DIR']).resolve();assert r.is_dir() and (r/'verified-image.json').is_file()
assert r.stat().st_uid==os.getuid() and r.stat().st_mode & 0o077==0,'private task-owned VM_DIR required'
owner=r/'.livekit-egress-vm-owner'
assert owner.is_file() and not owner.is_symlink()
assert owner.read_text().strip()=='livekit-rs-f11:noble-20261017-arm64:50e24e1fb79e89116e939d90aa7738f7af48c442fa1bc2510131e0b39c50348e'
for x in ['overlay.qcow2','seed','seed.iso','vars.fd','id_ed25519','id_ed25519.pub','boot.sh','boot-auto4.sh','auto4-profile.json','known_hosts','serial.log','keyscan-ed25519.txt','host-key-verification.json']:
 assert not (r/x).exists() and not (r/x).is_symlink(),x
subprocess.run(['bash',str(a.runner/'prepare-boot.sh')],check=True)
s=(r/'boot.sh').read_text();assert s.count('-smp 2 ')==1
(r/'boot-auto4.sh').write_text(s.replace('-smp 2 ','-smp 4 '));(r/'boot-auto4.sh').chmod(0o700)
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
(r/'auto4-profile.json').write_text(json.dumps({'status':'PREPARED_NOT_STARTED','cpus':4,'memory_mib':4096,'parent_boot_sha256':h(r/'boot.sh'),'boot_sha256':h(r/'boot-auto4.sh'),'reason':'Unchanged original participant cost2; no worker admission override','guest_user':'fixture','private_ssh_key':'id_ed25519','host_key_policy':'verify actual ED25519 fingerprint from owned QEMU serial before writing owned known_hosts','bootstrap':'actual SSH plus cloud-init status --wait; no fixed sleep' },indent=2)+'\n')

(r/'auto4-profile.json').chmod(0o600)
