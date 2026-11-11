#!/bin/bash
# Root coordinator executes only after verified image and host load checks.
set -euo pipefail
umask 077
: "${VM_DIR:?owned verified VM_DIR}"
: "${SSH_PORT:?dedicated localhost TCP port}"
case "$SSH_PORT" in *[!0-9]*|'') exit 2;; esac
[[ "$SSH_PORT" -ge 1024 && "$SSH_PORT" -le 65535 ]]
test -f "$VM_DIR/verified-image.json"
test ! -e "$VM_DIR/overlay.qcow2"
image="$VM_DIR/noble-server-cloudimg-arm64.img"
# Independent pre-use check; QEMU must never alter the verified input image.
[[ "$(shasum -a 256 "$image" | cut -d' ' -f1)" == 50e24e1fb79e89116e939d90aa7738f7af48c442fa1bc2510131e0b39c50348e ]]
ssh-keygen -q -t ed25519 -N '' -f "$VM_DIR/id_ed25519"
mkdir "$VM_DIR/seed"
cat > "$VM_DIR/seed/meta-data" <<'META'
instance-id: livekit-rs-owned-egress-fixture
local-hostname: livekit-egress-fixture
META
cat > "$VM_DIR/seed/user-data" <<CLOUD
#cloud-config
ssh_pwauth: false
disable_root: true
users:
  - name: fixture
    shell: /bin/bash
    groups: [sudo]
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock_passwd: true
    ssh_authorized_keys:
      - $(cat "$VM_DIR/id_ed25519.pub")
package_update: false
CLOUD
hdiutil makehybrid -o "$VM_DIR/seed.iso" "$VM_DIR/seed" -iso -joliet -default-volume-name CIDATA
cp /opt/homebrew/share/qemu/edk2-arm-vars.fd "$VM_DIR/vars.fd"
qemu-img create -f qcow2 -F qcow2 -b "$image" "$VM_DIR/overlay.qcow2" 24G
/usr/bin/python3 - "$VM_DIR" "$SSH_PORT" <<'PYBOOT'
import pathlib,sys,shlex
r=pathlib.Path(sys.argv[1]).resolve();port=int(sys.argv[2]);args=['/opt/homebrew/bin/qemu-system-aarch64','-machine','virt,accel=hvf','-cpu','host','-smp','2','-m','4096','-drive','if=pflash,format=raw,readonly=on,file=/opt/homebrew/share/qemu/edk2-aarch64-code.fd','-drive',f'if=pflash,format=raw,file={r}/vars.fd','-drive',f'if=virtio,format=qcow2,file={r}/overlay.qcow2','-drive',f'if=virtio,format=raw,readonly=on,file={r}/seed.iso','-netdev',f'user,id=n0,hostfwd=tcp:127.0.0.1:{port}-:22','-device','virtio-net-pci,netdev=n0','-display','none','-serial','stdio','-monitor','none']
assert ',' not in str(r),'QEMU drive path must not contain option delimiter'
(r/'boot.sh').write_text('#!/bin/bash\nexec '+shlex.join(args)+'\n')
PYBOOT
chmod 700 "$VM_DIR/boot.sh"
printf '%s\n' 'Prepared only. Coordinator starts boot.sh in an owned tracked process, waits real SSH+cloud-init, and owns stop/cleanup.'
