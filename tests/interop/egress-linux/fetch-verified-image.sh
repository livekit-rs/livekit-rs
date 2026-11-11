#!/bin/bash
# Root coordinator explicitly executes; owned resume does not discard partial image.
set -euo pipefail
umask 077
: "${VM_DIR:?new absolute owned VM_DIR required}"
[[ "$VM_DIR" == /* && "$VM_DIR" != / && ! -L "$VM_DIR" ]]
script_dir="$(cd "$(dirname "$0")" && pwd)"
owner='livekit-rs-f11:noble-20261017-arm64:50e24e1fb79e89116e939d90aa7738f7af48c442fa1bc2510131e0b39c50348e'
if [[ -e "$VM_DIR" ]]; then
 [[ "${RESUME:-0}" == 1 && -f "$VM_DIR/.livekit-egress-vm-owner" && ! -L "$VM_DIR/.livekit-egress-vm-owner" ]]
 [[ "$(cat "$VM_DIR/.livekit-egress-vm-owner")" == "$owner" ]]
else
 mkdir -p "$VM_DIR"
 printf '%s\n' "$owner" > "$VM_DIR/.livekit-egress-vm-owner"
fi
# Refuse symlinked state/output paths, including a replaced GPG directory.
for name in SHA256SUMS SHA256SUMS.new SHA256SUMS.gpg SHA256SUMS.gpg.new cloudimage-signing.asc cloudimage-signing.asc.new gnupg key-fingerprint.txt signature-status.txt fetch-checkpoint.json fetch-checkpoint.json.new verified-image.json verified-image.json.new noble-server-cloudimg-arm64.img noble-server-cloudimg-arm64.img.part noble-server-cloudimg-arm64.manifest noble-server-cloudimg-arm64.manifest.part; do
 [[ ! -L "$VM_DIR/$name" ]]
done
checkpoint(){ /usr/bin/python3 "$script_dir/verify-image.py" checkpoint "$VM_DIR" --state "$1"; }
fetch_metadata(){ curl --fail --location --proto '=https' --tlsv1.2 "$1" -o "$2.new"; mv "$2.new" "$2"; }
base=https://cloud-images.ubuntu.com/noble/20261017
fingerprint=D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81
checkpoint METADATA_PENDING
fetch_metadata "$base/SHA256SUMS" "$VM_DIR/SHA256SUMS"
fetch_metadata "$base/SHA256SUMS.gpg" "$VM_DIR/SHA256SUMS.gpg"
fetch_metadata "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x$fingerprint" "$VM_DIR/cloudimage-signing.asc"
if [[ ! -d "$VM_DIR/gnupg" ]]; then mkdir -m 700 "$VM_DIR/gnupg"; fi
gpg --homedir "$VM_DIR/gnupg" --batch --import "$VM_DIR/cloudimage-signing.asc"
gpg --homedir "$VM_DIR/gnupg" --batch --with-colons --fingerprint "$fingerprint" > "$VM_DIR/key-fingerprint.txt"
grep -q ":$fingerprint:" "$VM_DIR/key-fingerprint.txt"
gpg --homedir "$VM_DIR/gnupg" --batch --status-fd 1 --verify "$VM_DIR/SHA256SUMS.gpg" "$VM_DIR/SHA256SUMS" > "$VM_DIR/signature-status.txt"
grep -q "^\[GNUPG:\] VALIDSIG $fingerprint " "$VM_DIR/signature-status.txt"
/usr/bin/python3 "$script_dir/verify-image.py" metadata "$VM_DIR"
checkpoint SIGNED_PINNED_METADATA_VERIFIED
for name in noble-server-cloudimg-arm64.img noble-server-cloudimg-arm64.manifest; do
 if [[ -f "$VM_DIR/$name" ]]; then
  /usr/bin/python3 "$script_dir/verify-image.py" file "$VM_DIR" --name "$name" --path "$VM_DIR/$name"
  continue
 fi
 checkpoint "DOWNLOADING_$name"
 # Fully downloaded .part after prior interruption can promote without redownload.
 if ! /usr/bin/python3 "$script_dir/verify-image.py" file "$VM_DIR" --name "$name" --path "$VM_DIR/$name.part"; then
  curl --fail --location --proto '=https' --tlsv1.2 --continue-at - "$base/$name" -o "$VM_DIR/$name.part"
 fi
 /usr/bin/python3 "$script_dir/verify-image.py" file "$VM_DIR" --name "$name" --path "$VM_DIR/$name.part"
 mv "$VM_DIR/$name.part" "$VM_DIR/$name"
done
/usr/bin/python3 "$script_dir/verify-image.py" final "$VM_DIR"
chmod 400 "$VM_DIR/noble-server-cloudimg-arm64.img"
checkpoint SIGNATURE_AND_SHA256_VERIFIED_READY_NOT_BOOTED
