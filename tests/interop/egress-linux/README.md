# Controlled ARM64 Linux full-stack Egress functional runner

Historical F11 ACTUAL SCOPED PASS. Canonical signature/image hash, owned Linux boot, real
Go/Rust builds, official-reference recording and Rust recording were executed.
Source identities, setup failures and recording failures are retained in F11
evidence. This proves one TrackDirectFile audio leaf, not all Egress outputs.
Coordinator owns processes/submission/cleanup. No host /home/firewall is changed.

## Frozen inputs and minimum added dependencies

Existing host executables observed: QEMU ARM64/qemu-img, macOS hdiutil,
ssh/ssh-keygen, GnuPG, curl. QEMU EDK2 code and variable firmware present under
/opt/homebrew/share/qemu. No Docker/Colima/Lima/global install is needed.

Official Canonical dated Ubuntu24.04 ARM64 image:
https://cloud-images.ubuntu.com/noble/20261017/noble-server-cloudimg-arm64.img
SHA256 50e24e1fb79e89116e939d90aa7738f7af48c442fa1bc2510131e0b39c50348e.
Dated official SHA256SUMS actually fetched during preparation; ARM64 manifest
SHA256820cdc3ef86f90ed996308913718960b6dbd1bccf8cd7cfad4032d0344f60711.
The current 2026-11-05 campaign actually downloaded and verified this image and
its signed manifest; see SPEC0077 linux-runtime receipts. fetch-verified-image.sh
requires official cloud-image signing fingerprint
D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81 (Ubuntu official checksum guide),
verifies exact signer and signed manifest then exact frozen image/manifest hash.
Do not use mutable current URL or merely trust a matching locally supplied sum.
The original distro package license files /usr/share/doc/**/copyright and dated
package manifest are retained; do not invent one blanket image license.

Exact unchanged Egress v1.15.0 commit171423b0b3e7c6531de270b16f1db4f7a6bd2084:
actual .go-version1.26.8 (go.mod1.26.3 is minimum, not Docker toolchain pin).
Official go.dev metadata observed go1.26.8.linux-arm64.tar.gz SHA256
211ffced9dcb9633a55eac6364816ec0ddd951389a740e88fa8b3337971bdda0.
Original .gst-version1.24.13; guest Ubuntu apt1.24 package version must be
recorded, not falsely called exact Docker GStreamer image. Plugin availability
and real link/run are independent gates. Unexpected missing requirements fail.

## Ordered actual execution by coordinator

1. Choose a new private absolute VM_DIR path without whitespace/quotes and an
   owned free localhost SSH_PORT>=1024. Root executes fetch-verified-image.sh.
   Image is read-only. Failed verification stops before boot. For interrupted
   fetch ONLY, rerun with RESUME=1 and the identical VM_DIR: exact ownership
   marker and nonsymlink paths are required. Signed+pinned metadata is rechecked;
   valid complete files are SHA-checked/skipped, image.part uses HTTP Range
   resume and promotes only after full streaming SHA256. Failed downloads are
   retained, never broadly deleted. A corrupt final file fails instead of being
   overwritten. fetch-checkpoint.json distinguishes metadata/download/verified
   steps, always NOT_BOOTED. Curl Range rejection or a fully corrupt partial
   fails explicitly for coordinator inspection; no silent zero-offset restart.
   Final verified-image.json is written only after both exact artifact hashes.
   verify-image.py and archive hashing use bounded1MiB streaming reads compatible
   with system Python3.9; no hashlib.file_digest dependency or whole-image RAM read.
2. Root executes prepare-boot.sh: creates only task SSH key (no personal keys),
   cloud-init NoCloud CIDATA ISO via hdiutil, private EFI variable copy and24GiB
   qcow2 overlay. Launch generated boot.sh as owned tracked process, stdout to
   private serial log. Maximum2CPU/4096MiB, HVF. No package installation on host.
3. Wait actual SSH handshake and `cloud-init status --wait` success, not fixed
   sleep. Verify guest host public key fingerprint from owned serial/cloud-init
   evidence before populating owned known_hosts (no StrictHostKeyChecking=no).
   SSH: ssh -i $VM_DIR/id_ed25519 -o UserKnownHostsFile=$VM_DIR/known_hosts
   -p $SSH_PORT fixture@127.0.0.1. Never reuse ~/.ssh or overwrite host keys.
4. Transfer exact input archive from package-inputs.py (--repo repo --out new
   archive). It includes only Rust workspace/vendor/schema/locks/public fixtures
   and original Egress; excludes every target/node_modules/output/.git/cache.
   File-by-file SHA256 manifest travels alongside; verify archive+all extracted
   files before compiling. Refuses symlinks requiring explicit separate review.
   Transfer this runner separately with its own SOURCE.json hashes.
5. Execute guest-prerequisites.sh inside disposable ARM64 guest ONLY. Installs
   GStreamer compiler/runtime plugins+ffmpeg+nftables via signed Ubuntu apt,
   exact Go1.26.8 checksum and owned /home/egress/tmp. Add redis-server package
   and run dedicated broker instance/config on17842; do not use an unrelated
   preexisting database. Record all actual package versions/licenses. Guest
   bootstrap network is allowed only for authenticated package/toolchain setup.
6. Execute guest-language-toolchains.sh: exact Rust1.99.0 official distribution
   SHA5a30ce742be0835d9b23fc862db5cbbbc71464e1c9b1fca22917e10e4ba32a92
   installs only rustc/cargo/standard library under owned prefix. Exact
   Node24.22.1 LTS ARM64 SHA
   dbabf7a1cbf947e6059ffad2c27b80041db97dea1e553b62afdee7f13fca336e
   supports unchanged Agent SDK. Both checksums actually fetched from official
   HTTPS metadata during preparation; no toolchain downloaded/executed and no
   toolchain GPG verification is claimed. Set owned RUSTUP_HOME/CARGO_HOME paths
   listed in script. No rustup/latest or host-global install is used.
7. Execute guest-build.sh with SOURCE_ROOT/RUSTUP_HOME/CARGO_HOME. It requires
   actual rustc1.99.0, Go1.26.8, single-job nonincremental low-debug dev build.
   npm ci preserves committed locks and installs real Linux FFI optional native
   artifact; do not --ignore-scripts or substitute macOS binaries. Real produced
   Linux Egress/Rust binaries SHA256 stored. Expected basename egress on owned
   PATH: original worker uses exec.Command("egress","run-handler") cwd `/`.
8. All Redis/main/worker/native publisher must run INSIDE guest loopback. Thus
   actual Rust127.0.0.1 ICE candidate is genuinely reachable. Host WS/Redis
   connectivity alone is not proof of ICE; no10.0.2.2 candidate trick is used.
   Start broker then main then worker with separate private YAML. Real endpoints
   can use main17880, RTC TCP17881/UDP17882, Redis17842, worker health19080.
   Root must wait actual readiness and preserve failures, no fixed sleep only.
9. AFTER all dependency downloads, execute guest-lock-network.sh. It permits
   only loopback/new flows and established SSH responses, blocks new external,
   host-LAN and hostgateway flows. All actual acceptance task endpoints remain
   local. Inspect rules and test real local readiness before media. No host
   firewall/system configuration is modified.
10. Set INTEROP_PROJECT=$SOURCE_ROOT/livekit-rs-server/tests/interop,
    AGENT_SDK_ROOT=$SOURCE_ROOT/livekit-rs-server/tests/agent-sdk,
    LIVEKIT_URL=http://127.0.0.1:17880, EGRESS_WORKER_PID actual parent,
    EGRESS_OUTPUT_FILE=/home/fixture/fixture/output/owned-unique.ogg,
    EGRESS_MEDIA_REPORT=new private output. Run actual run-track-file.mjs.

## Actual acceptance semantics (executed against Go and Rust)

Unchanged ServerSDK2.19.2 creates room, unchanged rtc-node1.2.1 publishes
48k mono PCM440Hz,960samples/frame, paced by the unchanged SDK native bounded AudioSource queue. Actual publisher RTP packet+byte stats must
advance. StartTrackEgress gets real TR SID and **new DirectFileOutput**; a string
would select websocketUrl in official SDK and is NOT a filename.

Real original worker must launch actual child. Actual IOInfo state becomes
ACTIVE; continue real media8s; public StopEgress, wait real COMPLETE with times,
no errors, one positive FileInfo. Actual child exits. File in guest must exist,
size match FileInfo and hash; ffprobe must find one Opus48000 audio stream/Ogg
and duration>=5s. Actual ffmpeg decodes >=240000 samples; RMS>100 and440Hz
projection power>0.5 prove supplied sound, not empty headers or fake callback.
Actual signed WebhookReceiver must authenticate egress_started+egress_ended.
No canned egress file or direct synthetic callback may satisfy assertions.

Local storage source proof: pkg/config/output_file.go builds tmp LocalFilepath;
pkg/pipeline/sink/uploader/uploader.go chooses storage.NewLocal without cloud
configuration. Thus output is a real guest-local DirectFile, not an invented
returned URL. No S3/MinIO process is required for this narrow leaf.

## Cleanup and report limits

Fixture stops only its owned Egress by actual API, disconnects/ closes audio
handles/official rtc.dispose(), deletes own room, closes owned webhook listener.
Safe report hashes/counts/tone/codec/status only; no IDs/JWT/credentials/raw
SDP/file URL/log bodies. Success persists proof then removes generated recording;
failure recording stays private for root diagnosis/cleanup. Root waits handler,
SIGTERM worker/main/broker real exits and records results. Run guest cargo clean
and remove task target dirs. Poweroff guest and wait actual QEMU exit; then
remove only owned overlay/seed/EFI vars/private SSH keys/recordings/logs. Preserve
registered original reference inputs; downloaded image may be retained ONLY if
root deliberately registers verified reference image, otherwise remove it too.

The fresh current VM has booted; original Go/Egress and composed Rust08 plus
libc::c_char portability fix have actually compiled. These builds alone do not
certify fresh external worker media, webhook, decoded file or cleanup acceptance.
Use the new guest-only run-owned-stack.py to execute original Go and Rust
sequentially against a separately owned fresh Redis/main/worker stack for each.
Its explicit-track, auto-track and auto-room profiles retain real recording
content gates and actual process exit0/port release. This is functional-only, not performance
acceptance. Original SDK congestion/accuracy standards are untouched. Exact
Go-control same fixture remains a separate future controlled run, not assumed
from Rust-positive result or protocol-only oracle.

## Static correction checkpoint

Execution-before-use review corrected the draft manifest download-name mismatch
(image.manifest vs actual noble-server-cloudimg-arm64.manifest) and Python3.9
API availability. All six shell scripts passed bash -n; both Python scripts
passed AST parsing targeting3.9. No shell body/VM/download/verification runtime
was executed during these checks; actual signature/boot/guest/media remains
NOT_RUN. Source lock below hashes the corrected exact inputs.

## Actual setup corrections and immutable assertions

The upstream Go binary embeds cmd/server/templates. Build the real original
source template using pnpm10.18.3/frozen lock, as in guest-build.sh; never create
an empty placeholder. Required GStreamer headers include bad-plugin development
headers. Actual Ubuntu packages report1.24.2, separately from upstream1.24.13.
Host storage exhaustion interrupted a guest apt transaction; owned duplicate
installer cleanup/discard and guest dpkg recovery restored a clean package audit.
No production sources or locks changed; all1254 original snapshot files still hash
exactly after build. Source627 Rust/manifest/protobuf inputs match9fdc0c1.

The first publisher added a relative20ms sleep after asynchronous captureFrame;
both official and Rust whole-file tone assertions failed. Preserve those failures.
The corrected publisher uses native AudioSource bounded backpressure directly;
all PCM values/rate/frame size, SDK/worker, >=5s/240000samples/RMS>100/tone>0.5,
8s active capture, original lifecycle deadlines and signature checks are unchanged.
Official and Rust now pass near0.998 whole-file tone power. Specific PLC mechanism
is inferred from source timing and is not independently traced in this evidence.

The official control was source-built separately from fixed Serverv1.13.10:
GOTOOLCHAIN=go1.26.3 GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -mod=readonly
-p 1 -o owned-reference ./cmd/server. Run one main process at a time against the
same loopback ports/worker. Official and Rust binary identities are distinct and
both recorded. No host-only ICE reachability is substituted for guest loopback.

## Additional automatic per-track recording campaign (NOT_RUN)

`run-auto-track-file.mjs` is an independent original-worker acceptance profile.
It does not modify or replace the explicit `run-track-file.mjs` profile or claim
its historical PASS applies to automatic recording.

Run sequentially against the fixed original Go and the selected immutable Rust
binary with the same unchanged original Egress executable. Prerequisites, exact
ServerSDK2.19.2 and rtc-node inputs, real Linux worker PID and root-owned service
lifecycle are the same as above. Set `EGRESS_AUTO_DIR` to a **new** direct child
of `/home/fixture/fixture/output/` named `auto-<unique-attempt>`;
`EGRESS_MEDIA_REPORT` must also be a new safe receipt path. The runner refuses
existing directories and receipts. `EGRESS_OUTPUT_FILE` is not used here.

```sh
EGRESS_AUTO_DIR=/home/fixture/fixture/output/auto-unique-attempt \
EGRESS_MEDIA_REPORT=/home/fixture/fixture/output/auto-unique-attempt.json \
node /absolute/path/run-auto-track-file.mjs
```

Only public CreateRoom with `RoomEgress.tracks` and an actual PCM/RTP publication
can start the job. The runner contains no explicit StartEgress call. It requires
exactly one actual job referring to the published Track SID, original worker
handler PID, actual ACTIVE/Stop/COMPLETE callbacks and child exit, then an actual
file whose size/metadata match, Ogg/Opus48k via ffprobe, at least240000 genuinely
decoded samples, whole-file RMS>100 and440Hz power ratio>0.5. These are the same
content thresholds as the explicit original-worker profile. A queue reply, a
recording label or a placeholder file cannot satisfy this profile.

Fixed AutoTrackEgress has no per-request webhook field; the source automatic
launcher also does not copy `disable_manifest` into its DirectFileOutput. This
profile therefore does not fabricate per-request webhook evidence or disable
the actual manifest. It hashes all real artifacts in the freshly owned output
directory, then removes them on PASS. FAIL artifacts remain available for the
coordinator to inspect and persist necessary safe evidence before final cleanup.
The report is written RUNNING before work begins, preserving interrupted-run
semantics. It stores no raw IDs, room names, credentials, URLs or PCM. The runner
never stops the coordinator's worker/server/Redis and never cleans shared builds.

A successful automatic-track run closes only this concrete output/content
leaf. Automatic participant AV/composite, other storage/output types, worker
recovery and SIP/Ingress campaigns remain independent requirements.
