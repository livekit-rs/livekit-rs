#!/bin/bash
# Exact original AAC element, using unmodified official source at installed GST version.
# Never run on host. Root supplies independently reviewed tar + SHA (not a guessed hash).
set -euo pipefail
umask 077
[[ $(uname -m) == aarch64 && -f /etc/cloud/cloud.cfg ]]
: "${SOURCE_ROOT:?}" "${GST_BAD_ARCHIVE:?preprovisioned official gst-plugins-bad tar.xz}" "${FAAC_BUILD_DIR:?fresh private build directory}"
expected_sha=448e32787bc82b586c6cb2f81c9a8ef404fea4f77f25566fe06e597a3f59136b
[[ ${GST_BAD_SHA256:-$expected_sha} == "$expected_sha" ]]
GST_BAD_SHA256=$expected_sha
[[ -f $GST_BAD_ARCHIVE && ! -L $GST_BAD_ARCHIVE ]]
[[ ! -e $FAAC_BUILD_DIR && $FAAC_BUILD_DIR == /home/fixture/fixture/* ]]
[[ $GST_BAD_SHA256 =~ ^[a-f0-9]{64}$ ]]
printf '%s  %s\n' "$GST_BAD_SHA256" "$GST_BAD_ARCHIVE" | sha256sum -c -
# Extract the exact original Dockerfile install-dependencies apt package list;
# DO NOT execute its unrelated dist-upgrade, mutable rustup or cargo-c tail.
mapfile -t packages < <(python3 - "$SOURCE_ROOT/references/egress/build/gstreamer/install-dependencies" <<'PKGS'
import pathlib,sys,re
lines=pathlib.Path(sys.argv[1]).read_text().splitlines()
start=next(i for i,line in enumerate(lines) if line.startswith('apt-get install -y --no-install-recommends '))
p=[]
for line in lines[start+1:]:
 value=line.strip();continued=value.endswith(chr(92));value=value.removesuffix(chr(92)).strip()
 assert re.fullmatch(r'[a-z0-9][a-z0-9+.-]*',value)
 p.append(value)
 if not continued:break
assert 'libfaac-dev' in p
print('\n'.join(p))
PKGS
)
[[ ${#packages[@]} -gt 50 ]]
sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}" meson
# Full actual package/copyright evidence, no fake exact upstream Docker environment.
mkdir "$FAAC_BUILD_DIR"
dpkg-query -W > "$FAAC_BUILD_DIR/packages.tsv"
version=$(pkg-config --modversion gstreamer-1.0)
[[ $version == 1.24.2 ]]
[[ $(basename "$GST_BAD_ARCHIVE") == gst-plugins-bad-$version.tar.xz ]]
# Refuse traversal/symlinks before unmodified source extraction.
python3 - "$GST_BAD_ARCHIVE" "$FAAC_BUILD_DIR" "$version" <<'TAR'
import tarfile,pathlib,sys
with tarfile.open(sys.argv[1]) as t:
 members=t.getmembers();prefix='gst-plugins-bad-'+sys.argv[3]
 for m in members:
  p=pathlib.PurePosixPath(m.name);assert not p.is_absolute() and '..' not in p.parts and p.parts[0]==prefix
  assert m.isfile() or m.isdir(),m.name
 assert len(members)==3913 and sum(m.size for m in members)<256*1024*1024
 t.extractall(sys.argv[2],members=members,filter='data')
TAR
# Rehash actual transferred archive after tar validation/extraction as well.
printf '%s  %s\n' "$GST_BAD_SHA256" "$GST_BAD_ARCHIVE" | sha256sum -c -
cp "$FAAC_BUILD_DIR/gst-plugins-bad-$version/COPYING" "$FAAC_BUILD_DIR/gstreamer-source-COPYING"
[[ -f /usr/share/doc/libfaac0/copyright ]]
cp /usr/share/doc/libfaac0/copyright "$FAAC_BUILD_DIR/libfaac-package-copyright"
prefix="$FAAC_BUILD_DIR/install"
meson setup "$FAAC_BUILD_DIR/build" "$FAAC_BUILD_DIR/gst-plugins-bad-$version" --prefix="$prefix" -Dauto_features=disabled -Dfaac=enabled -Dgpl=enabled -Dtests=disabled -Dexamples=disabled -Ddoc=disabled -Dintrospection=disabled
ninja -j 1 -C "$FAAC_BUILD_DIR/build" install
plugin=$(find "$prefix" -type f -name libgstfaac.so)
[[ -n $plugin && $(printf '%s\n' "$plugin" | wc -l) -eq 1 ]]
printf 'export GST_PLUGIN_PATH_1_0=%q\n' "$(dirname "$plugin")" > "$FAAC_BUILD_DIR/faac.env"
export GST_PLUGIN_PATH_1_0=$(dirname "$plugin")
for element in faac x264enc mp4mux opusenc oggmux;do gst-inspect-1.0 "$element" > "$FAAC_BUILD_DIR/$element.inspect";done
sha256sum "$plugin" "$GST_BAD_ARCHIVE" > "$FAAC_BUILD_DIR/source-and-plugin.sha256"
# Coordinator must source faac.env for worker AND run-handler inherited environment.
