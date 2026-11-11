#!/bin/sh
set -eu
exec "${LIVEKIT_REFS_PYTHON:-python3}" "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/fetch.py" "$@"
