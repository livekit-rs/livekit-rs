#!/bin/bash
# Coordinator starts genuine fresh owned stack first; this script starts no services.
set -euo pipefail
: "${SOURCE_ROOT:?}" "${EGRESS_WORKER_PID:?real parent pid}" "${PROFILE:?track or participant}" "${SERVER_LABEL:?official or rust}" "${REPORT_DIR:?}"
[[ $(uname -m) == aarch64 && -f /etc/cloud/cloud.cfg ]]
[[ $SERVER_LABEL == official || $SERVER_LABEL == rust ]]
[[ $PROFILE == track || $PROFILE == participant ]]
[[ $REPORT_DIR == /home/fixture/fixture/output/* && -d $REPORT_DIR ]]
export INTEROP_PROJECT="$SOURCE_ROOT/tests/interop" AGENT_SDK_ROOT="$SOURCE_ROOT/tests/agent-sdk"
export LIVEKIT_URL=http://127.0.0.1:17880 LIVEKIT_API_KEY=devkey LIVEKIT_API_SECRET=secret EGRESS_WEBHOOK_PORT=17981
export AUTO_CONFIG_SOURCE=explicit-api AUTO_RECORDING_MODE="$PROFILE"
if [[ $PROFILE == track ]];then ext=ogg;script=run-auto-audio.mjs;else ext=mp4;script=run-auto-participant-av.mjs;fi
export EGRESS_OUTPUT_FILE="$REPORT_DIR/$SERVER_LABEL-$PROFILE.$ext" EGRESS_MEDIA_REPORT="$REPORT_DIR/$SERVER_LABEL-$PROFILE.json"
[[ ! -e $EGRESS_OUTPUT_FILE && ! -e $EGRESS_MEDIA_REPORT ]]
node "$SOURCE_ROOT/tests/interop/automatic-egress/$script"
# No report-only success: genuine child/RTP/ACTIVE/8s/Stop/COMPLETE45s/file/decode/signature gates stay original.
