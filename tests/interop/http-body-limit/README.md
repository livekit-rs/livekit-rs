# Real HTTP request-body differential

This runner sends the same original 13 requests to two coordinator-owned loopback
servers and compares their actual full responses in memory. It never launches
servers, changes the Pion offer, fabricates expected first-value responses, or
records credentials/SDP in the safe report.

Start fixed Go and the current Rust candidate using separate localhost ports and
`limit.max_api_request_body_size: 256`; the base profiles in
`../websocket-message-size/base-{go,rust}.yaml` provide an isolated example.
Confirm all HTTP/TCP/UDP ports belong to the task before starting. Bind binary
and configuration SHA256 to the execution report. The coordinator must stop and
wait for both actual process exits even if any test fails.

Generate a genuine offer with the locked `../../oracles/pion-body-limit-offer`
reference (see the public oracle staging helper), and pass the unchanged bytes:

    API_KEY=devkey API_SECRET=secret /usr/bin/python3 run-http.py \
      --official http://127.0.0.1:17880 --rust http://127.0.0.1:17980 \
      --limit 256 --offer-file /private/actual-offer.sdp \
      --out /private/new-safe-report.json

A nonzero exit remains a failed comparison. The runner deletes only its exact
owned room names after validating the returned names and verifies absence.
All final paired results, including transport exceptions and cleanup failures,
are retained. A successful component reader or independent Go adapter does not
stand in for this actual public HTTP gate. Disabled/negative/native caps and
other protocols are separate server profiles and remain separately tracked.
