Coordinator-only execution (NOT_RUN):

1. Retain github.com/livekit/webrtc-pion/v4@v4.2.18-warp.6 source and MIT license
under references (module checksum h1:UaGYlQ/Ma73LN12xPDlH3OBYNfOEDTVOtHdRZbHg2dY=).
The supplied generator asserts exact peerconnection.go/track_remote.go hashes.
2. generate.py --repository ROOT --pion-source ROOT/references/CHOSEN_PATH --out OWNED_NEW_DIR
3. In generated directory run pinned Go test -run '^TestF58SourceReceiverEligibility$' -v,
with RECONNECT_RECEIVER_CASES pointing at absolute cases.json. Preserve full stdout.
4. normalize_results.py --kind receiver --input ACTUAL_GO_STDOUT --output ACTUAL_TSV
5. RECONNECT_RECEIVER_GO_RESULTS=ACTUAL_TSV cargo test --release -p rtc-media --locked fixed_pion_receiver_callback_vectors -- --ignored

The driver invokes actual source functions, not a copied eligibility algorithm.
Full receiver network behavior is a separate actual ICE/SRTP test. All12 rows
must be present; no parser/error classifications are skipped.
