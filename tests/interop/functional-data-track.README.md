# Optional official SDK DataTrack functional profile

Run against an already started server with `enable_data_tracks: true`:

```sh
LIVEKIT_URL=ws://127.0.0.1:7880 node functional-data-track.mjs
```

Use the existing `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` environment variables when needed. They are used only to mint test tokens and are not written into evidence. The coordinator starts/stops the server and decides when browser work may run.

The default matrix is single and dual peer connections, each plain and E2EE. Narrow an investigation explicitly with `FUNCTIONAL_PC=single|dual|both` and `FUNCTIONAL_E2EE=true|false|both`; evidence records the selected cases. Failure exits nonzero. Output is `output/functional-data-track.json`, which must be archived with the actual server/SDK/source identity before normal task cleanup. No existing congestion runner, profile or threshold is imported or modified.

The fixture serves the installed, locked official `livekit-client`2.23.0 UMD and official E2EE worker unchanged. It uses public Room, LocalParticipant.publishDataTrack, LocalDataTrack.tryPush/flush/unpublish and RemoteDataTrack.subscribe APIs. Browser RTCPeerConnection/channel wrappers only observe standard platform calls and forward their original arguments and bytes; they do not implement another signaling or RTC client.

Assertions cover two-way exact payload digests at37/4096/32768bytes, public bigint user-timestamp extensions, independent unordered zero-retransmit `_data_track` channels, actual E2EE extension presence on observed serialized packets and successful decrypted application bytes, publisher ACL revocation/regrant, unpublish acknowledgement/removal and rejected later push. Larger frames exercise the official SDK packetizer; recorded evidence does not include keys, IVs, complete SDP, auth URLs or payload bytes. E2EE uses a public fixture passphrase solely in browser memory; it is not a product credential.

The public SDK version only exposes data-track publication name, so this profile does not claim schema/blob storage, typed encoding negotiation, target-FPS throttling, arbitrary extension injection, mobile SDK coverage, congestion stability or physical bandwidth accuracy. Any missing functionality or actual failed assertion remains a failure rather than a skipped capability. Before running a matrix after changes, inspect/keep the existing successful and failed evidence identities; do not combine separate runs into one pass.
