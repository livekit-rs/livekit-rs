# Invalid primary codec with the official Go SDK

The publisher and both subscribers use the unmodified official LiveKit Go SDK
pinned in go.mod. The publisher advertises VP9 and a VP8 backup using
WithBackupCodec. The first subscriber decodes RTP payload types through its
ordinary Pion track. A second, VP8-only subscriber creates real demand for the
backup while the server's automatic regression threshold is disabled.

Once both receivers have actual media, an SDK RTP interceptor replaces the
VP9 publisher's payload type and payload immediately before SRTP transmission.
The first changed packet uses the same publisher SSRC, a consecutive sequence
number and the already negotiated VP8 payload type. The changed packet carries
a distinct encoded frame. The receiver compares complete backup VP8 payloads
to the original backup frame to catch accidental forwarding of the injected
primary payload. It asserts the publication SID, downstream binding
transition, backup subscriber continuity and RoomService cleanup.

The driver compares PREFER_REGRESSION, numeric 99 and SIMULCAST with the
publisher and primary subscriber in dual or single peer connection mode.
The VP8-only observer always uses dual PC because the pinned SDK's
single-PC initial offer retries with its video-only codec list. The SDK exposes a fresh downstream binding
with the same publication SID for the first two policies. The SIMULCAST
control retains the first subscriber binding without switching it to VP8.
The VP8-only subscriber keeps its original binding throughout every case.

The three IVF fixtures each contain one independently encoded 160×90 keyframe.
They were generated with these FFmpeg inputs.

```sh
ffmpeg -f lavfi -i color=c=blue:s=160x90 -frames:v 1 -c:v libvpx -f ivf vp8.ivf
ffmpeg -f lavfi -i color=c=blue:s=160x90 -frames:v 1 -c:v libvpx-vp9 -f ivf vp9.ivf
ffmpeg -f lavfi -i color=c=red:s=160x90 -frames:v 1 -c:v libvpx -f ivf fault.ivf
```

Run from this directory with Go 1.26.3 and Python 3.11 or newer.

```sh
out=/absolute/path/to/new-fixture-directory
mkdir -p "$out"
GOTOOLCHAIN=go1.26.3 go build -mod=readonly -o "$out/probe" .
python3 run.py --go /absolute/path/to/livekit-v1.13.10 \
  --rust /absolute/path/to/livekit-server --probe "$out/probe" \
  --vp8 vp8.ivf --vp9 vp9.ivf --injected fault.ivf --output "$out/results"
```
