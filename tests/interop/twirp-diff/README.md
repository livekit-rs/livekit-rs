# W2-G1 Twirp / W5 HTTP differential (SPEC-0073)

- `gen_steps.py`: the step matrix. Every RoomService, Egress, Ingress, SIP
  and AgentDispatchService method gets these variants:
  - auth: none, garbage, bad secret, expired, wrong grant;
  - bodies: malformed JSON, malformed protobuf, unknown field, and empty in
    both JSON and protobuf.

  Also covered:
  - ordered room scenarios in JSON and protobuf (create, re-create, list,
    metadata, data, dispatch, remove, delete);
  - name/metadata validation and Authorization header forms;
  - Twirp routing errors;
  - W5 HTTP routes (`/`, `/rtc*`, `/whip/v1`, `/agent`, debug, metrics, CORS
    preflight, double slash).
- `go/main.go`: staged as `cmd/twirpdiff` in a copy of `references/livekit-stable`
  (same go.mod replaces as the observer). It mints one token per step with the
  real protocol `auth` package and sends byte-identical requests to both
  servers. Protobuf bodies are built and decoded with the pinned descriptors.
- `run.py`: starts both servers with the same single-node config on distinct
  ports (default 18880/28880), runs the client, stops them, then calls
  `compare.py`.
- `compare.py`: compares status, the kept headers and the JSON values or raw
  body. Its only normalizations are documented in its docstring:
  - server GUIDs are renamed by first appearance;
  - named clock fields are reduced to presence;
  - `turn_password` is reduced to a RandomSecret shape check.

```sh
(cd STAGE && GOTOOLCHAIN=go1.26.3 GOFLAGS=-mod=mod go build -o /abs/twirpdiff ./cmd/twirpdiff)
python3 -I gen_steps.py steps.json
python3 -I run.py --official /abs/go-bin --rust /abs/rust-bin --client /abs/twirpdiff \
  --steps steps.json --out OUTDIR [--only id-prefix,...] [--timeout 20s]
```

Not covered here: participant-dependent success paths. These need a joined
signalling client and belong to W3.
