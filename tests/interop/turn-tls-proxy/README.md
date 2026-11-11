# Real TURN over TLS through a trusted PROXY listener

Build `observe.go` with the fixed Go LiveKit Server v1.13.10 module and its
locked Pion dependencies. Run once per engine and profile against a fresh
output directory. Set LK_TURN_TLS_FIXTURE to the repository's public TLS
fixture directory and LK_TURN_PROXY_MODE to v1, v2, local-full, local-empty,
missing, bad-transport, proxy-unspec or untrusted. The boundary profiles are
v1-limit, v1-oversized, v1-double-space, v1-padded-port, v2-limit,
v2-oversized, local-limit and local-oversized. Pass `-profile proxy-v4
-transport tcp -binary /absolute/server -out /absolute/new-output`.

The positive rows negotiate certificate-verified TLS, assert the actual STUN
mapped address, authenticate TURN allocation, verify bidirectional relay, and
release the owned ports. LOCAL retains the real TCP tuple even if it contains
informational address bytes or no address block. The negative rows send a
real ClientHello and must not negotiate TLS; a certificate failure is not a
passing negative. The receipt separates the fixed Go listener's quiet
deadline from a socket close; it does not require the same TCP retirement
timing. Profiles with real v1/v2 positive relay guard the fixture.

The v1 length cases use a 107- or 108-byte line, counting CRLF. The v2
length cases use a 4,096- or 4,097-byte payload, excluding the 16-byte
prefix. Each limit case must complete the normal allocation and relay
exchange. Each oversized case, doubled v1 separator and zero-padded v1
port must fail before TLS completes.

The delayed v1 split profiles send the first fragment, wait 200 ms, and
send the remainder. The four-byte prefix split permits the signature
peek to complete on the second fragment. The five-byte signature,
tuple and CRLF splits put an incomplete line in the initial signature
read. The opaque UNKNOWN profile sends invalid UTF-8 and an embedded
NUL after the address-family token. The bare-LF profile is malformed.

The AF_UNIX profiles distinguish LOCAL, PROXY and payload length.
For a complete UNIX PROXY tuple, the fixed Go daemon negotiates TLS,
leaves a Binding request unanswered, challenges unauthenticated
Allocate and returns 400 after authenticated Allocate. These probes
do not count that outcome as relay success.

The retirement profiles hold the malformed connection open while
checking Binding on a fresh connection to the same listener.
`retire-tls` uses a certificate-validating TLS client after the malformed header.
`retire-plain` enables external TLS termination and sends plaintext
STUN. The fixture records whether the invalid socket is already
closed at the probe deadline and verifies closure after daemon stop.

## Unix allocation ownership

Use `LK_TURN_PROXY_MODE=unix-stream` or `unix-dgram` with
`LK_TURN_UNIX_OWNERSHIP=independent` for Rust and `go-alias` for the
fixed Go server. Set `LK_TURN_UNIX_NAMES=same` to reuse both forwarded
Unix names on distinct TCP connections. Otherwise the observer sends
distinct names.

Both TLS handshakes must verify. Neither logical Unix peer has an IP
address for Binding. The observer requires a quiet Binding, a real 401
challenge, and long-term credentials on each authenticated Allocate.
The first accepted allocation returns 400 because the response cannot
encode a mapped Unix address. Its same-transaction retransmission and fresh retry both return 437.
The observer compares exact response attribute order against the fixed
Go wire result, including the 401 realm and nonce attributes.

In independent mode the second connection also returns 400 and opens
its own relay port. Closing it releases only its port. The first
connection retains its original port and returns 437 on a fresh
transaction. In the Go alias control the second connection returns 437
and closing it releases the first connection's port. The observer
verifies relay port occupancy by attempting to bind both configured
UDP relay addresses during daemon operation. Both modes must release all
relay ports on client close and admit a fresh verified TLS connection
that can create and release a new allocation.
