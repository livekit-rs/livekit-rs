# rtc-tls

Shared verified TLS for HTTPS/WSS listeners, embedded TURN/TLS, and HTTP(S)
Webhook delivery. The provider uses stable RustCrypto primitives and explicit
rustls configuration; no ring, aws-lc, OpenSSL library, platform verifier, or
global provider installation is involved. OS APIs only supply trust-anchor DER.

## Implemented and tested matrix

| Area | Supported profile |
| --- | --- |
| TLS versions | TLS 1.2 and TLS 1.3 |
| TLS 1.3 AEAD | AES-128-GCM/SHA256, AES-256-GCM/SHA384, ChaCha20-Poly1305/SHA256 |
| TLS 1.2 AEAD | ECDHE_ECDSA / ECDHE_RSA with those three AEAD/hash combinations |
| Key exchange | X25519, P-256, P-384; low-order X25519 shares rejected |
| Endpoint keys | RSA PKCS#1 / PKCS#8; ECDSA P-256/P-384/P-521 PKCS#8 / SEC1; Ed25519 PKCS#8 |
| Certificate signatures | RSA PKCS#1/PSS SHA256/384/512; ECDSA P256/P384 SHA256/384, P521 SHA512; Ed25519 |
| Certificate validation | WebPKI chain, validity, hostname, serverAuth and private-key/leaf matching |
| Chain delivery | Leaf-first PEM fullchain, including intermediate CA; missing/wrong intermediate rejected |

RSA private operations use the signature-only adapter described in
[RSA-PUBLIC-ONLY.md](RSA-PUBLIC-ONLY.md). The **rsa crate** remains public-only;
that is not a claim that RSA endpoint private keys are unsupported. The separate
signer uses stable crypto-bigint full-width exponentiation, mandatory random
blinding, zeroization and verify-after-sign. No RSA decryption, encryption-padding
decoder, static-RSA exchange, SHA1/MD5 signature or CBC cipher suite is exposed.
Encrypted PEM keys have no passphrase input and are rejected, matching Go's
`tls.LoadX509KeyPair` behavior. Untested algorithms/combinations are not implied
by the passed matrix.

## Trust sources and HTTP delivery

Production client roots use system trust-anchor DER and configured PEM sources.
Linux follows Go's first-readable default bundle plus certificate directories;
`SSL_CERT_FILE` / `SSL_CERT_DIR` replace those categories independently. Missing
or invalid members do not discard other valid anchors. On macOS/Windows, OS
anchors are loaded and explicit PEM file/directory sources are additive product
extensions. All peer verification still runs in WebPKI; OS trust-chain evaluation
is not invoked. See the roots implementation notes and source notice.

Trust loading and the HTTPS connector are lazy. Plain HTTP works on hosts with
no CA store; the first actual HTTPS hop must obtain valid anchors. A missing
usable trust store fails that HTTPS request, without disabling pure HTTP startup.
`client_config_with_ca_file` is a separate explicit embedding/test helper that
adds a file to frozen Mozilla anchors.

HTTP and HTTPS initial URLs use the same standard URL parser, including
case-insensitive schemes. Redirects permit HTTP-to-HTTPS upgrades, verify every
HTTPS hop, and stop after ten requests. 301/302/303 convert POST to GET;
307/308 retain the body. Across origins, authorization/cookies are removed
permanently. This sensitive-header policy is stricter than Go's host/subdomain
rule and remains an explicit compatibility difference.

## Checks and provenance

```sh
cargo test --release --locked -p rtc-tls
cargo clippy --release --locked -p rtc-tls --all-targets -- -D warnings
python3 crates/rtc-tls/scripts/check-rsa-boundary.py --self-test
RTC_TLS_OPENSSL=/path/to/openssl cargo test --release --locked -p rtc-tls \
  --test expanded -- --ignored
RTC_TLS_OPENSSL=/path/to/openssl cargo test --release --locked -p rtc-tls \
  --test handshake -- --ignored
```

Coverage includes 90 in-memory cipher/KX/endpoint-key combinations, 30 independent
OpenSSL client combinations, an independent RSA OpenSSL server verified by the
production HTTP client for TLS 1.2/1.3, RSA signature vectors and tampering,
non-byte-aligned 2049-bit repeated signatures, entropy/fault rejection, fullchain
and private-CA positive/negative cases, key updates and record corruption.
OpenSSL is solely an independent test tool.

[SOURCE.json](SOURCE.json) freezes upstream adapter/math source hashes;
[the dependency lock](https://github.com/livekit-rs/livekit-docs/blob/main/server/specs/0006-tls-dependency-lock.json)
records registry checksums and actual reference Git commits. The alpha
rustls-rustcrypto and RSA rc packages are source references, not production
dependencies and not relabeled as stable. Their original MIT/Apache license
texts accompany this crate; the native-root adapter has its own ISC notice.
Public test keys must never be deployed. Actual browser/network and CI results
remain separate from source and unit-test coverage.
