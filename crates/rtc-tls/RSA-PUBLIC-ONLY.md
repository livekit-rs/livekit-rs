# RSA crate boundary and independent signing adapter

Date: 2026-10-23. Owner: `rtc-tls` maintainers. This document names the boundary
of the **rsa dependency**, not a restriction that RSA endpoint keys are rejected.
The server supports RSA PKCS#1 / PKCS#8 private-key TLS signatures.

## Applicable code paths

- `rsa_verify.rs` uses `rsa 0.9.10` only for public RSA key validation and
  PKCS#1-v1.5 / PSS signature verification (SHA-256/384/512).
- `rsa_sign.rs` uses borrowed ASN.1 DER parser types and the public DigestInfo
  prefix descriptor from that crate. It never constructs the crate's arithmetic
  `rsa::RsaPrivateKey`, calls its signing/decryption API, or invokes its hazmat
  private operation. The PKCS#1 ASN.1 parser `rsa::pkcs1::RsaPrivateKey` is a
  different borrowed data structure, with no private arithmetic methods.
- TLS signing uses an independent narrow adapter over stable
  `crypto-bigint 0.7.5`. RFC 8017 encoding and blinding/fault-check patterns are
  derived from fixed RustCrypto/RSA `0.10.0-rc.19` source; that prerelease package
  is **not** a dependency and is not relabeled as stable. Source hashes and the
  Git commit are recorded in `SOURCE.json` and SPEC-0006's dependency lock.

## Private operation

The stored `d` is `Zeroizing<BoxedUint>` with precision fixed by the public
modulus byte width (rounded to the arithmetic limb size). Signing uses
`BoxedMontyForm::pow(&d)`, whose implementation calls `pow_bounded_exp` with
`exponent.bits_precision()`, not the secret significant-bit count. It performs
full-modulus exponentiation and never uses CRT or secret prime moduli online.

Each signature requires fresh OS entropy and multiplicative blinding. Sampling
masks unused high bits for non-byte-aligned moduli, rejects values >= n and
non-invertible values, and computes the inverse with `invert_odd_mod` (not the
`_vartime` variant). Exhausted entropy or rejection attempts return an error;
there is no unblinded fallback. The only bounded exponentiation uses public e.
The unblinded result is raised to e and compared with the encoded input before
any signature is released. Local key initialization validates the product of
primes and `ed = 1 mod (p_i - 1)`; multi-prime keys use the same non-CRT signer.

Adapter-owned DER input, d, prime-validation intermediates, random blinding
bytes/inverses and private-operation buffers are explicitly zeroized. Debug
implementations redact key material. This is a source-level boundary and tests,
not a proof of every machine-code timing/cache property or a third-party audit.

## RUSTSEC-2023-0071

The advisory has no patched rsa release and includes 0.9.10. The non-applicable
record is limited to that dependency's **reachable public-only arithmetic**.
It does not declare the whole package safe or excuse its private-key paths.
The independent signer does not enter the advisory's private bigint/depadding
implementation. There is no RSA decryption endpoint, padding decoder, or
static-RSA key-exchange suite; TLS 1.2 RSA authentication uses ECDHE_RSA, and
TLS 1.3 uses RSA-PSS authentication.

Review this exception on **every rsa/crypto-bigint upgrade or RSA call-site
change**, and before each release. Remove it when the public verifier can use a
patched stable dependency or replace that dependency. Any affected rsa private
API becoming reachable blocks release. Additional advisories require their own
review; they are not covered by this entry.

## Checks and independent evidence

```sh
python3 crates/rtc-tls/scripts/check-rsa-boundary.py --self-test
cargo test --release --locked -p rtc-tls
RTC_TLS_OPENSSL=/path/to/openssl cargo test --release --locked -p rtc-tls \
  --test expanded -- --ignored
```

Regressions cover independent PKCS#1 vectors, independent PSS/PKCS#1 verification,
message/signature/DER tampering, wrong leaf/private key, invalid private exponent,
entropy failure, all-zero blinding rejection, fault-check failure, 32 repeated
PKCS#1/PSS signatures with a **true 2049-bit modulus**, and OpenSSL verification
of that modulus for all six SHA-2 signature schemes. The 2049-bit fixture uses
OpenSSL-generated 1025/1024-bit primes and is validated by `openssl rsa -check`;
OpenSSL `genpkey rsa_keygen_bits:2049` alone rounded down to 2048 in this version.

The broader matrix runs 90 in-memory TLS algorithm/key combinations and 30
independent OpenSSL client combinations, plus our HTTP client against an
independent RSA OpenSSL TLS 1.2/1.3 server with untrusted/name negative cases.
