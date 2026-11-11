# Third-party attribution

## RustCrypto / rustls-rustcrypto

- Upstream repository: https://github.com/RustCrypto/rustls-rustcrypto
- Frozen commit: `70f76c039e587192688af18a80d5d6435dedaf22`
- Upstream license expression: **MIT OR Apache-2.0**.
- Upstream copyright notice, reproduced verbatim:

  ```text
  Copyright (c) 2023 Steve Fan, ctz
  ```

The `rtc-tls` provider adapters follow and adapt the interface/glue patterns in
upstream `src/hash.rs`, `src/hmac.rs`, `src/kx.rs`, `src/sign/ecdsa.rs`,
`src/verify/ecdsa.rs` and `src/aead/gcm.rs`. The upstream crate itself is not a
production dependency. Its attribution and license texts are retained here for
the referenced/adapted portions rather than treating the absence of a direct
package dependency as removing their provenance.

Local adaptations narrow the provider to stable RustCrypto primitives and the
implemented TLS 1.2/1.3 cipher suites/signature algorithms, with local rustls
integration, validation, configuration and regression tests. See `README.md` for
the supported behavior and limitations and `SOURCE.json` for the frozen reference
file hashes.

The following files are **unmodified copies extracted from that exact commit**:

- [Original MIT license](third-party/rustls-rustcrypto/LICENSE-MIT), including its
  copyright and permission notice.
- [Original Apache License 2.0](third-party/rustls-rustcrypto/LICENSE-APACHE).

No upstream `NOTICE` file exists in the locked commit. This attribution document
is project-authored; it is not presented as an upstream NOTICE file. `SOURCE.json`
records SHA-256 hashes of both copied license files so their provenance can be
checked without the ignored `references/` checkout.

These upstream portions retain their upstream licensing. The workspace's existing
`Apache-2.0` license metadata for project-owned material is unchanged. Retain this
attribution and the supplied license files when packaging or redistributing the
provider's referenced/adapted portions, including with applicable binary release
notices. Dependencies have their own licenses; this document describes the
specific provider-adapter provenance and does not replace dependency attribution.


## RustCrypto / RSA signing reference

The sign-only encoding, blinding and verify-after-sign patterns in `src/rsa_sign.rs`
reference RustCrypto/RSA `0.10.0-rc.19`, commit
`db103005d1c9ae550efc5f6daa1b9cfe150e549a` (MIT OR Apache-2.0).
The rc crate is not a dependency. Private arithmetic is the local fixed-width
adapter over stable `crypto-bigint`; the production `rsa 0.9.10` dependency is
limited to public verification and borrowed ASN.1 containers, as documented in
`RSA-PUBLIC-ONLY.md`.

Unmodified upstream license files are retained as
`LICENSE-MIT-upstream-rsa.txt` and `LICENSE-APACHE-upstream-rsa.txt`.
`SOURCE.json` records exact source/license hashes. Preserve these notices when
redistributing the adapted portions. OS DER root-loading attribution is in
`NOTICE-rustls-native-certs.txt`; no OS cryptographic verifier is used.
