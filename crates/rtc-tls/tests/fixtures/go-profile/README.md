# Public Go verification-profile fixtures

These certificates and the one intentionally public test server private key are **test inputs only**. They were generated with OpenSSL; production TLS remains RustCrypto/WebPKI. Temporary issuer keys were removed after signing. SHA256.json binds the committed inputs.

- ca-leaf: self-signed RSA certificate with CA:TRUE, localhost DNS SAN and serverAuth EKU. It is explicitly trusted for the role-verdict comparison; the default WebPKI verifier must still reject it as an end entity.
- ca-leaf-client-only: same role/name shape but clientAuth-only EKU, for a rejected server purpose.
- through-nonca + nonca-issuer: signed chain whose issuer has CA:FALSE. Allowing a Go-compatible leaf role must not relax issuer-role constraints.
- outside-name-constraint + constrained-issuer: otherwise signed chain outside its issuer's permitted DNS subtree. Leaf-role compatibility must not bypass name constraints.

Tests also retain hostname, untrusted-root, expiry-time and corrupted-signature rejection. No DER rewriting occurs in the validator.
