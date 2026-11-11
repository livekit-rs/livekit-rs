# Public expanded TLS test fixtures

Generated 2026-10-23 using independent OpenSSL 3.6.4. All private keys here are
intentionally public test material and must never be used for deployment.
Root/intermediate CA private keys were discarded; certificates use long test
validity while validation remains enabled.

- RSA root signs RSA, P256, P384, P521 and Ed25519 localhost/serverAuth leaves.
- Separate Ed25519 issuer signs a P256 leaf.
- Fullchain contains P256 leaf plus RSA intermediate; only its distinct root is
  trusted by clients. Leaf-only and reversed-order cases are negative tests.
- PKCS1/SEC1 keys are equivalent encodings converted by OpenSSL.
- Fixed RSA PKCS1/PSS SHA256/384/512 vectors are independently OpenSSL-generated.
- True 2049-bit RSA uses OpenSSL `prime -generate` with 1025/1024-bit primes;
  Python built-in integer arithmetic forms standard PKCS1 parameters and DER,
  independently validated by `openssl rsa -check`, then converted to PKCS8.
  This is test-data generation only; no such generator is in production.
  Merely requesting 2049 bits from OpenSSL genpkey rounded down to 2048 here.
