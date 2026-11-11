# dimpl security dependency migration

Published dimpl 0.7.6 is retained at commit `0c196c24438dfa1a14eace130c27f54f1e482b66`, with MIT/Apache-2.0 licenses unchanged. `migration.patch` changes exactly three files: the manifest and RustCrypto signing/key-exchange boundary. No DTLS state machine, packet encoding, cipher suite, AEAD, HKDF or protocol algorithm changes.

The curve libraries move from p256/p384 0.13 and ECDSA 0.16 to 0.14/0.17, removing the affected crypto-bigint 0.5 dependency closure. Modern curve PKCS#8 decoding receives the same DER bytes, while the original DER/SPKI/certificate parser stays in place. A rand_core 0.10 adapter preserves the original OS entropy source for new curve generation; X25519 keeps its existing rand_core 0.6 interface. Random-source errors map to the existing StartKeyExchange error.

The coordinator must resolve crypto-bigint >=0.7.6 and pass cargo-deny; this patch neither suppresses RUSTSEC-2026-0387 nor certifies constant-time behavior by itself. Public provider regression covers both curves, both hash algorithms, PKCS#8/SEC1/PEM loading and negative signatures, immutable upstream signature fixtures, and P256/P384/X25519 ECDH/invalid peers. It does not alone establish DTLS 1.2/1.3/Auto network interoperability.

`SOURCE.json` binds the original 148-file published tree, three-file patch and entire reconstructed tree with SHA-256. Rebuild uses the local published reference input; the checker needs no compiler, network or fuzzy patching. All current Rust/security/interop results remain pending actual coordinator execution.

2026-10-25 isolated build correction: explicitly enable `rand_core 0.6/std`, required by the existing `CurveOsRng` error conversion to `std::io::Error`. No RNG algorithm, curve operation or wire behavior changes. The same three-file patch is regenerated from exact published0.7.6 source and byte reconstruction remains required.
