//! Public RustCrypto provider regression for the RUSTSEC-2026-0387 closure migration.
use dimpl::crypto::{Buf, HashAlgorithm, NamedGroup, SignatureAlgorithm};
use p256::pkcs8::EncodePrivateKey;
use x509_cert::der::Decode;

// This provider validates the signature using the certificate's SPKI; chain trust
// belongs to its caller. Replace only the equal-length public point in upstream
// certificate fixtures. The resulting certificate is deliberately NOT a newly
// signed/trusted certificate, and is used only at this public SPKI-verifier boundary.
fn certificate_with_point(template: &[u8], public: &[u8]) -> Vec<u8> {
    let parsed = x509_cert::Certificate::from_der(template).unwrap();
    let previous = parsed
        .tbs_certificate()
        .subject_public_key_info()
        .subject_public_key
        .as_bytes()
        .unwrap();
    assert_eq!(previous.len(), public.len());
    let offsets: Vec<_> = template
        .windows(previous.len())
        .enumerate()
        .filter_map(|(i, bytes)| (bytes == previous).then_some(i))
        .collect();
    assert_eq!(offsets.len(), 1);
    let mut cert = template.to_vec();
    cert[offsets[0]..offsets[0] + public.len()].copy_from_slice(public);
    cert
}

#[test]
fn both_curves_support_both_hashes_and_reject_tampering() {
    let p = dimpl::crypto::rust_crypto::default_provider();
    let p256 = p256::ecdsa::SigningKey::from_slice(&[1; 32]).unwrap();
    let p384 = p384::ecdsa::SigningKey::from_slice(&[2; 48]).unwrap();
    let keys = [
        (
            vec![
                p256.to_pkcs8_der().unwrap().as_bytes().to_vec(),
                p256::SecretKey::from_slice(&[1; 32])
                    .unwrap()
                    .to_sec1_der()
                    .unwrap()
                    .to_vec(),
                p256.to_pkcs8_pem(Default::default())
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
                p256::SecretKey::from_slice(&[1; 32])
                    .unwrap()
                    .to_sec1_pem(Default::default())
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ],
            certificate_with_point(
                include_bytes!("../../../vendor/dimpl/src/crypto/validation/p256_cert.der"),
                p256.verifying_key().to_sec1_point(false).as_bytes(),
            ),
        ),
        (
            vec![
                p384.to_pkcs8_der().unwrap().as_bytes().to_vec(),
                p384::SecretKey::from_slice(&[2; 48])
                    .unwrap()
                    .to_sec1_der()
                    .unwrap()
                    .to_vec(),
                p384.to_pkcs8_pem(Default::default())
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
                p384::SecretKey::from_slice(&[2; 48])
                    .unwrap()
                    .to_sec1_pem(Default::default())
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ],
            certificate_with_point(
                include_bytes!("../../../vendor/dimpl/src/crypto/validation/p384_cert.der"),
                p384.verifying_key().to_sec1_point(false).as_bytes(),
            ),
        ),
    ];
    for (forms, certificate) in &keys {
        for der in forms {
            let mut key = p.key_provider.load_private_key(der).unwrap();
            for hash in [HashAlgorithm::SHA256, HashAlgorithm::SHA384] {
                assert!(key.supported_hash_algorithms().contains(&hash));
                let mut sig = Buf::new();
                key.sign(b"migration signed data", hash, &mut sig).unwrap();
                p.signature_verification
                    .verify_signature(
                        certificate,
                        b"migration signed data",
                        &sig,
                        hash,
                        SignatureAlgorithm::ECDSA,
                    )
                    .unwrap();
                assert!(
                    p.signature_verification
                        .verify_signature(
                            certificate,
                            b"changed signed data",
                            &sig,
                            hash,
                            SignatureAlgorithm::ECDSA
                        )
                        .is_err()
                );
                let wrong_hash = if hash == HashAlgorithm::SHA256 {
                    HashAlgorithm::SHA384
                } else {
                    HashAlgorithm::SHA256
                };
                assert!(
                    p.signature_verification
                        .verify_signature(
                            certificate,
                            b"migration signed data",
                            &sig,
                            wrong_hash,
                            SignatureAlgorithm::ECDSA
                        )
                        .is_err()
                );
                let last = sig.len() - 1;
                sig[last] ^= 1;
                assert!(
                    p.signature_verification
                        .verify_signature(
                            certificate,
                            b"migration signed data",
                            &sig,
                            hash,
                            SignatureAlgorithm::ECDSA
                        )
                        .is_err()
                );
            }
        }
    }
    assert!(p.key_provider.load_private_key(&[0; 32]).is_err());
}

#[test]
fn all_provider_ecdh_groups_agree_and_reject_invalid_peers() {
    let p = dimpl::crypto::rust_crypto::default_provider();
    for (name, size) in [
        (NamedGroup::Secp256r1, 32),
        (NamedGroup::Secp384r1, 48),
        (NamedGroup::X25519, 32),
    ] {
        let group = p
            .kx_groups
            .iter()
            .find(|group| group.name() == name)
            .unwrap();
        let alice = group.start_exchange(Buf::new()).unwrap();
        let bob = group.start_exchange(Buf::new()).unwrap();
        let a_public = alice.pub_key().to_vec();
        let b_public = bob.pub_key().to_vec();
        let mut a_secret = Buf::new();
        let mut b_secret = Buf::new();
        alice.complete(&b_public, &mut a_secret).unwrap();
        bob.complete(&a_public, &mut b_secret).unwrap();
        assert_eq!(a_secret.len(), size);
        assert_eq!(&a_secret[..], &b_secret[..]);
        assert!(a_secret.iter().any(|byte| *byte != 0));
        for invalid in [vec![0; a_public.len()], vec![4], vec![]] {
            let exchange = group.start_exchange(Buf::new()).unwrap();
            assert!(exchange.complete(&invalid, &mut Buf::new()).is_err());
        }
    }
}

#[test]
fn immutable_published_signature_vectors_verify_independently() {
    let p = dimpl::crypto::rust_crypto::default_provider();
    let data = include_bytes!("../../../vendor/dimpl/src/crypto/validation/test_data.bin");
    for (cert, sig, hash) in [
        (
            include_bytes!("../../../vendor/dimpl/src/crypto/validation/p256_cert.der").as_slice(),
            include_bytes!("../../../vendor/dimpl/src/crypto/validation/p256_sha256_sig.der")
                .as_slice(),
            HashAlgorithm::SHA256,
        ),
        (
            include_bytes!("../../../vendor/dimpl/src/crypto/validation/p384_cert.der").as_slice(),
            include_bytes!("../../../vendor/dimpl/src/crypto/validation/p384_sha384_sig.der")
                .as_slice(),
            HashAlgorithm::SHA384,
        ),
    ] {
        p.signature_verification
            .verify_signature(cert, data, sig, hash, SignatureAlgorithm::ECDSA)
            .unwrap();
        assert!(
            p.signature_verification
                .verify_signature(
                    cert,
                    b"wrong fixed vector",
                    sig,
                    hash,
                    SignatureAlgorithm::ECDSA
                )
                .is_err()
        );
    }
}
