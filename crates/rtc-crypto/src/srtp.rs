// Copied from str0m-rust-crypto 0.6.0 (MIT OR Apache-2.0, (c) Martin Algesten).
// Production cipher implementation retained; local conformance tests follow.
// See crates/rtc-crypto/NOTICE for upstream attribution.
//! SRTP cipher implementations using RustCrypto.

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, Nonce};
use ctr::cipher::{KeyIvInit, StreamCipher};

use str0m_proto::crypto::CryptoError;
use str0m_proto::crypto::{AeadAes128Gcm, AeadAes128GcmCipher};
use str0m_proto::crypto::{AeadAes256Gcm, AeadAes256GcmCipher, Aes128CmSha1_80Cipher};
use str0m_proto::crypto::{SrtpProvider, SupportedAeadAes128Gcm};
use str0m_proto::crypto::{SupportedAeadAes256Gcm, SupportedAes128CmSha1_80};

// Type alias for AES-128 in CTR mode
type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;

// ============================================================================
// AES-128-CM-SHA1-80 Cipher
// ============================================================================

struct RustCryptoAes128CmSha1_80Cipher {
    key: [u8; 16],
}

impl std::fmt::Debug for RustCryptoAes128CmSha1_80Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoAes128CmSha1_80Cipher").finish()
    }
}

impl Aes128CmSha1_80Cipher for RustCryptoAes128CmSha1_80Cipher {
    fn encrypt(
        &mut self,
        iv: &[u8; 16],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        // AES-128 Counter Mode (CTR)
        let mut cipher = Aes128Ctr::new(&self.key.into(), iv.into());

        // Copy input to output
        output[..input.len()].copy_from_slice(input);

        // Apply CTR mode encryption (XOR with keystream)
        cipher.apply_keystream(&mut output[..input.len()]);

        Ok(())
    }

    fn decrypt(
        &mut self,
        iv: &[u8; 16],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        // AES-CTR is symmetric (same operation for encrypt and decrypt)
        self.encrypt(iv, input, output)
    }
}

// ============================================================================
// AEAD-AES-128-GCM Cipher
// ============================================================================

struct RustCryptoAeadAes128GcmCipher {
    cipher: Aes128Gcm,
}

impl std::fmt::Debug for RustCryptoAeadAes128GcmCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoAeadAes128GcmCipher").finish()
    }
}

impl AeadAes128GcmCipher for RustCryptoAeadAes128GcmCipher {
    fn encrypt(
        &mut self,
        iv: &[u8; AeadAes128Gcm::IV_LEN],
        aad: &[u8],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        assert!(
            aad.len() >= 12,
            "Associated data length MUST be at least 12 octets"
        );

        let nonce = Nonce::from_slice(iv);
        let payload = Payload { msg: input, aad };

        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| CryptoError::Other(format!("AES-GCM encrypt failed: {:?}", e)))?;

        output[..ciphertext.len()].copy_from_slice(&ciphertext);
        Ok(())
    }

    fn decrypt(
        &mut self,
        iv: &[u8; AeadAes128Gcm::IV_LEN],
        aads: &[&[u8]],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<usize, CryptoError> {
        assert!(input.len() >= AeadAes128Gcm::TAG_LEN);

        let nonce = Nonce::from_slice(iv);

        // Concatenate AAD slices if needed
        let aad_vec: Vec<u8>;
        let aad = if aads.len() == 1 {
            aads[0]
        } else {
            aad_vec = aads.concat();
            &aad_vec
        };

        let payload = Payload { msg: input, aad };

        let plaintext = self
            .cipher
            .decrypt(nonce, payload)
            .map_err(|e| CryptoError::Other(format!("AES-GCM decrypt failed: {:?}", e)))?;

        output[..plaintext.len()].copy_from_slice(&plaintext);
        Ok(plaintext.len())
    }
}

// ============================================================================
// AEAD-AES-256-GCM Cipher
// ============================================================================

struct RustCryptoAeadAes256GcmCipher {
    cipher: Aes256Gcm,
}

impl std::fmt::Debug for RustCryptoAeadAes256GcmCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoAeadAes256GcmCipher").finish()
    }
}

impl AeadAes256GcmCipher for RustCryptoAeadAes256GcmCipher {
    fn encrypt(
        &mut self,
        iv: &[u8; AeadAes256Gcm::IV_LEN],
        aad: &[u8],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), CryptoError> {
        assert!(
            aad.len() >= 12,
            "Associated data length MUST be at least 12 octets"
        );

        let nonce = Nonce::from_slice(iv);
        let payload = Payload { msg: input, aad };

        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| CryptoError::Other(format!("AES-GCM encrypt failed: {:?}", e)))?;

        output[..ciphertext.len()].copy_from_slice(&ciphertext);
        Ok(())
    }

    fn decrypt(
        &mut self,
        iv: &[u8; AeadAes256Gcm::IV_LEN],
        aads: &[&[u8]],
        input: &[u8],
        output: &mut [u8],
    ) -> Result<usize, CryptoError> {
        assert!(input.len() >= AeadAes256Gcm::TAG_LEN);

        let nonce = Nonce::from_slice(iv);

        // Concatenate AAD slices if needed
        let aad_vec: Vec<u8>;
        let aad = if aads.len() == 1 {
            aads[0]
        } else {
            aad_vec = aads.concat();
            &aad_vec
        };

        let payload = Payload { msg: input, aad };

        let plaintext = self
            .cipher
            .decrypt(nonce, payload)
            .map_err(|e| CryptoError::Other(format!("AES-GCM decrypt failed: {:?}", e)))?;

        output[..plaintext.len()].copy_from_slice(&plaintext);
        Ok(plaintext.len())
    }
}

// ============================================================================
// SRTP Profile Support Implementations
// ============================================================================

struct RustCryptoSupportedAes128CmSha1_80;

impl std::fmt::Debug for RustCryptoSupportedAes128CmSha1_80 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSupportedAes128CmSha1_80")
            .finish()
    }
}

impl SupportedAes128CmSha1_80 for RustCryptoSupportedAes128CmSha1_80 {
    fn create_cipher(&self, key: [u8; 16], _encrypt: bool) -> Box<dyn Aes128CmSha1_80Cipher> {
        Box::new(RustCryptoAes128CmSha1_80Cipher { key })
    }
}

struct RustCryptoSupportedAeadAes128Gcm;

impl std::fmt::Debug for RustCryptoSupportedAeadAes128Gcm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSupportedAeadAes128Gcm").finish()
    }
}

impl SupportedAeadAes128Gcm for RustCryptoSupportedAeadAes128Gcm {
    fn create_cipher(&self, key: [u8; 16], _encrypt: bool) -> Box<dyn AeadAes128GcmCipher> {
        let cipher = Aes128Gcm::new(GenericArray::from_slice(&key));
        Box::new(RustCryptoAeadAes128GcmCipher { cipher })
    }
}

struct RustCryptoSupportedAeadAes256Gcm;

impl std::fmt::Debug for RustCryptoSupportedAeadAes256Gcm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSupportedAeadAes256Gcm").finish()
    }
}

impl SupportedAeadAes256Gcm for RustCryptoSupportedAeadAes256Gcm {
    fn create_cipher(&self, key: [u8; 32], _encrypt: bool) -> Box<dyn AeadAes256GcmCipher> {
        let cipher = Aes256Gcm::new(GenericArray::from_slice(&key));
        Box::new(RustCryptoAeadAes256GcmCipher { cipher })
    }
}

// ============================================================================
// SRTP Provider Implementation
// ============================================================================

pub(super) struct RustCryptoSrtpProvider;

impl std::fmt::Debug for RustCryptoSrtpProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoSrtpProvider").finish()
    }
}

impl SrtpProvider for RustCryptoSrtpProvider {
    fn aes_128_cm_sha1_80(&self) -> &'static dyn SupportedAes128CmSha1_80 {
        &RustCryptoSupportedAes128CmSha1_80
    }

    fn aead_aes_128_gcm(&self) -> &'static dyn SupportedAeadAes128Gcm {
        &RustCryptoSupportedAeadAes128Gcm
    }

    fn aead_aes_256_gcm(&self) -> &'static dyn SupportedAeadAes256Gcm {
        &RustCryptoSupportedAeadAes256Gcm
    }

    fn srtp_aes_128_ecb_round(&self, key: &[u8], input: &[u8], output: &mut [u8]) {
        // Use aes crate for ECB mode
        use aes::Aes128;
        use aes::cipher::{BlockEncrypt, KeyInit};

        let cipher = Aes128::new(GenericArray::from_slice(key));

        // Input is 16 bytes, output needs to be 32 bytes (with PKCS7 padding)
        // For a 16-byte input, PKCS7 adds a full block of padding (16 bytes of 0x10)
        assert!(input.len() == 16);
        assert!(output.len() >= 32);

        // First block: encrypt the input
        let mut block1 = *GenericArray::from_slice(&input[0..16]);
        cipher.encrypt_block(&mut block1);
        output[0..16].copy_from_slice(&block1);

        // Second block: PKCS7 padding (16 bytes of 0x10)
        let mut block2 = GenericArray::from([0x10u8; 16]);
        cipher.encrypt_block(&mut block2);
        output[16..32].copy_from_slice(&block2);
    }

    fn srtp_aes_256_ecb_round(&self, key: &[u8], input: &[u8], output: &mut [u8]) {
        // Use aes crate for ECB mode
        use aes::Aes256;
        use aes::cipher::{BlockEncrypt, KeyInit};

        let cipher = Aes256::new(GenericArray::from_slice(key));

        // Input is 16 bytes, output needs to be 32 bytes (with PKCS7 padding)
        // For a 16-byte input, PKCS7 adds a full block of padding (16 bytes of 0x10)
        assert!(input.len() == 16);
        assert!(output.len() >= 32);

        // First block: encrypt the input
        let mut block1 = *GenericArray::from_slice(&input[0..16]);
        cipher.encrypt_block(&mut block1);
        output[0..16].copy_from_slice(&block1);

        // Second block: PKCS7 padding (16 bytes of 0x10)
        let mut block2 = GenericArray::from([0x10u8; 16]);
        cipher.encrypt_block(&mut block2);
        output[16..32].copy_from_slice(&block2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        compact
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn aes_gcm_matches_rfc7714_srtp_vector_and_rejects_tampering() {
        // RFC 7714 §16.1: SRTP encryption using AEAD_AES_128_GCM.
        let key: [u8; 16] = hex("000102030405060708090a0b0c0d0e0f").try_into().unwrap();
        let nonce: [u8; 12] = hex("51753c6580c2726f20718414").try_into().unwrap();
        let header = hex("8040f17b8041f8d35501a0b2");
        let plaintext = b"Gallia est omnis divisa in partes tres";
        let expected = hex(
            "f24de3a3fb34de6cacba861c9d7e4bcabe633bd50d294e6f42a5f47a51c7d19b36de3adf8833899d7f27beb16a9152cf765ee4390cce",
        );
        let p = RustCryptoSrtpProvider;
        let mut enc = p.aead_aes_128_gcm().create_cipher(key, true);
        let mut encrypted = vec![0; plaintext.len() + AeadAes128Gcm::TAG_LEN];
        enc.encrypt(&nonce, &header, plaintext, &mut encrypted)
            .unwrap();
        assert_eq!(encrypted, expected);
        let mut dec = p.aead_aes_128_gcm().create_cipher(key, false);
        let mut clear = vec![0; plaintext.len()];
        // Header may be supplied in multiple slices by the packet assembler.
        let size = dec
            .decrypt(
                &nonce,
                &[&header[..4], &header[4..]],
                &encrypted,
                &mut clear,
            )
            .unwrap();
        assert_eq!(&clear[..size], plaintext);
        encrypted[0] ^= 1;
        clear.fill(0xa5);
        assert!(
            dec.decrypt(&nonce, &[&header], &encrypted, &mut clear)
                .is_err()
        );
        assert!(
            clear.iter().all(|b| *b == 0xa5),
            "unauthenticated plaintext must not escape"
        );
    }

    #[test]
    fn aes_cm_matches_nist_ctr_known_answer() {
        // NIST SP 800-38A F.5.1, the counter-mode primitive used by AES-CM SRTP.
        let key = hex("2b7e151628aed2a6abf7158809cf4f3c").try_into().unwrap();
        let iv = hex("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff").try_into().unwrap();
        let plaintext = hex("6bc1bee22e409f96e93d7e117393172a ae2d8a571e03ac9c9eb76fac45af8e51");
        let expected = hex("874d6191b620e3261bef6864990db6ce 9806f66b7970fdff8617187bb9fffdff");
        let mut cipher = RustCryptoSrtpProvider
            .aes_128_cm_sha1_80()
            .create_cipher(key, true);
        let mut output = vec![0; plaintext.len()];
        cipher.encrypt(&iv, &plaintext, &mut output).unwrap();
        assert_eq!(output, expected);
        let mut recovered = vec![0; plaintext.len()];
        cipher.decrypt(&iv, &output, &mut recovered).unwrap();
        assert_eq!(recovered, plaintext);
    }

    #[test]
    fn srtp_kdf_ecb_round_matches_rfc3711_session_key() {
        // RFC 3711 B.3, kdr=0 and label=0: AES(master_key, salt || 0x0000).
        let master = hex("e1f97a0d3e018be0d64fa32c06de4139");
        let salt = hex("0ec675ad498afeebb6960b3aabe60000");
        let mut output = [0; 32];
        RustCryptoSrtpProvider.srtp_aes_128_ecb_round(&master, &salt, &mut output);
        assert_eq!(&output[..16], hex("c61e7a93744f39ee10734afe3ff7a087"));
    }
}
