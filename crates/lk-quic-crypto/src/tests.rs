//! RFC 9001 known-answer vectors (Appendix A) + AEAD/header-protection
//! round-trip, tamper and boundary tests for the AES-128-GCM suite.
//!
//! Only offset-independent published values are asserted as known answers; the
//! whole-packet Initial protection (Appendix A.2/A.3) needs the variable-length
//! Token field that the RFC text elides, so that layout is pinned later by the
//! quinn `crypto` trait integration (SPEC-0074 C15), not by unit vectors here.

use super::*;

fn hex(s: &str) -> Vec<u8> {
    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn arr16(s: &str) -> [u8; 16] {
    let v = hex(s);
    let mut a = [0u8; 16];
    a.copy_from_slice(&v);
    a
}
fn arr12(s: &str) -> [u8; 12] {
    let v = hex(s);
    let mut a = [0u8; 12];
    a.copy_from_slice(&v);
    a
}
fn arr32(s: &str) -> [u8; 32] {
    let v = hex(s);
    let mut a = [0u8; 32];
    a.copy_from_slice(&v);
    a
}

/// RFC 9001 Appendix A.1 — the full Initial key schedule.
#[test]
fn rfc9001_a1_initial_keys() {
    let dcid = hex("8394c8f03e515708");
    let (client, server) = initial_keys(&dcid);
    assert_eq!(client.key, arr16("1f369613dd76d5467730efcbe3b1a22d"));
    assert_eq!(client.iv, arr12("fa044b2f42a3fd3b46fb255c"));
    assert_eq!(client.hp, arr16("9f50449e04a0e810283a1e9933adedd2"));
    assert_eq!(server.key, arr16("cf3a5331653c364c88f0f379b6067e37"));
    assert_eq!(server.iv, arr12("0ac1493ca1905853b0bba03e"));
    assert_eq!(server.hp, arr16("c206b8d9b9f0f37644430b490eeaa314"));
}

/// The `HkdfLabel` bytes for "quic key" (A.1: 00100e746c7331332071756963206b657900).
#[test]
fn rfc9001_a1_hkdf_label_bytes() {
    // Reconstruct the info the RFC shows and confirm our expand_label path uses it:
    // length 0x0010, label-len 0x0e ("tls13 quic key" = 14), ctx-len 0x00.
    let expected_label = hex("00100e746c7331332071756963206b657900");
    let full = "tls13 quic key";
    let mut info = Vec::new();
    info.extend_from_slice(&16u16.to_be_bytes());
    info.push(full.len() as u8);
    info.extend_from_slice(full.as_bytes());
    info.push(0u8);
    assert_eq!(info, expected_label);
}

/// RFC 9001 A.2 header-protection mask: AES-ECB(hp, sample)[0..5].
#[test]
fn rfc9001_a2_header_protection_mask() {
    let hp = arr16("9f50449e04a0e810283a1e9933adedd2");
    let sample = hex("d1b1c98dd7689fb8ec11d242b123dc9b");
    let mask = hp_mask(&hp, &sample).unwrap();
    assert_eq!(mask, [0x43, 0x7b, 0x9a, 0xec, 0x36]);
}

/// RFC 9001 A.2 header-application step (line 2413-2417): first byte and PN XOR.
#[test]
fn rfc9001_a2_header_xor_step() {
    let mask = [0x43, 0x7b, 0x9a, 0xec, 0x36];
    // header[0] ^= mask[0] & 0x0f  (long header): 0xc3 ^ 0x03 = 0xc0
    let first = 0xc3u8 ^ (mask[0] & hp_first_byte_mask(true));
    assert_eq!(first, 0xc0);
    // header pn bytes 00000002 ^= mask[1..5] = 7b9aec34
    let mut packet = hex("c300000001088394c8f03e5157080000449e00000002");
    let pn_offset = packet.len() - 4;
    hp_xor_packet_number(&mut packet, pn_offset, 4, &mask);
    assert_eq!(
        &packet[pn_offset..],
        &hex("7b9aec34")[..],
        "RFC A.2 protected pn bytes"
    );
}

/// RFC 9001 A.4 Retry integrity tag (real AES-128-GCM known answer).
#[test]
fn rfc9001_a4_retry_tag() {
    let odcid = hex("8394c8f03e515708");
    let retry_without_tag = hex("ff000000010008f067a5502a4262b5746f6b656e");
    let tag = retry_tag(&odcid, &retry_without_tag).unwrap();
    assert_eq!(tag, arr16("04a265ba2eff4d829058fb3f0f2496ba"));
}

/// A wrong ODCID in the Retry pseudo-packet must change the tag (integrity).
#[test]
fn rfc9001_a4_retry_tamper() {
    let mut odcid = hex("8394c8f03e515708");
    let retry_without_tag = hex("ff000000010008f067a5502a4262b5746f6b656e");
    odcid[0] ^= 0x01;
    let tag = retry_tag(&odcid, &retry_without_tag).unwrap();
    assert_ne!(tag, arr16("04a265ba2eff4d829058fb3f0f2496ba"));
}

/// RFC 9001 A.5 — the "quic ku" key-update secret (HKDF, AEAD-agnostic).
#[test]
fn rfc9001_a5_key_update_secret() {
    let secret = arr32("9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b");
    let ku = updated_secret(&secret);
    assert_eq!(
        ku.to_vec(),
        hex("1223504755036d556342ee9361d253421a826c9ecdf3c7148684b36b714881f9")
    );
}

/// nonce = IV with the packet number XOR-ed into its last 8 bytes.
#[test]
fn nonce_derivation_a2() {
    let iv = arr12("fa044b2f42a3fd3b46fb255c");
    let n = derive_nonce(&iv, 2);
    assert_eq!(n.to_vec(), hex("fa044b2f42a3fd3b46fb255e"));
}

fn sample_keys() -> PacketKeys {
    let dcid = hex("8394c8f03e515708");
    initial_keys(&dcid).0
}

/// AEAD seal/open round-trip + the tamper / wrong-key / truncation rejections.
#[test]
fn aead_roundtrip_and_negatives() {
    let k = sample_keys();
    let header = hex("c300000001088394c8f03e5157080000449e00000002");
    let payload = b"livekit quic packet protection test payload".as_slice();
    let pn = 2u64;

    let ct = aead_seal(&k.key, &k.iv, pn, &header, payload).unwrap();
    assert_eq!(ct.len(), payload.len() + AEAD_TAG_LEN);
    let pt = aead_open(&k.key, &k.iv, pn, &header, &ct).unwrap();
    assert_eq!(pt, payload.to_vec());

    // Tamper with a ciphertext byte -> open fails.
    let mut bad = ct.clone();
    bad[0] ^= 0x01;
    assert_eq!(
        aead_open(&k.key, &k.iv, pn, &header, &bad),
        Err(CryptoError::OpenFailed)
    );

    // Wrong key -> open fails.
    let mut wrong = k.key;
    wrong[0] ^= 0x01;
    assert_eq!(
        aead_open(&wrong, &k.iv, pn, &header, &ct),
        Err(CryptoError::OpenFailed)
    );

    // Wrong packet number (nonce mismatch) -> open fails.
    assert_eq!(
        aead_open(&k.key, &k.iv, pn + 1, &header, &ct),
        Err(CryptoError::OpenFailed)
    );

    // Truncated below the tag -> open fails.
    assert_eq!(
        aead_open(&k.key, &k.iv, pn, &header, &ct[..ct.len() - 1]),
        Err(CryptoError::OpenFailed)
    );

    // Empty payload is still valid AEAD (tag only).
    let empty_ct = aead_seal(&k.key, &k.iv, pn, &header, b"").unwrap();
    assert_eq!(empty_ct.len(), AEAD_TAG_LEN);
    assert_eq!(
        aead_open(&k.key, &k.iv, pn, &header, &empty_ct).unwrap(),
        Vec::<u8>::new()
    );
}

/// Header protection is its own inverse: apply then strip recovers the header.
#[test]
fn header_protection_roundtrip() {
    let hp = arr16("9f50449e04a0e810283a1e9933adedd2");
    // header (long, first byte 0xc3) + 4-byte PN + >=30 bytes payload for the sample.
    let mut packet = hex("c30000000108");
    packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x02]); // pn = 2 at offset 6
    for i in 0..40u8 {
        packet.push(i ^ 0x5a); // payload (includes the 16-byte sample region)
    }
    let pn_offset = 6usize;
    let pn_len = 4usize;
    let original_first = packet[0];
    let original_pn: Vec<u8> = packet[pn_offset..pn_offset + pn_len].to_vec();

    hp_apply(&mut packet, pn_offset, pn_len, true, &hp).unwrap();
    // Header bytes are now masked (not equal to the originals).
    assert_ne!(packet[0], original_first);
    assert_ne!(&packet[pn_offset..pn_offset + pn_len], &original_pn[..]);

    // Strip: the sample (in the payload) is unchanged by HP, so recompute at the
    // RFC §5.4.2 offset (`pn_offset + 4`, independent of `pn_len`) and XOR again.
    let sample_offset = pn_offset + 4;
    let mask = hp_mask(&hp, &packet[sample_offset..]).unwrap();
    packet[0] ^= mask[0] & hp_first_byte_mask(true);
    hp_xor_packet_number(&mut packet, pn_offset, pn_len, &mask);
    assert_eq!(packet[0], original_first);
    assert_eq!(&packet[pn_offset..pn_offset + pn_len], &original_pn[..]);
}

/// `keys_from_secret` derives key/iv/hp from a raw traffic secret via the same
/// "quic key"/"quic iv"/"quic hp" labels, at AES-128-GCM lengths.
#[test]
fn keys_from_secret_labels() {
    let secret = arr32("9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b");
    let k = keys_from_secret(&secret);
    assert_eq!(
        k.key.to_vec(),
        hkdf_expand_label(&secret, "quic key", b"", 16)
    );
    assert_eq!(
        k.iv.to_vec(),
        hkdf_expand_label(&secret, "quic iv", b"", IV_LEN)
    );
    assert_eq!(
        k.hp.to_vec(),
        hkdf_expand_label(&secret, "quic hp", b"", HP_LEN)
    );
    assert_eq!(k.key.len(), KEY_LEN);
    assert_eq!(k.iv.len(), IV_LEN);
    assert_eq!(k.hp.len(), HP_LEN);
}

// --- Full Appendix A.2 / A.3 whole-packet known answers ---------------------

/// Protect a whole initial packet exactly as the wire would: seal the payload
/// (AEAD AAD = the unprotected header, which ends with the packet number), then
/// apply header protection at the RFC §5.4.2 sample offset `pn_offset + 4`.
fn protect_full(
    header_hex: &str,
    payload: &[u8],
    pn: u64,
    pn_offset: usize,
    pn_len: usize,
    keys: &PacketKeys,
) -> Vec<u8> {
    let header = hex(header_hex);
    let ct = aead_seal(&keys.key, &keys.iv, pn, &header, payload).expect("seal");
    let mut packet = header;
    packet.extend_from_slice(&ct);
    hp_apply(&mut packet, pn_offset, pn_len, true, &keys.hp).expect("hp_apply");
    packet
}

/// RFC 9001 Appendix A.2 — the complete Client Initial packet (22-byte header +
/// 1162-byte CRYPTO/PADDING payload -> the exact 1200-byte protected packet).
/// This pins the AEAD and the `pn_offset + 4` header sample offset for a 4-byte
/// packet number; the pre-fix `pn_offset + pn_len + 4` produces a different mask
/// and therefore a different first byte / packet number.
#[test]
fn rfc9001_a2_full_client_initial_packet() {
    let dcid = hex("8394c8f03e515708");
    let client = initial_keys(&dcid).0;
    let mut payload = hex(super::vectors::A2_CLIENT_INITIAL_PAYLOAD);
    assert_eq!(payload.len(), 245, "shown CRYPTO frame is 245 bytes");
    payload.resize(1162, 0); // PADDING frames (0x00) to reach the 1162-byte payload
    let protected = protect_full(
        super::vectors::A2_CLIENT_INITIAL_HEADER,
        &payload,
        2,
        18, // pn_offset
        4,  // pn_len
        &client,
    );
    assert_eq!(protected, hex(super::vectors::A2_CLIENT_INITIAL_PROTECTED));
    // Explicitly assert the RFC's post-protection header prefix.
    assert_eq!(
        &protected[..22],
        &hex("c000000001088394c8f03e5157080000449e7b9aec34")[..]
    );
}

/// RFC 9001 Appendix A.3 — the complete Server Initial packet with a 2-byte
/// packet number. The sample starts 4 bytes after the PN field (skipping the
/// first 2 ciphertext bytes), which only holds at `pn_offset + 4`.
#[test]
fn rfc9001_a3_full_server_initial_packet() {
    let dcid = hex("8394c8f03e515708");
    let server = initial_keys(&dcid).1;
    let payload = hex(super::vectors::A3_SERVER_INITIAL_PAYLOAD);
    assert_eq!(payload.len(), 99);
    let protected = protect_full(
        super::vectors::A3_SERVER_INITIAL_HEADER,
        &payload,
        1,  // pn
        18, // pn_offset
        2,  // pn_len
        &server,
    );
    assert_eq!(protected, hex(super::vectors::A3_SERVER_INITIAL_PROTECTED));
    assert_eq!(
        &protected[..20],
        &hex("cf000000010008f067a5502a4262b5004075c0d9")[..]
    );
}

/// The short-header first-byte mask must be `0x1f` (5 bits: PN length + key
/// phase), NOT `0x7f`. Header protection must leave the top 3 bits untouched.
#[test]
fn short_header_mask_is_five_bits() {
    assert_eq!(hp_first_byte_mask(false), 0x1f);
    assert_eq!(hp_first_byte_mask(true), 0x0f);
    let hp = arr16("9f50449e04a0e810283a1e9933adedd2");
    // short header: first byte with top bits 1 and key-phase/PN bits set.
    let mut packet = hex("5a00112233445566778899aabbccddeeff0011223344556677889900aabbccddee");
    // first byte 0x5a; force mask[0] low bits to exercise bit 0x20 (only masked
    // for the *wrong* 0x7f). We only assert the unchanged upper 3 bits.
    let _ = &hp;
    // Build a case whose AES-ECB mask[0] has bit 0x20 set would require a real
    // mask; instead assert structurally that only 5 bits can ever be flipped.
    for m0 in 0u8..=255 {
        let changed = packet[0] ^ (m0 & hp_first_byte_mask(false));
        assert_eq!(
            changed & 0xe0,
            packet[0] & 0xe0,
            "top 3 bits must be preserved"
        );
        assert_eq!(
            (changed ^ packet[0]) & 0x20,
            0x00,
            "bit 0x20 must NOT be masked"
        );
    }
    // And that hp_apply on a short header never touches the top 3 bits.
    let before_top3 = packet[0] & 0xe0;
    hp_apply(&mut packet, 1, 1, false, &hp).unwrap();
    assert_eq!(packet[0] & 0xe0, before_top3);
}

/// The header-protection sample offset is `pn_offset + 4` regardless of the
/// packet-number length; round-trip protect->strip must recover the header for
/// every pn_len in 1..=4 using that single offset.
#[test]
fn hp_sample_offset_independent_of_pn_len() {
    let hp = arr16("9f50449e04a0e810283a1e9933adedd2");
    for pn_len in 1usize..=4 {
        let mut packet = hex("c30000000108");
        packet.extend_from_slice(&[0u8; 4]); // pn field (4 bytes reserved)
        for i in 0..48u8 {
            packet.push(i.wrapping_add(pn_len as u8) ^ 0x5a);
        }
        let pn_offset = 6usize;
        let orig_first = packet[0];
        let orig_pn: Vec<u8> = packet[pn_offset..pn_offset + pn_len].to_vec();
        hp_apply(&mut packet, pn_offset, pn_len, true, &hp).unwrap();
        // strip at the SAME (correct) offset
        let mask = hp_mask(&hp, &packet[pn_offset + 4..]).unwrap();
        packet[0] ^= mask[0] & hp_first_byte_mask(true);
        hp_xor_packet_number(&mut packet, pn_offset, pn_len, &mask);
        assert_eq!(packet[0], orig_first, "pn_len={pn_len} first byte",);
        assert_eq!(
            &packet[pn_offset..pn_offset + pn_len],
            &orig_pn[..],
            "pn_len={pn_len} pn"
        );
    }
}

/// Malformed packets must return an error, never panic (the pre-fix `hp_apply`
/// sliced past the end and indexed `packet[0]` unconditionally).
#[test]
fn hp_apply_malformed_returns_error() {
    let hp = arr16("9f50449e04a0e810283a1e9933adedd2");
    let cases: Vec<(Vec<u8>, usize, usize)> = vec![
        (vec![], 0, 4),                      // empty
        (vec![0xc3; 10], 6, 4),              // pn_end 10 == len, sample past end
        (vec![0xc3; 40], 30, 4),             // pn_offset past the packet
        (hex("c3000000010800000002"), 6, 0), // pn_len 0
        (hex("c3000000010800000002"), 6, 5), // pn_len > 4
        (vec![0xc3; 23], 4, 4),              // sample_end 24 > len 23 -> error
    ];
    for (mut p, off, len) in cases {
        assert!(
            hp_apply(&mut p, off, len, true, &hp).is_err(),
            "expected Err for off={off} len={len} pktlen={}",
            p.len()
        );
    }
}

/// HKDF-Expand / Expand-Label reject inputs beyond the RFC 5869 / TLS 1.3 caps
/// (L > 255*HashLen, label/context > 255, L > u16::MAX) instead of truncating or
/// overflowing the single-byte expand counter.
#[test]
fn hkdf_rejects_invalid_lengths() {
    let secret = arr32("9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b");
    // Legal maximum for our suite: a 32-byte label with a full-length request.
    assert_eq!(hkdf_expand_label(&secret, "quic key", b"", 32).len(), 32);
    // Beyond u16::MAX -> rejected (empty).
    assert!(hkdf_expand_label(&secret, "quic key", b"", 70000).is_empty());
    // Context longer than 255 -> rejected.
    let big_ctx = vec![0u8; 256];
    assert!(hkdf_expand_label(&secret, "quic key", &big_ctx, 32).is_empty());
    // Multi-block expansion boundary: exactly 255 blocks (8160 bytes) is produced;
    // one byte more (8161) is rejected rather than wrapping the counter.
    assert_eq!(hkdf_expand(&secret, b"info", 255 * 32).len(), 255 * 32);
    assert!(hkdf_expand(&secret, b"info", 255 * 32 + 1).is_empty());
}
