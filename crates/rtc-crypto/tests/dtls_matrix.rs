//! Public migrated-provider handshake matrix. No sockets or generated secrets persist.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use p256::pkcs8::EncodePrivateKey;
use str0m_proto::crypto::DtlsVersion;
use str0m_proto::crypto::dtls::{DtlsCert, DtlsInstance, DtlsOutput, ProtocolVersion};
use x509_cert::der::Decode;

#[derive(Default)]
struct Observed {
    connected: bool,
    peer_cert: Vec<u8>,
    exporter: Vec<u8>,
    plaintext: Vec<u8>,
}

// DTLS authenticates the CertificateVerify signature under the advertised SPKI,
// while WebRTC authenticates its certificate via the caller's SDP fingerprint.
// This fixture binds a deterministic P384 key to an upstream certificate SPKI;
// its issuer signature is NOT regenerated and no PKI trust claim is made.
fn p384_endpoint() -> DtlsCert {
    let key = p384::ecdsa::SigningKey::from_slice(&[7; 48]).unwrap();
    let template = include_bytes!("../../../vendor/dimpl/src/crypto/validation/p384_cert.der");
    let cert = x509_cert::Certificate::from_der(template).unwrap();
    let previous = cert
        .tbs_certificate()
        .subject_public_key_info()
        .subject_public_key
        .as_bytes()
        .unwrap();
    let public = key.verifying_key().to_sec1_point(false);
    assert_eq!(previous.len(), public.as_bytes().len());
    let offsets: Vec<_> = template
        .windows(previous.len())
        .enumerate()
        .filter_map(|(i, p)| (p == previous).then_some(i))
        .collect();
    assert_eq!(offsets.len(), 1);
    let mut certificate = template.to_vec();
    certificate[offsets[0]..offsets[0] + previous.len()].copy_from_slice(public.as_bytes());
    DtlsCert {
        certificate,
        private_key: key.to_pkcs8_der().unwrap().as_bytes().to_vec(),
    }
}

fn drain(endpoint: &mut dyn DtlsInstance, state: &mut Observed, outgoing: &mut VecDeque<Vec<u8>>) {
    let mut buf = vec![0; 16384];
    for _ in 0..1000 {
        match endpoint.poll_output(&mut buf) {
            DtlsOutput::Packet(packet) => outgoing.push_back(packet.to_vec()),
            DtlsOutput::Connected => state.connected = true,
            DtlsOutput::PeerCert(cert) => state.peer_cert = cert.to_vec(),
            DtlsOutput::KeyingMaterial(material, _) => state.exporter = material.to_vec(),
            DtlsOutput::ApplicationData(data) => state.plaintext.extend_from_slice(data),
            DtlsOutput::BufferTooSmall { needed } => {
                assert!(needed <= 65536);
                buf.resize(needed, 0);
            }
            DtlsOutput::Timeout(_) => return,
            other => panic!("unexpected matrix output: {other:?}"),
        }
        assert!(outgoing.len() <= 1000, "bounded pending datagrams");
    }
    panic!("poll did not reach Timeout");
}

fn deliver(
    a: &mut dyn DtlsInstance,
    b: &mut dyn DtlsInstance,
    oa: &mut Observed,
    ob: &mut Observed,
    ab: &mut VecDeque<Vec<u8>>,
    ba: &mut VecDeque<Vec<u8>>,
) {
    for _ in 0..2000 {
        if let Some(packet) = ab.pop_front() {
            b.handle_packet(&packet).unwrap();
            drain(b, ob, ba);
        } else if let Some(packet) = ba.pop_front() {
            a.handle_packet(&packet).unwrap();
            drain(a, oa, ab);
        } else {
            return;
        }
    }
    panic!("bounded packet delivery exhausted");
}

#[test]
fn migrated_provider_dtls12_dtls13_auto_fallback_with_both_certificate_curves() {
    let provider = rtc_crypto::provider();
    let cert256 = rtc_crypto::dtls_cert().unwrap();
    let cert384 = p384_endpoint();
    for swap in [false, true] {
        let (ca, cb) = if swap {
            (&cert384, &cert256)
        } else {
            (&cert256, &cert384)
        };
        for (va, vb, expected) in [
            (
                DtlsVersion::Dtls12,
                DtlsVersion::Dtls12,
                ProtocolVersion::DTLS1_2,
            ),
            (
                DtlsVersion::Dtls13,
                DtlsVersion::Dtls13,
                ProtocolVersion::DTLS1_3,
            ),
            (
                DtlsVersion::Auto,
                DtlsVersion::Auto,
                ProtocolVersion::DTLS1_3,
            ),
            (
                DtlsVersion::Auto,
                DtlsVersion::Dtls12,
                ProtocolVersion::DTLS1_2,
            ),
            (
                DtlsVersion::Dtls12,
                DtlsVersion::Auto,
                ProtocolVersion::DTLS1_2,
            ),
            (
                DtlsVersion::Auto,
                DtlsVersion::Dtls13,
                ProtocolVersion::DTLS1_3,
            ),
            (
                DtlsVersion::Dtls13,
                DtlsVersion::Auto,
                ProtocolVersion::DTLS1_3,
            ),
        ] {
            let start = Instant::now();
            let mut a = provider
                .dtls_provider
                .new_dtls(ca, start, va, Some(1200))
                .unwrap();
            let mut b = provider
                .dtls_provider
                .new_dtls(cb, start, vb, Some(1200))
                .unwrap();
            let (mut oa, mut ob) = (Observed::default(), Observed::default());
            let (mut ab, mut ba) = (VecDeque::new(), VecDeque::new());
            a.set_active(true);
            drain(a.as_mut(), &mut oa, &mut ab);
            b.set_active(false);
            drain(b.as_mut(), &mut ob, &mut ba);
            for millis in (0..10000).step_by(10) {
                let now = start + Duration::from_millis(millis);
                a.handle_timeout(now).unwrap();
                drain(a.as_mut(), &mut oa, &mut ab);
                b.handle_timeout(now).unwrap();
                drain(b.as_mut(), &mut ob, &mut ba);
                deliver(a.as_mut(), b.as_mut(), &mut oa, &mut ob, &mut ab, &mut ba);
                if oa.connected
                    && ob.connected
                    && !oa.exporter.is_empty()
                    && !ob.exporter.is_empty()
                {
                    break;
                }
            }
            assert!(oa.connected && ob.connected, "swap={swap}, {va:?}/{vb:?}");
            assert_eq!(a.protocol_version(), Some(expected));
            assert_eq!(b.protocol_version(), Some(expected));
            assert_eq!(oa.peer_cert, cb.certificate);
            assert_eq!(ob.peer_cert, ca.certificate);
            assert!(!oa.exporter.is_empty());
            assert_eq!(oa.exporter, ob.exporter);
            a.send_application_data(b"client to server").unwrap();
            drain(a.as_mut(), &mut oa, &mut ab);
            b.send_application_data(b"server to client").unwrap();
            drain(b.as_mut(), &mut ob, &mut ba);
            deliver(a.as_mut(), b.as_mut(), &mut oa, &mut ob, &mut ab, &mut ba);
            assert_eq!(oa.plaintext, b"server to client");
            assert_eq!(ob.plaintext, b"client to server");
        }
    }
}
