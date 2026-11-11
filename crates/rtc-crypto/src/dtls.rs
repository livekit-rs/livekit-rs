//! DTLS provider using dimpl with its RustCrypto backend.
//!
//! Derived from str0m-rust-crypto 0.6.0 `dtls.rs` (MIT OR Apache-2.0). The only
//! behavioral change is certificate generation, see [`crate::cert`].

use std::sync::Arc;
use std::time::Instant;

use str0m_proto::crypto::CryptoError;
use str0m_proto::crypto::DtlsVersion;
use str0m_proto::crypto::dtls::ProtocolVersion;
use str0m_proto::crypto::dtls::{DtlsCert, DtlsImplError, DtlsInstance, DtlsOutput, DtlsProvider};

#[derive(Debug)]
pub(super) struct RustCryptoDtlsProvider;

impl DtlsProvider for RustCryptoDtlsProvider {
    fn generate_certificate(&self) -> Option<DtlsCert> {
        crate::cert::generate_self_signed().ok().map(|c| DtlsCert {
            certificate: c.certificate,
            private_key: c.private_key,
        })
    }

    fn new_dtls(
        &self,
        cert: &DtlsCert,
        now: Instant,
        dtls_version: DtlsVersion,
        mtu: Option<usize>,
    ) -> Result<Box<dyn DtlsInstance>, CryptoError> {
        let dimpl_cert = dimpl::DtlsCertificate {
            certificate: cert.certificate.clone(),
            private_key: cert.private_key.clone(),
        };

        // ICE verifies return routability before DTLS, making server cookies redundant.
        let mut builder = dimpl::Config::builder().use_server_cookie(false);
        if let Some(mtu) = mtu {
            builder = builder.mtu(mtu);
        }

        let config = builder
            .build()
            .map_err(|e| CryptoError::Other(format!("dimpl config creation failed: {e}")))?;

        let config = Arc::new(config);
        let dtls = match dtls_version {
            DtlsVersion::Dtls12 => dimpl::Dtls::new_12(config, dimpl_cert, now),
            DtlsVersion::Dtls13 => dimpl::Dtls::new_13(config, dimpl_cert, now),
            DtlsVersion::Auto => dimpl::Dtls::new_auto(config, dimpl_cert, now),
            _ => {
                return Err(CryptoError::Other(format!(
                    "Unsupported DTLS version: {dtls_version}"
                )));
            }
        };

        Ok(Box::new(RustCryptoDtlsInstance { dtls }))
    }
}

struct RustCryptoDtlsInstance {
    dtls: dimpl::Dtls,
}

impl std::fmt::Debug for RustCryptoDtlsInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustCryptoDtlsInstance").finish()
    }
}

impl DtlsInstance for RustCryptoDtlsInstance {
    fn set_active(&mut self, active: bool) {
        self.dtls.set_active(active);
    }

    fn handle_packet(&mut self, packet: &[u8]) -> Result<(), DtlsImplError> {
        self.dtls.handle_packet(packet)
    }

    fn poll_output<'a>(&mut self, buf: &'a mut [u8]) -> DtlsOutput<'a> {
        self.dtls.poll_output(buf)
    }

    fn handle_timeout(&mut self, now: Instant) -> Result<(), DtlsImplError> {
        self.dtls.handle_timeout(now)
    }

    fn send_application_data(&mut self, data: &[u8]) -> Result<(), DtlsImplError> {
        self.dtls.send_application_data(data)
    }

    fn is_active(&self) -> bool {
        self.dtls.is_active()
    }

    fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.dtls.protocol_version()
    }

    fn is_closing(&self) -> bool {
        self.dtls.is_closing()
    }

    fn is_closed(&self) -> bool {
        self.dtls.is_closed()
    }

    fn close(&mut self) -> Result<(), DtlsImplError> {
        self.dtls.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::time::Duration;

    #[derive(Default)]
    struct Observed {
        connected: bool,
        peer_cert: Vec<u8>,
        keying_material: Vec<u8>,
        plaintext: Vec<u8>,
    }

    fn drain(
        dtls: &mut dyn DtlsInstance,
        observed: &mut Observed,
        outgoing: &mut VecDeque<Vec<u8>>,
    ) {
        let mut buf = vec![0; 16_384];
        for _ in 0..100 {
            match dtls.poll_output(&mut buf) {
                DtlsOutput::Packet(p) => outgoing.push_back(p.to_vec()),
                DtlsOutput::Connected => observed.connected = true,
                DtlsOutput::PeerCert(c) => observed.peer_cert = c.to_vec(),
                DtlsOutput::KeyingMaterial(k, _) => observed.keying_material = k.to_vec(),
                DtlsOutput::ApplicationData(p) => observed.plaintext.extend_from_slice(p),
                DtlsOutput::Timeout(_) => return,
                DtlsOutput::BufferTooSmall { needed } => buf.resize(needed, 0),
                DtlsOutput::CloseNotify => panic!("unexpected close during handshake"),
                other => panic!("unhandled DTLS output: {other:?}"),
            }
        }
        panic!("DTLS output did not reach a timeout");
    }

    #[test]
    fn dtls12_generated_certificates_complete_handshake_and_exchange_application_data() {
        let start = Instant::now();
        let cert_a = crate::dtls_cert().unwrap();
        let cert_b = crate::dtls_cert().unwrap();
        let p = RustCryptoDtlsProvider;
        let mut a = p
            .new_dtls(&cert_a, start, DtlsVersion::Dtls12, Some(1200))
            .unwrap();
        let mut b = p
            .new_dtls(&cert_b, start, DtlsVersion::Dtls12, Some(1200))
            .unwrap();
        a.set_active(true);
        b.set_active(false);
        let (mut oa, mut ob) = (Observed::default(), Observed::default());
        let (mut ab, mut ba) = (VecDeque::new(), VecDeque::new());
        for n in 0..1000 {
            let now = start + Duration::from_millis(n * 10);
            a.handle_timeout(now).unwrap();
            b.handle_timeout(now).unwrap();
            drain(a.as_mut(), &mut oa, &mut ab);
            drain(b.as_mut(), &mut ob, &mut ba);
            while let Some(packet) = ab.pop_front() {
                b.handle_packet(&packet).unwrap();
            }
            while let Some(packet) = ba.pop_front() {
                a.handle_packet(&packet).unwrap();
            }
            if oa.connected
                && ob.connected
                && !oa.keying_material.is_empty()
                && !ob.keying_material.is_empty()
            {
                break;
            }
        }
        assert!(
            oa.connected && ob.connected,
            "both peers must complete DTLS 1.2"
        );
        assert_eq!(oa.peer_cert, cert_b.certificate);
        assert_eq!(ob.peer_cert, cert_a.certificate);
        assert!(!oa.keying_material.is_empty());
        assert_eq!(
            oa.keying_material, ob.keying_material,
            "DTLS-SRTP exporters agree"
        );
        a.send_application_data(b"reliable SCTP payload").unwrap();
        drain(a.as_mut(), &mut oa, &mut ab);
        while let Some(packet) = ab.pop_front() {
            b.handle_packet(&packet).unwrap();
        }
        drain(b.as_mut(), &mut ob, &mut ba);
        assert_eq!(ob.plaintext, b"reliable SCTP payload");
    }
}
