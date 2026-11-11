//! Client trust sources. Linux mirrors Go 1.26 root_unix/root_linux file/dir
//! overrides and partial-success policy. macOS/Windows export OS trust anchors;
//! explicit PEM paths remain an additive cross-platform product extension.
//! DER loading is the only OS operation: WebPKI still verifies every peer chain.
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, pem::PemObject};

use crate::TlsError;

#[derive(Default)]
struct Sources {
    file: Option<PathBuf>,
    dirs: Option<Vec<PathBuf>>,
}

impl Sources {
    fn from_env() -> Self {
        Self {
            file: std::env::var_os("SSL_CERT_FILE")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            dirs: std::env::var_os("SSL_CERT_DIR")
                .filter(|v| !v.is_empty())
                .map(|v| {
                    std::env::split_paths(&v)
                        .filter(|p| !p.as_os_str().is_empty())
                        .collect()
                }),
        }
    }
}

pub(crate) fn load() -> Result<RootCertStore, TlsError> {
    let sources = Sources::from_env();
    #[cfg(target_os = "linux")]
    let certs = collect_paths(
        &sources,
        &[
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/ca-bundle.pem",
            "/etc/pki/tls/cacert.pem",
            "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
            "/etc/ssl/cert.pem",
        ],
        &["/etc/ssl/certs", "/etc/pki/tls/certs"],
    );
    #[cfg(not(target_os = "linux"))]
    let certs = {
        // native-certs treats any explicit SSL_CERT_* as a wholesale replacement.
        // Preserve the existing additive extension without mutating process env.
        let explicit = sources.file.is_some() || sources.dirs.is_some();
        let mut certs = if explicit {
            os_certs_ignoring_env()
        } else {
            rustls_native_certs::load_native_certs().certs
        };
        if explicit {
            certs.extend(collect_paths(&sources, &[], &[]));
        }
        certs
    };
    store(certs)
}

/// Match Go's first readable bundle plus all selected directories. Missing or
/// malformed entries are ignored when another source supplies usable roots.
fn collect_paths(
    sources: &Sources,
    default_files: &[&str],
    default_dirs: &[&str],
) -> Vec<CertificateDer<'static>> {
    let mut out = Vec::new();
    let files = sources
        .file
        .as_ref()
        .map(|p| vec![p.clone()])
        .unwrap_or_else(|| default_files.iter().map(PathBuf::from).collect());
    for file in files {
        if let Ok(bytes) = std::fs::read(file) {
            out.extend(CertificateDer::pem_slice_iter(&bytes).filter_map(Result::ok));
            break;
        }
    }
    let dirs = sources
        .dirs
        .clone()
        .unwrap_or_else(|| default_dirs.iter().map(PathBuf::from).collect());
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                append_file(&path, &mut out);
            }
        }
    }
    out
}

fn append_file(path: &Path, out: &mut Vec<CertificateDer<'static>>) {
    if let Ok(bytes) = std::fs::read(path) {
        out.extend(CertificateDer::pem_slice_iter(&bytes).filter_map(Result::ok));
    }
}

fn store(certs: Vec<CertificateDer<'static>>) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    let mut seen = HashSet::new();
    for cert in certs {
        if seen.insert(cert.as_ref().to_vec()) {
            let _ = roots.add(cert);
        }
    }
    if roots.is_empty() {
        return Err(TlsError::Pem(
            "no usable client trust roots from system stores or SSL_CERT_FILE/SSL_CERT_DIR".into(),
        ));
    }
    Ok(roots)
}

// OS loading adaptations: Copyright (c) 2016, Joseph Birr-Pixton.
// Adapted from rustls-native-certs 0.8.5 under the ISC option; see
// NOTICE-rustls-native-certs.txt for the full permission notice. Only required to export OS DER while explicit PEM env paths are
// also configured; no chain validator, TLS engine or signing operation is used.
#[cfg(target_os = "macos")]
fn os_certs_ignoring_env() -> Vec<CertificateDer<'static>> {
    use security_framework::trust_settings::{Domain, TrustSettings, TrustSettingsForCertificate};
    let mut trusted = std::collections::HashMap::new();
    for domain in [Domain::User, Domain::Admin, Domain::System] {
        let settings = TrustSettings::new(domain);
        let Ok(certs) = settings.iter() else {
            continue;
        };
        for cert in certs {
            if let Ok(setting) = settings.tls_trust_settings_for_certificate(&cert) {
                trusted
                    .entry(cert.to_der())
                    .or_insert(setting.unwrap_or(TrustSettingsForCertificate::TrustRoot));
            }
        }
    }
    trusted
        .into_iter()
        .filter_map(|(der, setting)| {
            matches!(
                setting,
                TrustSettingsForCertificate::TrustRoot | TrustSettingsForCertificate::TrustAsRoot
            )
            .then(|| CertificateDer::from(der))
        })
        .collect()
}

#[cfg(windows)]
fn os_certs_ignoring_env() -> Vec<CertificateDer<'static>> {
    use schannel::cert_context::ValidUses;
    let Ok(store) = schannel::cert_store::CertStore::open_current_user("ROOT") else {
        return vec![];
    };
    store
        .certs()
        .filter_map(|cert| {
            let usable = match cert.valid_uses().ok()? {
                ValidUses::All => true,
                ValidUses::Oids(oids) => oids.iter().any(|oid| oid == "1.3.6.1.5.5.7.3.1"),
            };
            (usable && cert.is_time_valid().ok()?)
                .then(|| CertificateDer::from(cert.to_der().to_vec()))
        })
        .collect()
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn os_certs_ignoring_env() -> Vec<CertificateDer<'static>> {
    rustls_native_certs::load_native_certs().certs
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::ServerName;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    const ROOT: &[u8] = include_bytes!("../tests/fixtures/root.pem");
    const OTHER_ROOT: &[u8] = include_bytes!("../tests/fixtures/p384-root.pem");
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "livekit-roots-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn roots(sources: Sources, files: &[&str], dirs: &[&str]) -> Result<RootCertStore, TlsError> {
        store(collect_paths(&sources, files, dirs))
    }

    #[test]
    fn file_and_directory_override_default_categories_independently() {
        let t = Temp::new();
        let private = t.file("private.pem", ROOT);
        let public = t.file("public.pem", OTHER_ROOT);
        let dir = t.0.join("dir");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("public.pem"), OTHER_ROOT).unwrap();
        let file_only = roots(
            Sources {
                file: Some(private.clone()),
                dirs: None,
            },
            &[],
            &[dir.to_str().unwrap()],
        )
        .unwrap();
        assert_eq!(file_only.len(), 2);
        let private_dir = t.0.join("private");
        std::fs::create_dir(&private_dir).unwrap();
        std::fs::write(private_dir.join("root.pem"), ROOT).unwrap();
        let dir_only = roots(
            Sources {
                file: None,
                dirs: Some(vec![private_dir]),
            },
            &[public.to_str().unwrap()],
            &[],
        )
        .unwrap();
        assert_eq!(dir_only.len(), 2);
        let both = roots(
            Sources {
                file: Some(private),
                dirs: Some(vec![]),
            },
            &[public.to_str().unwrap()],
            &[dir.to_str().unwrap()],
        )
        .unwrap();
        assert_eq!(both.len(), 1);
    }

    #[test]
    fn partial_missing_and_invalid_sources_do_not_discard_a_valid_private_ca_directory() {
        let t = Temp::new();
        let dir = t.0.join("roots");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("root.pem"), ROOT).unwrap();
        std::fs::write(
            dir.join("garbage.pem"),
            b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::fs::write(dir.join("README"), b"not a certificate").unwrap();
        let store = roots(
            Sources {
                file: Some(t.0.join("missing.pem")),
                dirs: Some(vec![t.0.join("missing-dir"), dir]),
            },
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn an_invalid_explicit_file_can_still_use_default_directory_roots_like_go() {
        let t = Temp::new();
        let bad = t.file("bad.pem", b"not PEM");
        t.file("root.pem", ROOT);
        let store = roots(
            Sources {
                file: Some(bad),
                dirs: None,
            },
            &[],
            &[t.0.to_str().unwrap()],
        )
        .unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn no_usable_roots_is_an_error_and_duplicate_der_is_deduplicated() {
        let t = Temp::new();
        let bad = t.file("bad.pem", b"not PEM");
        assert!(
            roots(
                Sources {
                    file: Some(bad),
                    dirs: Some(vec![t.0.join("missing")])
                },
                &[],
                &[]
            )
            .is_err()
        );
        assert!(store(vec![CertificateDer::from(vec![1, 2, 3])]).is_err());
        let file = t.file("ca.pem", ROOT);
        assert_eq!(
            roots(
                Sources {
                    file: Some(file),
                    dirs: Some(vec![t.0.clone()])
                },
                &[],
                &[]
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[test]
    fn first_readable_bundle_stops_search_even_if_empty_as_go_does() {
        let t = Temp::new();
        let empty = t.file("empty", b"");
        let valid = t.file("root.pem", ROOT);
        assert!(
            roots(
                Sources::default(),
                &[empty.to_str().unwrap(), valid.to_str().unwrap()],
                &[]
            )
            .is_err()
        );
    }

    async fn handshake(roots: RootCertStore, name: &'static str) -> bool {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Arc::new(
            crate::server_config_from_pem(
                include_bytes!("../tests/fixtures/server.pem"),
                include_bytes!("../tests/fixtures/server-key.pem"),
            )
            .unwrap(),
        );
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let Ok(mut tls) = TlsAcceptor::from(server).accept(tcp).await else {
                return false;
            };
            let mut data = [0; 2];
            if tls.read_exact(&mut data).await.is_err() {
                return false;
            }
            data == *b"CA"
        });
        let client = Arc::new(crate::client_config_with_roots(roots).unwrap());
        let result = TlsConnector::from(client)
            .connect(
                ServerName::try_from(name).unwrap(),
                tokio::net::TcpStream::connect(addr).await.unwrap(),
            )
            .await;
        let success = match result {
            Ok(mut tls) => tls.write_all(b"CA").await.is_ok(),
            Err(_) => false,
        };
        let accepted = task.await.unwrap();
        success && accepted
    }

    #[tokio::test]
    async fn private_ca_directory_verifies_real_tls_but_wrong_name_and_untrusted_ca_fail() {
        let t = Temp::new();
        t.file("issuer.pem", ROOT);
        let roots = roots(
            Sources {
                file: None,
                dirs: Some(vec![t.0.clone()]),
            },
            &[],
            &[],
        )
        .unwrap();
        assert!(handshake(roots.clone(), "localhost").await);
        assert!(!handshake(roots, "wrong.example").await);
        let wrong = CertificateDer::pem_slice_iter(OTHER_ROOT)
            .map(Result::unwrap)
            .collect();
        assert!(!handshake(store(wrong).unwrap(), "localhost").await);
    }
    #[test]
    #[ignore = "subprocess helper for standard SSL_CERT_FILE/SSL_CERT_DIR loading"]
    fn environment_child_loads_private_ca() {
        let actual = load().unwrap();
        let expected = store(
            CertificateDer::pem_slice_iter(ROOT)
                .map(Result::unwrap)
                .collect(),
        )
        .unwrap();
        assert!(
            actual
                .roots
                .iter()
                .any(|root| root.subject == expected.roots[0].subject
                    && root.subject_public_key_info == expected.roots[0].subject_public_key_info)
        );
    }

    #[test]
    fn real_environment_file_and_directory_loading_isolated_in_subprocesses() {
        let t = Temp::new();
        let file = t.file("issuer.pem", ROOT);
        let dir = t.0.join("dir");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("issuer.pem"), ROOT).unwrap();
        for (file, dir) in [
            (file, t.0.join("missing-dir")),
            (t.0.join("missing-file"), dir),
        ] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "roots::tests::environment_child_loads_private_ca",
                    "--ignored",
                ])
                .env("SSL_CERT_FILE", file)
                .env("SSL_CERT_DIR", dir)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "private CA env child failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
