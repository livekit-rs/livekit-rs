//! go-redis `shouldRetry` classification for the failures the CLI session can
//! surface (references/go-modules/go-redis-v9.22.0/error.go:84). Batch 21m's
//! paired differential (`run-paired.py` in
//! `livekit-rs-server/tests/interop/cli-list-nodes-redis-tls`) measured the two
//! rules this file pins: a peer reset re-issues the command (official and Rust
//! both made 4 connections, and both made 5 dial attempts for one handshake
//! failure), while a TLS handshake failure ends the command after its single
//! 5-attempt dial group.

use super::Fault;
use std::io;

fn io_fault(kind: io::ErrorKind) -> Fault {
    Fault::Io {
        kind,
        message: format!("read tcp 127.0.0.1:1->127.0.0.1:2: {kind:?}"),
        inner: Some(format!("read: {kind:?}")),
    }
}

#[test]
fn non_timeout_network_errors_are_retried() {
    for kind in [
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::BrokenPipe,
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::Other,
    ] {
        assert!(
            io_fault(kind).retry(false),
            "{kind:?}: a Go net.Error that is not a timeout retries"
        );
    }
    assert!(
        io_fault(io::ErrorKind::UnexpectedEof).retry(false),
        "io.EOF retries"
    );
}

#[test]
fn deadline_errors_follow_the_command_timeout_flag() {
    for kind in [io::ErrorKind::TimedOut, io::ErrorKind::WouldBlock] {
        assert!(
            !io_fault(kind).retry(false),
            "{kind:?} off the dial path waits"
        );
        assert!(
            io_fault(kind).retry(true),
            "{kind:?} from a write is retried"
        );
    }
}

#[test]
fn protocol_and_server_and_handshake_faults_are_not_retried() {
    assert!(!Fault::Protocol("redis: invalid reply".into()).retry(true));
    assert!(!Fault::Handshake("context deadline exceeded".into()).retry(true));
    assert!(!Fault::Server(b"WRONGTYPE no".to_vec()).retry(true));
    assert!(
        Fault::Server(b"LOADING Redis is loading".to_vec()).retry(true),
        "the documented server prefixes still retry"
    );
}

#[test]
fn only_a_handshake_dial_reports_the_tls_stage() {
    let handshake = Fault::Handshake(
        "tls: failed to verify certificate: x509: certificate signed by unknown authority".into(),
    );
    assert!(handshake.handshake());
    assert_eq!(
        handshake.text(),
        "tls: failed to verify certificate: x509: certificate signed by unknown authority"
    );
    let wrapped = handshake.clone().prefix("failed to authenticate: ");
    assert!(wrapped.handshake(), "the stage survives one wrapping layer");
    assert!(wrapped.text().starts_with("failed to authenticate: tls: "));
    assert!(!Fault::Protocol("dial tcp 127.0.0.1:1: connect: refused".into()).handshake());
    assert!(!io_fault(io::ErrorKind::ConnectionReset).handshake());
}

#[test]
fn a_handshake_fault_discards_the_connection_without_retrying() {
    let tls = Fault::Handshake("context deadline exceeded".into());
    assert!(tls.bad_connection(), "the aborted dial is never pooled");
    assert!(!tls.server(), "a handshake fault is not a Redis reply");
    assert_eq!(tls.bytes(), b"context deadline exceeded".to_vec());
    let unwrapped = tls.clone().unwrap_once();
    assert_eq!(unwrapped.text(), tls.text(), "no layer to unwrap here");
}
