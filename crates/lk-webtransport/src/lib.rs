//! WebTransport-over-HTTP/3 primitives for LiveKit, built entirely on the
//! pure-Rust QUIC stack: `rtc-tls` (rustls custom provider, no ring/aws-lc)
//! -> `quinn` -> `h3`/`h3-quinn` -> this session layer.
//!
//! Mirrors the upstream Go server contract (`pkg/service/webtransport.go`
//! on `quic-go/webtransport-go`): one UDP socket with the `h3` ALPN and
//! datagrams enabled, h3 SETTINGS advertising `enable_webtransport`,
//! Extended CONNECT upgrades into [`session::WtSession`]s, and WebTransport
//! streams demuxed by their raw varint headers (`0x41 || session` bidirect‐
//! ional, `0x54 || session` unidirectional) with unframed payload afterwards
//! plus RFC 9297 QUIC datagrams.
//!
//! The layout follows the official `h3-webtransport` crate's server design
//! (single accept pump with first-frame classification through h3's
//! request resolver) while keeping both roles in one crate and taking
//! datagrams straight through Quinn's bare `send_datagram`/`read_datagram`
//! instead of the `h3-datagram` handler traits.

mod client;
mod server;
mod session;

pub use client::{
    AllowAnyCert, WtClient, client_config, client_config_insecure, connect, connect_observing,
    connect_with_headers, endpoint as client_endpoint,
};
pub use h3::webtransport::SessionId;
pub use server::{
    Accepted, H3_ALPN, ServerTls, WT_BUFFERED_STREAM_REJECTED_ERROR_CODE, WtServerConnection,
    accept_datagram, endpoint, is_webtransport_connect, reject, reject_with_body, serve,
    server_config, upgrade_connect,
};
pub use session::{
    BufBidi, BufRecv, BufSend, CLOSE_SESSION_CAPSULE_TYPE, ClientConnect, ConnectStream,
    ControlReader, ControlWriter, H3Bidi, H3Conn, H3Recv, H3Send, Result as WtResult,
    SESSION_CLOSE_OK, ServerConnect, SessionStreams, WT_SESSION_GONE_ERROR_CODE, WtBidiStream,
    WtError, WtRecvStream, WtSendOnlyStream, WtSession, remote_stream_reset_code,
};
