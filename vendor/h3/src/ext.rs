//! Extensions for the HTTP/3 protocol.

use std::str::FromStr;

/// Describes the `:protocol` pseudo-header for extended connect
///
/// See: <https://www.rfc-editor.org/rfc/rfc8441#section-4>
#[derive(Copy, PartialEq, Debug, Clone)]
pub struct Protocol(ProtocolInner);

impl Protocol {
    /// WebTransport protocol
    pub const WEB_TRANSPORT: Protocol = Protocol(ProtocolInner::WebTransport);
    /// WebTransport protocol under the token registered by
    /// draft-ietf-webtrans-http3-15, which superseded the `webtransport` one.
    ///
    /// LOCAL DEVIATION from upstream h3 0.0.8 (see
    /// webtransport-final-settings.patch): without this token a client speaking the
    /// final protocol id -- `webtransport-go` v0.12.0, which the LiveKit reference
    /// server pins -- has its CONNECT rejected as a malformed request, because
    /// `:protocol` is validated against this list while the headers are parsed.
    pub const WEB_TRANSPORT_H3: Protocol = Protocol(ProtocolInner::WebTransportH3);
    /// RFC 9298 protocol
    pub const CONNECT_UDP: Protocol = Protocol(ProtocolInner::ConnectUdp);

    /// Return a &str representation of the `:protocol` pseudo-header value
    #[inline]
    pub fn as_str(&self) -> &str {
        match self.0 {
            ProtocolInner::WebTransport => "webtransport",
            ProtocolInner::WebTransportH3 => "webtransport-h3",
            ProtocolInner::ConnectUdp => "connect-udp",
        }
    }

    /// Whether this protocol designates a WebTransport session, under either the
    /// final or the legacy token. `webtransport-go` accepts both on the server and
    /// sends the final one on the client, so an application routing CONNECTs has to
    /// treat either as a session request.
    #[inline]
    pub fn is_webtransport(&self) -> bool {
        matches!(self.0, ProtocolInner::WebTransport | ProtocolInner::WebTransportH3)
    }
}

#[derive(Copy, PartialEq, Debug, Clone)]
enum ProtocolInner {
    WebTransport,
    WebTransportH3,
    ConnectUdp,
}

/// Error when parsing the protocol
pub struct InvalidProtocol;

impl FromStr for Protocol {
    type Err = InvalidProtocol;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "webtransport" => Ok(Self(ProtocolInner::WebTransport)),
            "webtransport-h3" => Ok(Self(ProtocolInner::WebTransportH3)),
            "connect-udp" => Ok(Self(ProtocolInner::ConnectUdp)),
            _ => Err(InvalidProtocol),
        }
    }
}
