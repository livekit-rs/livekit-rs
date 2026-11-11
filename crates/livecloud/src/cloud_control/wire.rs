use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use msgtrans::{CompressionType, Packet, PacketType};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

use super::Error;

const SUBPROTOCOL: &str = "msgtrans.v1";
const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
pub(super) const CONFIG_CHANGED: u8 = 16;

pub(super) struct Wire {
    stream: WebSocketStream<TlsStream<TcpStream>>,
    request_id: u32,
}

impl Wire {
    pub(super) async fn connect(endpoint: &str, ca_file: Option<&Path>) -> Result<Self, Error> {
        let mut request = endpoint
            .into_client_request()
            .map_err(|_| Error::Endpoint)?;
        let uri = request.uri();
        if uri.scheme_str() != Some("wss")
            || uri.authority().is_none_or(|a| a.as_str().contains('@'))
        {
            return Err(Error::Endpoint);
        }
        let host = uri.host().ok_or(Error::Endpoint)?;
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let port = uri.port_u16().unwrap_or(443);
        let name = rtc_tls::rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|_| Error::Endpoint)?;
        let mut config = match ca_file {
            Some(path) => rtc_tls::client_config_with_ca_file(path),
            None => rtc_tls::client_config(),
        }
        .map_err(|_| Error::Tls)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, SUBPROTOCOL.parse().unwrap());
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|_| Error::Transport)?;
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .map_err(|_| Error::Tls)?;
        let limits = WebSocketConfig::default()
            .max_message_size(Some(MAX_PAYLOAD + 16))
            .max_frame_size(Some(MAX_PAYLOAD + 16));
        let (stream, response) =
            tokio_tungstenite::client_async_with_config(request, tls, Some(limits))
                .await
                .map_err(|_| Error::Transport)?;
        if response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            != Some(SUBPROTOCOL)
        {
            return Err(Error::Frame);
        }
        Ok(Self {
            stream,
            request_id: 0,
        })
    }

    pub(super) async fn request<T: Serialize, R: DeserializeOwned>(
        &mut self,
        business: u8,
        request: &T,
        budget: Duration,
    ) -> Result<(R, bool), Error> {
        timeout(budget, self.exchange(business, request))
            .await
            .map_err(|_| Error::Timeout)?
    }

    async fn exchange<T: Serialize, R: DeserializeOwned>(
        &mut self,
        business: u8,
        request: &T,
    ) -> Result<(R, bool), Error> {
        self.request_id = self.request_id.checked_add(1).ok_or(Error::Frame)?;
        let payload = serde_json::to_vec(request).map_err(|_| Error::Json)?;
        if payload.len() > MAX_PAYLOAD {
            return Err(Error::Frame);
        }
        let mut packet = Packet::request(self.request_id, payload);
        packet.set_biz_type(business);
        self.stream
            .send(Message::Binary(
                packet.try_encode().map_err(|_| Error::Frame)?,
            ))
            .await
            .map_err(|_| Error::Transport)?;
        let mut changed = false;
        loop {
            let response = self.read().await?;
            match response.packet_type() {
                PacketType::OneWay if response.biz_type() == CONFIG_CHANGED => changed = true,
                PacketType::Response
                    if response.message_id() == self.request_id
                        && response.biz_type() == business =>
                {
                    return Ok((
                        serde_json::from_slice(response.payload()).map_err(|_| Error::Json)?,
                        changed,
                    ));
                }
                _ => return Err(Error::Frame),
            }
        }
    }

    pub(super) async fn read(&mut self) -> Result<Packet, Error> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Binary(bytes))) => {
                    let packet = Packet::decode_exact(&bytes).map_err(|_| Error::Frame)?;
                    if packet.is_fragmented() || packet.compression() != CompressionType::None {
                        return Err(Error::Frame);
                    }
                    return Ok(packet);
                }
                Some(Ok(Message::Ping(_))) => {
                    self.stream.flush().await.map_err(|_| Error::Transport)?
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Text(_) | Message::Frame(_))) => return Err(Error::Frame),
                _ => return Err(Error::Transport),
            }
        }
    }
}
