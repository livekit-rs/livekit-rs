//! STUN discovery on a socket before the ICE reader takes ownership.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use rtc_turn::stun::{Class, Message, MessageBuilder, Method, attr};
use tokio::net::UdpSocket;

use super::MAX_DATAGRAM;

/// Send one Binding request from the socket that will carry the mapped ICE
/// candidate. No other task may receive from this socket during discovery.
pub(super) async fn gather(
    socket: &UdpSocket,
    server: SocketAddr,
    timeout: Duration,
) -> io::Result<SocketAddr> {
    let mut transaction = [0u8; 12];
    rtc_core::util::fill_random(&mut transaction);
    let request =
        MessageBuilder::new(Method::Binding, Class::Request, transaction).build(None, false);

    tokio::time::timeout(timeout, async {
        socket.send_to(&request, server).await?;
        let mut buffer = [0; MAX_DATAGRAM];
        loop {
            let (len, source) = socket.recv_from(&mut buffer).await?;
            let data = &buffer[..len];
            if source != server || data.get(8..20) != Some(transaction.as_slice()) {
                continue;
            }
            let response = Message::decode(data).map_err(io::Error::other)?;
            if response.method() != Method::Binding {
                continue;
            }
            match response.class() {
                Class::Success => {
                    return response
                        .xor_address(attr::XOR_MAPPED_ADDRESS)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "STUN response has no XOR-MAPPED-ADDRESS",
                            )
                        })?
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
                }
                Class::Error => {
                    return Err(io::Error::other("STUN Binding Error response"));
                }
                Class::Request | Class::Indication => continue,
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "STUN Binding request timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn accepts_complete_mapping_only_from_the_matching_stun_transaction() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let foreign = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let wrong_mapping = "203.0.113.9:1234".parse().unwrap();

        for endpoint in ["192.0.2.40:45678", "[2001:db8::a]:56789"] {
            let endpoint = endpoint.parse().unwrap();
            let respond = async {
                let mut request = [0; MAX_DATAGRAM];
                let (len, source) = server.recv_from(&mut request).await.unwrap();
                let request = Message::decode(&request[..len]).unwrap();
                assert_eq!(request.method(), Method::Binding);
                assert_eq!(request.class(), Class::Request);
                assert_eq!(source, socket.local_addr().unwrap());

                let mut wrong_source = MessageBuilder::response_to(&request, Class::Success);
                wrong_source.add_xor_address(attr::XOR_MAPPED_ADDRESS, wrong_mapping);
                foreign
                    .send_to(&wrong_source.build(None, false), source)
                    .await
                    .unwrap();

                let mut transaction = *request.transaction_id();
                transaction[0] ^= 1;
                let mut wrong_transaction =
                    MessageBuilder::new(Method::Binding, Class::Success, transaction);
                wrong_transaction.add_xor_address(attr::XOR_MAPPED_ADDRESS, wrong_mapping);
                server
                    .send_to(&wrong_transaction.build(None, false), source)
                    .await
                    .unwrap();

                let mut valid = MessageBuilder::response_to(&request, Class::Success);
                valid.add_xor_address(attr::XOR_MAPPED_ADDRESS, endpoint);
                server
                    .send_to(&valid.build(None, true), source)
                    .await
                    .unwrap();
            };
            let (mapping, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(
                    gather(&socket, server_addr, Duration::from_secs(1)),
                    respond
                )
            })
            .await
            .unwrap();
            assert_eq!(mapping.unwrap(), endpoint);
        }
    }
}
