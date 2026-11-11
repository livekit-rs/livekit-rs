//! Independent public-codec wire tests; actual HTTP/worker tests are separate.
#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read, Write};
    use tungstenite::{Error, Message, WebSocket, protocol::Role};
    #[derive(Default)]
    struct Transport {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }
    impl Read for Transport {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.input.read(b)
        }
    }
    impl Write for Transport {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn frame(body: &[u8], masked: bool) -> Vec<u8> {
        assert!(body.len() < 126);
        let mut out = vec![0x88, body.len() as u8 | if masked { 0x80 } else { 0 }];
        if masked {
            let key = [9, 8, 7, 6];
            out.extend(key);
            out.extend(body.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
        } else {
            out.extend(body);
        }
        out
    }
    fn payload(wire: &[u8], masked: bool) -> Vec<u8> {
        assert_eq!(wire[0], 0x88);
        assert_eq!(wire[1] & 0x80 != 0, masked);
        let n = (wire[1] & 0x7f) as usize;
        assert!(n < 126);
        let start = if masked { 6 } else { 2 };
        assert_eq!(wire.len(), start + n);
        (0..n)
            .map(|i| wire[start + i] ^ if masked { wire[2 + i % 4] } else { 0 })
            .collect()
    }
    fn exercise(role: Role, code: Option<u16>, reason: &str, invalid: bool) {
        let mut body = Vec::new();
        if let Some(code) = code {
            body.extend(code.to_be_bytes());
            body.extend(reason.as_bytes());
        }
        let transport = Transport {
            input: Cursor::new(frame(&body, role == Role::Server)),
            ..Default::default()
        };
        let mut socket = WebSocket::from_raw_socket(transport, role, None);
        let Message::Close(received) = socket.read().expect("actual public codec Close") else {
            panic!("Close required")
        };
        let (expected_code, received_reason) = if invalid {
            (Some(1002), "Protocol violation")
        } else {
            (code, reason)
        };
        assert_eq!(received.as_ref().map(|f| u16::from(f.code)), expected_code);
        assert_eq!(
            received.as_ref().map_or("", |f| f.reason.as_str()),
            received_reason,
            "input message must not be rewritten by reply policy"
        );
        assert!(matches!(
            socket.flush(),
            Ok(()) | Err(Error::ConnectionClosed)
        ));
        let actual = payload(&socket.get_ref().output, role == Role::Client);
        let mut expected = Vec::new();
        if let Some(code) = expected_code {
            expected.extend(code.to_be_bytes());
            let omit = cfg!(feature = "livekit-policy") && role == Role::Server && !invalid;
            if !omit {
                expected.extend(received_reason.as_bytes());
            }
        }
        assert_eq!(
            actual,
            expected,
            "exact queued reply code/reason, role={role:?}, feature={}",
            cfg!(feature = "livekit-policy")
        );
    }
    #[test]
    fn valid_server_ack_and_client_default_preserve_input_and_exact_wire() {
        for role in [Role::Server, Role::Client] {
            for (code, reason) in [
                (Some(1000), "fixture complete"),
                (Some(1001), "bye"),
                (Some(4001), "再见"),
                (None, ""),
            ] {
                exercise(role, code, reason, false);
            }
        }
    }
    #[test]
    fn invalid_code_transformation_and_reply_remain_upstream_in_both_roles() {
        for role in [Role::Server, Role::Client] {
            exercise(role, Some(1006), "invalid input reason", true);
        }
    }
}
