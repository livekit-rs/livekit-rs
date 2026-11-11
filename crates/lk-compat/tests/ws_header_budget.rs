//! Real tungstenite frame parser on a deterministic byte transport. This is a
//! protocol component gate, not an authenticated RTC/socket conformance claim.
use std::io::{self, Read, Write};
use tungstenite::error::{CapacityError, ProtocolError};
use tungstenite::protocol::{Role, WebSocketConfig};
use tungstenite::{Error, Message, WebSocket};

#[derive(Default)]
struct ByteTransport {
    input: Vec<u8>,
    position: usize,
    written: Vec<u8>,
    chunk: usize,
}
impl Read for ByteTransport {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.position == self.input.len() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = out
            .len()
            .min(self.input.len() - self.position)
            .min(self.chunk);
        out[..n].copy_from_slice(&self.input[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}
impl Write for ByteTransport {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.written.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn header(op: u8, fin: bool, len: usize, masked: bool) -> Vec<u8> {
    let mut bytes = vec![op | if fin { 128 } else { 0 }];
    let bit = if masked { 128 } else { 0 };
    if len < 126 {
        bytes.push(bit | len as u8);
    } else if len <= u16::MAX as usize {
        bytes.push(bit | 126);
        bytes.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        bytes.push(bit | 127);
        bytes.extend_from_slice(&(len as u64).to_be_bytes());
    }
    if masked {
        bytes.extend_from_slice(&[1, 2, 3, 4]);
    }
    bytes
}
fn frame(op: u8, fin: bool, body: &[u8], masked: bool) -> Vec<u8> {
    let mut bytes = header(op, fin, body.len(), masked);
    bytes.extend(body.iter().enumerate().map(|(i, byte)| {
        if masked {
            byte ^ [1, 2, 3, 4][i & 3]
        } else {
            *byte
        }
    }));
    bytes
}
fn socket(input: Vec<u8>, role: Role, chunk: usize) -> WebSocket<ByteTransport> {
    let transport = ByteTransport {
        input,
        chunk,
        ..Default::default()
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(1024))
        .max_frame_size(Some(4096));
    WebSocket::from_raw_socket(transport, role, Some(config))
}
fn assert_budget(error: Error) {
    assert!(
        matches!(
            error,
            Error::Capacity(CapacityError::MessageTooLong {
                size: 1025,
                max_size: 1024
            })
        ),
        "actual typed size failure, got {error:?}"
    );
}
#[test]
fn declared_single_over_budget_rejects_before_any_payload_arrives() {
    let input = header(2, true, 1025, true);
    let mut ws = socket(input.clone(), Role::Server, 1);
    assert_budget(ws.read().unwrap_err());
    assert_eq!(ws.get_ref().position, input.len());
}
#[test]
fn fragmented_budget_rejects_on_complete_header_without_tail_payload() {
    let mut input = frame(2, false, &[0; 1024], true);
    input.extend(header(0, true, 1, true));
    let mut ws = socket(input.clone(), Role::Server, 1);
    assert_budget(ws.read().unwrap_err());
    assert_eq!(ws.get_ref().position, input.len());
}
#[test]
fn split_mask_does_not_trigger_budget_until_the_complete_header_exists() {
    let first = frame(2, false, &[0; 1024], true);
    let h = header(0, true, 1, true);
    let mut input = first;
    input.extend_from_slice(&h[..h.len() - 1]);
    let mut ws = socket(input, Role::Server, 1);
    assert!(matches!(ws.read(), Err(Error::Io(ref e)) if e.kind()==io::ErrorKind::WouldBlock));
    ws.get_mut().input.push(*h.last().unwrap());
    assert_budget(ws.read().unwrap_err());
}
#[test]
fn control_125_bytes_does_not_consume_exhausted_data_message_budget() {
    let data = vec![0; 1024];
    let control = vec![17; 125];
    let mut input = frame(2, false, &data, true);
    input.extend(frame(9, true, &control, true));
    input.extend(frame(0, true, &[], true));
    let mut ws = socket(input, Role::Server, 1);
    assert_eq!(ws.read().unwrap(), Message::Ping(control.into()));
    assert_eq!(ws.read().unwrap(), Message::Binary(data.into()));
}
#[test]
fn unexpected_continuation_protocol_error_precedes_data_budget() {
    let mut ws = socket(header(0, true, 1025, true), Role::Server, 1);
    assert!(matches!(
        ws.read(),
        Err(Error::Protocol(ProtocolError::UnexpectedContinueFrame))
    ));
}
#[test]
fn new_data_during_fragmentation_is_not_reported_as_capacity() {
    let mut input = frame(2, false, &[0; 1024], true);
    input.extend(header(2, true, 1025, true));
    let mut ws = socket(input, Role::Server, 1);
    assert!(matches!(
        ws.read(),
        Err(Error::Protocol(ProtocolError::ExpectedFragment(_)))
    ));
}
#[test]
fn invalid_control_header_is_not_mislabeled_data_capacity() {
    let mut ws = socket(header(9, true, 126, true), Role::Server, 1);
    assert!(matches!(
        ws.read(),
        Err(Error::Protocol(ProtocolError::ControlFrameTooBig))
    ));
    let mut ws = socket(header(9, false, 126, true), Role::Server, 1);
    assert!(matches!(
        ws.read(),
        Err(Error::Protocol(ProtocolError::FragmentedControlFrame))
    ));
}
#[test]
fn reserved_opcode_keeps_original_parser_error_priority() {
    let mut ws = socket(header(3, true, 1025, true), Role::Server, 1);
    assert!(matches!(
        ws.read(),
        Err(Error::Protocol(ProtocolError::InvalidOpcode(3)))
    ));
}
#[test]
fn client_role_retains_original_payload_read_before_budget() {
    let mut ws = socket(header(2, true, 1025, false), Role::Client, 1);
    assert!(matches!(ws.read(), Err(Error::Io(ref e)) if e.kind()==io::ErrorKind::WouldBlock));
}
