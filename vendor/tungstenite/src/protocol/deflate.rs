//! RFC 7692 raw DEFLATE with fresh state per message. Gorilla websocket
//! v1.5.3 compression.go uses the same sync-flush suffix and final empty block.
use crate::error::{CapacityError, Error, ProtocolError, Result};
use bytes::Bytes;
use flate2::Compression;
use std::io::Write;
use zlib_rs::{Inflate, InflateFlush, Status};

pub(super) fn encode(payload: &[u8]) -> Result<Bytes> {
    let mut writer = flate2::write::DeflateEncoder::new(Vec::new(), Compression::fast());
    writer.write_all(payload)?;
    writer.flush()?;
    let mut wire = std::mem::take(writer.get_mut());
    if !wire.ends_with(&[0, 0, 0xff, 0xff]) {
        return Err(ProtocolError::InvalidCompression.into());
    }
    wire.truncate(wire.len() - 4);
    Ok(wire.into())
}

pub(super) struct Decoder {
    // A raw decoder that rejects distances before the current message's
    // history. The miniz wrapping API can fill such references with zeros.
    inner: Inflate,
    ended: bool,
    pending: Vec<u8>,
}
impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder")
            .field("ended", &self.ended)
            .field("pending_bytes", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl Decoder {
    pub(super) fn new() -> Self {
        Self {
            inner: Inflate::new(false, 15),
            ended: false,
            pending: Vec::new(),
        }
    }
    pub(super) fn decode(&mut self, wire: &[u8], fin: bool, limit: Option<usize>) -> Result<Bytes> {
        let mut input = std::mem::take(&mut self.pending);
        input.extend_from_slice(wire);
        if fin {
            input.extend_from_slice(&[0, 0, 0xff, 0xff, 1, 0, 0, 0xff, 0xff]);
        }
        let mut output = Vec::new();
        let used = self.feed(&input, limit, &mut output)?;
        if fin && !self.ended {
            return Err(ProtocolError::InvalidCompression.into());
        }
        if !fin && !self.ended {
            self.pending.extend_from_slice(&input[used..]);
        }
        Ok(output.into())
    }
    fn feed(
        &mut self,
        mut input: &[u8],
        limit: Option<usize>,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        let original_len = input.len();
        // Fixed scratch size; a tiny compressed message never authorizes an
        // unchecked decompressed allocation. Optional decoded cap is independent
        // of Gorilla's wire-length read limit and is None by default.
        let mut scratch = [0u8; 4096];
        while !self.ended {
            let before_in = self.inner.total_in();
            let before_out = self.inner.total_out();
            let status = self
                .inner
                .decompress(input, &mut scratch, InflateFlush::SyncFlush)
                .map_err(|_| Error::Protocol(ProtocolError::InvalidCompression))?;
            let used = (self.inner.total_in() - before_in) as usize;
            let produced = (self.inner.total_out() - before_out) as usize;
            if let Some(max_size) = limit {
                if self.inner.total_out() > max_size as u64 {
                    return Err(CapacityError::DecompressedMessageTooLong {
                        size: usize::try_from(self.inner.total_out()).unwrap_or(usize::MAX),
                        max_size,
                    }
                    .into());
                }
            }
            output.extend_from_slice(&scratch[..produced]);
            input = &input[used..];
            if status == Status::StreamEnd {
                self.ended = true;
                break;
            }
            if used == 0 && produced == 0 {
                break;
            }
            if input.is_empty() && produced < scratch.len() {
                break;
            }
        }
        Ok(original_len - input.len())
    }
}

/// Match Gorilla's extension-list parser: extension names are case-sensitive,
/// parameters can be tokens or quoted strings, and malformed remainder in one
/// header does not authorize negotiation from later text in that same header.
pub fn offers_permessage_deflate<'a>(headers: impl IntoIterator<Item = &'a str>) -> bool {
    fn token(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
    }
    fn space(input: &[u8]) -> &[u8] {
        input.trim_ascii_start()
    }
    'headers: for value in headers {
        let mut input = value.as_bytes();
        loop {
            input = space(input);
            let n = input.iter().take_while(|byte| token(**byte)).count();
            if n == 0 {
                continue 'headers;
            }
            let offered = &input[..n] == b"permessage-deflate";
            input = &input[n..];
            loop {
                input = space(input);
                if input.first() != Some(&b';') {
                    break;
                }
                input = space(&input[1..]);
                let n = input.iter().take_while(|byte| token(**byte)).count();
                if n == 0 {
                    continue 'headers;
                }
                input = space(&input[n..]);
                if input.first() == Some(&b'=') {
                    input = space(&input[1..]);
                    if input.first() == Some(&b'"') {
                        input = &input[1..];
                        let mut escaped = false;
                        let mut end = None;
                        for (n, byte) in input.iter().enumerate() {
                            if escaped {
                                escaped = false;
                            } else if *byte == b'\\' {
                                escaped = true;
                            } else if *byte == b'"' {
                                end = Some(n + 1);
                                break;
                            }
                        }
                        // Gorilla nextTokenOrQuoted returns an empty rest on
                        // unterminated quoting; preserve that parser behavior.
                        input = end.map_or(&[][..], |n| &input[n..]);
                    } else {
                        let n = input.iter().take_while(|byte| token(**byte)).count();
                        input = &input[n..];
                    }
                }
                input = space(input);
                if !input.is_empty() && !matches!(input[0], b',' | b';') {
                    continue 'headers;
                }
            }
            if !input.is_empty() && input[0] != b',' {
                continue 'headers;
            }
            if offered {
                return true;
            }
            if input.is_empty() {
                continue 'headers;
            }
            input = &input[1..];
        }
    }
    false
}
