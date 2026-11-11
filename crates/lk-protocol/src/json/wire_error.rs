//! Schema-bound classification of an already failed stored-message decode.
//! No error text is inspected. Only real UTF-8 validation of known string fields
//! selects InvalidUtf8; unknown bytes fields are never treated as strings.
use prost_reflect::{Kind, MessageDescriptor};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireDecodeFailure {
    InvalidUtf8,
    Malformed,
}

/// Inspect wire fields in receive order after generated prost decoding failed.
/// Bounds are 100 nested messages/groups, 1,000,000 wire fields and 64 MiB input.
/// Exhaustion conservatively returns Malformed, not a claim of full Go error
/// precedence beyond these bounds. This helper does not replace decoding.
pub fn classify(name: &str, bytes: &[u8]) -> WireDecodeFailure {
    if bytes.len() > 64 * 1024 * 1024 {
        return WireDecodeFailure::Malformed;
    }
    let Some(descriptor) = super::pool().get_message_by_name(name) else {
        return WireDecodeFailure::Malformed;
    };
    let mut fields = 1_000_000;
    inspect(bytes, Some(&descriptor), None, 100, &mut fields)
        .err()
        .unwrap_or(WireDecodeFailure::Malformed)
}
fn varint(bytes: &[u8], at: &mut usize) -> Result<u64, WireDecodeFailure> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let b = *bytes.get(*at).ok_or(WireDecodeFailure::Malformed)?;
        *at += 1;
        if shift == 63 && b > 1 {
            return Err(WireDecodeFailure::Malformed);
        }
        value |= u64::from(b & 127) << shift;
        if b < 128 {
            return Ok(value);
        }
    }
    Err(WireDecodeFailure::Malformed)
}
fn advance(bytes: &[u8], at: &mut usize, n: usize) -> Result<(), WireDecodeFailure> {
    *at = at
        .checked_add(n)
        .filter(|end| *end <= bytes.len())
        .ok_or(WireDecodeFailure::Malformed)?;
    Ok(())
}
fn delimited<'a>(bytes: &'a [u8], at: &mut usize) -> Result<&'a [u8], WireDecodeFailure> {
    let n = usize::try_from(varint(bytes, at)?).map_err(|_| WireDecodeFailure::Malformed)?;
    let start = *at;
    advance(bytes, at, n)?;
    Ok(&bytes[start..*at])
}
fn scalar_wire(kind: &Kind) -> Option<u8> {
    match kind {
        Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => Some(1),
        Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => Some(5),
        Kind::String | Kind::Bytes | Kind::Message(_) => None,
        _ => Some(0),
    }
}
fn packed(bytes: &[u8], wire: u8) -> Result<(), WireDecodeFailure> {
    let mut at = 0;
    while at < bytes.len() {
        match wire {
            0 => {
                varint(bytes, &mut at)?;
            }
            1 => advance(bytes, &mut at, 8)?,
            5 => advance(bytes, &mut at, 4)?,
            _ => return Err(WireDecodeFailure::Malformed),
        }
    }
    Ok(())
}
// Returns bytes consumed; an enclosing group may be followed by sibling fields.
fn inspect(
    bytes: &[u8],
    descriptor: Option<&MessageDescriptor>,
    end_group: Option<u32>,
    depth: usize,
    remaining: &mut usize,
) -> Result<usize, WireDecodeFailure> {
    if depth == 0 {
        return Err(WireDecodeFailure::Malformed);
    }
    let mut at = 0;
    while at < bytes.len() {
        if *remaining == 0 {
            return Err(WireDecodeFailure::Malformed);
        }
        *remaining -= 1;
        let key = varint(bytes, &mut at)?;
        let tag = key >> 3;
        let wire = (key & 7) as u8;
        if tag == 0 || tag > 0x1fff_ffff {
            return Err(WireDecodeFailure::Malformed);
        }
        let tag = tag as u32;
        if wire == 4 {
            return if end_group == Some(tag) {
                Ok(at)
            } else {
                Err(WireDecodeFailure::Malformed)
            };
        }
        let field = descriptor.and_then(|d| d.get_field(tag));
        match wire {
            0 => {
                varint(bytes, &mut at)?;
            }
            1 => advance(bytes, &mut at, 8)?,
            2 => {
                let value = delimited(bytes, &mut at)?;
                if let Some(field) = field {
                    match field.kind() {
                        Kind::String => {
                            std::str::from_utf8(value)
                                .map_err(|_| WireDecodeFailure::InvalidUtf8)?;
                        }
                        Kind::Message(message) if !field.is_group() => {
                            inspect(value, Some(&message), None, depth - 1, remaining)?;
                        }
                        kind if field.is_list() => {
                            if let Some(wire) = scalar_wire(&kind) {
                                packed(value, wire)?;
                            }
                        }
                        _ => {}
                    }
                }
            }
            3 => {
                let group = field.filter(|f| f.is_group()).and_then(|f| match f.kind() {
                    Kind::Message(m) => Some(m),
                    _ => None,
                });
                let consumed = inspect(
                    &bytes[at..],
                    group.as_ref(),
                    Some(tag),
                    depth - 1,
                    remaining,
                )?;
                advance(bytes, &mut at, consumed)?;
            }
            5 => advance(bytes, &mut at, 4)?,
            _ => return Err(WireDecodeFailure::Malformed),
        }
    }
    if end_group.is_some() {
        Err(WireDecodeFailure::Malformed)
    } else {
        Ok(at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const INBOUND: &str = "livekit.SIPInboundTrunkInfo";
    #[test]
    fn actual_utf8_and_first_failure_order() {
        assert_eq!(
            classify(INBOUND, &[0x0a, 1, 0xff]),
            WireDecodeFailure::InvalidUtf8
        );
        assert_eq!(
            classify(INBOUND, &[0xff, 0x0a, 1, 0xff]),
            WireDecodeFailure::Malformed
        );
        assert_eq!(
            classify(INBOUND, &[0x0a, 1, 0xff, 0xff]),
            WireDecodeFailure::InvalidUtf8
        );
        assert_eq!(
            classify(INBOUND, &[0x4a, 3, 0x0a, 1, 0xff]),
            WireDecodeFailure::InvalidUtf8
        ); // headers map key
        assert_eq!(
            classify(INBOUND, &[0x4a, 3, 0x12, 1, 0xff]),
            WireDecodeFailure::InvalidUtf8
        ); // headers map value
    }
    #[test]
    fn unknown_payload_is_opaque_and_groups_must_close() {
        assert_eq!(
            classify(INBOUND, &[0xa2, 6, 1, 0xff]),
            WireDecodeFailure::Malformed
        ); // field100 is unknown
        assert_eq!(
            classify(INBOUND, &[0xa3, 6, 0x0a, 1, 0xff, 0xa4, 6]),
            WireDecodeFailure::Malformed
        ); // unknown group contains opaque string
        assert_eq!(
            classify(INBOUND, &[0xa3, 6, 0xa4, 6, 0x0a, 1, 0xff]),
            WireDecodeFailure::InvalidUtf8
        );
        assert_eq!(
            classify(INBOUND, &[0xa3, 6, 0xac, 6, 0x0a, 1, 0xff]),
            WireDecodeFailure::Malformed
        );
        assert_eq!(
            classify(INBOUND, &[0x0a, 2, 0xff]),
            WireDecodeFailure::Malformed
        );
    }
}
