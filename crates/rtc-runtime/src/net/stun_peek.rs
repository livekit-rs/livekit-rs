//! Minimal STUN inspection used only for demultiplexing. Full STUN handling,
//! including integrity checks, stays inside str0m.

const MAGIC_COOKIE: [u8; 4] = [0x21, 0x12, 0xa4, 0x42];
const ATTR_USERNAME: u16 = 0x0006;
const HEADER_LEN: usize = 20;

/// True when `data` looks like a STUN message (RFC 7983 demux: first byte 0..=3,
/// magic cookie present, length multiple of 4 and consistent).
pub fn is_stun(data: &[u8]) -> bool {
    if data.len() < HEADER_LEN || data[0] > 3 || data[4..8] != MAGIC_COOKIE {
        return false;
    }
    let len = u16::from_be_bytes([data[2], data[3]]) as usize;
    len.is_multiple_of(4) && HEADER_LEN + len <= data.len()
}

/// For a STUN request sent to us, USERNAME is `<our ufrag>:<peer ufrag>`.
/// Returns our ufrag.
pub fn local_ufrag(data: &[u8]) -> Option<&str> {
    if !is_stun(data) {
        return None;
    }
    let len = u16::from_be_bytes([data[2], data[3]]) as usize;
    let mut attrs = &data[HEADER_LEN..HEADER_LEN + len];
    while attrs.len() >= 4 {
        let typ = u16::from_be_bytes([attrs[0], attrs[1]]);
        let alen = u16::from_be_bytes([attrs[2], attrs[3]]) as usize;
        let padded = (alen + 3) & !3;
        if attrs.len() < 4 + alen {
            return None;
        }
        if typ == ATTR_USERNAME {
            let value = std::str::from_utf8(&attrs[4..4 + alen]).ok()?;
            return value.split(':').next();
        }
        if attrs.len() < 4 + padded {
            return None;
        }
        attrs = &attrs[4 + padded..];
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding_with_username(user: &str) -> Vec<u8> {
        let mut attr = Vec::new();
        attr.extend_from_slice(&ATTR_USERNAME.to_be_bytes());
        attr.extend_from_slice(&(user.len() as u16).to_be_bytes());
        attr.extend_from_slice(user.as_bytes());
        while attr.len() % 4 != 0 {
            attr.push(0);
        }
        let mut msg = vec![0x00, 0x01];
        msg.extend_from_slice(&(attr.len() as u16).to_be_bytes());
        msg.extend_from_slice(&MAGIC_COOKIE);
        msg.extend_from_slice(&[7u8; 12]);
        msg.extend_from_slice(&attr);
        msg
    }

    #[test]
    fn extracts_local_ufrag() {
        let msg = binding_with_username("srvUfrag:cliUfrag");
        assert!(is_stun(&msg));
        assert_eq!(local_ufrag(&msg), Some("srvUfrag"));
    }

    #[test]
    fn rejects_non_stun() {
        // DTLS record (content type 22)
        assert!(!is_stun(&[22u8; 40]));
        assert_eq!(local_ufrag(&[0x80u8; 40]), None);
    }
}
