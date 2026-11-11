//! MIT Pion SDP v3.0.20 validation state machine, not str0m error rewriting.
//! base_lexer.go/unmarshal.go/util.go at the fixed server dependency lock.
//! Validates source grammar only; WebRTC/ICE/DTLS semantics stay in real RTC.
struct Lexer<'a> {
    bytes: &'a [u8],
    pos: usize,
}
type Result<T> = std::result::Result<T, String>;
fn quoted_byte(b: u8) -> String {
    let s = match b {
        b'"' => "\\\"".into(),
        b'\\' => "\\\\".into(),
        7 => "\\a".into(),
        8 => "\\b".into(),
        12 => "\\f".into(),
        10 => "\\n".into(),
        13 => "\\r".into(),
        9 => "\\t".into(),
        11 => "\\v".into(),
        32..=126 => (b as char).to_string(),
        _ => format!("\\x{b:02x}"),
    };
    format!("\"{s}\"")
}
fn invalid(kind: &str, value: &str) -> String {
    format!("sdp: invalid {kind} `{value}`")
}
impl<'a> Lexer<'a> {
    fn read(&mut self) -> Result<u8> {
        let byte = *self.bytes.get(self.pos).ok_or("EOF")?;
        self.pos += 1;
        Ok(byte)
    }
    fn syntax(&self) -> String {
        let pos = self.pos.saturating_sub(1);
        format!(
            "sdp: syntax error at pos {pos}: {}",
            quoted_byte(self.bytes.get(pos).copied().unwrap_or(0))
        )
    }
    fn newlines(&mut self) {
        while self
            .bytes
            .get(self.pos)
            .is_some_and(|b| matches!(b, b'\r' | b'\n'))
        {
            self.pos += 1;
        }
    }
    fn whitespace(&mut self) {
        while self
            .bytes
            .get(self.pos)
            .is_some_and(|b| matches!(b, b' ' | b'\t'))
        {
            self.pos += 1;
        }
    }
    fn key(&mut self) -> Result<Option<u8>> {
        self.newlines();
        let key = match self.read() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        let second = match self.read() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        if second != b'=' {
            return Err(self.syntax());
        }
        Ok(Some(key))
    }
    fn uint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        loop {
            let c = match self.read() {
                Ok(c) => c,
                Err(_) if value > 0 => break,
                Err(e) => return Err(e),
            };
            if matches!(c, b'\r' | b'\n') {
                self.pos -= 1;
                break;
            }
            if matches!(c, b' ' | b'\t') {
                self.whitespace();
                break;
            }
            if !c.is_ascii_digit() {
                return Err(self.syntax());
            }
            value = value.wrapping_mul(10).wrapping_add(u64::from(c - b'0'));
        }
        Ok(value)
    }
    fn field(&mut self) -> Result<String> {
        let start = self.pos;
        let end;
        loop {
            let before = self.pos;
            let c = match self.read() {
                Ok(c) => c,
                Err(_) if before > start => {
                    end = before;
                    break;
                }
                Err(e) => return Err(e),
            };
            if matches!(c, b'\r' | b'\n') {
                self.pos -= 1;
                end = before;
                break;
            }
            if matches!(c, b' ' | b'\t') {
                self.whitespace();
                end = before;
                break;
            }
        }
        Ok(String::from_utf8_lossy(&self.bytes[start..end]).into_owned())
    }
    fn line(&mut self) -> Result<String> {
        let start = self.pos;
        let mut trim = 1;
        loop {
            let c = self.read()?;
            if c == b'\r' {
                trim += 1;
            }
            if c == b'\n' {
                return Ok(
                    String::from_utf8_lossy(&self.bytes[start..self.pos - trim]).into_owned()
                );
            }
        }
    }
    fn connection(&mut self) -> Result<()> {
        let network = self.field()?;
        if network != "IN" {
            return Err(invalid("value", &network));
        }
        let address = self.field()?;
        if !matches!(address.as_str(), "IP4" | "IP6") {
            return Err(invalid("value", &address));
        }
        self.field()?;
        self.newlines();
        Ok(())
    }
    fn origin(&mut self) -> Result<()> {
        self.field()?;
        self.uint()?;
        self.uint()?;
        let network = self.field()?;
        if network != "IN" {
            return Err(invalid("value", &network));
        }
        let address = self.field()?;
        if !address.is_empty() && !matches!(address.as_str(), "IP4" | "IP6") {
            return Err(invalid("value", &address));
        }
        self.field()?;
        self.newlines();
        Ok(())
    }
    fn media(&mut self) -> Result<()> {
        let media = self.field()?;
        if !matches!(
            media.as_str(),
            "audio" | "video" | "text" | "application" | "message"
        ) {
            return Err(invalid("value", &media));
        }
        let field = self.field()?;
        let parts: Vec<_> = field.split('/').collect();
        if parts[0]
            .parse::<i64>()
            .ok()
            .is_none_or(|p| !(0..=65535).contains(&p))
        {
            return Err(invalid("port value", parts[0]));
        }
        if parts.len() > 1 && parts[1].parse::<i64>().is_err() {
            return Err(invalid("value", &format!("[{}]", parts.join(" "))));
        }
        let protocol = self.field()?;
        if protocol.split('/').any(|p| {
            !matches!(
                p,
                "UDP"
                    | "RTP"
                    | "AVP"
                    | "SAVP"
                    | "SAVPF"
                    | "TLS"
                    | "DTLS"
                    | "SCTP"
                    | "AVPF"
                    | "TCP"
                    | "MSRP"
                    | "BFCP"
                    | "UDT"
                    | "IX"
                    | "MRCPv2"
                    | "FEC"
            )
        }) {
            return Err(invalid("numeric value", &protocol));
        }
        while !self.field()?.is_empty() {}
        self.newlines();
        Ok(())
    }
}
fn time_valid(value: &str) -> bool {
    let numeric = if value.as_bytes().last().is_some_and(|b| b"dhms".contains(b)) {
        &value[..value.len() - 1]
    } else {
        value
    };
    !numeric.is_empty() && numeric.parse::<i64>().is_ok()
}
fn bandwidth(value: &str) -> bool {
    let parts: Vec<_> = value.split(':').collect();
    parts.len() == 2
        && (parts[0].starts_with("X-") || matches!(parts[0], "CT" | "AS" | "TIAS" | "RS" | "RR"))
        && !parts[1].is_empty()
        && parts[1].bytes().all(|b| b.is_ascii_digit())
        && parts[1].parse::<u64>().is_ok()
}
fn next(state: u8, key: u8) -> Option<u8> {
    Some(match (state, key) {
        (1, b'v') => 2,
        (2, b'o') => 3,
        (3, b's') => 4,
        (4, b'i') => 7,
        (4 | 7, b'u') => 10,
        (4 | 7 | 10, b'e') => 6,
        (4 | 6 | 7 | 10, b'p') => 8,
        (4 | 6 | 7 | 8 | 10, b'c') => 5,
        (4 | 5 | 6 | 7 | 8 | 10, b'b') => 5,
        (4..=10, b't') => 9,
        (9, b'r') => 9,
        (9, b'z') => 13,
        (9 | 13, b'k') => 11,
        (9 | 11 | 13, b'a') => 11,
        (9 | 11 | 12 | 13 | 14 | 15 | 16, b'm') => 12,
        (12 | 14 | 15 | 16, b'a' | b'k') => 14,
        (12 | 14 | 15 | 16, b'b' | b'c') => 15,
        (12 | 14 | 15 | 16, b'i') => 16,
        _ => return None,
    })
}
fn raw(bytes: &[u8]) -> Result<()> {
    let mut lexer = Lexer { bytes, pos: 0 };
    let mut state = 1;
    while let Some(key) = lexer.key()? {
        let target = next(state, key).ok_or_else(|| lexer.syntax())?;
        match key {
            b'v' => {
                let version = lexer.uint()?;
                if version != 0 {
                    return Err(invalid("value", &version.to_string()));
                }
                lexer.newlines();
            }
            b'o' => lexer.origin()?,
            b'c' => lexer.connection()?,
            b'm' => lexer.media()?,
            b't' => {
                lexer.uint()?;
                lexer.uint()?;
                lexer.newlines();
            }
            b'r' => {
                for _ in 0..2 {
                    let field = lexer.field()?;
                    if !time_valid(&field) {
                        return Err(invalid("value", &field));
                    }
                }
                loop {
                    let field = lexer.field()?;
                    if field.is_empty() {
                        break;
                    }
                    if !time_valid(&field) {
                        return Err(invalid("value", &field));
                    }
                }
                lexer.newlines();
            }
            b'z' => {
                loop {
                    lexer.uint()?;
                    let offset = lexer.field()?;
                    if offset.is_empty() {
                        break;
                    }
                    if !time_valid(&offset) {
                        return Err(invalid("value", &offset));
                    }
                }
                lexer.newlines();
            }
            b'b' => {
                let value = lexer.line()?;
                if !bandwidth(&value) {
                    return Err(format!(
                        "sdp: invalid {} `b={value}`",
                        if state >= 12 { "syntax" } else { "value" }
                    ));
                }
            }
            b'u' => {
                let value = lexer.line()?;
                crate::workers::ingress::FixedGoUrl::validate_sdp_uri(&value)?;
            }
            _ => {
                lexer.line()?;
            }
        }
        state = target;
    }
    Ok(())
}
#[cfg(test)]
pub(super) fn validate_sdp_only(bytes: &[u8]) -> Result<()> {
    raw(bytes)
}
/// Exact source wrapper, without inventing WebRTC semantic requirements at HTTP.
pub(super) fn validate(bytes: &[u8]) -> Result<()> {
    raw(bytes).map_err(|error| format!("failed to unmarshal SDP: {error}"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn syntax_is_not_webrtc_semantic_validation() {
        assert!(validate(b"v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n").is_ok());
        assert_eq!(
            validate(b"bad SDP").unwrap_err(),
            "failed to unmarshal SDP: sdp: syntax error at pos 1: \"a\""
        );
    }
}
