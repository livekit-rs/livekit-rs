// Derived from protocol/livekit/sip.go and sip_validation.go, Apache-2.0.
use crate::{service_bus::rpc::RpcError, sip::domain};
use lk_protocol::livekit as pb;
use std::collections::HashMap;
fn invalid(error: impl Into<String>) -> RpcError {
    RpcError::new("invalid_argument", error)
}
fn headers(values: &HashMap<String, String>) -> Result<(), RpcError> {
    for (name, value) in values {
        domain::validate_header_name(name, true)
            .map_err(|e| invalid(format!("invalid header name: {e}")))?;
        if matches!(name.to_ascii_lowercase().as_str(), "to" | "via") {
            let hint = if name.eq_ignore_ascii_case("to") {
                "; use CreateSIPParticipantRequest.to_user_override instead"
            } else {
                ""
            };
            tracing::warn!(warning = %format!("header {name} is not supported and will be rejected in a future release{hint}"), "SIP soft validation failure");
        }
        let result = domain::validate_header_value(name, value);
        if let Some(e) = result.hard {
            return Err(invalid(format!("invalid header value for {name}: {e}")));
        }
        for warning in result.soft {
            tracing::warn!(%warning, "SIP soft validation failure");
        }
    }
    Ok(())
}
fn inline(value: &pb::SipOutboundConfig) -> Result<(), RpcError> {
    if value.hostname.is_empty() {
        return Err(invalid("no outbound hostname specified"));
    }
    domain::validate_hostname(&value.hostname, "trunk hostname").map_err(invalid)?;
    domain::validate_hostname(&value.from_host, "from_host").map_err(invalid)?;
    for name in value
        .attributes_to_headers
        .values()
        .chain(value.headers_to_attributes.keys())
    {
        domain::validate_header_name(name, false)
            .map_err(|e| invalid(format!("invalid header name: {e}")))?;
    }
    Ok(())
}
pub(super) fn create(req: &pb::CreateSipParticipantRequest) -> Result<(), RpcError> {
    if req.sip_trunk_id.is_empty() && req.trunk.is_none() && req.sip_number.is_empty() {
        return Err(invalid("missing sip trunk id and sip number"));
    }
    if let Some(t) = &req.trunk {
        inline(t)?;
    }
    if req.sip_call_to.is_empty() {
        return Err(invalid("missing sip callee number"));
    }
    if req.sip_call_to.contains('@') {
        return Err(invalid(
            "SipCallTo should be a phone number or SIP user, not a full SIP URI",
        ));
    }
    if !req.to_user_override.is_empty() {
        if req.to_user_override.contains('@') {
            return Err(invalid(
                "ToUserOverride should be a phone number or SIP user, not a full SIP URI",
            ));
        }
        if let Some(c) = req
            .to_user_override
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || "-.!%*_+`'~".contains(*c)))
        {
            let cause = if c.is_ascii() {
                format!("char {} not allowed", c as u32)
            } else {
                format!(
                    "char {} out of range, consider explicilty adding utf8 characters",
                    c as u32
                )
            };
            return Err(invalid(format!(
                "ToUserOverride contains invalid characters: {cause}"
            )));
        }
    }
    if req.room_name.is_empty() {
        return Err(invalid("missing room name"));
    }
    if let Some(name) = &req.display_name {
        if name.len() > 128 {
            return Err(invalid("DisplayName too long (max 128 characters)"));
        }
        if !quoted_contents(name) {
            return Err(invalid(
                "DisplayName must be a valid quoted string: invalid syntax",
            ));
        }
    }
    if let Some(d) = &req.destination {
        if !d.city.is_empty() && d.country.is_empty() && d.region.is_empty() {
            return Err(invalid(
                "if city is specified, country or region must also be specified",
            ));
        }
        if !d.country.is_empty() && !super::countries::valid(&d.country) {
            return Err(invalid(
                "country must be a valid ISO 3166-1 alpha-2 code (2-letter like 'US', 'IN', 'UK')",
            ));
        }
    }
    headers(&req.headers)?;
    domain::validate_media(req.media.as_ref()).map_err(invalid)
}
pub(super) fn transfer(req: &mut pb::TransferSipParticipantRequest) -> Result<(), RpcError> {
    if req.room_name.is_empty() {
        return Err(invalid("missing room name"));
    }
    if req.participant_identity.is_empty() {
        return Err(invalid("missing participant identity"));
    }
    if req.transfer_to.is_empty() {
        return Err(invalid("missing transfer to"));
    }
    let uri = req
        .transfer_to
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(&req.transfer_to);
    if uri.starts_with("sip:") || uri.starts_with("sips:") {
        req.transfer_to = format!("<{uri}>");
    } else if uri.starts_with("tel:") {
        req.transfer_to = uri.to_owned();
    } else {
        return Err(invalid(
            "transfer_to must be a valid SIP(s) or TEL URI (sip:, sips: or tel:)",
        ));
    }
    headers(&req.headers)
}
/// Validity only, not JSON unescaping. Go allows byte escapes and \a/\v/octal;
/// valid quoted strings are not equivalent to serde_json string syntax.
fn quoted_contents(input: &str) -> bool {
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' | '\n' => return false,
            '\\' => match chars.next() {
                Some('a' | 'b' | 'f' | 'n' | 'r' | 't' | 'v' | '\\' | '"') => {}
                Some(code @ ('x' | 'u' | 'U')) => {
                    let width = match code {
                        'x' => 2,
                        'u' => 4,
                        _ => 8,
                    };
                    let mut scalar = 0u32;
                    for _ in 0..width {
                        let Some(n) = chars
                            .next()
                            .and_then(|c| c.to_digit(16).filter(|_| c.is_ascii()))
                        else {
                            return false;
                        };
                        scalar = (scalar << 4) | n;
                    }
                    if code != 'x' && char::from_u32(scalar).is_none() {
                        return false;
                    }
                }
                Some(c @ '0'..='7') => {
                    let mut n = c as u32 - '0' as u32;
                    for _ in 0..2 {
                        let Some(c @ '0'..='7') = chars.next() else {
                            return false;
                        };
                        n = (n << 3) | (c as u32 - '0' as u32);
                    }
                    if n > 255 {
                        return false;
                    }
                }
                _ => return false,
            },
            _ => {}
        }
    }
    true
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quoted_go_not_json_and_destination_source_table() {
        for text in [r"\a\v\377\xff", r"\u2764", "é\t\r"] {
            assert!(quoted_contents(text), "{text:?}");
        }
        for text in [r"\'", r"\400", r"\uD800", r"\U00110000", "\"", "\n"] {
            assert!(!quoted_contents(text), "{text:?}");
        }
        for country in ["US", "UK", "XK", "GB"] {
            assert!(super::super::countries::valid(country));
        }
        for country in ["us", "USA", "XX", "EU", "ZZ", "001"] {
            assert!(!super::super::countries::valid(country));
        }
    }
    #[test]
    fn transfer_mutates_before_header_validation_and_prefix_is_case_sensitive() {
        let mut req = pb::TransferSipParticipantRequest {
            room_name: "r".into(),
            participant_identity: "i".into(),
            transfer_to: "sip:to@example.org".into(),
            ..Default::default()
        };
        req.headers.insert("bad key".into(), "x".into());
        assert!(transfer(&mut req).is_err());
        assert_eq!(req.transfer_to, "<sip:to@example.org>");
        req.headers.clear();
        req.transfer_to = "SIP:x".into();
        assert!(transfer(&mut req).is_err());
        req.transfer_to = "<tel:+123>".into();
        transfer(&mut req).unwrap();
        assert_eq!(req.transfer_to, "tel:+123");
    }
}
