// Copyright 2023 LiveKit, Inc. Licensed under Apache-2.0.
// Fixed protocol/sip/sip.go conflict buckets and network matching.
use lk_protocol::livekit as pb;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

pub fn normalize_number(number: &str) -> String {
    if number.is_empty() {
        return String::new();
    }
    let tail = number.strip_prefix('+').unwrap_or(number);
    if tail.is_empty()
        || !tail
            .bytes()
            .all(|b| b.is_ascii_digit() || b"- ()".contains(&b))
    {
        return number.to_string();
    }
    let digits: String = number
        .chars()
        .filter(|c| !matches!(c, ' ' | '-' | '(' | ')'))
        .collect();
    if digits.starts_with('+') {
        digits
    } else {
        format!("+{digits}")
    }
}
pub fn match_numbers(number: &str, allowed: &[String]) -> bool {
    allowed.is_empty()
        || allowed
            .iter()
            .any(|a| a == number || normalize_number(a) == normalize_number(number))
}
fn valid_mask(mask: &str) -> bool {
    !mask.contains(['(', ')', '+', '*', ';', ',', ' ', '\t', '\n', '\r']) && !mask.contains("://")
}
fn parsed_ip(value: &str) -> Option<(IpAddr, Option<&str>)> {
    if let Some((ip, zone)) = value.split_once('%') {
        let ip = ip.parse::<std::net::Ipv6Addr>().ok()?;
        if zone.is_empty() {
            return None;
        }
        Some((ip.into(), Some(zone)))
    } else {
        Some((value.parse().ok()?, None))
    }
}
fn match_ip_mask(ip: IpAddr, zone: Option<&str>, mask: &str) -> bool {
    if let Some((base, bits)) = mask.split_once('/') {
        if zone.is_some() {
            return false;
        }
        let Some((base, None)) = parsed_ip(base) else {
            return false;
        };
        if bits.is_empty()
            || (bits.len() > 1 && bits.starts_with('0'))
            || !bits.bytes().all(|b| b.is_ascii_digit())
        {
            return false;
        }
        let Ok(n) = bits.parse::<u8>() else {
            return false;
        };
        match (ip, base) {
            (IpAddr::V4(ip), IpAddr::V4(base)) if n <= 32 => {
                let mask = if n == 0 { 0 } else { u32::MAX << (32 - n) };
                u32::from(ip) & mask == u32::from(base) & mask
            }
            (IpAddr::V6(ip), IpAddr::V6(base)) if n <= 128 => {
                let mask = if n == 0 { 0 } else { u128::MAX << (128 - n) };
                u128::from(ip) & mask == u128::from(base) & mask
            }
            _ => false,
        }
    } else {
        parsed_ip(mask).is_some_and(|(other, other_zone)| ip == other && zone == other_zone)
    }
}
pub fn match_address_masks(address: &str, host: &str, masks: &[String]) -> bool {
    let Some((ip, zone)) = parsed_ip(address) else {
        return true;
    };
    let valid: Vec<_> = masks.iter().filter(|m| valid_mask(m)).collect();
    valid.is_empty()
        || valid
            .into_iter()
            .any(|m| m == host || match_ip_mask(ip, zone, m))
}
fn id(value: &str) -> &str {
    if value.is_empty() { "<new>" } else { value }
}
fn name(value: &str) -> &str {
    if value.is_empty() {
        "<blank name>"
    } else {
        value
    }
}
pub(super) fn quote(value: &str) -> String {
    let mut out = String::from("\"");
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{b}' => out.push_str("\\v"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 32 || c == '\u{7f}' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if super::go_print::is_print(c) => out.push(c),
            c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}
fn numbers(values: &[String]) -> String {
    if values.is_empty() {
        "<any>".into()
    } else {
        format!(
            "[{}]",
            values
                .iter()
                .map(|v| quote(v))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}
/// Exact fixed conflict validation, separate from runtime best-match selection.
pub fn validate_inbound_conflicts(trunks: &[pb::SipInboundTrunkInfo]) -> Result<(), String> {
    let mut buckets: HashMap<String, HashMap<String, &pb::SipInboundTrunkInfo>> = HashMap::new();
    for trunk in trunks {
        let called: Vec<String> = if trunk.numbers.is_empty() {
            vec![String::new()]
        } else {
            trunk.numbers.iter().map(|n| normalize_number(n)).collect()
        };
        let mut seen = HashSet::new();
        for called in called {
            if !seen.insert(called.clone()) {
                continue;
            }
            let bucket = buckets.entry(called).or_default();
            if trunk.allowed_numbers.is_empty() {
                if let Some(other) = bucket
                    .get("")
                    .filter(|other| other.sip_trunk_id != trunk.sip_trunk_id)
                {
                    return Err(format!(
                        "Conflicting inbound SIP Trunks: {} and {}, using the same number(s) {} without AllowedNumbers set",
                        quote(id(&trunk.sip_trunk_id)),
                        quote(id(&other.sip_trunk_id)),
                        numbers(&trunk.numbers)
                    ));
                }
                bucket.insert(String::new(), trunk);
            } else {
                let mut seen = HashSet::new();
                for n in &trunk.allowed_numbers {
                    let normalized = normalize_number(n);
                    if !seen.insert(normalized.clone()) {
                        continue;
                    }
                    if let Some(other) = bucket
                        .get(&normalized)
                        .filter(|other| other.sip_trunk_id != trunk.sip_trunk_id)
                    {
                        return Err(format!(
                            "Conflicting inbound SIP Trunks: {} and {}, using the same number(s) {} and AllowedNumber {}",
                            quote(id(&trunk.sip_trunk_id)),
                            quote(id(&other.sip_trunk_id)),
                            numbers(&trunk.numbers),
                            quote(n)
                        ));
                    }
                    bucket.insert(normalized, trunk);
                }
            }
        }
    }
    Ok(())
}
fn rule_pin_room(info: &pb::SipDispatchRuleInfo) -> Result<(&str, &str), String> {
    match info.rule.as_ref().and_then(|r| r.rule.as_ref()) {
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(r)) => Ok((&r.pin, &r.room_name)),
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(r)) => {
            Ok((&r.pin, &r.room_prefix))
        }
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleCallee(r)) => Ok((&r.pin, &r.room_prefix)),
        None => Err("unsupported SIP Dispatch Rule: <nil>".into()),
    }
}
pub fn dispatch_priority(info: &pb::SipDispatchRuleInfo) -> i32 {
    let Some(rule) = info.rule.as_ref().and_then(|r| r.rule.as_ref()) else {
        return i32::MAX;
    };
    let (base, pin) = match rule {
        pb::sip_dispatch_rule::Rule::DispatchRuleDirect(r) => (0, &r.pin),
        pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(r) => (1, &r.pin),
        pb::sip_dispatch_rule::Rule::DispatchRuleCallee(r) => (2, &r.pin),
    };
    base + if pin.is_empty() { 100 } else { 0 }
        + if info.inbound_numbers.is_empty() {
            1000
        } else {
            0
        }
        + if info.numbers.is_empty() { 1000 } else { 0 }
}
pub fn validate_dispatch_conflicts(rules: &[pb::SipDispatchRuleInfo]) -> Result<(), String> {
    let mut buckets: HashMap<(String, String, String, String), &pb::SipDispatchRuleInfo> =
        HashMap::new();
    for rule in rules {
        let (pin, _) = rule_pin_room(rule)?;
        let defaults = vec![String::new()];
        let trunks = if rule.trunk_ids.is_empty() {
            &defaults
        } else {
            &rule.trunk_ids
        };
        let inbound = if rule.inbound_numbers.is_empty() {
            &defaults
        } else {
            &rule.inbound_numbers
        };
        let numbers = if rule.numbers.is_empty() {
            &defaults
        } else {
            &rule.numbers
        };
        for trunk in trunks {
            for inbound in inbound {
                for n in numbers {
                    let key = (
                        pin.to_string(),
                        trunk.clone(),
                        normalize_number(inbound),
                        normalize_number(n),
                    );
                    if let Some(other) = buckets.get(&key) {
                        return Err(format!(
                            "Dispatch rule for the same trunk, inbound number, number, and PIN combination already exists in dispatch rule {} {}",
                            quote(id(&other.sip_dispatch_rule_id)),
                            quote(name(&other.name))
                        ));
                    }
                    buckets.insert(key, rule);
                }
            }
        }
    }
    Ok(())
}
