//! `rtc.udp_port` (PortRange) and `rtc.node_ip` (NodeIP) scalar forms, ported from
//! mediatransportutil `rtcconfig/config.go:136-263`.
//!
//! Both are structs in Go with a custom `UnmarshalYAML` that reads the node's scalar
//! text, so YAML accepts `7882`, `"7882-7892"`, `"1.2.3.4"` or `"1.2.3.4,::1"`. A
//! mapping form yields an empty text and is silently ignored upstream; we mirror
//! that. The generated flags still see the struct leaves (`rtc.udp_port.start`,
//! `rtc.node_ip.ipv4`, ...).

use std::net::IpAddr;

use crate::strconv::atoi;
use crate::yamlv3::{Dec, Decode, Fail, GoType, Id, Kind};

/// Go `rtcconfig.PortRange{Start, End int}`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PortRange {
    pub start: i64,
    pub end: i64,
}

impl PortRange {
    /// Go `PortRange.Valid()`: only `Start != 0` matters.
    pub fn valid(&self) -> bool {
        self.start != 0
    }

    /// Go `PortRange.UnmarshalString`. Mutates in place exactly like Go: a single
    /// port only sets `start` (a previous `end` survives).
    pub fn unmarshal_string(&mut self, s: &str) -> Result<(), String> {
        if s.is_empty() {
            return Ok(());
        }
        if s.contains('-') {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() != 2 {
                return Err(format!("invalid port range {s}, should be <start>-<end>"));
            }
            let start = atoi(parts[0].trim_matches(' '))
                .map_err(|e| format!("invalid start port {}: {e}", parts[0]))?;
            let end = atoi(parts[1].trim_matches(' '))
                .map_err(|e| format!("invalid end port {}: {e}", parts[1]))?;
            if end <= start {
                return Err(format!(
                    "end port {end} must be greater than start port {start}"
                ));
            }
            self.start = start;
            self.end = end;
            return Ok(());
        }
        let port = atoi(s).map_err(|e| format!("invalid port {s}: {e}"))?;
        self.start = port;
        Ok(())
    }

    /// Go `PortRange.ToSlice()`.
    pub fn to_slice(&self) -> Vec<i64> {
        self.iter().collect()
    }

    /// Iterate the configured mux ports without materializing the whole range.
    pub fn iter(&self) -> std::ops::RangeInclusive<i64> {
        self.start..=if self.end == 0 {
            self.start
        } else {
            self.end.max(self.start)
        }
    }

    /// `fmt.Sprintf("%s", PortRange.MarshalYAML())` as printed by the upstream
    /// `ports` subcommand. This reproduces Go's `%!s(...)` verb-mismatch output
    /// byte-for-byte: a single port is an `int` (`%!s(int=7882)`), an invalid range
    /// yields `nil` (`%!s(<nil>)`), a range is the string `7882-7892`.
    pub fn go_sprintf_s_marshal(&self) -> String {
        if self.end == 0 {
            return format!("%!s(int={})", self.start);
        }
        if self.end <= self.start {
            return "%!s(<nil>)".to_string();
        }
        format!("{}-{}", self.start, self.end)
    }
}

/// Go `rtcconfig.NodeIP{V4, V6 string}`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeIp {
    pub v4: String,
    pub v6: String,
}

/// Go `net.ParseIP` + `To4()` + `String()`: returns the normalised text and
/// whether it is an IPv4 (including IPv4-mapped IPv6) address.
pub(crate) fn go_parse_ip(s: &str) -> Option<(String, bool)> {
    match s.parse::<IpAddr>().ok()? {
        IpAddr::V4(v4) => Some((v4.to_string(), true)),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => Some((v4.to_string(), true)),
            None => Some((v6.to_string(), false)),
        },
    }
}

impl NodeIp {
    pub fn is_empty(&self) -> bool {
        self.v4.is_empty() && self.v6.is_empty()
    }

    /// Go `NodeIP.UnmarshalString`: merges into the existing value and fails if a
    /// family is already set. Like Go it may partially apply before failing.
    pub fn unmarshal_string(&mut self, s: &str) -> Result<(), String> {
        if s.is_empty() {
            return Ok(());
        }
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() > 2 {
            return Err(format!("invalid node ip {s}, should be <ipv4>,<ipv6>"));
        }
        for part in parts {
            let Some((ip, is_v4)) = go_parse_ip(part.trim_matches(' ')) else {
                return Err(format!("invalid ip {part}"));
            };
            if is_v4 {
                if !self.v4.is_empty() {
                    return Err(format!(
                        "multiple ipv4 addresses provided: {} and {ip}",
                        self.v4
                    ));
                }
                self.v4 = ip;
            } else {
                if !self.v6.is_empty() {
                    return Err(format!(
                        "multiple ipv6 addresses provided: {} and {ip}",
                        self.v6
                    ));
                }
                self.v6 = ip;
            }
        }
        Ok(())
    }
}

/// `UnmarshalYAML(value *yaml.Node)` = `UnmarshalString(value.Value)`; any
/// error is returned as-is (yaml.v3 `fail(err)`, no position). `null` never
/// reaches the unmarshaler (`d.prepare`) and keeps the value.
fn decode_scalar_text(
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
    unmarshal: impl FnOnce(&str) -> Result<(), String>,
) -> Result<bool, Fail> {
    if d.is_null(n) {
        return Ok(match d.kind(n) {
            Kind::Scalar => false,
            _ => d.not_a_collection(n, go),
        });
    }
    unmarshal(d.value(n)).map_err(Fail)?;
    Ok(true)
}

impl GoType for PortRange {
    fn go_type() -> String {
        "rtcconfig.PortRange".into()
    }
}

impl Decode for PortRange {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        decode_scalar_text(d, n, go, |s| self.unmarshal_string(s))
    }
}

impl GoType for NodeIp {
    fn go_type() -> String {
        "rtcconfig.NodeIP".into()
    }
}

impl Decode for NodeIp {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        decode_scalar_text(d, n, go, |s| self.unmarshal_string(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_range_forms() {
        let mut p = PortRange::default();
        p.unmarshal_string("7882").unwrap();
        assert_eq!(
            p,
            PortRange {
                start: 7882,
                end: 0
            }
        );
        p.unmarshal_string("7882-7892").unwrap();
        assert_eq!(
            p,
            PortRange {
                start: 7882,
                end: 7892
            }
        );
        // single port only rewrites start
        p.unmarshal_string("9000").unwrap();
        assert_eq!(
            p,
            PortRange {
                start: 9000,
                end: 7892
            }
        );
    }

    #[test]
    fn port_range_errors() {
        let mut p = PortRange::default();
        assert_eq!(
            p.unmarshal_string("1-2-3").unwrap_err(),
            "invalid port range 1-2-3, should be <start>-<end>"
        );
        assert_eq!(
            p.unmarshal_string("a-2").unwrap_err(),
            "invalid start port a: strconv.Atoi: parsing \"a\": invalid syntax"
        );
        assert_eq!(
            p.unmarshal_string("1-b").unwrap_err(),
            "invalid end port b: strconv.Atoi: parsing \"b\": invalid syntax"
        );
        assert_eq!(
            p.unmarshal_string("10-5").unwrap_err(),
            "end port 5 must be greater than start port 10"
        );
        assert_eq!(
            p.unmarshal_string("x").unwrap_err(),
            "invalid port x: strconv.Atoi: parsing \"x\": invalid syntax"
        );
        assert_eq!(p, PortRange::default());
    }

    #[test]
    fn sprintf_quirk() {
        assert_eq!(
            PortRange {
                start: 7882,
                end: 0
            }
            .go_sprintf_s_marshal(),
            "%!s(int=7882)"
        );
        assert_eq!(
            PortRange {
                start: 7882,
                end: 7892
            }
            .go_sprintf_s_marshal(),
            "7882-7892"
        );
        assert_eq!(
            PortRange {
                start: 9000,
                end: 7892
            }
            .go_sprintf_s_marshal(),
            "%!s(<nil>)"
        );
    }

    #[test]
    fn node_ip_forms() {
        let mut n = NodeIp::default();
        n.unmarshal_string("1.2.3.4, 2001:db8::1").unwrap();
        assert_eq!(n.v4, "1.2.3.4");
        assert_eq!(n.v6, "2001:db8::1");
        assert_eq!(
            n.unmarshal_string("5.6.7.8").unwrap_err(),
            "multiple ipv4 addresses provided: 1.2.3.4 and 5.6.7.8"
        );
        let mut m = NodeIp::default();
        assert_eq!(m.unmarshal_string("nope").unwrap_err(), "invalid ip nope");
        assert_eq!(
            m.unmarshal_string("1.1.1.1,::1,::2").unwrap_err(),
            "invalid node ip 1.1.1.1,::1,::2, should be <ipv4>,<ipv6>"
        );
    }
}
